#!/usr/bin/env python

import socket
import errno
import struct
import threading
import time
from datetime import datetime
from collections import deque

################################################################################

class IceccMonitorError(Exception):
    pass

class IceccMonitorTimeout(IceccMonitorError):
    pass

class IceccMonitorProtocolError(IceccMonitorError):
    pass

class IceccMonitorIOError(IceccMonitorError):
    pass

class IceccMonitorConnectionError(IceccMonitorIOError):
    pass

class IceccMonitorConnectionClosed(IceccMonitorIOError):
    pass

################################################################################

class IceccMonitorClient:
    class State:
        # The state always increases and never decreases
        STATE_NEW, STATE_REQUEST_OPEN, STATE_CONNECTING, STATE_CONNECTED, \
            STATE_OPEN, STATE_REQUEST_CLOSE, STATE_SOCKET_CLOSED, STATE_CLOSED = range(8)

    def __init__(self, host, port, protocol_handler):
        self.host = host
        self.port = port
        self.protocol_handler = protocol_handler
        self.socket = None
        self.thread = threading.Thread(target=self._main_loop)
        self.thread.daemon = True

        self.lock = threading.Condition()
        # Variables protected by the lock. All changes to these are notified on the lock.
        # If an error is set, the state is at least STATE_REQUEST_CLOSE.
        # If state is at least STATE_OPEN, data_blocks can contain data.
        self.state = self.State.STATE_NEW
        self.data_blocks = deque()
        self.error = None

    def _set_state(self, state):
        with self.lock:
            # Only allow increases in state
            if state >= self.state:
                self.state = state
            self.lock.notifyAll()

    def _get_state(self):
        with self.lock:
            return self.state

    def _set_error(self, error):
        with self.lock:
            # Never replace an existing, more prior error
            if not self.error:
                self.error = error
            if self.state < self.State.STATE_REQUEST_CLOSE:
                self.state = self.State.STATE_REQUEST_CLOSE
            self.lock.notifyAll()

    def _enqueue_data_block(self, data):
        with self.lock:
            assert self.state == self.State.STATE_OPEN
            self.data_blocks.append(data)
            self.lock.notifyAll()

    def _main_loop(self):
        # Wait for request to connect
        with self.lock:
            while self.state < self.State.STATE_REQUEST_OPEN:
                self.lock.wait()

        if self.state == self.State.STATE_REQUEST_OPEN:
            # Connect requested
            self._set_state(self.State.STATE_CONNECTING)
            try:
                self._connect()
                self._set_state(self.State.STATE_CONNECTED)
                self._login()
                self._set_state(self.State.STATE_OPEN)
            except IceccMonitorConnectionClosed as e:
                # This means we're in STATE_REQUEST_CLOSE
                pass
            except IceccMonitorIOError as e:
                self._set_error(e)
            except Exception as e:
                self._set_error(IceccMonitorConnectionError(e))

        while self._get_state() == self.State.STATE_OPEN:
            try:
                data_block = self._read_data_block()
                self._enqueue_data_block(data_block)
            except IceccMonitorConnectionClosed as e:
                # This means we're in STATE_REQUEST_CLOSE
                pass
            except IceccMonitorIOError as e:
                self._set_error(e)
            except Exception as e:
                self._set_error(IceccMonitorIOError(e))

        assert self.state == self.State.STATE_REQUEST_CLOSE
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        except:
            # Ignore failures while closing
            pass
        self._set_state(self.State.STATE_SOCKET_CLOSED)

    def _read_bytes(self, num_bytes):
        data = ''
        while self._get_state() <= self.State.STATE_OPEN and len(data) < num_bytes:
            try:
                chunk = self.socket.recv(num_bytes - len(data))
                if chunk == '':
                    break
                data += chunk
            except socket.timeout:
                # Just recheck if state has changed
                continue
            except Exception as e:
                raise IceccMonitorIOError(e)
        if self._get_state() >= self.State.STATE_REQUEST_CLOSE:
            raise IceccMonitorConnectionClosed('Connection thread has shut down')
        if len(data) < num_bytes:
            raise IceccMonitorIOError('Connection closed by server')
        return data

    def _connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        # This is to be able to check for close requests while reading
        self.socket.settimeout(0.5)

    def _login(self):
        # We start by sending our maximum supported protocol version
        self.socket.sendall(self.protocol_handler.get_version_packet())
        # Server sends their maximum version, and we determine a common version to use
        version_reply = self._read_bytes(4)
        self.protocol_handler.negotiate_version(version_reply)

        # Second time around, we make sure we both agree on the selected version
        self.socket.sendall(self.protocol_handler.get_version_packet())
        version_reply = self._read_bytes(4)
        self.protocol_handler.confirm_version(version_reply)

        # Login as monitor
        self.socket.sendall(self.protocol_handler.get_login_packet())

    def _read_data_block(self):
        # Data block is assumed to have a four-byte header describing the size
        # of the remaining data (in network byte order)
        data_block_header = self._read_bytes(4)
        data_block_len, = struct.unpack('!L', data_block_header)
        data_block = self._read_bytes(data_block_len)
        return data_block

    def start_thread(self):
        self.thread.start()

    def request_connect(self):
        self._set_state(self.State.STATE_REQUEST_OPEN)

    def request_close(self):
        self._set_state(self.State.STATE_REQUEST_CLOSE)

    def wait_for_connect(self, timeout):
        end_time = time.time() + timeout
        with self.lock:
            while self.state < self.State.STATE_OPEN:
                remaining_time = end_time - time.time()
                if remaining_time <= 0:
                    raise IceccMonitorConnectionError('Connection timed out')
                self.lock.wait(remaining_time)
            if self.error:
                raise self.error

    def wait_for_close(self, timeout):
        end_time = time.time() + timeout
        with self.lock:
            while self.state < self.State.STATE_SOCKET_CLOSED:
                if self.error:
                    # At this point, we really don't care about IOErrors
                    return False
                remaining_time = end_time - time.time()
                if remaining_time <= 0:
                    raise IceccMonitorTimeout
                self.lock.wait(remaining_time)

        remaining_time = end_time - time.time()
        if remaining_time <= 0:
            # Even if we're out of time, give us the chance to join properly
            remaining_time = 0.1
        self.thread.join(remaining_time)
        if not self.thread.is_alive():
            self._set_state(self.State.STATE_CLOSED)
        else:
            raise IceccMonitorTimeout

    def get_data_block(self, block, timeout):
        end_time = time.time() + timeout
        with self.lock:
            while self.state <= self.State.STATE_OPEN:
                if self.data_blocks:
                    data = self.data_blocks.popleft()
                    return data
                if not block:
                    return None

                if timeout > 0:
                    remaining_time = end_time - time.time()
                    if remaining_time <= 0:
                        raise IceccMonitorTimeout
                else:
                    # block==False and timeout=0 means wait indefinitely
                    # However, wait(None) will disable KeyboardInterrupt.
                    remaining_time = 60
                self.lock.wait(remaining_time)

            if self.error:
                raise self.error
            assert self.state > self.State.STATE_OPEN
            raise IceccMonitorConnectionClosed('Connection is closed')

################################################################################

class DataExtractor:

    def __init__(self, data):
        self.data = data

    def get_int(self):
        value = struct.unpack('!L', self.data[:4])[0]
        self.data = self.data[4:]
        return value

    def get_string(self):
        str_len = struct.unpack('!L', self.data[:4])[0]
        value = self.data[4:(4+str_len-1)]
        self.data = self.data[4+str_len:]
        return value

    def get_timestamp(self):
        timestamp = struct.unpack('!L', self.data[:4])[0]
        self.data = self.data[4:]
        value = datetime.fromtimestamp(timestamp)
        return value

    def get_hexdump(self):
        value = ':'.join(x.encode('hex') for x in self.data)
        self.data = ''
        return value

    def empty(self):
        return len(self.data) == 0


class IceccMonitorProtocolHandler:
    M_JOB_LOCAL_DONE = 79
    M_MON_LOGIN = 82
    M_MON_GET_CS = 83
    M_MON_JOB_BEGIN = 84
    M_MON_JOB_DONE = 85
    M_MON_LOCAL_JOB_BEGIN = 86
    M_MON_STATS = 87

    PROTOCOL_MAX_VERSION = 34
    PROTOCOL_MIN_VERSION = 29

    def __init__(self):
        self.protocol_version = self.PROTOCOL_MAX_VERSION

    def get_login_packet(self):
        data_block = struct.pack('!L', self.M_MON_LOGIN)
        data_block_header = struct.pack('!L', len(data_block))
        return data_block_header + data_block

    def get_version_packet(self):
        return struct.pack('<L', self.protocol_version)

    def _parse_version_packet(self, data):
        version, = struct.unpack('<L', data)
        return version

    def negotiate_version(self, version_packet):
        other_version = self._parse_version_packet(version_packet)
        if other_version < self.PROTOCOL_MIN_VERSION:
            raise IceccMonitorProtocolError('Server version too old')
        if other_version < self.PROTOCOL_MAX_VERSION:
            self.protocol_version = other_version

    def confirm_version(self, version_packet):
        other_version = self._parse_version_packet(version_packet)
        if other_version != self.protocol_version:
            raise IceccMonitorProtocolError('Server version mismatch')

    def parse_data(self, data):
        msg = DataExtractor(data)
        msg_type = msg.get_int()
        values = {}
        values['MsgTypeId'] = msg_type

        if msg_type == self.M_MON_STATS:
            host_id = msg.get_int()
            status_str = msg.get_string()
            assert msg.empty()

            values['HostId'] = host_id
            for line in status_str.splitlines():
                (key, value) = line.split(':', 1)
                values[key] = value
            values['MsgType'] = "StatusUpdate"
            # Note: State:Offline means this daemon has died and the host_id should be removed.

        elif msg_type == self.M_MON_LOCAL_JOB_BEGIN:
            host_id = msg.get_int()
            job_id = msg.get_int()
            start_time = msg.get_timestamp()
            filename = msg.get_string()
            assert msg.empty()

            values.update({'HostId': host_id, 'JobId': job_id, 'StartTime': start_time, 'Filename': filename})
            values['MsgType'] = "LocalJobBegin"

        elif msg_type == self.M_JOB_LOCAL_DONE:
            job_id = msg.get_int()
            assert msg.empty()

            values['JobId'] = job_id
            values['MsgType'] = "LocalJobEnd"

        elif msg_type == self.M_MON_GET_CS:
            filename = msg.get_string()
            lang_id = msg.get_int()
            job_id = msg.get_int()
            host_id = msg.get_int()
            assert msg.empty()

            languages = ['C', 'C++', 'ObjC', '<custom>']
            try:
                language = languages[lang_id]
            except IndexError:
                language = '<unknown>'

            values.update({'HostId': host_id, 'JobId': job_id, 'Language': language, 'Filename': filename})
            values['MsgType'] = "CompilationRequest"

        elif msg_type == self.M_MON_JOB_BEGIN:
            job_id = msg.get_int()
            start_time = msg.get_timestamp()
            host_id = msg.get_int()
            assert msg.empty()

            values.update({'HostId': host_id, 'JobId': job_id, 'StartTime': start_time})
            values['MsgType'] = "LocalJobBegin"

        elif msg_type == self.M_MON_JOB_DONE:
            job_id = msg.get_int()
            exit_code = msg.get_int()
            real_msec = msg.get_int()
            user_msec = msg.get_int()
            sys_msec = msg.get_int()
            pfaults = msg.get_int()
            in_compressed = msg.get_int()
            in_uncompressed = msg.get_int()
            out_compressed = msg.get_int()
            out_uncompressed = msg.get_int()
            flags = msg.get_int()
            assert msg.empty()

            from_submitter = (flags == 1)

            values.update({'JobId': job_id, 'ExitCode': exit_code, 'RealTimeUsed': real_msec,
            'UserTimeUsed': user_msec, 'SystemTimeUsed': sys_msec, 'PageFaults': pfaults,
            'InputSizeCompressed': in_compressed, 'InputSize': in_uncompressed,
            'OutputSizeCompressed': out_compressed, 'OutputSize': out_uncompressed,
            'FromSubmitter': from_submitter})
            values['MsgType'] = "RemoteJobEnd"

        else:
            values['HexDump'] = msg.get_hexdump()
            assert msg.empty()

            values['RawData'] = data
            values['MsgType'] = "UnknownMessageType"
            values['Error'] = True

        return values

################################################################################

class IceccMonitor:
    def __init__(self, host='localhost', port=8765):
        self.protocol_handler = IceccMonitorProtocolHandler()
        self.client = IceccMonitorClient(host, port, self.protocol_handler)

    def connect(self, timeout=30):
        self.client.start_thread()
        self.client.request_connect()
        self.client.wait_for_connect(timeout)

    def get_message(self, block=True, timeout=0):
        data_block = self.client.get_data_block(block, timeout)
        msg = self.protocol_handler.parse_data(data_block)
        return msg

    def close(self, timeout=10):
        self.client.request_close()
        return self.client.wait_for_close(timeout)

################################################################################

if __name__ == "__main__":
    # When run as main program, just dump incoming messages to stdout
    ic = IceccMonitor()
    try:
        ic.connect()

        while True:
            msg = ic.get_message()
            print(msg)

    except IceccMonitorConnectionError as e:
        print "Failure when connecting to server:", e

    except IceccMonitorIOError as e:
        print "Server communication error:", e

    except KeyboardInterrupt:
        print "Aborting"
        pass

    finally:
        ic.close()
