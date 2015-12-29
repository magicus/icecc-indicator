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

    def __init__(self, host, port):
        self.host = host
        self.port = port
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
        # handshake first time
        # FIXME: adjust protocol version?
        proto_ver = struct.pack('<L', 32)

        self.socket.sendall(proto_ver)

        header_data = self._read_bytes(4)
        proto_ver2, = struct.unpack('<L', header_data)
        # FIXME: verify protocol version
        print "got proto ver 2", proto_ver2

        # handshake second time
        # FIXME: adjust protocol version?
        self.socket.sendall(proto_ver)

        header_data = self._read_bytes(4)
        proto_ver3, = struct.unpack('<L', header_data)
        # FIXME: verify protocol version
        print "got proto ver 3", proto_ver3

        # login as monitor
        login_msg = struct.pack('!L', IceccMonitorMessageParser.M_MON_LOGIN)

        header = struct.pack('!L', len(login_msg))
        self.socket.sendall(header + login_msg)

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

class IceccMonitorMessageParser(object):
    M_JOB_LOCAL_DONE = 79
    M_MON_LOGIN = 82
    M_MON_GET_CS = 83
    M_MON_JOB_BEGIN = 84
    M_MON_JOB_DONE = 85
    M_MON_LOCAL_JOB_BEGIN = 86
    M_MON_STATS = 87

    def __init__(self, data):
        self.data = data
        self.values = {}

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

    def empty(self):
        return len(self.data) == 0

    def parse_data(self):
        msg = self
        msg_type = msg.get_int()
        self.values['msg_type'] = msg_type

        if msg_type == IceccMonitorMessageParser.M_MON_STATS:
            host_id = msg.get_int()
            payload_str = msg.get_string()
            assert msg.empty()

            self.values['host_id'] = host_id
            for line in payload_str.splitlines():
                (key, value) = line.split(':', 1)
                self.values[key] = value
            self.values['msg_desc'] = "Status Update"
            # Note: State:Offline means this daemon has died and the host_id should be removed.
        elif msg_type == IceccMonitorMessageParser.M_MON_LOCAL_JOB_BEGIN:
            host_id = msg.get_int()
            job_id = msg.get_int()
            start_time = msg.get_timestamp()
            file_name = msg.get_string()
            assert msg.empty()

            self.values.update({'host_id': host_id, 'job_id': job_id, 'start_time': start_time, 'file_name': file_name})
            self.values['msg_desc'] = "Local Job Begin"
        elif msg_type == IceccMonitorMessageParser.M_JOB_LOCAL_DONE:
            job_id = msg.get_int()
            assert msg.empty()

            self.values['job_id'] = job_id
            self.values['msg_desc'] = "Local Job End"
        elif msg_type == IceccMonitorMessageParser.M_MON_GET_CS:
            #    M_MON_GET_CS, S 83 --  MonGetCSMsg(job->id(), submitter->hostId(), m)
            # FIXME

            #void GetCSMsg::send_to_channel(MsgChannel *c) const
            #{
                #Msg::send_to_channel(c);
                #c->write_environments(versions);
                #*c << shorten_filename(filename);
                #*c << (uint32_t) lang;
                #*c << count;
                #*c << target;
                #*c << arg_flags;
                #*c << client_id;

                #if (IS_PROTOCOL_22(c)) {
                    #*c << preferred_host;
                #}

                #if (IS_PROTOCOL_31(c)) {
                    #*c << uint32_t(minimal_host_version >= 31 ? 1 : 0);
                #}
                #if (IS_PROTOCOL_34(c)) {
                    #*c << minimal_host_version;
                #}
            #}

            #void MonGetCSMsg::send_to_channel(MsgChannel *c) const
            #{
                #if (IS_PROTOCOL_29(c)) {
                    #Msg::send_to_channel(c);
                    #*c << shorten_filename(filename);
                    #*c << (uint32_t) lang;
                #} else {
                    #GetCSMsg::send_to_channel(c);
                #}

                #*c << job_id;
                #*c << clientid;
            #}
            self.values['msg_desc'] = "Get Compile Server (Request compilation work?)"

        elif msg_type == IceccMonitorMessageParser.M_MON_JOB_BEGIN:
            #    M_MON_JOB_BEGIN T 84 -- MonJobBeginMsg(m->job_id, m->stime, cs->hostId())
            # FIXME
            #void MonJobBeginMsg::fill_from_channel(MsgChannel *c)
            #{
                #Msg::fill_from_channel(c);
                #*c >> job_id;
                #*c >> stime;
                #*c >> hostid;
            #}

            self.values['msg_desc'] = "Remote Job Begin"

        elif msg_type == IceccMonitorMessageParser.M_MON_JOB_DONE:
            #    M_MON_JOB_DONE U 85 -- MonJobDoneMsg(*m) or  MonJobDoneMsg(JobDoneMsg((*jit)->id(),  255)) (when daemon dies)
            # FIXME
            #void JobDoneMsg::send_to_channel(MsgChannel *c) const
            #{
                #Msg::send_to_channel(c);
                #*c << job_id;
                #*c << (uint32_t) exitcode;
                #*c << real_msec;
                #*c << user_msec;
                #*c << sys_msec;
                #*c << pfaults;
                #*c << in_compressed;
                #*c << in_uncompressed;
                #*c << out_compressed;
                #*c << out_uncompressed;
                #*c << flags;
            #}

            self.values['msg_desc'] = "Remote Job End"

        else:
            print "unhandled message type:", msg_type
            self.values['raw_data'] = reply.data
            hexstr = ':'.join(x.encode('hex') for x in (reply.data))
            print(reply.type, hexstr, reply.data)
            self.values['msg_desc'] = "Unknown"
            self.values['error'] = True
            self.values['payload'] = reply.data

        return self.values

################################################################################

class IceccMonitor:
    def __init__(self, host='localhost', port=8765):
        self.client = IceccMonitorClient(host, port)

    def connect(self, timeout=30):
        self.client.start_thread()
        self.client.request_connect()
        self.client.wait_for_connect(timeout)

    def get_message(self, block=True, timeout=0):
        data_block = self.client.get_data_block(block, timeout)
        parser = IceccMonitorMessageParser(data_block)
        msg = parser.parse_data()
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
