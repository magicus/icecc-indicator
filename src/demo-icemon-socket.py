#!/usr/bin/env python

"""
Simple socket client thread sample.

Eli Bendersky (eliben@gmail.com)
This code is in the public domain
"""
import socket
import errno
import struct
import threading
import Queue
import time
from pprint import pprint
from datetime import datetime


class ClientCommand(object):
    """ A command to the client thread.
        Each command type has its associated data:

        CONNECT:    (host, port) tuple
        SEND:       Data string
        RECEIVE:    None
        CLOSE:      None
    """
    CONNECT, RECEIVE, CLOSE = range(3)

    def __init__(self, type):
        self.type = type



class ClientReply(object):
    """ A reply from the client thread.
        Each reply type has its associated data:

        ERROR:      The error string
        SUCCESS:    Depends on the command - for RECEIVE it's the received
                    data string, for others None.
    """
    ERROR, SUCCESS = range(2)

    def __init__(self, type, data=None):
        self.type = type
        self.data = data


class ConnState:
    STATE_NEW, STATE_REQUEST_OPEN, STATE_CONNECTING, STATE_CONNECTED, STATE_OPEN, STATE_REQUEST_CLOSE, STATE_CLOSING, STATE_CLOSED, STATE_FAILED = range(9)

class IceccMonitorClientThread(threading.Thread):
    """ Implements the threading.Thread interface (start, join, etc.) and
        can be controlled via the cmd_q Queue attribute. Replies are placed in
        the reply_q Queue attribute.
    """
    def __init__(self, connection):
        super(IceccMonitorClientThread, self).__init__()
        self.connection = connection
        self.cmd_q = Queue.Queue()
        self.reply_q = Queue.Queue()
        self.alive = threading.Event()
        self.alive.set()
        self.socket = None
        self.isClosed = False
        self.lock = threading.Condition()
        self.state = ConnState.STATE_NEW

    def _set_state(self, state):
        with self.lock:
            self.state = state
            self.lock.notifyAll()

    def run(self):
        print "running"
        with self.lock:
            while self.state < ConnState.STATE_REQUEST_OPEN:
                self.lock.wait()

        print "req conn"
        # Connect requested
        self._connect()

        print "connected"
        with self.lock:
            state = self.state

        while state <= ConnState.STATE_OPEN:
            try:
                self._handle_messages()
                with self.lock:
                    state = self.state
            except IOError as e:
                self._set_state(ConnState.STATE_REQUEST_CLOSE)

        print "closing"
        self.socket.close()
        self._set_state(ConnState.STATE_CLOSING)
        print "closed"

    def request_connect(self):
        with self.lock:
            if self.state < ConnState.STATE_REQUEST_OPEN:
                self._set_state(ConnState.STATE_REQUEST_OPEN)

    def request_close(self, timeout=5):
        with self.lock:
            if self.state >= ConnState.STATE_CLOSED:
                return
            if self.state >= ConnState.STATE_REQUEST_CLOSE:
                # Already requested
                lock.wait(timeout)
                if self.state >= ConnState.STATE_CLOSED:
                    return
                raise IOError('Close timed out')
            self._set_state(ConnState.STATE_REQUEST_CLOSE)

        with self.lock:
            lock.wait(timeout)
            if self.state <= ConnState.STATE_CLOSING:
                raise IOError('Close timed out')

        threading.Thread.join(self, timeout)
        if not self.is_alive():
            self._set_state(ConnState.STATE_CLOSED)
        else:
            self._set_state(ConnState.STATE_FAILED)

    def _connect(self):
        try:
            self._set_state(ConnState.STATE_CONNECTING)

            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.connection.host, self.connection.port))
            self.socket.settimeout(0.5)

            self._set_state(ConnState.STATE_CONNECTED)

            # handshake first time
            # FIXME: adjust protocol version?
            proto_ver = struct.pack('<L', 32)

            self.socket.sendall(proto_ver)

            header_data = self._recv_n_bytes(4)
            proto_ver2 = struct.unpack('<L', header_data)[0]
            # FIXME: verify protocol version
            print "got proto ver 2", proto_ver2

            # handshake second time
            # FIXME: adjust protocol version?
            self.socket.sendall(proto_ver)

            header_data = self._recv_n_bytes(4)
            proto_ver3 = struct.unpack('<L', header_data)[0]
            # FIXME: verify protocol version
            print "got proto ver 3", proto_ver3

            # login as monitor
            login_msg = struct.pack('!L', ServerMessage.M_MON_LOGIN)

            header = struct.pack('!L', len(login_msg))
            self.socket.sendall(header + login_msg)
            self._set_state(ConnState.STATE_OPEN)
        except IOError as e:
            print "got io error in connect", e
            self.socket.close()
            self._set_state(ConnState.STATE_FAILED)

    def _handle_messages(self):
        try:
            print "about recv"
            header_data = self._recv_n_bytes(4)
            print "got header"
            msg_len = struct.unpack('!L', header_data)[0]
            data = self._recv_n_bytes(msg_len)
            hexstr = ':'.join(x.encode('hex') for x in (data))
            print('REPLY', hexstr, data)
            self.reply_q.put(self._success_reply(data))

        except IOError as e:
            print "got io error", e
            self.reply_q.put(self._error_reply(e))
            self._set_state(ConnState.STATE_REQUEST_CLOSE)

    def _recv_n_bytes(self, n):
        """ Convenience method for receiving exactly n bytes from self.socket
            (assuming it's open and connected).
        """
        data = ''
        with self.lock:
            state = self.state
        while state <= ConnState.STATE_OPEN and len(data) < n:
            try:
                chunk = self.socket.recv(n - len(data))
                if chunk == '':
                    break
                data += chunk
                with self.lock:
                    state = self.state
            except socket.timeout:
                with self.lock:
                    state = self.state

                if state >= ConnState.STATE_REQUEST_CLOSE:
                    raise IOError('Connection thread has shut down')
                continue
        if state >= ConnState.STATE_REQUEST_CLOSE:
            raise IOError('Connection thread has shut down')
        assert (len(data) == n)
        return data

    def get_next(self, timeout=10):
        with self.lock:
            state = self.state

        if state > ConnState.STATE_OPEN:
            raise IOError('Channel closed')

        reply = self.reply_q.get(True, timeout)
        if reply.type != ClientReply.SUCCESS:
            print "failure receive"
            raise reply.data
        return reply.data

    def _error_reply(self, errstr):
        return ClientReply(ClientReply.ERROR, errstr)

    def _success_reply(self, data=None):
        return ClientReply(ClientReply.SUCCESS, data)

class ServerMessage(object):
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

        if msg_type == ServerMessage.M_MON_STATS:
            host_id = msg.get_int()
            payload_str = msg.get_string()
            assert msg.empty()

            self.values['host_id'] = host_id
            for line in payload_str.splitlines():
                (key, value) = line.split(':', 1)
                self.values[key] = value
            self.values['msg_desc'] = "Status Update"
            # Note: State:Offline means this daemon has died and the host_id should be removed.
        elif msg_type == ServerMessage.M_MON_LOCAL_JOB_BEGIN:
            host_id = msg.get_int()
            job_id = msg.get_int()
            start_time = msg.get_timestamp()
            file_name = msg.get_string()
            assert msg.empty()

            self.values.update({'host_id': host_id, 'job_id': job_id, 'start_time': start_time, 'file_name': file_name})
            self.values['msg_desc'] = "Local Job Begin"
        elif msg_type == ServerMessage.M_JOB_LOCAL_DONE:
            job_id = msg.get_int()
            assert msg.empty()

            self.values['job_id'] = job_id
            self.values['msg_desc'] = "Local Job End"
        elif msg_type == ServerMessage.M_MON_GET_CS:
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

        elif msg_type == ServerMessage.M_MON_JOB_BEGIN:
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

        elif msg_type == ServerMessage.M_MON_JOB_DONE:
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

        print "values:"
        pprint(self.values)

class IceccMonitorConnection:
    def __init__(self, host, port=8765):
        self.host = host
        self.port = port
        self.clientThread = IceccMonitorClientThread(self)
        sct = self.clientThread

    def connect(self, timeout=10):
        self.clientThread.start()
        self.clientThread.request_connect()

        with self.clientThread.lock:
            self.clientThread.lock.wait(timeout)
            if (self.clientThread.state != ConnState.STATE_OPEN):
                raise IOError('Connection timed out')

    def get_message(self, block=True, timeout=10):

        print "aj"
        data = self.clientThread.get_next(timeout)

        hexstr = ':'.join(x.encode('hex') for x in (data))
        print('REPLY',  hexstr, data)
        msg_obj = ServerMessage(data)
        msg_obj.parse_data()

        msg = msg_obj.values
        print "we got msg"
        if 'error' in msg:
            print "error when revcv"
            raise SystemExit
        return msg

    def close(self, timeout=10):
        print "oj"
        with self.clientThread.lock:
            self.clientThread._set_state(ConnState.STATE_REQUEST_CLOSE)

        with self.clientThread.lock:
            self.clientThread.lock.wait(timeout)
            if (self.clientThread.state != ConnState.STATE_CLOSED):
                raise IOError('Close timed out')

#------------------------------------------------------------------------------
if __name__ == "__main__":

    ic = IceccMonitorConnection('localhost')
    try:
        ic.connect()
    except IOError as e:
        if e.errno == errno.ECONNREFUSED:
            print "refuuuuused"

        ic.close()
        raise
    except KeyboardInterrupt:
        print "aborting"
        ic.close();
        print "closed"
        raise

    try:

        quit_loop = False
        while not quit_loop:
            try:
                msg = ic.get_message()
                print "we got msg 2"
                print msg
            except Queue.Empty as e:
                print "idle"
                continue
            except KeyboardInterrupt as e:
                print "control c"
                quit_loop = True
        ic.close();

    except KeyboardInterrupt:
        print "aborting"
