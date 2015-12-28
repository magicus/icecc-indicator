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

    def __init__(self, type, data=None):
        self.type = type
        self.data = data



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
        self.msgs = Queue.Queue()

        self.handlers = {
            ClientCommand.CONNECT: self._handle_CONNECT,
            ClientCommand.CLOSE: self._handle_CLOSE,
            ClientCommand.RECEIVE: self._handle_RECEIVE,
        }

    def run(self):
        while self.alive.isSet():
            try:
                # Queue.get with timeout to allow checking self.alive
                cmd = self.cmd_q.get(True, 0.1)
                self.handlers[cmd.type](cmd)
            except Queue.Empty as e:
                continue

    def stop(self):
        self.alive.clear()

    def join(self, timeout=None):
        self.alive.clear()
        threading.Thread.join(self, timeout)

    def _handle_CONNECT_inner(self, cmd):
        try:
            print "inner con"
            self.socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.connection.host, self.connection.port))
            self.socket.settimeout(0.5)
            print "con"
            self.reply_q.put(self._success_reply())
            print "q put"
        except socket.error as e:
            if e.errno == errno.ECONNREFUSED:
                print "refuuuuused"
                self.reply_q.put(self._error_reply("REFUSED"+str(e)))
            else:
                self.reply_q.put(self._error_reply(str(e)))
        except IOError as e:
            self.reply_q.put(self._error_reply(str(e)))

    def _handle_CONNECT(self, cmd):
        print "at connect"
        self._handle_CONNECT_inner(cmd)
        print "connected"
        reply = self.reply_q.get(True, 5)
        print "gotten", reply
        print(reply.type, reply.data)
        if reply.type == ClientReply.ERROR:
            print "error"
            raise SystemExit

        self.handshake()

        self.login()

    def handshake(self):
        sct = self

        proto_ver = struct.pack('<L', 32)
        try:
            self.socket.sendall(proto_ver)

            header_data = self._recv_n_bytes(4)
            if len(header_data) == 4:
                proto_ver2 = struct.unpack('<L', header_data)[0]
                print "got proto ver 2", proto_ver2
            else:
                print "error io"
                raise IOError("bad proto")

            self.socket.sendall(proto_ver)

            header_data = self._recv_n_bytes(4)
            if len(header_data) == 4:
                proto_ver3 = struct.unpack('<L', header_data)[0]
                print "got proto ver 3", proto_ver3
            else:
                print "error io 2"
                raise IOError("bad proto 2")

        except IOError as e:
            self.reply_q.put(self._error_reply(str(e)))

    def login(self):
        login_msg = struct.pack('!L', ServerMessage.M_MON_LOGIN)

        header = struct.pack('!L', len(login_msg))
        try:
            self.socket.sendall(header + login_msg)
            print "sent login"
            self.reply_q.put(self._success_reply())
        except IOError as e:
            print "login errir"
            self.reply_q.put(self._error_reply(str(e)))

    def _handle_CLOSE(self, cmd):
        print "closing"
        self.socket.close()
        print "closed"
        reply = ClientReply(ClientReply.SUCCESS)
        self.reply_q.put(reply)
        print "replied"

    def _handle_RECEIVE(self, cmd):
        try:
            print "about recv"
            header_data = self._recv_n_bytes(4)
            print "got header"
            if len(header_data) == 4:
                msg_len = struct.unpack('!L', header_data)[0]
                data = self._recv_n_bytes(msg_len)
                if len(data) == msg_len:
                    self.reply_q.put(self._success_reply(data))
                    return
            self.reply_q.put(self._error_reply('Socket closed prematurely'))
        except IOError as e:
            print "got io error", e
            self.reply_q.put(self._error_reply(str(e)))
            if not self.alive.isSet():
                self.reply_q.put(self._error_reply('Thread closed'))

    def _recv_n_bytes(self, n):
        """ Convenience method for receiving exactly n bytes from self.socket
            (assuming it's open and connected).
        """
        data = ''
        while len(data) < n and self.alive.isSet():
            try:
                chunk = self.socket.recv(n - len(data))
                if chunk == '':
                    break
                data += chunk
            except socket.timeout:
                print "timeout 2"
                if not self.alive.isSet():
                    print "shutting down"
                    raise IOError('Connection thread has shut down')
                continue
        return data

    def stop_loop(self):
        self.quit_loop = True

    def get_next(self):
        sct = self
        sct.cmd_q.put(ClientCommand(ClientCommand.RECEIVE))
        reply = sct.reply_q.get(True, 1)
        hexstr = ':'.join(x.encode('hex') for x in (reply.data))
        print('REPLY', reply.type, hexstr, reply.data)
        if reply.type == ClientReply.ERROR:
            print "error when revcv"
            print(reply.type, reply.data)
            raise SystemExit
        msg = ServerMessage(reply.data)
        msg.parse_data()
        self.msgs.put(msg.values)

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

            self.values['msg_desc'] = "Get Compile Server (Request compilation work?)"

        elif msg_type == ServerMessage.M_MON_JOB_BEGIN:
            #    M_MON_JOB_BEGIN T 84 -- MonJobBeginMsg(m->job_id, m->stime, cs->hostId())
            # FIXME

            self.values['msg_desc'] = "Remote Job Begin"

        elif msg_type == ServerMessage.M_MON_JOB_DONE:
            #    M_MON_JOB_DONE U 85 -- MonJobDoneMsg(*m) or  MonJobDoneMsg(JobDoneMsg((*jit)->id(),  255)) (when daemon dies)
            # FIXME

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
        self.clientThread.cmd_q.put(ClientCommand(ClientCommand.CONNECT, (self.host, 8765)))
        reply = self.clientThread.reply_q.get(True, timeout)
        print "done connecting", reply

    def get_message(self, block=True, timeout=0):
        print "aj"
        self.clientThread.get_next()
        msg = self.clientThread.msgs.get(True, 1)
        print "we got msg"
        if 'error' in msg:
            print "error when revcv"
            raise SystemExit
        return msg

    def close(self):
        print "oj"
        self.clientThread.stop()
        print "stopped, closing"
        self.clientThread.cmd_q.put(ClientCommand(ClientCommand.CLOSE))
        print "waiting for reply"
        reply = self.clientThread.reply_q.get(True, 10)
        print "reply gotten"
        print(reply.type, reply.data)
        print "joining"
        self.clientThread.join(5);
        #

#------------------------------------------------------------------------------
if __name__ == "__main__":

    ic = IceccMonitorConnection('localhost')
    ic.connect()
    try:
        #sct = SocketClientThread()
        #sct.start()
        #sct.cmd_q.put(ClientCommand(ClientCommand.CONNECT, ('localhost', 8765)))

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

#        sct.cmd_q.put(ClientCommand(ClientCommand.CLOSE))
#        reply = sct.reply_q.get(True)
#        print(reply.type, reply.data)
#        pass
    except KeyboardInterrupt:
        print "aborting"
