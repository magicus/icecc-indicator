#!/usr/bin/env python

""" 
Simple socket client thread sample.

Eli Bendersky (eliben@gmail.com)
This code is in the public domain
"""
import socket
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
    CONNECT, SEND_HANDSHAKE, RECEIVE_HANDSHAKE, SEND, RECEIVE, CLOSE = range(6)
    
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


class SocketClientThread(threading.Thread):
    """ Implements the threading.Thread interface (start, join, etc.) and
        can be controlled via the cmd_q Queue attribute. Replies are placed in
        the reply_q Queue attribute.
    """
    def __init__(self, cmd_q=Queue.Queue(), reply_q=Queue.Queue()):
        super(SocketClientThread, self).__init__()
        self.cmd_q = cmd_q
        self.reply_q = reply_q
        self.alive = threading.Event()
        self.alive.set()
        self.socket = None
        
        self.handlers = {
            ClientCommand.CONNECT: self._handle_CONNECT,
            ClientCommand.CLOSE: self._handle_CLOSE,
            ClientCommand.SEND_HANDSHAKE: self._handle_SEND_HANDSHAKE,
            ClientCommand.RECEIVE_HANDSHAKE: self._handle_RECEIVE_HANDSHAKE,
            ClientCommand.SEND: self._handle_SEND,
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
                
    def join(self, timeout=None):
        self.alive.clear()
        threading.Thread.join(self, timeout)
    
    def _handle_CONNECT(self, cmd):
        try:
            self.socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((cmd.data[0], cmd.data[1]))
            self.reply_q.put(self._success_reply())
        except IOError as e:
            self.reply_q.put(self._error_reply(str(e)))
    
    def _handle_CLOSE(self, cmd):
        self.socket.close()
        reply = ClientReply(ClientReply.SUCCESS)
        self.reply_q.put(reply)
        
    def _handle_SEND_HANDSHAKE(self, cmd):
        proto_ver = struct.pack('<L', 32)
        try:
            self.socket.sendall(proto_ver)
            self.reply_q.put(self._success_reply())
        except IOError as e:
            self.reply_q.put(self._error_reply(str(e)))
    
    def _handle_RECEIVE_HANDSHAKE(self, cmd):
        try:
            header_data = self._recv_n_bytes(4)
            if len(header_data) == 4:
                proto_ver = struct.unpack('<L', header_data)[0]
                self.reply_q.put(self._success_reply(proto_ver))
                return
        except IOError as e:
            self.reply_q.put(self._error_reply(str(e)))
    
    def _handle_SEND(self, cmd):
        header = struct.pack('!L', len(cmd.data))
        try:
            print "hex send"
            

            hexstr = ':'.join(x.encode('hex') for x in (header + cmd.data))


            print hexstr
            self.socket.sendall(header + cmd.data)
            self.reply_q.put(self._success_reply())
        except IOError as e:
            self.reply_q.put(self._error_reply(str(e)))
    
    def _handle_RECEIVE(self, cmd):
        try:
            header_data = self._recv_n_bytes(4)
            if len(header_data) == 4:
                msg_len = struct.unpack('!L', header_data)[0]
                data = self._recv_n_bytes(msg_len)
                if len(data) == msg_len:
                    self.reply_q.put(self._success_reply(data))
                    return
            self.reply_q.put(self._error_reply('Socket closed prematurely'))
        except IOError as e:
            self.reply_q.put(self._error_reply(str(e)))
    
    def _recv_n_bytes(self, n):
        """ Convenience method for receiving exactly n bytes from self.socket
            (assuming it's open and connected).
        """
        data = ''
        while len(data) < n:
            chunk = self.socket.recv(n - len(data))
            if chunk == '':
                break
            data += chunk
        return data        
        
    def _error_reply(self, errstr):
        return ClientReply(ClientReply.ERROR, errstr)

    def _success_reply(self, data=None):
        return ClientReply(ClientReply.SUCCESS, data)

class ServerMessage(object):
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

        if msg_type == 87:
            host_id = msg.get_int()
            payload_str = msg.get_string()
            assert msg.empty()

            self.values['host_id'] = host_id
            for line in payload_str.splitlines():
                (key, value) = line.split(':', 1)
                self.values[key] = value
            self.values['msg_desc'] = "Status Update"
            # Note: State:Offline means this daemon has died and the host_id should be removed.
        elif msg_type == 86:
            host_id = msg.get_int()
            job_id = msg.get_int()
            start_time = msg.get_timestamp()
            file_name = msg.get_string()
            assert msg.empty()

            self.values.update({'host_id': host_id, 'job_id': job_id, 'start_time': start_time, 'file_name': file_name})
            self.values['msg_desc'] = "Local Job Begin"
        elif msg_type == 79:
            job_id = msg.get_int()
            assert msg.empty()

            self.values['job_id'] = job_id
            self.values['msg_desc'] = "Local Job End"
        else:
            print "undhandle message type:", msg_type
            self.values['raw_data'] = reply.data
            hexstr = ':'.join(x.encode('hex') for x in (reply.data))
            print(reply.type, hexstr, reply.data)
            self.values['msg_desc'] = "Unknown"
        
        print "values:"
        pprint(self.values)

#------------------------------------------------------------------------------
if __name__ == "__main__":
    sct = SocketClientThread()
    sct.start()
    sct.cmd_q.put(ClientCommand(ClientCommand.CONNECT, ('localhost', 8765)))
    reply = sct.reply_q.get(True)
    print(reply.type, reply.data)
    sct.cmd_q.put(ClientCommand(ClientCommand.SEND_HANDSHAKE))
    reply = sct.reply_q.get(True)
    print(reply.type, reply.data)
    sct.cmd_q.put(ClientCommand(ClientCommand.RECEIVE_HANDSHAKE))
    reply = sct.reply_q.get(True)
    print(reply.type, reply.data)
    sct.cmd_q.put(ClientCommand(ClientCommand.SEND_HANDSHAKE))
    reply = sct.reply_q.get(True)
    print(reply.type, reply.data)
    sct.cmd_q.put(ClientCommand(ClientCommand.RECEIVE_HANDSHAKE))
    reply = sct.reply_q.get(True)
    print(reply.type, reply.data)
    login_msg = struct.pack('!L', 82)
    sct.cmd_q.put(ClientCommand(ClientCommand.SEND, login_msg))
    reply = sct.reply_q.get(True)
    print(reply.type, reply.data)
    while True:
        sct.cmd_q.put(ClientCommand(ClientCommand.RECEIVE))
        reply = sct.reply_q.get(True)
        msg = ServerMessage(reply.data)
        msg.parse_data()

# Relevant message types:    
#    M_MON_LOGIN, R 82 -- handled, Only sent.
#    M_MON_GET_CS, S 83 --  MonGetCSMsg(job->id(), submitter->hostId(), m)
#    M_MON_JOB_BEGIN T 84 -- MonJobBeginMsg(m->job_id, m->stime, cs->hostId())
#    M_MON_JOB_DONE U 85 -- MonJobDoneMsg(*m) or  MonJobDoneMsg(JobDoneMsg((*jit)->id(),  255)) (when daemon dies)
#    M_MON_LOCAL_JOB_BEGIN V 86 -- handled.
#    M_MON_STATS W 87 -- handled. Sent at login, and when daemon resents M_STATS message.
#    M_JOB_LOCAL_DONE O 79 -- handled.

    sct.cmd_q.put(ClientCommand(ClientCommand.CLOSE))
    reply = sct.reply_q.get(True)
    print(reply.type, reply.data)
    pass
