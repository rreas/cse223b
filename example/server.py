#!/usr/bin/env python
import sys
from time import sleep
sys.path.append('./gen-py')

# Thrift.
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from simple import *
from simple.ttypes import *

HOST = 'localhost'
PORT = 3342

# Implementation.
class SimpleServer:
    def add(self, a, b):
        print 'sleeping'
        sleep(10)
        return a + b

handler = SimpleServer()
processor = Adder.Processor(handler)
transport = TSocket.TServerSocket(HOST, PORT)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()
server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

print 'Starting server...'
server.serve()

