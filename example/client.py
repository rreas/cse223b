#!/usr/bin/env python

import sys
import threading
sys.path.append('./gen-py')

# Thrift.
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from simple import *
from simple.ttypes import *

HOST = 'localhost'
PORT = 3342

plock = threading.Lock()

def call_remote_add():
    try:
        transport = TTransport.TBufferedTransport(
                TSocket.TSocket(HOST, PORT))
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Adder.Client(protocol)
    
        transport.open()
        res = client.add(15, 43)
        transport.close()

        plock.acquire()
        print "Add:", res
        plock.release()
    
    except Thrift.TException, tx:
        print "Caught exception:", tx.message

threads = []
for i in range(10):
    t = threading.Thread(target=call_remote_add)
    t.start()
    threads.append(t)

