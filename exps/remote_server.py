import sys
sys.path.append('.')
sys.path.append('tests/')
import string
import random
import threading
import multiprocessing

from time import sleep, time
from ChordServer import *
from KeyValue import KeyValueStore
from KeyValue.ttypes import KeyValueStatus, ChordStatus

# Thrift.
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from contextlib import contextmanager
from helpers import encode_node
from TestHelpers import *

num_data_elements = 1000
myport = random.randint(20000, 40000)
myip = sys.argv[1]

otherip = None
otherport = None
if len(sys.argv) > 2:
    otherip = sys.argv[2]
    otherport = int(sys.argv[3])

# Start server and add to ring if needed.
handler = ChordServer(myip, myport, otherip, otherport)
processor = KeyValueStore.Processor(handler)
transport = TSocket.TServerSocket(myip, myport)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()
server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
print "starting server on ip, port", myip, myport, "..."
server.serve()

