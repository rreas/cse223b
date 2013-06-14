import sys
sys.path.append('.')
sys.path.append('tests/')
import string
import random
import threading
import multiprocessing

from nose.tools import *
from time import sleep, time
import numpy as np
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

# Characters for data.
data_source = string.ascii_lowercase + string.digits

def fake_data(length):
    data = [random.choice(data_source) for x in range(length)]
    return ''.join(data)

num_data_elements = 1000

# Create a bunch of fake data.
all_data = set()
while(len(all_data) < num_data_elements):
    all_data.add(fake_data(10))

serverip = sys.argv[1]
serverport = int(sys.argv[2])

# Store my data on server.
with connect(serverport, serverip) as client:
    for s in all_data:
        client.put(s, s)
        sys.stdout.write('.')
        sys.stdout.flush()

# Read 1k keys.
while True:
    reads = np.zeros((len(all_data),))
    with connect(serverport, serverip) as client:
        for ix, s in enumerate(all_data):
            st = time()
            resp = client.put(s,s)
            fn = time()
            reads[ix] = fn-st
        print "done."
    reads *= 1000
    print "Read 1k keys (mean, max):", np.mean(reads), np.max(reads)

