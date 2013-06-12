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

num_data_elements = 500
server_range = [1, 2, 4, 8, 16, 32]

# Create a bunch of fake data.
all_data = set()
while(len(all_data) < num_data_elements):
    all_data.add(fake_data(10))
 
writes = np.zeros((len(all_data),))
reads = np.zeros((len(all_data),))
trials = 10
 
for num_servers in server_range:
    print "Starting trials with", num_servers, "server."

    # Assign random ports.
    ports = set()
    while len(ports) < num_servers:
        ports.add(random.randint(4000, 20000))
    
    # Spawn servers and store data.
    servers = {}
    
    for expid in range(trials):
        print "Trial", expid
    
        try:
            print "Starting servers."
            servers = create_servers_from_port_list(ports)
            print "Done."
        
            # Store all the data.
            print "Putting data."
            with connect(min(ports)) as client:
                for ix, s in enumerate(all_data):
                    st = time()
                    client.put(s,s)
                    fn = time()
                    sys.stdout.write('.')
                    sys.stdout.flush()
                    writes[ix] += fn-st
            print "\nDone."
        
            print "Reading data."
            with connect(min(ports)) as client:
                for ix, s in enumerate(all_data):
                    st = time()
                    resp = client.get(s)
                    fn = time()
                    assert resp.value == s
                    sys.stdout.write('.')
                    sys.stdout.flush()
                    reads[ix] += fn-st
            print "\nDone."
        
        finally:
            for proc in servers.values():
                proc.terminate()

    reads = reads*1000/trials
    writes = writes*1000/trials
    print "Read mean, std:", np.mean(reads), np.std(reads)
    print "Read min, max:", np.min(reads), np.max(reads)
    print "Write mean, std:", np.mean(writes), np.std(writes)
    print "Write min, max:", np.min(writes), np.max(writes)
    
