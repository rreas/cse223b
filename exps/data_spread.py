# Plot min, max, average
# function of # of servers

import sys
sys.path.append('.')
sys.path.append('tests/')
import string
import random
import threading

import multiprocessing
from nose.tools import *
from time import sleep
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
data_source = string.ascii_lowercase + string.digits + string.ascii_uppercase

def fake_data(length):
    data = [random.choice(data_source) for x in range(length)]
    return ''.join(data)

num_data_elements = 10000
num_servers = 10

# Assign random ports.
ports = set()
while len(ports) < num_servers:
    ports.add(random.randint(4000, 8000))

# Create a bunch of fake data.
all_data = set()
while(len(all_data) < num_data_elements):
    all_data.add(fake_data(10))

# Spawn servers and store data.
servers = {}

try:
    print "Starting servers."
    servers = create_servers_from_port_list(ports)
    print "Done."

    # Store all the data.
    print "Putting data."
    with connect(min(ports)) as client:
        for s in all_data:
            client.put(s,s)
            sys.stdout.write('.')
            sys.stdout.flush()
    print "\nDone."

    for port in servers.keys():
        with connect(port) as client:
            print "PORT", port, "has", client.get_key_count(), "keys"

finally:
    for proc in servers.values():
        proc.terminate()

