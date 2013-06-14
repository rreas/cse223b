import sys
sys.path.append('.')
sys.path.append('tests/')
import string
import random
from multiprocessing import Process, Lock

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

num_data_elements = 10
num_servers = 4
num_clients = 1

# Assign random ports.
ports = set()
while len(ports) < num_servers:
    ports.add(random.randint(10000, 20000))

# Create a bunch of fake data.
all_data = set()
while(len(all_data) < num_data_elements):
    all_data.add(fake_data(10))

# Spawn servers and store data.
servers = {}
clients = []
lock = Lock()

# l = lock, p = port list, d = data, i = id.
def run_client(l, p, d, i):

    while(True):
        reads = np.zeros((len(d),))
        pick = random.choice(p)

        with connect(port) as client:
            for ix,s in enumerate(all_data):
                st = time()
                resp = client.get(s)
                fn = time()
                reads[ix] = fn-st

        # Make ms.
        reads *= 1000

        with l:
            print "Client", i, "time:", np.mean(reads), np.max(reads)


try:
    print "Starting servers."
    servers = create_servers_from_port_list(ports)
    print "Done."

    print "Putting data."
    with connect(min(ports)) as client:
        for s in all_data:
            client.put(s,s)
            sys.stdout.write('.')
            sys.stdout.flush()
    print "\nDone."

    for client_id in range(num_clients):
        client = Process(target=run_client, args=(lock, list(p), list(all_data),
            client_id))
        client.start()
        clients.append(client)

except KeyboardInterrupt:
    for client in clients:
        client.terminate()

    for proc in servers.values():
        proc.terminate()

