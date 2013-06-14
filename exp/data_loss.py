import sys
sys.path.append('.')
sys.path.append('tests/')
import string
import random
import threading

import multiprocessing
from nose.tools import *
from time import sleep, time
from ChordServer import *
from KeyValue import KeyValueStore
from KeyValue.ttypes import KeyValueStatus, ChordStatus
import numpy as np

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

# Create a bunch of fake data.
all_data = set()
while(len(all_data) < 2000):
    all_data.add(fake_data(10))

ports = set(range(3000, 3008))
plock = threading.Lock() # Just to avoid crashing while getting data.
crash_prob = 0.1
should_quit = False

def crash_with_prob():
    while(True):
        if should_quit:
            return
        if len(ports) == 2: # Don't crash the last server.
            return

        sleep(0.1)
        if random.random() < crash_prob:
            with plock:
                port = random.choice(list(ports))
                print "\nI KILL YOU PORT", port
                ports.remove(port)
                servers[port].terminate()
                del servers[port]
            sleep(1)

# Spawn servers and store data.
try:
    print "Starting servers."
    servers = create_servers_in_range(min(ports), max(ports)+1)
    print "Done."
    sleep(5)

    results = np.zeros((len(all_data),))

    # Store all the data.
    print "Putting data."
    with connect(min(ports)) as client:
        for s in all_data:
            client.put(s,s)
            sys.stdout.write('.')
            sys.stdout.flush()
    print "\nDone."

    # Start thread that can crash stuff.
    thread = threading.Thread(target=crash_with_prob)
    thread.start()

    # Calculate data loss.
    expected = len(all_data)
    count = 0

    for i, s in enumerate(all_data):
        with plock:
            with connect(min(ports)) as client:
                st = time()
                resp = client.get(s)
                fn = time()
                results[i] = fn-st
                if resp.value == s:
                    count = count + 1
        sys.stdout.write('.')
        sys.stdout.flush()

    print "\nExpected vs. count:", expected, count
    should_quit = True
    sleep(1) # Just give thread time to exit.

    for val in results:
        print val

finally:
    for proc in servers.values():
        proc.terminate()
