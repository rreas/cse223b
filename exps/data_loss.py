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
while(len(all_data) < 1000):
    all_data.add(fake_data(10))

ports = set(range(3000, 3011))
plock = threading.Lock() # Just to avoid crashing while getting data.
crash_prob = 0.4
should_quit = False

def crash_with_prob():
    while(True):
        if should_quit:
            return
        if len(ports) == 2: # Don't crash the last server.
            return

        sleep(0.001)
        if random.random() < crash_prob:
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

    # Start thread that can crash stuff.
    thread = threading.Thread(target=crash_with_prob)
    thread.start()

    # Calculate data loss.
    expected = len(all_data)
    count = 0

    for i, s in enumerate(all_data):
        with connect(min(ports)) as client:
            try:
                client.put(s,s)
            except Thrift.TException, tx:
                continue
        sys.stdout.write('.')
        sys.stdout.flush()

    thread.join()

    print "Starting get."

    for i, s in enumerate(all_data):
        with connect(min(ports)) as client:
            resp = client.get(s)
            if resp.value == s:
                count = count + 1
        sys.stdout.write('.')
        sys.stdout.flush()


    print "\nExpected vs. count:", expected, count
    should_quit = True
    sleep(1) # Just give thread time to exit.

    #for val in results:
    #    print val

finally:
    for proc in servers.values():
        proc.terminate()

