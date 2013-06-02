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


class TestReplication:

    def test_data_replication(self):
        #For each server, check that the successor has its data
        ports = range(3342, 3345)
        servers = create_servers_in_range(3342, 3345)

        try:
            #For each port, get its successor
            for port in ports:
                print "current port is %d\n" % port
                key = str(port) + "-key"
                value = str(port) + "-value"
                with connect(port) as client:   
                 
                    #For each server, put a key that should be sent to its successor
                    status = client.put(key, value)
                    assert status == ChordStatus.OK

                    #Now find the node that is the master for the key we just put
                    node_key = client.get_successor_for_key(str(get_hash(key)))
                    new_port = int(node_key.split(":")[1])

                    #Check the new_port's successor to see if it stored the replicate
                    if new_port == port:
                        #If the key was stored on port (ourselves)
                        succ = client.get_successor()
                    else:
                        #Otherwise, get successor
                        with connect(new_port) as client:
                            succ = client.get_successor()

                    #Connect to the successor that should have the replicate
                    succ_port = int(succ.replace("localhost:", ""))
                    with connect(succ_port) as client:
                        response = client.get_replicate_list()
                        #It should only have one source
                        assert len(response.replicate_list) == 1

                        #Make sure the right source (port) is in the list
                        find_result = str(response.replicate_list).find(str(new_port))
                        print find_result
                        assert find_result != -1

        finally:
            for port, name in servers.items():
                name.terminate()

    
    def test_crash_some_servers(self):
        ports = range(3342, 3347)
        servers = create_servers_in_range(3342, 3347)
                
        try:
            print ports[0]
            with connect(ports[0]) as client:
                client.put('connie', 'lol')
                client.put('rakesh', 'lol++')
                key = client.get_successor_for_key(str(get_hash('connie')))
                
                print "key is " + key
                print "hash is " + str(get_hash('connie'))
                port = int(key.split(":")[1])
                resp = client.get('connie')
                assert resp.value == 'lol'

            print "crashing server with port",port
            servers[port].terminate()
            del servers[port]

            # Can get the data from somewhere (replication).
            with connect(ports[0]) as client:
                resp = client.get('connie')
                assert resp.value == 'lol'

            # New keys still get propagated around.
            with connect(ports[0]) as client:
                client.put('russell', 'organic')

            with connect(ports[1]) as client:
                resp = client.get('russell')
                assert resp.value == 'organic'

        finally:
            for port, name in servers.items():
                name.terminate()

