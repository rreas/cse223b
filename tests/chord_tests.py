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


@contextmanager
def connect(port):
    transport = TTransport.TBufferedTransport(
            TSocket.TSocket('localhost', port))
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = KeyValueStore.Client(protocol)
    transport.open()
    yield client
    transport.close()


def start_server_with_name_port(port, chord_name=None, chord_port=None):
    handler = ChordServer('localhost', port, chord_name, chord_port)
    processor = KeyValueStore.Processor(handler)
    transport = TSocket.TServerSocket('localhost', port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    server.serve()


def spawn_server(port, chord_name=None, chord_port=None):
    p = multiprocessing.Process(target=start_server_with_name_port,
            args=(port, chord_name, chord_port))
    p.start()
    sleep(1)
    return p


def create_servers_in_range(from_port, to_port):
    ports = range(from_port, to_port)
    servers = {}

    for port in ports:
        servers[port] = {}
        if port == ports[0]:
            servers[port] = spawn_server(port)
        else:
            servers[port] = spawn_server(port,
                                         chord_name="localhost",
                                         chord_port=ports[0])
        sleep(2)

    for port in ports:
        with connect(port) as client:
            client.print_details()
    return servers


class TestChord:

    #Test connecting one server-client
    def test_chord_create(self):
        a = spawn_server(3342)
        print "Spawned server at: ", a.name, a.pid
        try:
            with connect(3342) as client:
                assert type(client) == KeyValueStore.Client
        finally:
            a.terminate()
    
    #Test get/put for one node
    def test_chord_put_and_get(self):
        a = spawn_server(3342)
        print "Spawned server at: ", a.name, a.pid
        try:
            status = ""
            with connect(3342) as client:
                assert type(client) == KeyValueStore.Client 
                status = client.put('foo', 'bar')

                assert status == ChordStatus.OK
                response = client.get('foo')

                assert type(response) == GetValueResponse
                assert response.status == ChordStatus.OK
                assert response.value == 'bar'
        finally:
            a.terminate()

    #Test two nodes, they should be each other's successor, predecessor
    def test_chord_join_simple(self):
        a = spawn_server(3342)
        b = spawn_server(3343, chord_name="localhost", chord_port=3342)
        a_key = encode_node('localhost', 3342)
        b_key = encode_node('localhost', 3343)

        # Time for stabilize to run.
        sleep(5)

        try:
            with connect(3342) as client:
                assert type(client) == KeyValueStore.Client

            with connect(3343) as client:
                pred_b = client.get_predecessor()
                assert pred_b == a_key
                succ_b = client.get_successor()
                assert succ_b == a_key
                succ_b = client.get_successor_for_key(str(get_hash(b_key)))
                assert succ_b == a_key
                status = client.put('keya', 'valuea')
                assert status == ChordStatus.OK

            with connect(3342) as client:
                pred_a = client.get_predecessor()
                assert pred_a == b_key
                succ_a = client.get_successor_for_key(str(get_hash(a_key)))
                assert succ_a == b_key
                status = client.put('keyb', 'valueb')
                assert status == ChordStatus.OK

            for portno in [3342, 3343]:
                with connect(portno) as client:
                    resp = client.get('keya')
                    assert resp.value == 'valuea'
                    resp = client.get('keyb')
                    assert resp.value == 'valueb'

        finally:
            a.terminate()
            b.terminate()

    def test_chord_join_complex(self):
        ports = range(3342, 3345)
        pred_set = set(ports)
        succ_set = set(ports)
        servers = {}

        for port in ports:
            if port == ports[0]:
                servers[port] = spawn_server(port)
            else:
                servers[port] = spawn_server(port,
                                             chord_name="localhost",
                                             chord_port=ports[0])
            sleep(2)

        for port in ports:
            with connect(port) as client:
                client.print_details()

        try:
            #We want to check their successors and predecessors
            for port in ports:
                with connect(port) as client:

                    #Check predecessor
                    pred = client.get_predecessor()
                    pred_port = int(pred.replace("localhost:", ""))
                    assert pred_port in pred_set
                    pred_set.remove(pred_port)

                    #Check successor
                    succ = client.get_successor()
                    succ_port = int(succ.replace("localhost:", ""))
                    assert succ_port in succ_set
                    succ_set.remove(succ_port)

                    assert succ is not pred
        finally:
            for port, name in servers.items():
                name.terminate()


    def test_crash_server_check(self):
        ports = range(3342, 3345)
        pred_set = set(ports)
        succ_set = set(ports)
        servers = {}

        for port in ports:
            if port == ports[0]:
                servers[port] = spawn_server(port)
            else:
                servers[port] = spawn_server(port,
                                             chord_name="localhost",
                                             chord_port=ports[0])
            sleep(2)

        for port in ports:
            with connect(port) as client:
                client.print_details()

        try:
            #We want to check kill a server and see if the successors and predecessors update
            print "Now terminating server: %d" % port
            servers[port].terminate()
            del servers[port]
            ports.remove(port)

            for port in ports:
                with connect(port) as client:

                    #Check predecessor
                    pred = client.get_predecessor()
                    pred_port = int(pred.replace("localhost:", ""))
                    assert pred_port in pred_set
                    pred_set.remove(pred_port)

                    #Check successor
                    succ = client.get_successor()
                    succ_port = int(succ.replace("localhost:", ""))
                    assert succ_port in succ_set
                    succ_set.remove(succ_port)

                    assert succ is not pred
        finally:
            for port, name in servers.items():
                name.terminate()

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
        servers = {}

        for port in ports:
            if port == ports[0]:
                servers[port] = spawn_server(port)
            else:
                servers[port] = spawn_server(port,
                                             chord_name="localhost",
                                             chord_port=ports[0])

        for port in ports:
            with connect(port) as client:
                client.print_details()
                
        try:
            with connect(ports[0]) as client:
                client.put('connie', 'lol')
                client.put('rakesh', 'lol++')
                key = client.get_successor_for_key(str(get_hash('connie')))
                
                print "key is " + key
                print "hash is " + str(get_hash('connie'))
                port = int(key.split(":")[1])
                resp = client.get('connie')
                assert resp.value == 'lol'

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

    def test_new_node_joining_copies_data(self):
        ports = [3342, 3343]

        try:
            a = spawn_server(ports[0])
        
            # put a key on A
            with connect(ports[0]) as client:
                client.put("27", "someval")

            # Join a new server B.
            b = spawn_server(ports[1],
                             chord_name="localhost",
                             chord_port=ports[0])

            with connect(ports[0]) as client:
                succ = client.get_successor_for_key(str(get_hash("27")))
                assert succ == "localhost:" + str(ports[1])
                val = client.get("27")
                assert val == "someval"

        finally:
            a.terminate()
            b.terminate()

