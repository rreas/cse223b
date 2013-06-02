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

