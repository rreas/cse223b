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

@contextmanager
def connect(port):
    try:
        transport = TTransport.TBufferedTransport(
                TSocket.TSocket('localhost', port))
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = KeyValueStore.Client(protocol)
        transport.open()
        yield client
        transport.close()
    except Thrift.TException, tx:
        print "Caught exception:", tx.message

def encode_node(hostname, port):
    return hostname + ":" + str(port)

def start_server_with_name_port(name, port, first=False, chord_name=None, chord_port=None):
    if first:
        handler = ChordServer('localhost', port)
    else:
        handler = ChordServer('localhost', port, chord_name=chord_name, chord_port=chord_port)
    processor = KeyValueStore.Processor(handler)
    transport = TSocket.TServerSocket('localhost', port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    server.serve()

def spawn_server(name, port, first=False, chord_name=None, chord_port=None):
    if first:
        p = multiprocessing.Process(target=start_server_with_name_port,
            args=(name, port, True))
    else:
        p = multiprocessing.Process(target=start_server_with_name_port,
            args=(name, port, False, chord_name, chord_port))
    p.start()
    sleep(1)
    return p
 
class TestChord:
   
    def test_chord_create(self):
        a = spawn_server("A", 3342, first=True)
        print "Spawned server at: ", a.name, a.pid
        try:
            status = ""
            with connect(3342) as client:
                assert type(client) == KeyValueStore.Client
        finally:
            a.terminate()
    
    def test_chord_put_and_get(self):
        a = spawn_server("A", 3342, first=True)
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

    def test_chord_join_simple(self):
        a = spawn_server("A", 3342, first=True)
        b = spawn_server("B", 3343, chord_name="localhost", chord_port=3342)
        print "Spawned server at: ", a.name, a.pid
        print "Spawned server at: ", b.name, b.pid

        try:
            status = ""
            with connect(3342) as client:
                assert type(client) == KeyValueStore.Client

            with connect(3343) as client:
                #status = client.join("A")
                pred_b = client.get_predecessor()
                assert pred_b = encode_node('localhost', 3342)

            with connect(3342) as client:
                pred_a = client.get_predecessor()
                assert pred_a = encode_node('localhost', 3343)
        finally:
            a.terminate()
            b.terminate()

