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

def start_server_with_name_port(name, port):
    handler = ChordServer('localhost', port)
    processor = KeyValueStore.Processor(handler)
    transport = TSocket.TServerSocket('localhost', port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    server.serve()

def add_server_with_name_port(name, port):
    handler = ChordServer('localhost', port, chord_name=name, chord_port=port)
    processor = KeyValueStore.Processor(handler)
    transport = TSocket.TServerSocket('localhost', port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    server.serve()

def spawn_server(name, port):
    p = multiprocessing.Process(target=start_server_with_name_port,
            args=(name, port))
    p.start()
    sleep(1)
    return p
 
class TestChord:
   
    def test_chord_create(self):
        a = spawn_server("A", 3342)
        print "Spawned server at: ", a.name, a.pid
        try:
            status = ""
            with connect(3345) as client:
                assert type(client) == KeyValueStore.Client
        finally:
            a.terminate()
    
    def test_chord_put_and_get(self):
        a = spawn_server("A", 3342)
        print "Spawned server at: ", a.name, a.pid
        try:
            status = ""
            with connect(3345) as client:
                assert type(client) == KeyValueStore.Client 
                status = client.put('foo', 'bar')
                
                assert status == ChordStatus.OK
                response = client.get('foo')

                assert type(response) == GetValueResponse
                assert response.status == ChordStatus.OK
                assert response.value == 'bar'
        finally:
            a.terminate()

    # def test_chord_join_simple(self):
    #     a = spawn_server("A", 3342)
    #     b = spawn_server("B", 3343)
    #     try:
    #         with connect(3342) as client:
    #             assert type(client) == KeyValueStore.Client

    #         with connect(3343) as client:
    #             status = client.join("A")
    #             pred_b = client.predecessor()
    #         assert status == ChordStatus.OK
    #         assert pred_b == "A"

    #         with connect(3342) as client:
    #             pred_a = client.predecessor()
    #         assert pred_a == "B"
    #     finally:
    #         a.terminate()
    #         b.terminate()

