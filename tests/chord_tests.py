import multiprocessing
from nose.tools import *
from time import sleep
from server import ChordServer
from KeyValue import KeyValueStore
from KeyValue.ttypes import KeyValueStatus, ChordStatus

# Thrift.
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

def start_server_with_name_port(name, port):
    handler = ChordServer(name)
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
    sleep(3)
    return p

def get_client(port):
    try:
        transport = TTransport.TBufferedTransport(
                TSocket.TSocket('localhost', port))
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = KeyValueStore.Client(protocol)
        return client, transport
    except Thrift.TException, tx:
        print "Caught exception:", tx.message

def test_chord_create():
    a = spawn_server("A", 3342)
    client, transport = get_client(3342)
    transport.open()
    status = client.create()
    transport.close()
    assert status == ChordStatus.OK
    a.terminate()

def test_chord_join_simple():
    a = spawn_server("A", 3342)
    b = spawn_server("B", 3343)
    client, transport = get_client(3342)
    transport.open()
    status = client.create()
    transport.close()
    assert status == ChordStatus.OK
   
    client, transport = get_client(3343)
    transport.open()
    status = client.join("A")
    succ_b = client.predecessor()
    transport.close()
    assert status == ChordStatus.OK
    assert succ_b == "A"

#    client, transport = get_client(3342)
#    transport.open()
#    status = client.predecessor()
#    transport.close()
#    assert status == ChordStatus.OK
 
