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


def create_servers_from_port_list(port_set_or_range):
    ports = list(port_set_or_range)
    servers = { ports[0]: spawn_server(ports[0]) }

    for port in ports[1:]:
        servers[port] = spawn_server(port,
                                     chord_name="localhost",
                                     chord_port=ports[0])
        sleep(2)
    
    return servers


def create_servers_in_range(from_port, to_port):
    return create_servers_from_port_list(range(from_port, to_port))


