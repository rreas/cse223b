from contextlib import contextmanager
from hashlib import md5

# Thrift imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from KeyValue import KeyValueStore
from KeyValue.ttypes import *

DELIMITER = ":"

def decode_node(node_key):
    return node_key.split(DELIMITER)

def encode_node(hostname, port):
    return hostname + DELIMITER + str(port)

def get_hash(key):
    return int(md5(key).hexdigest(), 16)

def is_key_between(key, begin, end):
    if key == begin:
        return end
    if end < begin:
        if key > end and key > begin:
            return True
        if key < end and key < begin:
            return True
    elif key > begin and key < end:
        return True

    return False

@contextmanager
def remote(node):
    node_decoded = decode_node(node)

    try:
        transport = TTransport.TBufferedTransport(TSocket.TSocket(node_decoded[0], int(node_decoded[1])))
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = KeyValueStore.Client(protocol)
        transport.open()
        yield client
        transport.close()
    except Thrift.TException, tx:
        print "Caught exception:", tx.message, node_decoded[0], node_decoded[1]
        yield None
