import sys
import threading
import hashlib

from time import sleep
sys.path.append('./gen-py')

# Thrift.
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from KeyValue.KeyValueStore import *
from KeyValue.ttypes import KeyValueStatus, ChordStatus

class ChordServer:

    def __init__(self, name):
        self.name = name

    def get(self, key):
        pass

    def create(self):
        return ChordStatus.OK

    def join(self, existing_node_id):
        self.predecessor = None
        successor = find_successor

    def notify(self, notifier_node_id):
        pass

    def find_successor(self, key_id):
        pass

    def closest_preceding_node(self, key_id):
        pass

    def predecessor(self):
        pass

