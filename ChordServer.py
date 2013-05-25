import sys
sys.path.append('./gen-py')

# Thrift imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from KeyValue import KeyValueStore
#import KeyValueStore
from KeyValue.ttypes import *
#from ttypes import *
from hashlib import md5
from bisect import bisect
from time import sleep
from math import pow
import threading2


def enum(**enums):
    return type('Enum', (), enums)

def decode_node(node_key):
    return node_key.split(DELIMITER)

def encode_node(hostname, port):
    return hostname + ":" + str(port)

def get_hash(key):
    return int(md5(key).hexdigest(), 16)

DELIMITER = ":"

# This is dependent on the hash we use.
# MD5 creates a 128 bit digest.
FINGER_TABLE_LENGTH = 128;
MAX = pow(2, FINGER_TABLE_LENGTH);
Operations = enum(
    GET = 1,
    GET_SUCCESSOR_FOR_KEY = 2,
    GET_INIT_DATA = 3,
    GET_PREDECESSOR = 4,
    GET_SUCCESSOR = 5,
    NOTIFY = 6,
    PUT = 7
    )

lock = threading2.Lock()
class ChordServer(KeyValueStore.Iface): # probably need the thrift interface inside the parantheses here.
    
    # If chord_name and chord_port are not given, this is the first node in Chord ring.
    # Otherwise, connect to the chord server to find the position.
    def __init__(self, hostname, port, chord_name = None, chord_port = None):
        #initialize data structures
        self.hostname = hostname
        self.port = port
        self.node_key = encode_node(self.hostname, self.port)
        self.kvstore = {}
        self.successor = self.node_key
        self.predecessor = None
        self.finger_hash_table = []
        self.finger_node_table = []
        self.hashcode = get_hash(self.node_key)
        #self.f = open(str(self.port) , 'w')
        # Join an existing chord ring.
        if chord_name is not None:
            assert(chord_port is not None)
            self.successor = self.remote_request(encode_node(chord_name, chord_port), Operations.GET_SUCCESSOR_FOR_KEY, str(self.hashcode))
            self.initialize()
        else:
            # There is no finger table to borrow, so create one.
            self.initialize_finger_tables()

        self.initialize_threads()
        

    def initialize(self):
        data_response = self.remote_request(self.successor, Operations.GET_INIT_DATA, str(self.hashcode))
        # TODO: if status is not OK, throw Exception?
        self.kvstore = (data_response.kvstore)
        self.build_finger_tables(data_response.finger_hash_table, data_response.finger_node_table)
        del self.finger_node_table[len(self.finger_node_table) - 1]
        del self.finger_hash_table[len(self.finger_hash_table) - 1]
        self.finger_node_table.insert(0, self.successor)
        self.finger_hash_table.insert(0, get_hash(self.successor))

        status = self.remote_request(self.successor, Operations.NOTIFY, self.node_key)
        # TODO: if status != OK, throw Exception?

    def build_finger_tables(self, hash_table, node_table):
        assert(len(hash_table) == len(node_table))
        for i in range(0, len(hash_table)):
            self.finger_node_table.append(node_table[i])
            self.finger_hash_table.append(int(hash_table[i], 16))

    def initialize_finger_tables(self):
        for i in range(0, FINGER_TABLE_LENGTH):
            self.finger_node_table.append(self.node_key)
            self.finger_hash_table.append(self.hashcode)

    def initialize_threads(self):
        stabilizer = threading2.Thread(target = self.stabilize)
        stabilizer.daemon = True
        stabilizer.start()

        '''fixer = threading2.Thread(target = self.fix_finger_table)
        fixer.daemon = True
        fixer.start()'''

    def get_successor_for_key(self, hashcode):
        
        #print "get_successor_for_key ", hashcode
        '''if type(hashcode) == str:
            hashcode_int = int((hashcode))
        else:
            hashcode_int = hashcode
            hashcode = str(hashcode)'''

        hashcode_int = int(hashcode)
        if self.successor == self.node_key:
            return self.node_key

        if get_hash(self.successor) < self.hashcode:
            if hashcode_int > get_hash(self.successor) and hashcode_int > get_hash(self.node_key):
                return self.successor
            if hashcode_int < get_hash(self.successor) and hashcode_int < get_hash(self.node_key):
                return self.successor
        elif hashcode_int > self.hashcode and hashcode_int < get_hash(self.successor):
            return self.successor

        #index = bisect(self.finger_hash_table, hashcode_int)
        index = self.getIndex(hashcode_int)
        if index == 0:
            return self.node_key

        target_node = self.finger_node_table[index - 1]
        if target_node != self.node_key:
            return self.remote_request(self.finger_node_table[index - 1], Operations.GET_SUCCESSOR_FOR_KEY, hashcode)
        else:
            return self.node_key

    def getIndex(self, hashcode):
        for i in range(FINGER_TABLE_LENGTH - 1, -1, -1):
            if (hashcode > self.finger_hash_table[i]):
                return i + 1
        return 0

    def get_init_data(self, hashcode):
        '''if type(hashcode) == str:
            hashcode_int = int((hashcode))
        else:
            hashcode_int = hashcode'''
        hashcode_int = int(hashcode)
        # dummy for now.
        data_response = DataResponse()
        data_response.kvstore = {}
        data_response.finger_node_table = self.finger_node_table
        data_response.finger_hash_table = []
        for i in range(0, len(self.finger_hash_table)):
            data_response.finger_hash_table.append(str(self.finger_hash_table[i]))

        data_response.status = ChordStatus.OK
        return data_response


    def get_predecessor(self):
        return self.predecessor
        
    def get_successor(self):
        return self.successor

    def get(self, key):
        hashedKey = get_hash(key)
        #print "Key ", hashedKey
        # If the key is present locally, retrieve the value (even if the local node is a replica)
        # Else, get it from the node we deem to be the master.
        response = GetValueResponse()
        if key in self.kvstore:
            response.status = ChordStatus.OK
            response.value = self.kvstore[key]
            return response


        master_node = self.get_successor_for_key(str(hashedKey))
        return self.remote_request(master_node, Operations.GET, key)

    def put(self, key, value):
        # If there is no one else, forever alone, me gusta.
        if self.successor == self.node_key:
            self.kvstore[key] = value
            return ChordStatus.OK

        hashedKey = get_hash(key)
        # Does it belong to the local node?
        '''if hashedKey <= self.hashcode and hashedKey > get_hash(self.predecessor):
            self.kvstore[key] = value
            return ChordStatus.OK'''

        master_node = self.get_successor_for_key(str(hashedKey))
        if master_node != self.node_key:
            return self.remote_request(master_node, Operations.PUT, key, value)

        #print "Putting key", key
        self.kvstore[key] = value
        return ChordStatus.OK

    def remote_request(self, node, op, key = None, value = None):
        node_decoded = decode_node(node)
        #lock.acquire()
        try:
            transport = TTransport.TBufferedTransport(TSocket.TSocket(node_decoded[0], int(node_decoded[1])))
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = KeyValueStore.Client(protocol)
            transport.open()

            if op == Operations.GET:
                response = client.get(key)
            elif op == Operations.GET_SUCCESSOR_FOR_KEY:
                response = client.get_successor_for_key(key)
            elif op == Operations.GET_INIT_DATA:
                response = client.get_init_data(key)
            elif op == Operations.GET_PREDECESSOR:
                response = client.get_predecessor()
            elif op == Operations.GET_SUCCESSOR:
                response = client.get_successor()
            elif op == Operations.NOTIFY:
                response = client.notify(key)
            elif op == Operations.PUT:
                response = client.put(key,value)

            transport.close()
            #lock.release()
            return response
        except Thrift.TException, tx:
            #lock.release()
            print "Caught exception:", tx.message, node_decoded[0], node_decoded[1]

    def notify(self, node):
        # TODO: Probably need a check to see if it is truly the predecessor (see Chord)
        print "%s thinks it is our predecessor" %(node)
        self.predecessor = node
        return ChordStatus.OK

    def stabilize(self):
        while True:
            sleep(3)
            #print "Stabilizing"
            if self.successor != self.node_key:
                x = self.remote_request(self.successor, Operations.GET_PREDECESSOR)
            else:
                x = self.predecessor
            if x is not None and x != self.node_key:
                # TODO: Do we need better checks or does it get stabilized eventually to the right node.  
                # print "New successor ", x
                self.successor = x
                print "notifying %s that it is our successor" %(x)
                status = self.remote_request(x, Operations.NOTIFY, self.node_key)
                # TODO check status and take action.

            self.fix_finger_table()
            #self.print_details()

    def fix_finger_table(self):
        #while True:
            #print "Fixing"
        for i in range(0, FINGER_TABLE_LENGTH):
            #sleep(5)
            hashkey = (self.hashcode + int(pow(2, i))) % MAX
            successor = self.get_successor_for_key(str(hashkey))
            if successor is not None and self.finger_node_table[i] != successor:
                self.finger_node_table[i] = successor
                self.finger_hash_table[i] = get_hash(self.finger_node_table[i])
            #self.f.write(str(hashkey) + "  " + str(i) + "  " + self.finger_node_table[i] + "\n")
        #self.f.write("===============================\n\n")

    def print_details(self):
        print "Node_Key ", self.node_key
        print "HashCode ", self.hashcode
        print "successor ", self.successor
        print "predecessor ", self.predecessor
        print "===============\n\n"

if __name__ == '__main__':
    if len(sys.argv) > 3:
        processor = KeyValueStore.Processor(ChordServer(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]))
    else:
        processor = KeyValueStore.Processor(ChordServer(sys.argv[1], sys.argv[2]))
    transport = TSocket.TServerSocket(sys.argv[1], sys.argv[2])
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    print 'Starting the server...'
    server.serve()
    print 'done.'
