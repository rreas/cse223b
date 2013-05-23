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

import threading2



def enum(**enums):
    return type('Enum', (), enums);

def decode_node(node_key):
    return node_key.split(DELIMITER);

def encode_node(hostname, port):
    return hostname + ":" + str(port);

DELIMITER = ":"
Operations = enum(
    GET = 1,
    GET_SUCCESSOR_FOR_KEY = 2,
    GET_INIT_DATA = 3,
    GET_PREDECESSOR = 4,
    GET_SUCCESSOR = 5,
    NOTIFY = 6,
    PUT = 7
    );


class ChordServer(KeyValueStore.Iface): # probably need the thrift interface inside the parantheses here.
    
    # If chord_name and chord_port are not given, this is the first node in Chord ring.
    # Otherwise, connect to the chord server to find the position.
    def __init__(self, hostname, port, chord_name = None, chord_port = None):
        #initialize data structures
        self.hostname = hostname;
        self.port = port;
        self.kvstore = {};
        self.successor = encode_node(self.hostname, self.port);
        self.predecessor = None;
        self.finger_table = [];
        self.hashcode = md5(encode_node(self.hostname, self.port)).hexdigest();
        # Join an existing chord ring.
        if chord_name is not None:
            assert(chord_port is not None);
            self.successor = self.remote_request(encode_node(chord_name, chord_port), Operations.GET_SUCCESSOR_FOR_KEY, self.hashcode);
            self.initialize();

        t = threading2.Thread(target = self.stabilize);
        t.daemon = True;
        t.start();

    def initialize(self):
        data_response = self.remote_request(self.successor, Operations.GET_INIT_DATA, self.hashcode);
        # TODO: if status is not OK, throw Exception?
        self.kvstore.update(data_response.kvstore);
        self.finger_table.append(data_response.finger_table);
        del self.finger_table[len(self.finger_table) - 1]
        self.finger_table.insert(0, self.successor);

        status = self.remote_request(self.successor, Operations.NOTIFY, encode_node(self.hostname, self.port));
        # TODO: if status != OK, throw Exception?

    def get_successor_for_key(self, hashcode):
        if self.successor == encode_node(self.hostname, self.port):
            return encode_node(self.hostname, self.port);

        index = bisect(self.finger_table, hashcode);
        # If the current node cannot resolve the successor, ask the last node in the finger table.
        if index == len(self.finger_table):
            return self.remote_request(self.finger_table[len(self.finger_table - 1)], Operations.GET_SUCCESSOR, hashcode);
        else:
         return self.finger_table[index];

    def get_init_data(self, hashcode):
        # dummy for now.
        data_response = DataResponse();
        data_response.kvstore = {}
        data_response.finger_table = [];
        data_response.status = ChordStatus.OK;
        return data_response;


    def get_predecessor(self):
        return self.predecessor;
        
    def get_successor(self):
        return self.successor;

    def get(self, key):
        # If the key is present locally, retrieve the value (even if the local node is a replica)
        # Else, get it from the node we deem to be the master.
        response = GetValueResponse();
        if key in self.kvstore:
            response.status = ChordStatus.OK;
            response.value = self.kvstore[key];
            return response;

        hashedKey = md5(key).hexdigest();
        master_node = self.get_successor_for_key(hashedKey);
        return self.remote_request(master_node, Operations.GET, key);

    def put(self, key, value):
        # If there is no one else, forever alone, me gusta.
        if self.successor == encode_node(self.hostname, self.port):
            self.kvstore[key] = value;
            return ChordStatus.OK;

        hashedKey = md5(key).hexdigest();
        # Does it belong to the local node?
        if hashedKey <= self.hashcode and hashedKey > md5(self.predecessor).hexdigest():
            self.kvstore[key] = value;
            return ChordStatus.OK;

        master_node = self.get_successor_for_key(hashedKey);
        return self.remote_request(master_node, Operations.PUT, key, value);

    def remote_request(self, node, op, key = None, value = None):
        node_decoded = decode_node(node);
        try:
            transport = TTransport.TBufferedTransport(TSocket.TSocket(node_decoded[0], int(node_decoded[1])));
            protocol = TBinaryProtocol.TBinaryProtocol(transport);
            client = KeyValueStore.Client(protocol);
            transport.open();

            if op == Operations.GET:
                response = client.get(key);
            elif op == Operations.GET_SUCCESSOR_FOR_KEY:
                response = client.get_successor_for_key(key);
            elif op == Operations.GET_INIT_DATA:
                response = client.get_init_data(key);
            elif op == Operations.GET_PREDECESSOR:
                response = client.get_predecessor();
            elif op == Operations.GET_SUCCESSOR:
                response = client.get_successor();
            elif op == Operations.NOTIFY:
                response = client.notify(key);
            elif op == Operations.PUT:
                response = client.put(key,value);

            transport.close();
            return response;
        except Thrift.TException, tx:
            print "Caught exception:", tx.message;

    def notify(self, node):
        # TODO: Probably need a check to see if it is truly the predecessor (see Chord)
        self.predecessor = node;
        return ChordStatus.OK;


    def stabilize(self):
        while True:
            sleep(3);
            #print "Stabilizing";
            if self.successor != encode_node(self.hostname, self.port):
                x = self.remote_request(self.successor, Operations.GET_PREDECESSOR);
                if x is not None:
                    x_hashed = md5(x).hexdigest();
                    if x_hashed > self.hashcode and x_hashed < md5(self.successor).hexdigest():
                        self.successor = x;
                        status = self.remote_request(x, Operations.NOTIFY, encode_node(self.hostname, self.port));
                        # TODO check status and take action.



if __name__ == '__main__':
    if len(sys.argv) > 3:
        processor = KeyValueStore.Processor(ChordServer(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]));
    else:
        processor = KeyValueStore.Processor(ChordServer(sys.argv[1], sys.argv[2]));
    transport = TSocket.TServerSocket(sys.argv[1], sys.argv[2]);
    tfactory = TTransport.TBufferedTransportFactory();
    pfactory = TBinaryProtocol.TBinaryProtocolFactory();
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory);
    print 'Starting the server...';
    server.serve();
    print 'done.';