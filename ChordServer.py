import sys,os
sys.path.append('./gen-py')
from time import sleep
from math import pow
from collections import defaultdict as defaultdick

# Thrift imports
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

import threading2

from KeyValue import KeyValueStore
from KeyValue.ttypes import *
from helpers import *

# This is dependent on the hash we use.
# MD5 creates a 128 bit digest.
FINGER_TABLE_LENGTH = 128
MAX = long(pow(2, FINGER_TABLE_LENGTH))

# The number of successive server failures that can be tolerated.
class ChordServer(KeyValueStore.Iface):

    # If chord_name and chord_port are not given, this is the first node in Chord ring.
    # Otherwise, connect to the chord server to find the position.
    def __init__(self, hostname, port, chord_name=None, chord_port=None):
        #initialize data structures
        self.hostname = hostname
        self.port = port
        self.node_key = encode_node(self.hostname, self.port)
        self.kvstore = {}
        self.replicas = defaultdick(dict)
        self.successor = self.node_key
        self.predecessor = None
        self.successor_list = []
        self.finger_table = []
        self.hashcode = get_hash(self.node_key)
        self.lock = threading2.Lock()
        self.f = open(str(port), 'w')
        
        # Property can be changed by external code.
        self.successor_list_length = 5

        # Join an existing chord ring. Start by obtaining the successor.
        if chord_name is not None:
            assert(chord_port is not None)
            remote_node = encode_node(chord_name, chord_port)
            with remote(remote_node) as client:
                if client is None:
                    print "Unable to contact successor, exiting..."
                    os._exit(1)
                self.successor = client.get_successor_for_key(
                        str(self.hashcode))

            self.initialize()
        else:
            self.initialize_successor_list()
            self.build_finger_table()
        self.initialize_threads()
        #self.write_details()

    def initialize(self):
        ''' Get the initial data from the successor. Once this method completes,
        the current node should be able to serve the keys that it owns and also 
        have a successor_list to work with'''
        with remote(self.successor) as client:
            if client is None:
                print "Unable to contact successor for init_data, exiting.."
                os._exit(1)
            data_response = client.get_init_data(str(self.node_key))

        self.kvstore = data_response.kvstore
        self.successor_list = data_response.successor_list

        # Remove the last entry in the list and add the successor at the beginning 
        # of the list and list update is complete
        del self.successor_list[len(self.successor_list) - 1]
        self.successor_list.insert(0, self.successor)

        self.build_finger_table();
        #self.inform_predecessor(self.node_key)
        # TODO: check status?
        with remote(self.successor) as client:
            if client is None:
                print "Unable to notify successor during init, exiting.."
                os._exit(1)
            status = client.notify(self.node_key)

    def initialize_threads(self):
        stabilizer = threading2.Thread(target=self.stabilize)
        stabilizer.daemon = True
        stabilizer.start()

    def initialize_successor_list(self):
        ''' If there are no other nodes in Chord, initialize the 
        successor_list to just the current node'''
        for i in range(0, self.successor_list_length):
            self.successor_list.append(self.node_key)

    def build_finger_table(self):
        for i in range(0, FINGER_TABLE_LENGTH):
            if self.successor == self.node_key:
                self.finger_table.append(self.successor)
                continue
            hashkey = (self.hashcode + long(pow(2, i))) % MAX
            with remote(self.successor) as client:
                if client is None:
                    print "Unable to build_finger_table. Exiting"
                    os._exit(1)
                self.finger_table.append(client.get_successor_for_key(str(hashkey)))

    def get_best_guess(self, hashcode):
        with self.lock:
            for i in range(FINGER_TABLE_LENGTH - 1, -1, -1):
                node = self.finger_table[i]
                node_hashcode = get_hash(node)
                if is_hashcode_between(node_hashcode, self.hashcode, hashcode):
                    return node, i
            return self.node_key, -1

    def get_remote_successor_for_key_with_retry(self, target_node, index, hashcode):
        while(target_node != self.node_key):
            try:
                with remote(target_node) as client:
                    if client is None:
                        target_node, index = self.handle_finger_failure(index)

                    else:
                        return client.get_successor_for_key(hashcode)
            except:
                pass
        return target_node

    def get_successor_for_key(self, hashcode):
        ''' Given a particular hashed key, find its successor in Chord. 
        '''
        hashcode_int = int(hashcode)
        if self.successor == self.node_key:
            return self.node_key

        # If the key is located between the current node and its successor,
        # just return the successor.
        with self.lock:
            if is_hashcode_between(hashcode_int, self.hashcode, get_hash(self.successor)):
                return self.successor

        target_node, index = self.get_best_guess(hashcode_int)
        return self.get_remote_successor_for_key_with_retry(target_node, index, hashcode)

    def get_init_data(self, node_key):
        ''' Provide the data required by a new server to be able to serve keys
        and handle server failures'''
        # dummy for now.
        data_response = DataResponse()
        data_response.kvstore = self.get_kv_for_node(node_key)
        data_response.successor_list = self.successor_list
        data_response.status = ChordStatus.OK
        return data_response

    def get_kv_for_node(self, node):
        '''Returns a dictionary containing key, value pairs 
        that should be transferred to the new node. These are the set of keys
        between the current node's predecessor and the new node. If there is 
        just a single node, then the keys between the current node and the new node.
        '''
        return_dict = {}
        if self.predecessor is None:
            for key in self.kvstore:
                if is_hashcode_between(get_hash(key), self.hashcode, get_hash(node)):
                    return_dict[key] = self.kvstore[key]
        else:
            for key in self.kvstore:
                if is_hashcode_between(get_hash(key), get_hash(self.predecessor), get_hash(node)):
                    return_dict[key] = self.kvstore[key]

        return return_dict

    def get_predecessor(self):
        return str(self.predecessor)

    def get_successor_list(self):
        response = SuccessorListResponse()
        response.status = ChordStatus.OK
        response.successor_list = self.successor_list
        return response
    
    def get_successor(self):
        return str(self.successor)

    def get_key_count(self):
        """Number of keys managed by this node."""
        with self.lock:
            return len(self.kvstore.keys())

    def kv_get(self, key):
        response = GetValueResponse()
        response.status = ChordStatus.OK
        with self.lock:
            if key in self.kvstore:
                response.value = self.kvstore[key]
            else:
                response.status = ChordStatus.ERROR

        return response

    def get_with_retry(self, target_node, key):
        while target_node != self.node_key:
            try:
                with remote(target_node) as client:
                    if client is None:
                        self.handle_successor_failure()
                        target_node = self.successor
                    else:
                        return client.get(key)
            except:
                pass
        response = GetValueResponse()
        response.status = ChordStatus.ERROR
        return response

    def get(self, key):
        #self.f.write("received get at " + self.node_key + "\n")
        #print "Node %s received GET request for %s" %(self.node_key, key)
        ''' Get the key from the master node. If the current node does not know
        who the master is, it will ask its successor about it'''
        master_node = self.get_successor_for_key(str(get_hash(key)))

        if master_node == self.node_key:
            return self.kv_get(key)

        elif master_node == self.successor:
            #TODO : will need a retry if replication is supported.
            with remote(self.successor) as client:
                if client is None:
                    self.handle_successor_failure()
                    response = GetValueResponse()
                    response.status = ChordStatus.ERROR
                    return response
                return client.kv_get(key)
        else:
            return self.get_with_retry(master_node, key)

    def put_with_retry(self, target_node, key, value):
        while(target_node != self.node_key):
            try:
                with remote(target_node) as client:
                    if client is None:
                        # In case the master was the successor, need to fix that.
                        self.handle_successor_failure()
                        #target_node = self.get_successor_for_key(str(get_hash(key)))
                        target_node = self.successor
                    else:
                        # this is an important difference between this method and 
                        # put_on_successor_with_retry which tries to directly add in
                        # the successor.
                        return client.put(key, value)
            except:
                # The node might fail just when we are about to put the key, value.
                # Keep trying with the next one.
                pass

        with self.lock:
            self.kvstore[key] = value

        return ChordStatus.OK

    def kv_put(self, key, value):
        with self.lock:
            self.kvstore[key] = value
        #print "%s received %s" %(self.node_key, key)
        return ChordStatus.OK

    def put_on_successor_with_retry(self, key, value):
        # TODO: Change this if we are not exiting.
        while True:
            try:
                with remote(self.successor) as client:
                    if client is None:
                        self.handle_successor_failure()
                    else:
                        return client.kv_put(key, value)
            except:
                pass

    def put(self, key, value):
        #print "Node %s received put request for %s" %(self.node_key, key)
        ''' Find the master node and store the key, value there.'''
        master_node = self.get_successor_for_key(str(get_hash(key)))

        if master_node == self.node_key:
            with self.lock:
                self.kvstore[key] = value

            return ChordStatus.OK

        elif master_node == self.successor:
            # User kv_put to directly add on the successor.
            return self.put_on_successor_with_retry(key, value)

        else:
            return self.put_with_retry(master_node, key, value)

    def notify(self, node):
        # TODO: Probably need a check to see if it is truly the predecessor (see Chord)
        self.predecessor = node
        return ChordStatus.OK

    def notify_predecessor(self, node):
        # A new successor! update attributes and successor_list
        with self.lock:
            self.successor = node
            # This should be sufficient temporarily. Stabilize will make up for any
            # inconsistencies.
            del self.successor_list[len(self.successor_list) - 1]
            self.successor_list.insert(0, self.successor)
        return ChordStatus.OK

    def inform_predecessor(self, node):
        with remote(self.successor) as client:
            if client is None:
                print "Successor failed before node join completed. Exiting.."
                os._exit(1)
            predecessor = client.get_predecessor()

        if predecessor == str(None):
            self.predecessor = self.successor
        else:
            self.predecessor = predecessor

        with remote(self.predecessor) as client:
            if client is None:
                print "Could not connect to predecessor. Not a big deal."
                return
            # We probably don't care too much about status. This will stabilize later anyway.
            status = client.notify_predecessor(self.node_key)

    def stabilize(self):
        ''' Every interval do:
            1) Find if the current node has a new successor, in case a new node joined 
            the ring between it and its successor.
            2) If there is a new successor, notify it and update the successor_list.
            3) In case the successor has failed, try the next successor in the list and 
            update attributes.
        '''
        first_run = True
        while True:
            if first_run is True:
                first_run = False
            else:
                sleep(2)

            if self.successor != self.node_key:
                # We think we have a successor.

                with remote(self.successor) as client:
                    if client is None:
                        # Could not connect to successor.
                        print 'Successor of %s failed' %(self.node_key)
                        self.handle_successor_failure()
                        self.fix_finger_table();
                        continue

                    # See if there is really a node between us.
                    x = client.get_predecessor()

            else:
                # Otherwise check the predecessor?
                x = self.predecessor

            if x is not None and x != self.node_key:
                # We think we have a successor.

                if self.successor != x:
                    # A new node joined the ring and is the current
                    # node's successor. This requires updating the
                    # successor_list.

                    with remote(x) as client:
                        if client is None:
                            # Were told about an unreachable successor.
                            continue
                        self.successor = x
                        status = client.notify(self.node_key)
                    # TODO check status and take action.

            # This is just maintenance work?
            # Can this move to another thread?
            if self.successor != self.node_key:
                with remote(self.successor) as client:
                    if client is None:
                        self.handle_successor_failure()
                        self.fix_finger_table();
                        continue
                    response = client.get_successor_list()
                    # Get updated successor_list from successor and make adjustments.
                    self.successor_list = response.successor_list
                    del self.successor_list[len(self.successor_list) - 1]
                    self.successor_list.insert(0, self.successor)

            #self.print_details()
            #self.print_successor_list()
            self.fix_finger_table();

    def fix_finger_table(self):
        #while True:
            #print "Fixing"
        for i in range(0, FINGER_TABLE_LENGTH):
            #sleep(5)
            hashkey = (self.hashcode + long(pow(2, i))) % MAX
            self.finger_table[i] = self.get_successor_for_key(str(hashkey))
            #self.f.write(str(float(hashkey)) + "  " + str(i) + "  " + self.finger_table[i] + "\n")
        #self.f.write("===============================\n")

    def handle_successor_failure(self):
        ''' If the successor has failed/unreachable, the first alive 
        and reachable node in the successor_list becomes the successor and 
        the list is updated. If there is no successor, exit'''
        with self.lock:
            response = ChordStatus.ERROR
            for i in range(0, len(self.successor_list)):
                print "Node %s Trying %s as successor" %(self.node_key, self.successor_list[i])
                if self.successor_list[i] == self.node_key:
                    continue

                with remote(self.successor_list[i]) as client:
                    if client is None:
                        continue
                    response = client.get_successor_list()

                self.successor = self.successor_list[i]
                with remote(self.successor) as client:
                    if client is None:
                        continue
                    client.notify(self.node_key)

                self.successor_list = response.successor_list
                del self.successor_list[len(self.successor_list) - 1]
                self.successor_list.insert(0, self.successor)
                return

            # TODO either partitioned or there were more than 'r' failures.
            print "Exiting"
            os._exit(1)

    def handle_finger_failure(self, index):
        with self.lock:
            # Find the highest entry in the table that is not the same as
            # dead node. Saves the trouble of network call to the same node.
            dead_node = self.finger_table[index]
            i = index - 1
            while i > -1:
                if self.finger_table[i] == dead_node:
                    i -= 1
                else:
                    break
            if i == -1:
                return self.node_key, -1

            # Now try checking if that node is alive, otherwise check the next one
            # and so on.
            alive = -1;
            for i in range(i, -1, -1):
                with remote(self.finger_table[i]) as client:
                    if client is None:
                        continue
                    alive = i
                    break
            # No server we know about is alive.
            if alive < 0:
                return self.node_key, -1

            alive_node = self.finger_table[alive]
            for i in range(alive, index + 1):
                self.finger_table[i] = alive_node

            "Returning ", alive_node
            return alive_node, alive


    def print_details(self):
        print "Node_Key ", self.node_key
        print "HashCode ", float(self.hashcode)
        print "successor ", self.successor
        print "predecessor ", self.predecessor
        print "kvstore size", len(self.kvstore)
        print "===============\n\n"

    def write_details(self):
        self.f.write( "Node_Key " +  self.node_key )
        self.f.write( "\nHashCode " + str(self.hashcode))
        self.f.write( "\nsuccessor " + self.successor)
        #self.f.write( "predecessor " + self.predecessor)
        self.f.write( "\nkvstore size" + str(len(self.kvstore)))
        self.f.write( "\n===============\n\n")

    def print_successor_list(self):
        for i in range(0, len(self.successor_list)):
            print self.successor_list[i]
        print "=========================\n\n"

    def ping(self):
        return ChordStatus.OK

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
