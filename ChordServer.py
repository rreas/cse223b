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
        self.hashcode = get_hash(self.node_key)
        self.lock = threading2.Lock()
        
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
        self.initialize_threads()
        

    def initialize(self):
        ''' Get the initial data from the successor. Once this method completes,
        the current node should be able to serve the keys that it owns and also 
        have a successor_list to work with'''
        with remote(self.successor) as client:
            if client is None:
                print "Unable to contact successor for init_data, exiting.."
                os._exit(1)
            data_response = client.get_init_data(str(self.hashcode))

        self.kvstore = data_response.kvstore
        self.successor_list = data_response.successor_list

        # Remove the last entry in the list and add the successor at the beginning 
        # of the list and list update is complete
        del self.successor_list[len(self.successor_list) - 1]
        self.successor_list.insert(0, self.successor)

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

    def get_successor_for_key(self, hashcode):
        ''' Given a particular hashed key, find its successor in Chord. 
        This is O(n). Each node asks its successor to find the master,
        unless the master is the successor itself.'''
        hashcode_int = int(hashcode)
        if self.successor == self.node_key:
            return self.node_key

        # If the key is located between the current node and its successor,
        # just return the successor.
        if is_key_between(hashcode_int, self.hashcode, get_hash(self.successor)):
            return self.successor

        # Pass the buck to the successor and let it find the master.
        with remote(self.successor) as client:
            if client is None:
                self.handle_successor_failure()
                return None
            return client.get_successor_for_key(hashcode)

    def get_init_data(self, hashcode):
        ''' Provide the data required by a new server to be able to serve keys
        and handle server failures'''

        # dummy for now.
        data_response = DataResponse()
        data_response.kvstore = {}
        data_response.successor_list = self.successor_list
        data_response.status = ChordStatus.OK
        return data_response

    def get_predecessor(self):
        return str(self.predecessor)

    def get_successor_list(self):
        response = SuccessorListResponse()
        response.status = ChordStatus.OK
        response.successor_list = self.successor_list
        return response
    
    def get_successor(self):
        return str(self.successor)

    def get(self, key):
        ''' Get the key from the master node. If the current node does not know
        who the master is, it will ask its successor about it'''
        master_node = self.get_successor_for_key(str(get_hash(key)))

        if master_node == self.node_key:
            response = GetValueResponse()
            with self.lock:
                response.value = self.kvstore[key]
            response.status = ChordStatus.OK
            return response

        else:
            with remote(master_node) as client:
                if client is None:
                    self.handle_successor_failure()
                    response = GetValueResponse()
                    response.status = ChordStatus.ERROR
                    return response
                return client.get(key)

    def put(self, key, value):
        ''' Find the master node and store the key, value there.'''

        master_node = self.get_successor_for_key(str(get_hash(key)))

        if master_node == self.node_key:
            with self.lock:
                self.kvstore[key] = value
                self.replicate_dat_shit(key, value)
            return ChordStatus.OK  
        else:
            with remote(master_node) as client:
                if client is None:
                    self.handle_successor_failure()
                    return ChordStatus.ERROR

                status = client.put(key, value)
                return status


    def replicate_dat_shit(self, key, value, replicas=1):
        #Replicate this shit! With thrift RPC replicate call
        if self.successor == self.node_key:
            return ChordStatus.OK

        #Else, send to successor
        with remote(self.successor) as client:
            status = client.replicate(key, value, self.node_key)
            return status

    def replicate(self, key, value, source):
        with self.lock:
            #Store in our replicas dict
            #self.kvstore[key] = value
            self.replicas[source][key] = value
        return ChordStatus.OK

    def notify(self, node):
        # TODO: Probably need a check to see if it is truly the predecessor (see Chord)
        self.predecessor = node
        return ChordStatus.OK

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
                        self.handle_successor_failure()
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
                            # TODO: what do we do here? The current node heard about a new successor,
                            # was unable to contact it. For now, doing nothing and continuing as if 
                            # the new node did not join.

                            # QUESTION: Should this just handle succ fail?
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
                        continue
                    response = client.get_successor_list()
                    # Get updated successor_list from successor and make adjustments.
                    self.successor_list = response.successor_list
                    del self.successor_list[len(self.successor_list) - 1]
                    self.successor_list.insert(0, self.successor)

            #self.print_details()
            #self.print_successor_list()

    # QUESTION: If we are here should we retry the last thing?
    # QUESTION: Why do we exit?  Can't it just be self?
    def handle_successor_failure(self):
        ''' If the successor has failed/unreachable, the first alive 
        and reachable node in the successor_list becomes the successor and 
        the list is updated. If there is no successor, exit'''
        with self.lock:
            response = ChordStatus.ERROR
            for i in range(0, len(self.successor_list)):
                # print "Trying ", self.successor_list[i]
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

    def print_details(self):
        print "Node_Key ", self.node_key
        print "HashCode ", self.hashcode
        print "successor ", self.successor
        print "predecessor ", self.predecessor
        print "===============\n\n"

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
