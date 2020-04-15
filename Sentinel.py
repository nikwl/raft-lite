#!/usr/bin/env python

import time
import json
import random
import threading
import copy

from Interface import Listener, Talker
from MessageProtocol import BaseMessage, RequestVotesMessage, AppendEntriesMessage, parse_json_message

# Adjust these to test
address_book_fname = 'address_book.json'
total_nodes = 20
start_port = 5557

class Sentinel(threading.Thread):
    def __init__(self, config_file, name, role, verbose=False):
        threading.Thread.__init__(self) 
        self._termate_lock = threading.Lock()
        self._terminate = False
        self.verbose = verbose

        # Thread should die when the calling thread does
        self.daemon = True

        # Load the list of known nodes
        self.data = self._load_config(config_file, name)
        self.leader_address = self.data['leader']['port']         # Special priority channel for adding new nodes

        # Timing conditions
        self.election_timeout = random.uniform(0.4, 0.5)          # Failed vote backoff, used for pretty much all timing related things
        self.heartbeat_frequency = 0.2                            # How often to send heartbeat (should be less than the election timeout)

        # State variables
        self.current_role = role                                  # Your role in the system (follower, candidate, leader, pending)
        self.current_term = 1                                     # Election term
        self.voted_for = None                                     # Who have you voted in this term 
        self.current_num_nodes = len(self.data) - 1               # Number of nodes in the system
        self.current_leader = None                                # Who is the system leader
        self.current_nodes = {}                                   # Set containing the last contact with each node
        self.node_name = name                                     # Your name, used to determine your address

        # If you're not in the address book, then you'll be talking on the leader address until you're added to the system
        if self.node_name not in self.data:
            self.address = self.leader_address
            self.current_role = 'pending'
        else:
            self.address = self.data[self.node_name]['port'] 
        
        # This is what the Listener uses to identify itself
        identity = {
            'address': self.address,
            'node_name': self.node_name
        }
        self.listener = Listener(port_list=copy.deepcopy(self.data), identity=identity)
        self.listener.start()

        # If you're in the address book, you can start talking right away, else wait
        if self.node_name in self.data:
            self.talker = Talker(port=self.address)
            self.talker.start()

    def stop(self):
        self.talker.stop()
        self.listener.stop()
        self._terminate = True

    def run(self):
        # Wait for the Interface to be ready
        time.sleep(self.listener.initial_backoff)

        # Your role determines your action. Every time a state chance occurs, this loop will facilitate a transition to the next state. 
        while not self._terminate:
            if self.current_role == 'leader':
                self._leader()
            elif self.current_role == 'follower':
                self._follower()
            elif self.current_role == 'candidate':
                self._candidate()
            elif self.current_role == 'pending':
                self._initiate_connection()

    def _initiate_connection(self):
        '''
            _initiate_connection: Nodes will start here if they do not exist in the address book. This
                means that they need to be added to the system. To add themselves to the system they 
                must send a connection request over a special leader address, which allows them to send 
                messages directly to the leader node. When a leader emerges, the leader will observe the
                request and respond to the node with a new address. This response will be heard by other
                nodes, who will also add this node to the system. 
        '''

        # The talker is a publisher that only takes its own port as input
        self.talker = Talker(port=self.address) # Your address is currenty the leader address
        self.talker.start()

        # Address may already be in use, wait until it's not
        self.talker.wait_until_ready()

        # If you're connecting to an existing network, send a request to the leader address
        self._send_connection_request()

        # Keep track of when you've sent the last request
        most_recent_request = time.time()
        resend_window = 5.0

        if(self.verbose):
            print(self.address + ': requesting access to the network')

        # Loop until a state change
        while not self._terminate:
            # Watch for messages
            incoming_message = self._get_message()
            if (incoming_message is not None):

                # If the message is a new connection response, and the names match you have been added to the network
                if (incoming_message.type == BaseMessage.ConnectionResponse):
                    if ((incoming_message.data['node_name'] == self.node_name) and (incoming_message.receiver == self.address)):
                        
                        # Stop the current talker to free up the master channel
                        self.talker.stop()

                        # Create a new talker on the new address
                        self.address = incoming_message.data['new_address']
                        self.talker = Talker(port=self.address)
                        self.talker.start()

                        # Update some metadata
                        self.current_num_nodes = incoming_message.data['current_num_nodes']
                        self.current_leader = incoming_message.sender
                        self.current_term = incoming_message.term

                        # Send ack
                        self._send_acknowledge(incoming_message.sender, BaseMessage.ConnectionResponse)

                        #print(self.address + ' ive been added to the network ')
                        self.current_role = 'follower'
                        return
            
            # If you haven't sent a request in a while, send another
            if ((time.time() - most_recent_request) > resend_window):
                if(self.verbose):
                    print(self.address + ': re-requesting access to the network')
                self._send_connection_request()
                most_recent_request = time.time()

        # If you reach here, then the thread is exiting
        return

    def _follower(self):
        ''' 
            _follower: Nodes will start here if they have been newly added to the network. The responsibilities
                of the follower nodes are as follows:
                    - Observe and respond to heartbeat sent by the leader node.
                    - If no heartbeat is heard, promote self to candidate. 
                    - If RequestVotes is heard, vote for that node for the current term. 
        ''' 

        # This will be when you've seen the most recent heartbeat
        most_recent_heartbeat = time.time()

        while not self._terminate:
            incoming_message = self._get_message()
            if (incoming_message is not None):

                # Incoming message is a heartbeat, restart the timer and send an ack
                if (incoming_message.type == BaseMessage.Heartbeat):
                    self._send_acknowledge(incoming_message.sender, BaseMessage.Heartbeat)
                    most_recent_heartbeat = time.time()

                    # Update the number of nodes, and the current leader
                    self.current_num_nodes = incoming_message.data['current_num_nodes']
                    self.current_leader = incoming_message.sender
                    
                # Incoming message is a new election candidate
                elif (incoming_message.type == BaseMessage.RequestVotes):

                    # If this election is for a new term, update your term
                    if (incoming_message.term > self.current_term):
                        self._increment_term(incoming_message.term)
                        
                    # If you haven't already voted, you can now
                    if (self.voted_for is None):
                        self._send_vote(incoming_message.sender)

                    # If there's currently a candidate running, don't promote yourself, ie count this as a heartbeat 
                    most_recent_heartbeat = time.time()

                # Incoming message is a new connection response, someone has been added to the network, update your node counter
                elif (incoming_message.type == BaseMessage.ConnectionResponse):
                    self.current_num_nodes = self.current_num_nodes + 1

            # If you haven't heard a heartbeat in a while, promote yourself to a candidate
            if ((time.time() - most_recent_heartbeat) > (self.election_timeout)):
                self.current_role = 'candidate'
                return

        # If you reach here, then the thread is exiting
        return

    def _candidate(self):
        ''' 
            _candidate: Nodes will start here if they have not heard the leader for a period of time. The 
                responsibilities of the candidate nodes are as follows:
                    - Increment term and vote for self.
                    - Send RequestVotes to all other nodes. 
                    - Wait for responses. There are several possible outcomes:
                        + If you recieve more than half of the votes in the system, promote yourself. 
                        + If you see someone else starting an election for a term higher than your own, 
                            vote for them, update yourself, and then demote yourself. 
                        + If you see a heartbeat for a term higher than or equal to your own, update 
                            yourself, and then demote yourself. 
                        + If you see a connection response, add this new person to the network. This only
                            happens if you're out of sync but it must be accounted for. 
                        + If you have not won after the election timeout, either you lost, or nodes have
                            been removed from the network. In either case, update your current number of 
                            nodes and then demote yourself. 
        ''' 

        if(self.verbose):
            print(self.address + ': became candidate')

        # If you're a candidate, then this is a new term
        self._increment_term()

        # Request for people to vote for you
        self._send_request_vote()

        # Vote for yorself
        self._send_vote(self.address)
        
        # Keep track of votes for and against you
        votes_for_me = 0
        total_votes = 0

        # Keep track of how long the election has been going
        time_election_going = time.time()

        while not self._terminate:
            incoming_message = self._get_message()
            if (incoming_message is not None):
                
                # If it is a vote, then tally for or against you
                if (incoming_message.type == BaseMessage.RequestVoteResponse):

                    # Tally any votes for you
                    if (incoming_message.data['vote'] == self.address):
                        votes_for_me = votes_for_me + 1

                    print(self.address + ' votes for me ' + str(votes_for_me))
                    print(self.address + ' num nodes ' + str(self.current_num_nodes))
                        
                    # If you have a majority, promote yourself
                    total_votes = total_votes + 1 
                    if ((votes_for_me > int(self.current_num_nodes / 2)) or (self.current_num_nodes == 1)):
                        self.current_role = 'leader'
                        return
                
                # If there's an election for someone else on a higher term, update your term, vote for them, and demote yourself
                elif (incoming_message.type == BaseMessage.RequestVotes):
                    if (incoming_message.term > self.current_term):
                        # It's a new term, catch up
                        self._increment_term(incoming_message.term)

                        # You're old news, vote for this guy
                        self._send_vote(incoming_message.sender)

                        # Demote yourself
                        self.current_role = 'follower'
                        return

                # If you see a heartbeat on the current term then someone else has been elected, update your term and demote yourself
                elif (incoming_message.type == BaseMessage.Heartbeat):
                    if (incoming_message.term >= self.current_term):
                        # It's a new term, catch up
                        self._increment_term(incoming_message.term)
                        self.current_leader = incoming_message.sender

                        # Demote yourself
                        self.current_role = 'follower'
                        return
                
                # If the message is a new connection response, someone has been added to the network, update your node counter
                elif (incoming_message.type == BaseMessage.ConnectionResponse):
                    self.current_num_nodes = self.current_num_nodes + 1
            
            # If this election has been going for a while we're probably deadlocked, update num nodes, backoff and demote
            if ((time.time() - time_election_going) > self.election_timeout):
                if(self.verbose):
                    print(self.address + ': election timed out')

                # This little bit here lets elections progress without getting what might be a majority. But it also lets a leader be elected
                # after the crashing of more than half of the nodes. 
                self.current_num_nodes = total_votes
                self.current_role = 'follower'
                return
        
        # If you reach here, then the thread is exiting
        return

    def _leader(self):
        ''' 
            _leader: Nodes will start here if they have won an election and promoted themselves. The 
                responsibilities of the leader nodes are as follows:
                    - Send a periodic heartbeat. 
                    - Keep track of who is in the system. Your counter will be broadcast to the other nodes
                        and will ensure that future elections proceed smoothly. If you haven't heard from 
                        a node for a while, then mark it as stale and update your counter. 
                    - If there's a vote going on with a term term higher than your own, vote for them, 
                        update yourself, and then demote yourself. If there's a vote going on with a 
                        term equal to yours, do nothing, they will see your heartbeat and back off.  
                    - If you see an ack then there are two possible outcomes:
                        + If the ack is a heartbeat ack, update the list of active nodes. 
                        + If the ack is a conection request ack, then a new person has been added to the 
                            network. 
                    - Check the leader messages, which are used only for incoming connection requests. If
                        you see a new connection request, allow the node into the network. 
        ''' 
        
        if(self.verbose):
            print(self.address + ': became leader')

        # First things first, send a heartbeat
        self._send_heartbeat()

        # You're the current leader
        self.current_leader = self.address
        
        # Keep track of when you've sent the last heartbeat
        most_recent_heartbeat = time.time()

        while not self._terminate:
            # First, send a heartbeat
            if ((time.time() - most_recent_heartbeat) > self.heartbeat_frequency):
                self._send_heartbeat()
                most_recent_heartbeat = time.time()

                # And then check to see when you've last recieved contact from other nodes, update the number of current nodes accordingly
                num_nodes_not_stale = 1
                for n in self.current_nodes:
                    if (self.current_nodes[n] is not None):
                        if ((time.time() - self.current_nodes[n]) > self.election_timeout):
                            self.current_nodes[n] = None
                        else:
                            num_nodes_not_stale = num_nodes_not_stale + 1
                self.current_num_nodes = num_nodes_not_stale  

                if(self.verbose):
                    print(self.address + ': sending heartbeat to ' + str(self.current_num_nodes) + ' active nodes')              

                #print(self.address + ' sending heartbeat on term ' + str(self.current_term))
                #print(self.address + ' current num nodes ' + str(self.current_num_nodes))

            # Watch for messages
            incoming_message = self._get_message()
            if (incoming_message is not None):

                # Incoming message is a request votes, if there's a vote going on with a term above yours, update your term, vote for them, and demote yourself
                if (incoming_message.type == BaseMessage.RequestVotes):
                    if (incoming_message.term > self.current_term):
                        if(self.verbose):
                            print(self.address + ': saw higher term, demoting')

                        # It's a new term, catch up
                        self._increment_term(incoming_message.term)

                        # You're old news, vote for this guy
                        self._send_vote(incoming_message.sender)

                        # Reset your node list
                        self.current_nodes = {}

                        # Demote yourself
                        self.current_role = 'follower'
                        return
                
                # Incoming message is a heartbeat that is not yours, demote yourself. 
                elif ((incoming_message.type == BaseMessage.Heartbeat) and (incoming_message._sender != self.address) and (incoming_message._term >= self.term)):
                    if(self.verbose):
                        print(self.address + ': saw another leader, demoting')

                    # Reset your node list
                    self.current_nodes = {}

                    # Demote yourself
                    self.current_role = 'follower'
                    return


                # Incoming message is an ack, either a heartbeat ack, or a connection accept ack
                elif (incoming_message.type == BaseMessage.Acknowledge):

                    # If the message is an ack to your heartbeat, add this node to the list of active nodes
                    if ((incoming_message.receiver == self.address) and (incoming_message.data['ack_reason'] == BaseMessage.Heartbeat)):
                        self.current_nodes[incoming_message.sender] = time.time()

                    # If the message is an ack to a connection accept
                    elif ((incoming_message.data['ack_reason'] == BaseMessage.ConnectionResponse)):
                        # Increment the number of active nodes
                        self.current_num_nodes = self.current_num_nodes + 1
                        if(self.verbose):
                            print(self.address + ': okaying addition of node: ' + incoming_message.sender)

            # Watch for leader messages, ie network updates
            incoming_message = self._get_leader_message()
            if (incoming_message is not None):
                # Incoming message is a new connection request, send a response
                if (incoming_message.type == BaseMessage.ConnectionRequest):
                    self._send_connection_response(incoming_message)

        return
                    
    def _send_message(self, message):
        # A wrapper to send a message 
        self.talker.send_message(message.jsonify())

    def _get_message(self):
        # A wrapper to recieve a message. If there are no pending messages, returns none
        return parse_json_message(self.listener.get_message())

    def _get_leader_message(self):
        # A wrapper to recieve a leader message. If there are no pending messages, returns none
        return parse_json_message(self.listener.get_leader_message())
    
    def _increment_term(self, term=None):
        self.voted_for = None
        if (term is None):
            self.current_term = self.current_term + 1
        else:
            self.current_term = term

    def _send_request_vote(self):
        request_votes = RequestVotesMessage(
            type_ =   BaseMessage.RequestVotes, 
            term_ =   self.current_term, 
            sender_ = self.address
        )
        self._send_message(request_votes)

    def _send_heartbeat(self):
        heartbeat_message = AppendEntriesMessage(
            type_ =   BaseMessage.Heartbeat, 
            term_ =   self.current_term, 
            sender_ = self.address,
            data_ =   {
                'current_num_nodes': self.current_num_nodes,
                'current_leader': self.current_leader
            }
        )
        self._send_message(heartbeat_message)

    def _send_vote(self, candidate):
        vote_message = AppendEntriesMessage(
            type_ =     BaseMessage.RequestVoteResponse,
            term_ =     self.current_term,
            sender_ =   self.address,
            receiver_ = candidate,
            data_ =     {
                'vote': candidate
            }
        )
        self._send_message(vote_message)
        self.voted_for = candidate
    
    def _send_acknowledge(self, receiver, reason):
        acknowledge_message = AppendEntriesMessage(
            type_ =     BaseMessage.Acknowledge,
            term_ =     self.current_term,
            sender_ =   self.address,
            receiver_ = receiver,
            data_ =     {
                'ack_reason': reason
            }
        )
        self._send_message(acknowledge_message)
    
    def _send_connection_request(self):
        connection_message = AppendEntriesMessage(
            type_ =     BaseMessage.ConnectionRequest,
            term_ =     self.current_term,
            sender_ =   self.address,     # Will be equal to leader channel
            receiver_ = self.leader_address,
            data_ =     {
                'node_name': self.node_name,
                'new_address': None
            }
        )
        self._send_message(connection_message)

    def _send_connection_response(self, receiver):
        connection_message = AppendEntriesMessage(
            type_ =     BaseMessage.ConnectionResponse,
            term_ =     self.current_term,
            sender_ =   self.address,
            receiver_ = receiver.sender,     # Will still be equal to leader channel
            data_ =     {
                'node_name': receiver.data['node_name'],     # Make sure it gets to the right person
                'new_address': receiver.data['new_address'], # Auto populated by the Interface
                'current_num_nodes': self.current_num_nodes,
                'current_leader': self.current_leader,
                'leader_name': self.node_name
            }
        )
        self._send_message(connection_message)

    def _load_config(self, config_file, name):
        with open(config_file, 'r') as infile:
            data = json.load(infile)
        return data

def test_failures():
    '''
        test_failures: Creates a bunch of nodes and then crashes half of them. 
            A leader should emerge after the crash. 
    '''
    # Create the address book with x number of nodes
    d = {'leader':{'ip': 'localhost',
            'port': '5553'}}

    node_num = 1
    for p in range(start_port, start_port+total_nodes):
        name = 'node' + str(node_num)
        d[str(name)] = { 
            'ip': 'localhost',
            'port': str(p)
        }
        node_num = node_num + 1
        
    with open(address_book_fname, 'w') as outfile:
        json.dump(d, outfile)

    # Create and start the sentinals
    s = []
    node_num = 1
    for p in range(start_port, start_port+total_nodes):
        name = 'node' + str(node_num)
        s.append(Sentinel(address_book_fname, name, 'follower'))
        node_num = node_num + 1
    for n in s:
        n.start()

    # Wait for a bit
    time.sleep(10)

    # Kill half of them
    num_to_kill = int(total_nodes / 2) - 1
    for n in s:
        num_to_kill = num_to_kill - 1
        n.stop()
        if num_to_kill == 0:
            break

    # A leader should emerge
    time.sleep(10)

    # Kill the rest of them
    for n in s:
        n.stop()

def test_add_remove():
    '''
        test_failures: Creates 1 known node, and then slowly adds more. You should 
            observe the number of known nodes slowly increasing as the leader allows
            them to join. Then the leader. The remaining nodes should fight over 
            the position, several of them may be elected, but one should emerge.
    '''

    # Create and start the sentinals
    addresss_book = {
        'leader':{
            'ip': 'localhost',
            'port': '5553'},
        'node1':{
            'ip': 'localhost',
            'port': '5554'}
    }
    with open(address_book_fname, 'w') as outfile:
        json.dump(addresss_book, outfile)

    node1 = Sentinel(address_book_fname, 'node1', 'follower', verbose=True)
    node2 = Sentinel(address_book_fname, 'node2', 'follower', verbose=True)
    node3 = Sentinel(address_book_fname, 'node3', 'follower', verbose=True)
    node4 = Sentinel(address_book_fname, 'node4', 'follower', verbose=True)

    node1.start()
    node2.start()
    node3.start()
    node4.start()

    time.sleep(5)

    # Kill the leader and one other
    node1.stop()
    node2.stop()

    time.sleep(10)

    node3.stop()
    node4.stop()

if __name__ == '__main__':
    test_add_remove()