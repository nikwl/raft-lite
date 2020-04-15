#!/usr/bin/env python

import time
import json
import random
import threading
import copy

from Interface import Listener, Talker
from MessageProtocol import BaseMessage, RequestVotesMessage, AppendEntriesMessage, parse_json_message

total_nodes = 10
start_port = 5557

class Sentinel(threading.Thread):
    def __init__(self, config_file, name, role):
        threading.Thread.__init__(self) 

        # Thread should die when the calling thread does
        self.daemon = True

        # Load the list of known nodes
        self.data = self._load_config(config_file, name)
        self.leader_address = self.data['leader']['port']                 # Special priority channel for adding new nodes

        # Timing conditions
        scale = 10.0
        self.random_range = [0.1, 1.0]
        self.random_backoff = random.uniform(self.random_range[0], self.random_range[1])            # Failed vote backoff
        self.heartbeat_frequency = 1.0 / scale                    # How often to send heartbeat
        self.heartbeat_timeout = (self.heartbeat_frequency * 2)   # How often to check for heartbeat
        self.node_stale_after = self.heartbeat_timeout

        # State variables
        self.current_role = role                                  # Your role in the system (follower, candidate, leader)
        self.current_term = 1                                     # Election term
        self.voted_for = None                                     # Who have you voted in this term 
        self.current_num_nodes = len(self.data) - 1               # Number of nodes in the system
        self.current_leader = None                                # Who is the system leader
        self.current_nodes = {}                                   # Set containing the last contact with each node

        # Takes care of shutting the system down safely
        self._termate_lock = threading.Lock()
        self._terminate = False

        # Your information
        self.node_name = name

        # If you're not in the address book, then you'll be talking on the leader address
        if self.node_name not in self.data:
            self.address = self.leader_address
            self.current_role = 'pending'
        else:
            self.address = self.data[self.node_name]['port']                # Your addressing information
        
        # This is what the Listener uses to identify itself
        identity = {
            'address': self.address,
            'node_name': self.node_name
        }
        self.listener = Listener(port_list=copy.deepcopy(self.data), identity=identity)
        self.listener.start()

        # If you're in the address book, you can start talking right away, else wait
        if self.node_name in self.data:
            # The talker is a publisher that only takes its own port as input
            self.talker = Talker(port=self.address)
            self.talker.start()

    def stop(self):
        self.talker.stop()
        self.listener.stop()
        self._termate_lock.acquire()
        self._terminate = True
        self._termate_lock.release()

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
        # The talker is a publisher that only takes its own port as input
        self.talker = Talker(port=self.address) # Your address is currenty the leader address
        self.talker.start()

        # Address may already be in use, wait until it's not
        self.talker.wait_until_ready()
        #print(self.address + ' got use of leader address')

        # If you're connecting to an existing network, send a request to the leader address
        self._send_connection_request()

        # Keep track of when you've sent the last request
        most_recent_request = time.time()
        resend_window = 1.0

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
                print(self.address + ' resending connection request')
                self._send_connection_request()
                most_recent_request = time.time()

        # If you reach here, then the thread is exiting
        return

    def _follower(self):
        ''' Responsibilites the node has as a follower ''' 

        # Keep track of when you've seen the last heartbeat
        most_recent_heartbeat = time.time()

        # Loop until a state change
        while not self._terminate:
            # Watch for messages
            incoming_message = self._get_message()
            if (incoming_message is not None):

                # Incoming message is a heartbeat, restart the timer and send an ack
                if (incoming_message.type == BaseMessage.Heartbeat):
                    self._send_acknowledge(incoming_message.sender, BaseMessage.Heartbeat)
                    most_recent_heartbeat = time.time()
                    self.current_leader = incoming_message.sender
                    #print(self.address + ' heard heartbeat')

                    # Update the number of nodes in the system
                    self.current_num_nodes = incoming_message.data['current_num_nodes']
                    
                # Incoming message is a new election candidate
                elif (incoming_message.type == BaseMessage.RequestVotes):

                    # If this election is for a new term, update your term
                    if (incoming_message.term > self.current_term):
                        self._increment_term(incoming_message.term)
                        
                    # If you haven't already voted, you can now
                    if (self.voted_for is None):
                        self._send_vote(incoming_message.sender)

                    # If there's currently a candidate running, don't promote yourself 
                    most_recent_heartbeat = time.time()

                # If the message is a new connection response, someone has been added to the network, update your node counter
                elif (incoming_message.type == BaseMessage.ConnectionResponse):
                    self.current_num_nodes = self.current_num_nodes + 1

            # If you haven't heard a heartbeat in a while, promote yourself to a candidate
            if ((time.time() - most_recent_heartbeat) > (self.heartbeat_timeout + random.uniform(self.random_range[0], self.random_range[1]))):
                self.current_role = 'candidate'
                return

        # If you reach here, then the thread is exiting
        return

    def _candidate(self):
        ''' Responsibilites the node has as a candidate '''  

        # If you're a candidate, then this is a new term
        self._increment_term()

        # Request for people to vote for you
        self._send_request_vote()
        print(self.address + ' im running')

        # Vote for yorself
        self._send_vote(self.address)
        
        # Keep track of votes for and against you
        votes_for_me = 0
        total_votes = 0

        # Keep track of when you've seen the last heartbeat
        time_election_going = time.time()
        election_length = 1.0

        # Loop until a state change
        while not self._terminate:
            # Watch for messages
            incoming_message = self._get_message()
            if (incoming_message is not None):
                
                # If it is a vote, then tally for or against you
                if (incoming_message.type == BaseMessage.RequestVoteResponse):

                    # Tally any votes for you
                    if (incoming_message.data['vote'] == self.address):
                        votes_for_me = votes_for_me + 1
                        
                    # Increment the total votes
                    total_votes = total_votes + 1 
                    #print(self.address + ' total votes ' + str(votes_for_me))
                    #print(self.address + ' votes for me ' + str(votes_for_me))

                    # If you have a majority, promote yourself
                    if ((votes_for_me > int((self.current_num_nodes + 1) / 2)) or (self.current_num_nodes == 1)):
                        self.current_role = 'leader'
                        return

                    # If dont have a majority, demote yourself
                    elif (total_votes == self.current_num_nodes):
                        time.sleep(random.uniform(self.random_range[0], self.random_range[1]))
                        self.current_role = 'follower'
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
            if ((time.time() - time_election_going) > election_length):
                print(self.address + ' I think were deadlocked, backing off ')
                self.current_num_nodes = total_votes
                time.sleep(random.uniform(self.random_range[0], self.random_range[1]))
                self.current_role = 'follower'
                return
        
        # If you reach here, then the thread is exiting
        return

    def _leader(self):
        ''' Responsibilites the node has as a leader ''' 
        
        # First things first, send a heartbeat
        self._send_heartbeat()
        print(self.address + ' i got elected')

        # You're the current leader
        self.current_leader = self.address
        
        # Keep track of when you've sent the last heartbeat
        most_recent_heartbeat = time.time()

        # Loop until a state change
        while not self._terminate:
            # Send a heartbeat
            if ((time.time() - most_recent_heartbeat) > self.heartbeat_frequency):
                self._send_heartbeat()
                most_recent_heartbeat = time.time()

                # And then check up on active nodes
                #print(self.address + ' current num nodes bef ' + str(self.current_num_nodes))
                num_nodes_not_stale = 1
                for n in self.current_nodes:
                    if (self.current_nodes[n] is not None):
                        if ((time.time() - self.current_nodes[n]) > self.node_stale_after):
                            self.current_nodes[n] = None
                        else:
                            num_nodes_not_stale = num_nodes_not_stale + 1

                self.current_num_nodes = num_nodes_not_stale                

                #for n in self.current_nodes:
                #    print(self.current_nodes[n])

                #print(self.address + ' sending heartbeat on term ' + str(self.current_term))
                print(self.address + ' current num nodes ' + str(self.current_num_nodes))

            # Watch for leader messages (network updates)
            incoming_message = self._get_leader_message()
            if (incoming_message is not None):
                # If the message is a new connection request
                if (incoming_message.type == BaseMessage.ConnectionRequest):

                    # Send the node its new port number, which is its address, which was updated by the Interface
                    self._send_connection_response(incoming_message)

            # Watch for messages
            incoming_message = self._get_message()
            if (incoming_message is not None):

                # If there's a vote going on with a term above yours, update your term, vote for them, and demote yourself
                if (incoming_message.type == BaseMessage.RequestVotes):
                    if (incoming_message.term > self.current_term):

                        # It's a new term, catch up
                        self._increment_term(incoming_message.term)

                        # You're old news, vote for this guy
                        self._send_vote(incoming_message.sender)

                        # Reset your contact list 
                        self.current_nodes = {}

                        # Demote yourself
                        self.current_role = 'follower'
                        print(self.address + ' i got demoted')
                        return
                
                # If message is an ack, this is a heartbeat ack, or this is a connection accept ack
                elif (incoming_message.type == BaseMessage.Acknowledge):

                    # If the message is an ack to your heartbeat, add this node to the list of active nodes
                    if ((incoming_message.receiver == self.address) and (incoming_message.data['ack_reason'] == BaseMessage.Heartbeat)):
                        self.current_nodes[incoming_message.sender] = time.time()

                        #print(self.address + ' recieved heartbeat ack from ' + incoming_message.sender)
                        #for n in self.current_nodes:
                        #    print(self.address + ' recieved heartbeat ack from ' + n)

                    # If the message is an ack to a connection accept
                    elif ((incoming_message.data['ack_reason'] == BaseMessage.ConnectionResponse)):
                        # Increment the number of active nodes
                        self.current_num_nodes = self.current_num_nodes + 1
                        print(self.address + ' okaying addition of node ' + incoming_message.sender)
                        print(self.address + ' current num nodes ' + str(self.current_num_nodes))

        # If you reach here, then the thread is exiting
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

def test1():
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
        
    with open('address_book2.json', 'w') as outfile:
        json.dump(d, outfile)

    s = []

    node_num = 1
    for p in range(start_port, start_port+total_nodes):
        name = 'node' + str(node_num)
        s.append(Sentinel('address_book2.json', name, 'follower'))
        node_num = node_num + 1

    for n in s:
        n.start()

    time.sleep(10)

    num_to_kill = int(total_nodes / 2)
    for n in s:
        num_to_kill = num_to_kill - 1
        n.stop()
        if num_to_kill == 0:
            break
    
    time.sleep(10)

    for n in s:
        n.stop()

def test2():
    addresss_book = {
        'leader':{
            'ip': 'localhost',
            'port': '5553'},
        'node1':{
            'ip': 'localhost',
            'port': '5554'}
        #'node2':{
        #    'ip': 'localhost',
        #    'port': '5555'},
    }

    with open('address_book2.json', 'w') as outfile:
        json.dump(addresss_book, outfile)

    node1 = Sentinel('address_book2.json', 'node1', 'follower')
    node2 = Sentinel('address_book2.json', 'node2', 'follower')
    node3 = Sentinel('address_book2.json', 'node3', 'follower')
    node4 = Sentinel('address_book2.json', 'node4', 'follower')

    node1.start()
    node2.start()
    node3.start()
    node4.start()

    time.sleep(10)

    node1.stop()
    node2.stop()
    node3.stop()

    time.sleep(10)

    node4.stop()

if __name__ == '__main__':
    test1()