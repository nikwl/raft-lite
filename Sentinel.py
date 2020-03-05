#!/usr/bin/env python

import time
import json
import random
import threading

from Interface import Listener, Talker
from MessageProtocol import BaseMessage, RequestVotesMessage, AppendEntriesMessage, parse_json_message

total_nodes = 100

class Sentinel(threading.Thread):
    def __init__(self, config_file, name, role):
        threading.Thread.__init__(self) 

        # Thread should die when the calling thread does
        self.daemon = True

        # Load the list of known nodes
        self.data = self.load_config(config_file, name)

        # Timing conditions
        scale = 10.0
        self.random_backoff = random.uniform(0.1, 1.0)                      # Failed vote backoff
        self.heartbeat_frequency = 1.0 / scale                              # How often to send heartbeat
        self.heartbeat_timeout = (self.heartbeat_frequency * 4)             # How often to check for heartbeat

        # State variables
        self.role = role                            # Your role in the system (follower, candidate, leader)
        self.current_term = 1                       # Election term
        self.i_voted = False                        # Have you voted in this term 
        self.current_leader = None                  # Who is the system leader
        self.current_num_nodes = len(self.data)     # Number of nodes in the system

        # Your information
        self.address = self.data[name]['port']      # Your addressing information

        # This listener is a subscriber that listens to all known ports
        self.listener = Listener(port_list=[self.data[n]['port'] for n in self.data])
        self.listener.start()

        # The talker is a publisher that only takes its own port as input
        self.talker = Talker(port=self.data[name]['port'])
        self.talker.start()

        # Takes care of shutting the system down safely
        self._termate_lock = threading.Lock()
        self._terminate = False

    def stop(self):
        self.talker.stop()
        self.listener.stop()
        self._termate_lock.acquire()
        self._terminate = True
        self._termate_lock.release()

    def run(self):
        # Your role determines your action. Every time a state chance occurs, this loop will facilitate a transition to the next state. 
        while not self._terminate:
            if self.role == 'leader':
                self.leader()
            elif self.role == 'follower':
                self.follower()
            elif self.role == 'candidate':
                self.candidate()

    def follower(self):
        ''' Responsibilites the node has as a follower ''' 

        # Keep track of when you've seen the last heartbeat
        most_recent_heartbeat = time.time()

        # Loop until a state change
        while True:
            # Watch for messages
            incoming_message = self._get_message()
            if (incoming_message is not None):

                # Incoming message is a heartbeat, restart the timer and send an ack
                if (incoming_message.type == BaseMessage.Heartbeat):
                    #self.send_acknowledge(incoming_message.sender)
                    most_recent_heartbeat = time.time()
                    
                # Incoming message is a new election candidate
                elif (incoming_message.type == BaseMessage.RequestVotes):

                    # If this election is for a new term, update your term
                    if (incoming_message.term > self.current_term):
                        self.increment_term(incoming_message.term)
                        
                    # If you haven't already voted, you can now
                    if (not self.i_voted):
                        self.send_vote(incoming_message.sender)

                    # If there's currently a candidate running, don't promote yourself 
                    most_recent_heartbeat = time.time()

            # If you haven't heard a heartbeat in a while, promote yourself to a candidate
            if ((time.time() - most_recent_heartbeat) > self.heartbeat_timeout):
                self.role = 'candidate'
                return

    def candidate(self):
        ''' Responsibilites the node has as a candidate '''  

        # If you're a candidate, then this is a new term
        self.increment_term()

        # Request for people to vote for you
        self.send_request_vote()

        # Vote for yorself
        self.send_vote(self.address)
        
        # Keep track of votes for and against you
        votes_for_me = 0
        total_votes = 0

        # Loop until a state change
        while True:
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

                    # If you have a majority, promote yourself
                    if (votes_for_me > (int(self.current_num_nodes + 1) / 2)):
                        self.role = 'leader'
                        return

                    # If dont have a majority, demote yourself
                    elif (total_votes == self.current_num_nodes):
                        time.sleep(self.random_backoff)
                        self.role = 'follower'
                        return
                
                # If there's an election for someone else on a higher term, update your term, vote for them, and demote yourself
                elif (incoming_message.type == BaseMessage.RequestVotes):
                    if (incoming_message.term > self.current_term):
                        # It's a new term, catch up
                        self.increment_term(incoming_message.term)

                        # You're old news, vote for this guy
                        self.send_vote(incoming_message.sender)

                        # Demote yourself
                        self.role = 'follower'
                        return

                # If you see a heartbeat on the current term then someone else has been elected, update your term and demote yourself
                elif (incoming_message.type == BaseMessage.Heartbeat):
                    if (incoming_message.term >= self.current_term):
                        # It's a new term, catch up
                        self.increment_term(incoming_message.term)

                        # Demote yourself
                        self.role = 'follower'
                        return

    def leader(self):
        ''' Responsibilites the node has as a leader ''' 

        # First things first, send a heartbeat
        self.send_heartbeat()
        print(self.address + ' i got elected')

        # Keep track of when you've sent the last heartbeat
        most_recent_heartbeat = time.time()

        # Loop until a state change
        while True:
            # Send a heartbeat
            if ((time.time() - most_recent_heartbeat) > self.heartbeat_frequency):
                self.send_heartbeat()
                most_recent_heartbeat = time.time()
                print(self.address + ' sending heartbeat on term ' + str(self.current_term))

            # Watch for messages
            incoming_message = self._get_message()
            if (incoming_message is not None):

                # If there's a vote going on with a term above yours, update your term, vote for them, and demote yourself
                if (incoming_message.type == BaseMessage.RequestVotes):
                    if (incoming_message.term > self.current_term):

                        # It's a new term, catch up
                        self.increment_term(incoming_message.term)

                        # You're old news, vote for this guy
                        self.send_vote(incoming_message.sender)

                        # Demote yourself
                        self.role = 'follower'
                        print(self.address + ' i got demoted')
                        return


    def _send_message(self, message):
        # A wrapper to send a message 
        self.talker.send_message(message.jsonify())

    def _get_message(self):
        # A wrapper to recieve a message. If there are no pending messages, returns none
        return parse_json_message(self.listener.get_message())
    
    def increment_term(self, term=None):
        self.i_voted = False
        if (term is None):
            self.current_term = self.current_term + 1
        else:
            self.current_term = term

    def send_heartbeat(self):
        heartbeat_message = AppendEntriesMessage(
            type_ =   BaseMessage.Heartbeat, 
            term_ =   self.current_term, 
            sender_ = self.address
        )
        self._send_message(heartbeat_message)

    def send_vote(self, candidate):
        vote_message = AppendEntriesMessage(
            type_ =    BaseMessage.RequestVoteResponse,
            term_ =    self.current_term,
            sender_=   self.address,
            receiver_= candidate,
            data_ =    {
                'vote': candidate
            }
        )
        self._send_message(vote_message)
        self.i_voted = True

    def send_request_vote(self):
        request_votes = RequestVotesMessage(
            type_ =   BaseMessage.RequestVotes, 
            term_ =   self.current_term, 
            sender_ = self.address
        )
        self._send_message(request_votes)
    
    def send_acknowledge(self, receiver):
        acknowledge_message = AppendEntriesMessage(
                type_ =    BaseMessage.Acknowledge,
                term_ =    self.current_term,
                sender_=   self.address,
                receiver_= receiver
            )
        self._send_message(acknowledge_message)

    def load_config(self, config_file, name):
        with open(config_file, 'r') as infile:
            data = json.load(infile)
        return data

if __name__ == '__main__':
    d = {}
    node_num = 1
    for p in range(5555, 5555+total_nodes):
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
    for p in range(5555, 5555+total_nodes):
        name = 'node' + str(node_num)
        s.append(Sentinel('address_book2.json', name, 'follower'))
        node_num = node_num + 1

    for n in s:
        n.start()

    time.sleep(10)

    for n in s:
        n.stop()
    