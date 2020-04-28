#!/usr/bin/env python

import copy
import time
import json
import random
import threading
from collections import deque 

from Interface import Listener, Talker
from MessageProtocol import MessageType, MessageDirection, RequestVotesResults, AppendEntriesResults, RequestVotesMessage, AppendEntriesMessage, parse_json_message

# Adjust these to test
address_book_fname = 'address_book.json'
total_nodes = 5
start_port = 5557

class RaftNode(threading.Thread):
    def __init__(self, config_file, name, role, verbose=True):
        threading.Thread.__init__(self) 
        
        self._terminate = False
        self.daemon = True

        self.verbose = verbose

        # Where incoming client requests go
        self.client_queue = deque()
        self.client_lock = threading.Lock()

        # List of known nodes
        address_book = self._load_config(config_file, name)
        self.node_addresses = [address_book[a]['port'] for a in address_book if a != 'leader']

        # Timing variables
        self.election_timeout = random.uniform(0.4, 0.5)        # Failed vote backoff, used for pretty much all timing related things
        self.heartbeat_frequency = 0.2                          # How often to send heartbeat (should be less than the election timeout)
        self.resend_time = 2.0                                  # How often to resend an append entries

        # State variables that I've added
        self.address = address_book[name]['port']               # Your address
        self.current_num_nodes = len(self.node_addresses)       # Number of nodes in the system
        self.current_role = role                                # Your role in the system (follower, candidate, leader, pending)
        self.leader_id = None                                   # Who you think the current leader is
        
        # Persistent state variables
        self.current_term = 1                                   # Election term
        self.voted_for = None                                   # Who have you voted in this term 
        self.log = [{
            'term': 1,
            'entry': 'Init Entry'  
        }]
    
        # Volatile state variables
        self.commit_index = 0                                   # Index of the highest known committed entry in the system
        self.last_applied_index = 0                             # Index of the highest entry you have committed
        self.last_applied_term = 1                              # Term of the highest entry you have committed

        # Volotile state leader variables 
        self.next_index = [None for _ in range(self.current_num_nodes)]     # Index to send to each node next. None means up to date
        self.match_index = [0 for _ in range(self.current_num_nodes)]       # Index of highest committed entry on each node
        self.heard_from = [0 for _ in range(self.current_num_nodes)]

        # Start both ends of your interface
        identity = {
            'address':      self.address,
            'node_name':    name
        }
        self.listener = Listener(port_list=address_book, identity=identity)
        self.listener.start()
        self.talker = Talker(identity=identity)
        self.talker.start()

    def stop(self):
        self.talker.stop()
        self.listener.stop()
        self._terminate = True

    def client_request(self, value, id_num=None):
        '''
            client_request: Public function to enqueue a value. If the node
                is not the leader, this request will be forewarded to the leader
                before being appended. If the system is in a transitionary state, 
                the request may not be appended at all, so this should be retried.
                If no id_num is specified, the id is set to -1.
            Inputs:
                value: any singleton data type.
                id_num: an identifying number. Can be used to query this log entry. 
        '''
        if (id_num is None):
            id_num = -1
        with self.client_lock:
            # Create and enqueue the entry
            entry = {
                'term': self.current_term,
                'entry': value,
                'id': id_num
            }
            self.client_queue.append(entry)

    def check_committed_entry(self, id_num=None):
        ''' 
            check_committed_entry: Public function to check the last entry committed.
                Optionally specify an id_num to search for. If specified, will return 
                the most recent entry with this id_num. 
            Inputs:
                id_num: returns the most recent entry with this id_num. 
        '''
        if (id_num is None):
            return self.log[self.commit_index]['entry']
        entry_list = [e for e in self.log[:self.commit_index + 1] if e['id'] == id_num]
        if entry_list:
            return entry_list[-1]['entry']
        return None

    def check_role(self):
        ''' 
            check_role: Public function to check the role of a given node.
        '''
        with self.client_lock:
            role = copy.deepcopy(self.current_role)
        return role

    def pause(self):
        '''
            pause: Allows the user to pause a node. In this state nodes are "removed" 
                from the system until un_pause is called. 
        '''
        self._set_current_role('none')
        if (self.verbose):
            print(self.address + ': pausing...')
        
    def un_pause(self):
        '''
            un_pause: Allows the user to unpause a node. If the node was not already 
                paused, does nothing.
        '''
        if (self.check_role() == 'none'):
            self._set_current_role('follower')
            if (self.verbose):
                print(self.address + ': unpausing...')
        else:
            if (self.verbose):
                print(self.address + ': node was not paused, doing nothing')

    def run(self):
        '''
            run: Called when the node starts. Facilitates state transitions. 
        '''
        # Wait for the interface to be ready
        time.sleep(self.listener.initial_backoff)

        # Your role determines your action. Every time a state chance occurs, this loop will facilitate a transition to the next state. 
        while not self._terminate:
            if self.check_role() == 'leader':
                self._leader()
            elif self.check_role() == 'follower':
                self._follower()
            elif self.check_role() == 'candidate':
                self._candidate()
            else:
                time.sleep(1)

    def _follower(self):
        ''' 
            _follower: Nodes will start here if they have been newly added to the network. The responsibilities
                of the follower nodes are as follows:
                    - Respond to election requests with a vote if the candidate is more up-to-date than they are. 
                    - Replicate entries sent by the leader. 
                    - Commit entries committed by the leader. 
                    - Promote self to candidate if the leader appears to have crashed. 
        ''' 

        # This will be when you've seen the most recent heartbeat
        most_recent_heartbeat = time.time()

        while ((not self._terminate) and (self.check_role() == 'follower')):
            incoming_message = self._get_message()
            if (incoming_message is not None):

                # Followers only handle requests
                if (incoming_message.direction == MessageDirection.Request):

                    # Incoming message is a new election candidate
                    if (incoming_message.type == MessageType.RequestVotes):

                        # If this election is for a new term, update your term
                        if (incoming_message.term > self.current_term):
                            self._increment_term(incoming_message.term)
                            
                        # If you haven't already voted and you're less up to date than the candidate, send your vote
                        if ((self.voted_for is None) and (incoming_message.last_log_index >= self.last_applied_index) and (incoming_message.last_log_term >= self.last_applied_term)):
                            self._send_vote(incoming_message.sender)
                        else: 
                            self._send_vote(incoming_message.sender, False)

                        # If there's currently a candidate running, then you shouldn't promote yourself
                        most_recent_heartbeat = time.time()

                    # Incoming message is a heartbeat, make sure you're up to date, restart the timer
                    elif (incoming_message.type == MessageType.Heartbeat):
                        if (incoming_message.term > self.current_term):
                            self._increment_term(incoming_message.term)
                        self.leader_id = incoming_message.leader_id
                        most_recent_heartbeat = time.time()
                        #print(self.address + ": commit index " + str(self.commit_index))

                        # If leader has been resolved, check for any client requests
                        if (self.leader_id):
                            client_request = self._get_client_request()
                            if (client_request is not None):
                                self._send_acknowledge(MessageType.Heartbeat, incoming_message.leader_id, incoming_message.leader_commit, False, client_request)
                        
                    # Incoming message is some data to append
                    elif (incoming_message.type == MessageType.AppendEntries):
                        
                        # Reply false if message term is less than current_term, this is an invalid entry
                        if (incoming_message.term < self.current_term):
                            self._send_acknowledge(MessageType.AppendEntries, incoming_message.leader_id, incoming_message.leader_commit, False)

                        # Reply false if log doesnt contain an entry at prev_log_index whose term matches prev_log_term
                        elif (not self._verify_entry(incoming_message.prev_log_index, incoming_message.prev_log_term)):
                            self._send_acknowledge(MessageType.AppendEntries, incoming_message.leader_id, incoming_message.leader_commit, False)

                        # Else if the previous index and term match, append the entry and reply true
                        else:
                            if (incoming_message.leader_commit > self.commit_index):
                                self._append_entry(incoming_message.entries, commit=True, prev_index=incoming_message.prev_log_index)
                            else:
                                self._append_entry(incoming_message.entries, commit=False, prev_index=incoming_message.prev_log_index)
                            self._send_acknowledge(MessageType.AppendEntries, incoming_message.leader_id, incoming_message.leader_commit, True)
                    
                    # Incoming message is a commit message
                    elif (incoming_message.type == MessageType.Committal):
                        self._commit_entry(incoming_message.prev_log_index, incoming_message.prev_log_term)

            # If you haven't heard a heartbeat in a while, promote yourself to a candidate
            if ((time.time() - most_recent_heartbeat) > (self.election_timeout)):
                self._set_current_role('candidate')
                return

        return

    def _candidate(self):
        ''' 
            _candidate: Nodes will start here if they have not heard the leader for a period of time. The 
                responsibilities of the candidate nodes are as follows:
                    - Call for a new election and await the results: 
                        + If you recieve more than half of the votes in the system, promote yourself. 
                        + If you see someone else starting an election for a term higher than your own, 
                            vote for them, update yourself, and then demote yourself. 
                        + If you see a heartbeat for a term higher than or equal to your own, update 
                            yourself, and then demote yourself. 
                        + If you have not won after the election timeout, restart the election. 
        ''' 

        if(self.verbose):
            print(self.address + ': became candidate')

        # If you're a candidate, then this is a new term
        self._increment_term()

        # Request for nodes to vote for you
        self._send_request_vote()

        # Vote for yorself
        self._send_vote(self.address, True)
        
        # Keep track of votes for and against you
        votes_for_me = 0
        total_votes = 0

        # Keep track of how long the election has been going
        time_election_going = time.time()

        while ((not self._terminate) and (self.check_role() == 'candidate')):
            incoming_message = self._get_message()
            if (incoming_message is not None):

                # Handle responses to your election
                if (incoming_message.direction == MessageDirection.Response):
                
                    # If it is a vote, then tally for or against you
                    if (incoming_message.type == MessageType.RequestVotes):
                        if (incoming_message.results.vote_granted):
                            votes_for_me += 1
                        total_votes += 1

                        #print(self.address + ": votes for me " + str(votes_for_me))
                        #print(self.address + ": total votes " + str(total_votes))
                            
                        # If you have a majority, promote yourself
                        if ((votes_for_me > int(self.current_num_nodes / 2)) or (self.current_num_nodes == 1)):
                            self._set_current_role('leader')
                            return

                # Handle outside requests
                elif (incoming_message.direction == MessageDirection.Request):

                    # If there's an election for someone else on a higher term, update your term, vote for them, and demote yourself
                    if (incoming_message.type == MessageType.RequestVotes):
                        if (incoming_message.term > self.current_term):
                            self._increment_term(incoming_message.term)
                            self._send_vote(incoming_message.sender)
                            self._set_current_role('follower')
                            return

                    # If you see a heartbeat on the current term then someone else has been elected, update your term and demote yourself
                    elif (incoming_message.type == MessageType.Heartbeat):
                        if (incoming_message.term >= self.current_term):
                            self._increment_term(incoming_message.term)
                            self.leader_id = incoming_message.leader_id
                            self._set_current_role('follower')
                            return
            
            # If this election has been going for a while we're probably deadlocked, restart the election
            if ((time.time() - time_election_going) > self.election_timeout):
                if(self.verbose):
                    print(self.address + ': election timed out')
                self._set_current_role('candidate')
                return
        
        return

    def _leader(self):
        ''' 
            _leader: Nodes will start here if they have won an election and promoted themselves. The 
                responsibilities of the leader nodes are as follows:
                    - Send a periodic heartbeat. 
                    - Keep track of who is active in the system and their status. This is done using 
                        self.next_index and self.match_index.
                    - Accept client requests and replicate them on the system. When a new request is 
                        made, send an append entries to all up-to-date nodes and away a response. When 
                        more than half of the nodes respond, commit that entry. 
                    - Catch up nodes that are behind by resending append entries after a small 
                        amount of time. 
                    - If there's a vote going on with a term term higher than your own, vote for them, 
                        update yourself, and then demote yourself. 
        ''' 
        
        if(self.verbose):
            print(self.address + ': became leader')

        # First things first, send a heartbeat
        self._send_heartbeat()

        # Keep track of when you've sent the last heartbeat
        most_recent_heartbeat = time.time()

        # You're the current leader
        self.leader_id = self.address

        # Assume all other nodes are up to date with your log
        self.match_index = [None for _ in range(self.current_num_nodes)]
        self.next_index = [None for _ in range(self.current_num_nodes)]

        # Reset heard from
        self.heard_from = [time.time() for _ in range(self.current_num_nodes)]

        # Broadcast an entry to get everyone on the same page
        entry = {'term': self.current_term, 'entry': 'Leader Entry', 'id': -1}
        self._broadcast_append_entries(entry)

        while ((not self._terminate) and (self.check_role() == 'leader')):

            # First, send a heartbeat
            if ((time.time() - most_recent_heartbeat) > self.heartbeat_frequency):
                self._send_heartbeat()
                most_recent_heartbeat = time.time()
                if (self.verbose):
                    print(self.address + ': sent heartbeat')
                    #print(self.address + ': max committed index: ' + str(self.commit_index))

            # If you haven't heard from a node in a while and it's not up to date, resend the most recent append entries
            for node, index in enumerate(self.next_index):
                if ((index is not None) and ((time.time() - self.heard_from[node]) > self.resend_time)):
                    self._send_append_entries(index - 1, self.log[index - 1]['term'], self.log[index], self.node_addresses[node])
                    self.heard_from[node] = time.time()

            # Watch for messages
            incoming_message = self._get_message()
            if (incoming_message is not None):

                # Handle incoming responses 
                if (incoming_message.direction == MessageDirection.Response):

                    # Incoming message is an append entries, update next_index and see if there's more log to send
                    if (incoming_message.type == MessageType.AppendEntries):
                        sender_index = self._get_node_index(incoming_message.sender)
                        self.heard_from[sender_index] = time.time()

                        # If you haven't already gotten a positive ack for this operation 
                        if (self.next_index[sender_index] is not None):

                            # If the append entries was successful, then increment next index to send, otherwise decrement
                            if (incoming_message.results.success):
                                self.match_index[sender_index] = self.next_index[sender_index]
                                self.next_index[sender_index] += 1
                            else:
                                self.next_index[sender_index] -= 1

                            # Are there more entries to send to bring this node up to date?
                            if self.next_index[sender_index] > self._log_max_index():
                                self.next_index[sender_index] = None
                            else:
                                next_index = self.next_index[sender_index]
                                self._send_append_entries(next_index - 1, self.log[next_index - 1]['term'], self.log[next_index], incoming_message.sender)
                            
                            if (self.verbose):
                                print(self.address + ": updated standing is " + str(self.match_index))

                            # Determine the 'committable' indices
                            log_lengths = [int(i) for i in self.match_index if (i is not None)]
                            log_lengths.sort(reverse=True)
                            max_committable_index = 0
                            for index in log_lengths:
                                # Count how many other nodes this index is replicated on
                                replicated_on = sum([1 if index <= i else 0 for i in log_lengths])
                                if (replicated_on >= (int(self.current_num_nodes / 2) + 1)):
                                    max_committable_index = index

                            # If there's a new committable index, then send the commit
                            if (max_committable_index > self.commit_index):
                                    self._broadcast_commmit_entries(max_committable_index)

                    # If its a heartbeat ack then it's a new request
                    elif (incoming_message.type == MessageType.Heartbeat):
                        client_request = incoming_message.entries
                        self._broadcast_append_entries(client_request)

                # Handle incoming requests
                elif (incoming_message.direction == MessageDirection.Request):

                    # Incoming message is a request votes, if there's a vote going on with a term above yours, update your term, vote for them, and demote yourself
                    if (incoming_message.type == MessageType.RequestVotes):
                        if (incoming_message.term > self.current_term):
                            self._increment_term(incoming_message.term)
                            self._send_vote(incoming_message.sender)
                            self._set_current_role('follower')

                            if(self.verbose):
                                print(self.address + ': saw higher term, demoting')
                            return
            
            # Get any pending client requests
            client_request = self._get_client_request()
            if (client_request is not None):
                self._broadcast_append_entries(client_request)

        return

    def _send_message(self, message):
        '''
            _send_message: A wrapper to send a message.
            Inputs:
                message: (RequestVotesMessage or AppendEntriesMessage)
        '''
        self.talker.send_message(message.jsonify())

    def _get_message(self):
        '''
            _get_message: A wrapper to get a message. If there are no pending 
                messages returns None. 
        '''
        return parse_json_message(self.listener.get_message())

    def _load_config(self, config_file, name):
        with open(config_file, 'r') as infile:
            data = json.load(infile)
        return data

    def _get_client_request(self):
        '''
            _get_node_index: Pops the most recent client request. 
        '''
        with self.client_lock:
            if self.client_queue:
                value = self.client_queue.popleft()
            else:
                value = None
        return value
    
    def _set_current_role(self, role):
        '''
            _set_current_role: Set the node role. Options are ['follower', 
            'leader', 'candidate']
            Inputs: 
                role: (str)
                    Node's new role
        '''
        with self.client_lock:
            self.current_role = role

    def _get_node_index(self, node_address):
        '''
            _get_node_index: Retuns the index of a specific node address,
                e.g. for
                    self.node_addresses = ['5556', '5558']
                    _get_node_index('5556') <- returns 0
            Inputs: 
                node_address: (str)
                    The query address
        '''
        return self.node_addresses.index(node_address)

    def _log_max_index(self):
        '''
            _log_max_index: Returns the max index of the log. Used for 
                convenience. 
        '''
        return len(self.log) - 1

    def _increment_term(self, term=None):
        '''
            _increment_term: Should be called whenever changing terms. 
                Does the following: clears the voted for status, increments
                the term. Useful for ensuring that both of these are done
                whenever the term is changed
            Input: 
                term: (int or None)
                    If term is none, will increment by one. Else will change
                    the term to whatever was input.
        '''
        self.voted_for = None
        if (term is None):
            self.current_term = self.current_term + 1
        else:
            self.current_term = term

    def _verify_entry(self, prev_index, prev_term):
        '''
            _verify_entry: Should be called whenever checking if an entry can 
                be appended to the log. Checks that the log has enough entries
                that the target index will not cause an error. Then checks if
                the target index has the same term as the target term. If it 
                does then this is a valid entry. 
            Input: 
                prev_index: (int)
                    Index to check.
                prev_term: (int)
                    Term to check.
        '''
        
        if (len(self.log) <= prev_index):
            return False
        if (self.log[prev_index]['term'] == prev_term):
            return True
        return False

    def _append_entry(self, entry, commit=False, prev_index=None):
        '''
            _append_entry: Appends entries to the log and optionally commits. 
                Assumes that the entry has already been verified (see 
                _verify_entry).
            Inputs:
                entry: (dict with the attributes 'term' and 'entry') 
                    Stores  whatever information to append to the log. 
                commit: (bool) 
                    If True, will update the last applied index and term after
                    writing to the log. If false will do nothing. 
                prev_index: (int) 
                    If None, will append the entry to the end of the log. Else 
                    will append the entry to the index specified.
        '''

        # If prev_term was specified, might have to cut the log short
        if (prev_index is None):
            prev_index = len(self.log) - 1
        else:
            self.log = self.log[:prev_index+1]
        
        # Add this to the log
        prev_term = self.log[-1]['term']
        self.log.append(entry)

        # Maybe commit
        if (commit):
            self._commit_entry(prev_index, entry['term'])

        return prev_index, prev_term

    def _commit_entry(self, index, term):
        '''
            _commit_entry: Update internal varables to reflect a commit at the 
                given index and term. Specifically the below variables should be
                updated whenever a commmit is made. 
            Inputs:
                index: (int) 
                    Index to commit.
                term: (int) 
                    Term corresponding to this commit.
        '''
        self.last_applied_index = index
        self.last_applied_term = term
        self.commit_index = index

    def _broadcast_append_entries(self, entry):
        '''
            _broadcast_append_entries: Should be called only by the leader. 
                Sends an append entries message to all nodes for the given entry.
            Inputs:
                entry: (dict with the attributes 'term' and 'entry') 
                    Entry to append to all nodes.    
        '''
        # Append the new entry 
        prev_index, prev_term = self._append_entry(entry, commit=False)

        # Send out other append entries
        for node, index in enumerate(self.next_index):
            # Only send out append entries to nodes that are up-to-date. Also they will now be out of date so update next. 
            if (index is None):
                self._send_append_entries(prev_index, prev_term, entry, self.node_addresses[node])
                self.next_index[node] = self._log_max_index()

        # Update your own information
        self.next_index[self._get_node_index(self.address)] =  None
        self.match_index[self._get_node_index(self.address)] = self._log_max_index()

    def _broadcast_commmit_entries(self, index):
        '''
            _broadcast_commmit_entries: Should be called only by the leader. 
                Sends a commit message to all nodes for the given index.
            Inputs:
                index: (int) 
                    Index to commit.          
        '''
        # Commit yourself
        self._commit_entry(index, self.log[index]['term'])

        # Commit everybody else
        for node, index in enumerate(self.match_index):
            if (index >= index):
                self._send_committal(index, self.node_addresses[node])

    def _send_request_vote(self, receiver=None):
        message = RequestVotesMessage(
            type_ =   MessageType.RequestVotes, 
            term =   self.current_term, 
            sender = self.address,
            receiver = receiver, 
            direction = MessageDirection.Request, 
            candidate_id = self.address, 
            last_log_index = self.last_applied_index,
            last_log_term = self.last_applied_term
        )
        self._send_message(message)

    def _send_vote(self, candidate, vote_granted=True):
        message = RequestVotesMessage(
            type_ =   MessageType.RequestVotes, 
            term =   self.current_term,
            sender = self.address,
            receiver = candidate, 
            direction = MessageDirection.Response, 
            candidate_id = candidate, 
            last_log_index = self.last_applied_index,
            last_log_term = self.last_applied_term,
            results = RequestVotesResults(
                term = self.current_term,
                vote_granted = vote_granted
            )
        )
        self._send_message(message)
        self.voted_for = candidate

    def _send_heartbeat(self):
        message = AppendEntriesMessage(
            type_ = MessageType.Heartbeat,
            term = self.current_term,
            sender = self.address,
            receiver = None,
            direction = MessageDirection.Request,
            leader_id = self.address,
            prev_log_index = None,
            prev_log_term = None,
            entries = None,
            leader_commit = self.commit_index
        )
        self._send_message(message)

    def _send_append_entries(self, index, term, entries, receiver=None):
        message = AppendEntriesMessage(
            type_ = MessageType.AppendEntries,
            term = self.current_term,
            sender = self.address,
            receiver = receiver,
            direction = MessageDirection.Request,
            leader_id = self.address,
            prev_log_index = index,
            prev_log_term = term,
            entries = entries,
            leader_commit = self.commit_index
        )
        self._send_message(message)
    
    def _send_committal(self, index, receiver=None):
        message = AppendEntriesMessage(
            type_ = MessageType.Committal,
            term = self.current_term,
            sender = self.address,
            receiver = receiver,
            direction = MessageDirection.Request,
            leader_id = self.address,
            prev_log_index = self.last_applied_index,
            prev_log_term = self.last_applied_term,
            entries = None,
            leader_commit = self.commit_index
        )
        self._send_message(message)

    def _send_acknowledge(self, reason, receiver, leader_commit, success, entry=None):
        message = AppendEntriesMessage(
            type_ = reason,
            term = self.current_term,
            sender = self.address,
            receiver = receiver,
            direction = MessageDirection.Response,
            leader_id =self.leader_id ,
            prev_log_index = self.last_applied_index,
            prev_log_term = self.last_applied_term,
            entries = entry,
            leader_commit = leader_commit, 
            results = AppendEntriesResults(
                term = self.current_term,
                success = success
            ) 
        )
        self._send_message(message)

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
        s.append(RaftNode(address_book_fname, name, 'follower'))
        node_num = node_num + 1
    for n in s:
        n.start()

    time.sleep(2)

    # Make some requests
    for i in range(10):
        s[0].client_request(i)
    
    time.sleep(2)

    # Kill half of them, including the leader
    l = [n for n in s if (n.check_role() == 'leader')][0]
    l.stop()
    num_to_kill = int(total_nodes / 2) - 1
    for n in s:
        num_to_kill = num_to_kill - 1
        if num_to_kill == 0:
            break
        else:
            n.stop()

    # Wait for a bit
    time.sleep(10)

    # Kill the rest of them
    for n in s:
        n.stop() 

if __name__ == '__main__':
    test_failures()