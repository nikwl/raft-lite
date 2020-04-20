import time

class MessageType():
	RequestVotes = 0
	RequestVoteResponse = 1
	Heartbeat = 2
	Acknowledge = 3
	AppendEntries = 4
	Committal = 5

class MessageDirection():
	Request = 0
	Response = 1	

class RequestVotesResults(): 
	def __init__(self, term=None, vote_granted=None, message=None):
		if (message is not None):
			self.un_jsonify(message)
		else:
			self.term = term
			self.vote_granted = vote_granted

	@property
	def term(self):
		return self.term

	@property
	def vote_granted(self):
		return self.vote_granted

	def un_jsonify(self, message):
		self.term = 			message['term']     
		self.vote_granted =	message['vote_granted']     

	def jsonify(self):
		return {
			'term':      		self.term,
			'vote_granted': 	self.vote_granted
		}

class AppendEntriesResults():
	def __init__(self, term=None, success=None, message=None):
		if (message is not None):
			self.un_jsonify(message)
		else:
			self.term = term
			self._success = success

	@property
	def term(self):
		return self.term

	@property
	def success(self):
		return self._success

	def un_jsonify(self, message):
		self.term = 	message['term']     
		self._success =	message['success']     

	def jsonify(self):
		return {
			'term':      	self.term,
			'success':      self._success
		}

class BaseMessage():

	def __init__(self, type_, term, sender, receiver, direction, results):
		self.timestamp = int(time.time())
		self.type_ = type_
		self.term = term				
		self.sender = sender
		self.receiver = receiver
		self.direction = direction
		self.results = results

	@property
	def timestamp(self):
		return self.timestamp

	@property
	def type(self):
		return self.type_

	@property
	def term(self):
		return self.term

	@property
	def sender(self):
		return self.sender

	@property
	def receiver(self):
		return self.receiver

	@property
	def direction(self):
		return self.direction

	@property
	def results(self):
		return self.results

	def un_jsonify(self, message):
		self.type_ = 			message['type']     
		self.term = 			message['term']     
		self.timestamp = 		message['timestamp']
		self.sender = 			message['sender']
		self.receiver = 		message['receiver'] 
		self.direction = 		message['direction']
		if (self.type_ == MessageType.RequestVotes):
			self.results = RequestVotesResults(message=message['results'])
		else:
			self.results = AppendEntriesResults(message=message['results'])

	def jsonify(self):
		return {
			'type':      	self.type_,
			'term':      	self.term,
			'timestamp': 	self.timestamp,
			'sender':    	self.sender,
			'receiver':  	self.receiver,
			'direction': 	self.direction,
			'results': 		self.results.jsonify()
		}

class RequestVotesMessage(BaseMessage):
	def __init__(self, type_=None, term=None, sender=None, receiver=None, direction=None, results=None, candidate_id=None, last_log_index=None, last_log_term=None, message=None):
		if (message is not None):
			self.un_jsonify(message)
		else:
			if (results is None):
				results = RequestVotesResults()
			BaseMessage.__init__(self, type_, term, sender, receiver, direction, results)
			self.candidate_id = candidate_id
			self.last_log_index = last_log_index
			self.last_log_term = last_log_term
		
	@property
	def candidate_id(self):
		return self.candidate_id

	@property
	def last_log_index(self):
		return self.last_log_index

	@property
	def last_log_term(self):
		return self.last_log_term

	def un_jsonify(self, message):
		BaseMessage.un_jsonify(self, message)
		self.candidate_id = 	message['candidate_id']
		self.last_log_index = 	message['last_log_index']
		self.last_log_term = 	message['last_log_term']

	def jsonify(self):
		message = BaseMessage.jsonify(self)
		message.update({
			'candidate_id': 	self.candidate_id,
			'last_log_index': 	self.last_log_index,
			'last_log_term': 	self.last_log_term
		})
		return message

class AppendEntriesMessage(BaseMessage):
	def __init__(self, type_=None, term=None, sender=None, receiver=None, direction=None, results=None, leader_id=None, prev_log_index=None, prev_log_term=None, entries=None, leader_commit=None, message=None):
		if (message is not None):
			self.un_jsonify(message)
		else:
			if (results is None):
				results = AppendEntriesResults()
			BaseMessage.__init__(self, type_, term, sender, receiver, direction, results)
			self.leader_id = leader_id
			self.prev_log_index = prev_log_index
			self.prev_log_term = prev_log_term
			self.entries = entries
			self.leader_commit = leader_commit

	@property
	def leader_id(self):
		return self.leader_id

	@property
	def prev_log_index(self):
		return self.prev_log_index

	@property
	def prev_log_term(self):
		return self.prev_log_term

	@property
	def entries(self):
		return self.entries

	@property
	def leader_commit(self):
		return self.leader_commit

	def un_jsonify(self, message):
		BaseMessage.un_jsonify(self, message)
		self.leader_id = 		message['leader_id']
		self.prev_log_index = 	message['prev_log_index']
		self.prev_log_term = 	message['prev_log_term']
		self.entries = 			message['entries']
		self.leader_commit = 	message['leader_commit']

	def jsonify(self):
		message = BaseMessage.jsonify(self)
		message.update({
			'leader_id': 		self.leader_id,
			'prev_log_index': 	self.prev_log_index,
			'prev_log_term': 	self.prev_log_term,
			'entries': 			self.entries,
			'leader_commit': 	self.leader_commit
		})
		return message

def parse_json_message(json_message):
	if json_message is None:
		return None
	elif json_message['type'] == MessageType.RequestVotes:
		return RequestVotesMessage(message=json_message)
	else:
		return AppendEntriesMessage(message=json_message)
