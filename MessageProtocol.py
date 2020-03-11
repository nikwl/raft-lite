import time

class BaseMessage():
	Heartbeat = 0
	Acknowledge = 1
	AppendEntries = 2
	RequestVotes = 3
	RequestVoteResponse = 4
	ConnectionRequest = 5
	ConnectionResponse = 6

	def __init__(self, type_, term_, sender_, receiver_, data_):
		self._timestamp = int(time.time())
		self._type = type_
		self._term = term_
		self._sender = sender_
		self._receiver = receiver_
		self._data = data_

	@property
	def type(self):
		return self._type

	@property
	def term(self):
		return self._term

	@property
	def timestamp(self):
		return self._timestamp

	@property
	def sender(self):
		return self._sender

	@property
	def receiver(self):
		return self._receiver

	@property
	def data(self):
		return self._data

	def jsonify(self):
		return {
			'type':      self._type,
			'term':      self._term,
			'timestamp': self._timestamp,
			'sender':    self._sender,
			'receiver':  self._receiver,
			'data':      self._data
		}

class RequestVotesMessage(BaseMessage):
	def __init__(self, type_, term_, sender_=None, receiver_=None, data_=None):
		BaseMessage.__init__(self, type_, term_, sender_, receiver_, data_)

class AppendEntriesMessage(BaseMessage):
	def __init__(self, type_, term_, sender_=None, receiver_=None, data_=None):
		BaseMessage.__init__(self, type_, term_, sender_, receiver_, data_)

def parse_json_message(json_message):
	if json_message is None:
		return None
	elif json_message['type'] == BaseMessage.RequestVotes:
		return RequestVotesMessage(json_message['type'], json_message['term'], json_message['sender'], json_message['receiver'], json_message['data'])
	else:
		return AppendEntriesMessage(json_message['type'], json_message['term'], json_message['sender'], json_message['receiver'], json_message['data'])
