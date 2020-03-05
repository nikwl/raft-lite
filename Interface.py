import zmq
import multiprocessing
import Queue
import time

class Talker(multiprocessing.Process):
	def __init__(self, port):
		super(Talker, self).__init__()

		# Port to talk from
		self.port = port

		# Place to store outgoing messages
		self.messages = multiprocessing.Queue()

		# Backoff for this amount of time after creating the sockets
		self.initial_backoff = 1.0

		# How you kill the thread
		self._stop_event = multiprocessing.Event()

	def stop(self):
		self._stop_event.set()

	def run(self):
		# All of the zmq initialization has to be in the same function for some reason
		context = zmq.Context()
		socket = context.socket(zmq.PUB)
		socket.bind("tcp://127.0.0.1:%s" % self.port)

		# Need to backoff to give the connections time to initizalize
		time.sleep(self.initial_backoff)

		while not self._stop_event.is_set():
			# If there's nothing in the queue Queue.Empty will be thrown 
			try:
				socket.send_json(self.messages.get_nowait())
			except Queue.Empty:
				pass

	def send_message(self, msg):
		self.messages.put(msg)

class Listener(multiprocessing.Process):
	def __init__(self, port_list):
		super(Listener, self).__init__()

		# List of ports to subscribe to
		self.port_list = port_list

		# Place to store incoming messages
		self.messages = multiprocessing.Queue()

		# Backoff for this amount of time after creating the sockets
		self.initial_backoff = 1.0

		# How you kill the thread
		self._stop_event = multiprocessing.Event()

	def stop(self):
		self._stop_event.set()

	def run(self):
		# All of the zmq initialization has to be in the same function for some reason
		context = zmq.Context()
		sub_sock = context.socket(zmq.SUB)
		for p in self.port_list:
			sub_sock.connect ("tcp://127.0.0.1:%s" % p)
		sub_sock.setsockopt(zmq.SUBSCRIBE, '')

		# Need to backoff to give the connections time to initizalize
		time.sleep(self.initial_backoff)
		
		while not self._stop_event.is_set():
			self.messages.put(sub_sock.recv_json())
	
	def get_message(self):
		# If there's nothing in the queue Queue.Empty will be thrown
		try:
			msg = self.messages.get_nowait()
		except Queue.Empty:
			return None
		return msg