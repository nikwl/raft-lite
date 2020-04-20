import zmq
import multiprocessing
import Queue
import time

from MessageProtocol import BaseMessage

class Talker(multiprocessing.Process):
	def __init__(self, port):
		super(Talker, self).__init__()

		# Port to talk from
		self.port = port

		# Backoff for this amount of time after creating the sockets
		self.initial_backoff = 1.0

		# Place to store outgoing messages
		self.messages = multiprocessing.Queue()

		# When the thread is ready
		self._ready_event = multiprocessing.Event()

		# How you kill the thread
		self._stop_event = multiprocessing.Event()

	def stop(self):
		self._stop_event.set()

	def run(self):
		# All of the zmq initialization has to be in the same function for some reason
		context = zmq.Context()
		pub_socket = context.socket(zmq.PUB)
		while True:
			try:
				pub_socket.bind("tcp://127.0.0.1:%s" % self.port)
				break
			except zmq.ZMQError:
				time.sleep(0.1)

		# Need to backoff to give the connections time to initizalize
		time.sleep(self.initial_backoff)

		# Signal that you're ready
		self._ready_event.set()

		while not self._stop_event.is_set():
			# If there's nothing in the queue Queue.Empty will be thrown 
			try:
				pub_socket.send_json(self.messages.get_nowait())
			except Queue.Empty:
				pass
			time.sleep(0.01)
		
		pub_socket.unbind("tcp://127.0.0.1:%s" % self.port)
		pub_socket.close()

	def send_message(self, msg):
		self.messages.put(msg)
	
	def wait_until_ready(self):
		while not self._ready_event.is_set():
			time.sleep(0.1)
		return True

class Listener(multiprocessing.Process):
	def __init__(self, port_list, identity):
		super(Listener, self).__init__()

		# List of ports to subscribe to
		self.port_list = port_list
		self.identity = identity

		# Backoff for this amount of time after creating the sockets
		self.initial_backoff = 1.0

		# Place to store incoming messages
		self.messages = multiprocessing.Queue()
		self.leader_messages = multiprocessing.Queue()

		# How you kill the thread
		self._stop_event = multiprocessing.Event()

	def stop(self):
		self._stop_event.set()

	def run(self):
		# All of the zmq initialization has to be in the same function for some reason
		context = zmq.Context()
		sub_sock = context.socket(zmq.SUB)
		sub_sock.setsockopt(zmq.SUBSCRIBE, '')
		for p in [self.port_list[n]['port'] for n in self.port_list]:
			sub_sock.connect("tcp://127.0.0.1:%s" % p)
		
		# Get the next availible port number
		next_port = max([int(self.port_list[n]['port']) for n in self.port_list]) + 1

		# Need to backoff to give the connections time to initizalize
		time.sleep(self.initial_backoff)

		while not self._stop_event.is_set():
			try:
				msg = sub_sock.recv_json(zmq.NOBLOCK)

				# Check if this message is a connection request. If it is it can only be read by the leader, put it in the leader queue
				if (msg['type'] == BaseMessage.ConnectionRequest):
					#print(self.identity['address'] + ' got a new connection request')
					# Update the message with the next port number
					msg['data']['new_address'] = str(next_port)
					
					# Increment the port tracker
					next_port = next_port + 1

					# Put in the leader queue
					self.leader_messages.put(msg)
					continue

				# Check if this message is a connection response. If it is then it was sent by the leader, and we should add this address to the subscribe list
				elif (msg['type'] == BaseMessage.ConnectionResponse):
					p = str(msg['data']['new_address'])
					sub_sock.connect("tcp://127.0.0.1:%s" % p)

					# If it was to us, we need to update our identity 
					if ((msg['data']['node_name'] == self.identity['node_name']) and (msg['receiver'] == self.identity['address'])):
						self.identity['address'] = msg['data']['new_address']
					
				# Check if this message is a heartbeat from someone else. If it is then they are the leader so empty the leader queue
				if ((msg['type'] == BaseMessage.Heartbeat) and (msg['sender'] != self.identity['address'])):
					try:
						while True:
							self.leader_messages.get_nowait()
					except Queue.Empty:
						pass

				self.messages.put(msg)

			except zmq.Again:
				pass

			time.sleep(0.01)
		
		sub_sock.close()
	
	def get_message(self):
		# If there's nothing in the queue Queue.Empty will be thrown
		try:
			msg = self.messages.get_nowait()
		except Queue.Empty:
			return None
		return msg

	def get_leader_message(self):
		# If there's nothing in the queue Queue.Empty will be thrown
		try:
			msg = self.leader_messages.get_nowait()
		except Queue.Empty:
			return None
		return msg