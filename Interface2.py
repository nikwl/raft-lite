import zmq
import threading
from threading import Lock
from collections import deque
import time

from MessageProtocol import BaseMessage

class Talker(threading.Thread):
	def __init__(self, port):
		threading.Thread.__init__(self) 
		self.daemon = True

		# Port to talk from
		self.port = port

		# Backoff for this amount of time after creating the sockets
		self.initial_backoff = 1.0

		# Place to store outgoing messages
		self.messages = deque([])
		self.messages_lock = Lock()

		# When the thread is ready
		self.ready = False
		self.ready_lock = Lock()

		# How you kill the thread
		self.stop = False
		self.stop_lock = Lock()

	def stop(self):
		with self.stop_lock:
			self.stop = True

	def run(self):
		# All of the zmq initialization has to be in the same function for some reason
		context = zmq.Context()
		socket = context.socket(zmq.PUB)
		try:
			socket.bind("tcp://127.0.0.1:%s" % self.port)
		except zmq.ZMQError:
			time.sleep(0.1)

		# Need to backoff to give the connections time to initizalize
		time.sleep(self.initial_backoff)

		# Signal that you're ready
		with self.ready_lock:
			self.ready = True

		while True:
			with self.messages_lock:
				try:
					socket.send_json(self.messages.popleft())
				except IndexError:
					pass
			with self.stop_lock:
				if (self.stop_lock):
					break

	def send_message(self, msg):
		with self.messages_lock:
			self.messages.append(msg)
	
	def wait_until_ready(self):
		while True:
			with self.ready_lock:
				if (self.ready):
					break
			time.sleep(0.1)
		return True

class Listener(threading.Thread):
	def __init__(self, port_list, identity):
		threading.Thread.__init__(self) 
		self.daemon = True

		# List of ports to subscribe to
		self.port_list = port_list
		self.identity = identity

		# Backoff for this amount of time after creating the sockets
		self.initial_backoff = 1.0

		# Place to store incoming messages
		self.messages = deque([])
		self.messages_lock = Lock()
		self.leader_messages = deque([])
		self.leader_messages_lock = Lock()

		# How you kill the thread
		self.stop = False
		self.stop_lock = Lock()

	def stop(self):
		with self.stop_lock:
			self.stop = True

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
		
		while True:
			try:
				msg = sub_sock.recv_json(zmq.NOBLOCK)

				# Check if this message is a connection request. If it is it can only be read by the leader, put it in the leader queue
				if (msg['type'] == BaseMessage.ConnectionRequest):
					print(self.identity['address'] + ' got a new connection request')
					# Update the message with the next port number
					msg['data']['new_address'] = str(next_port)
					
					# Increment the port tracker
					next_port = next_port + 1

					# Put in the leader queue
					with self.leader_messages_lock:
						self.leader_messages.append(msg)
					continue
				
				# Check if this message is a connection response. If it is then it was sent by the leader, and we should add this address to the subscribe list
				elif (msg['type'] == BaseMessage.ConnectionResponse):
					p = str(msg['data']['new_address'])
					sub_sock.connect("tcp://127.0.0.1:%s" % p)
					print('added node ' + p + ' to socket')
					
					# If it was to us, we need to update our identity 
					if ((msg['data']['node_name'] == self.identity['node_name']) and (msg['receiver'] == self.identity['address'])):
						self.identity['address'] = msg['data']['new_address']
					
				# Check if this message is a heartbeat from someone else. If it is then they are the leader so empty the leader queue
				if ((msg['type'] == BaseMessage.Heartbeat) and (msg['sender'] != self.identity['address'])):
					self.leader_messages.clear()
				
				with self.messages_lock:
					self.messages.append(msg)
		
			except zmq.Again:
				pass

			with self.stop_lock:
				if (self.stop):
					break
	
	def get_message(self):
		# If there's nothing in the queue Queue.Empty will be thrown
		with self.messages_lock:
			try:
				msg = self.messages.popleft()
			except IndexError:
				return None
		return msg

	def get_leader_message(self):
		# If there's nothing in the queue Queue.Empty will be thrown
		with self.leader_messages_lock:
			try:
				msg = self.leader_messages.popleft()
			except IndexError:
				return None
		return msg