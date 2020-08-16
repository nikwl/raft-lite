"""Example using zmq with asyncio coroutines"""
# Copyright (c) PyZMQ Developers.
# This example is in the public domain (CC-0)

import time

import zmq
from zmq.asyncio import Context, Poller
import asyncio

url = 'tcp://127.0.0.1:5555'
addr = '127.0.0.1:5555'

class Talker():
	def __init__(self, identity):
		# Port to talk from
		self.address = identity['my_id']

		# Backoff amounts
		self.initial_backoff = 1.0
		self.operation_backoff = 0.0001

		# Place to store outgoing messages
		self.messages = asyncio.Queue()

		self._stop_event = False

	def stop(self):
		self._stop_event = True

	async def run(self):
		pub_socket = Context.instance().socket(zmq.PUB)
		while not self._stop_event:
			try:
				pub_socket.bind("tcp://%s" % self.address)
				break
			except zmq.ZMQError:
				time.sleep(0.1)

		# Need to backoff to give the connections time to initizalize
		time.sleep(self.initial_backoff)

		while not self._stop_event:
			try:
				print(self.messages.empty())
				m = await self.messages.get()
				print(m)
				await pub_socket.send_json(m)
			except asyncio.QueueEmpty:
				await asyncio.sleep(self.operation_backoff)
			except KeyboardInterrupt:
				break
		
		pub_socket.unbind("tcp://%s" % self.address)
		pub_socket.close()

	async def send_message(self, msg):
		await self.messages.put(msg)

class Listener():
	def __init__(self, port_list, identity):
		# List of ports to subscribe to
		self.address_list = port_list
		self.identity = identity

		# Backoff amounts
		self.initial_backoff = 1.0
		self.operation_backoff = 0.0001

		# Place to store incoming messages
		self.messages = asyncio.Queue()

		self._stop_event = False

	def stop(self):
		self._stop_event = True

	async def run(self):
		sub_sock = Context.instance().socket(zmq.SUB)
		sub_sock.setsockopt(zmq.SUBSCRIBE, b'')
		for a in self.address_list:
			sub_sock.connect("tcp://%s" % a)

		# Poller lets you specify a timeout
		poller = Poller()
		poller.register(sub_sock, zmq.POLLIN)

		# Need to backoff to give the connections time to initizalize
		time.sleep(self.initial_backoff)

		while not self._stop_event:
			try:
				obj = dict(poller.poll(100))
				if sub_sock in obj and obj[sub_sock] == zmq.POLLIN:
					msg = sub_sock.recv_json()	
					if ((msg['receiver'] == self.identity['my_id']) or (msg['receiver'] is None)):
						await self.messages.put(msg)
				await asyncio.sleep(self.operation_backoff)
			except KeyboardInterrupt:
				break
		
		sub_sock.close()
	
	async def get_message(self):
		# If there's nothing in the queue Queue.Empty will be thrown
		try:
			await self.messages.get_nowait()
		except asyncio.QueueEmpty:
			return None

async def ping():
    """print dots to indicate idleness"""
    while True:
        await asyncio.sleep(0.5)
        print('.')

async def receiver():
    """receive messages with polling"""
    pull = Context.instance().socket(zmq.PULL)
    pull.connect(url)
    poller = Poller()
    poller.register(pull, zmq.POLLIN)
    while True:
        events = await poller.poll()
        if pull in dict(events):
            print("recving", events)
            msg = await pull.recv_multipart()
            print('recvd', msg)


async def sender():
    """send a message every second"""
    tic = time.time()
    push = Context.instance().socket(zmq.PUSH)
    push.bind(url)
    while True:
        print("sending")
        await push.send_multipart([str(time.time() - tic).encode('ascii')])
        await asyncio.sleep(1)

async def driver(t, l):
	i = 0
	while True:
		await t.send_message('[{}] says hi!'.format(i))
		m = await l.get_message()
		print(m)
		await asyncio.sleep(1)

def main():
	id_ = {'my_id': addr}
	t = Talker(id_)
	l = Listener([addr], id_)
	asyncio.get_event_loop().run_until_complete(asyncio.wait([
		driver(t, l),
		t.run(),
		l.run(),
	]))


main()