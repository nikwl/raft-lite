#!/usr/bin/env python

from Raft import RaftNode
import json
import time

address_book_fname = 'address_book.json'

if __name__ == '__main__':
    d = {"node0": {"ip": "127.0.0.1", "port": "5567"}, 
         "node1": {"ip": "127.0.0.1", "port": "5566"}, 
         "node2": {"ip": "127.0.0.1", "port": "5565"},
         "node3": {"ip": "127.0.0.1", "port": "5564"}}
        
    with open(address_book_fname, 'w') as outfile:
        json.dump(d, outfile)

    s0 = RaftNode(address_book_fname, 'node0', 'follower')
    s1 = RaftNode(address_book_fname, 'node1', 'follower')

    s0.start()
    s1.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        s0.stop()
        s1.stop()