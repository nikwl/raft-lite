#!/usr/bin/env python

from Sentinel import Sentinel
import json
import time

address_book_fname = 'address_book.json'

if __name__ == '__main__':
    d = {"node0": {"ip": "localhost", "port": "5567"}, 
         "node1": {"ip": "localhost", "port": "5566"}, 
         "node2": {"ip": "localhost", "port": "5565"},
         "node3": {"ip": "localhost", "port": "5564"}}
        
    with open(address_book_fname, 'w') as outfile:
        json.dump(d, outfile)

    s0 = Sentinel(address_book_fname, 'node0', 'follower')
    s1 = Sentinel(address_book_fname, 'node1', 'follower')

    s0.start()
    s1.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        s0.stop()
        s1.stop()