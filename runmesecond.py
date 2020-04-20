#!/usr/bin/env python

from Sentinel import Sentinel
import time

address_book_fname = 'address_book.json'

if __name__ == '__main__':
    s2 = Sentinel(address_book_fname, 'node2', 'follower')
    s3 = Sentinel(address_book_fname, 'node3', 'follower')

    s2.start()
    s3.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        s2.stop()
        s3.stop()