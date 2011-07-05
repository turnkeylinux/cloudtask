#!/usr/bin/python

from multiprocessing import Queue
from multiprocessing.queues import Empty

q = Queue()
for i in range(10):
    q.put(i)

def qgetall(q):
    vals = []
    while True:
        try:
            val = q.get(False)
            vals.append(val)
        except Empty:
            break

    return vals
