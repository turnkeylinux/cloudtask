#!/usr/bin/python
from multiprocessing import Semaphore
from multiprocessing.queues import Queue

class SemQueue(Queue):
    """Queue that uses a semaphore to reliably count items in it"""
    def __init__(self, maxsize=0):
        self.sem = Semaphore(0)
        Queue.__init__(self, maxsize)

    def put(self, obj, block=True, timeout=None):
        Queue.put(self, obj, block, timeout)
        self.sem.release()

    def get(self, block=True, timeout=None):
        ret = Queue.get(self, block, timeout)
        self.sem.acquire()
        return ret

    def qsize(self):
        return self.sem.get_value()

def test():
    q = SemQueue()
    for i in range(3):
        q.put(i)

    return q
