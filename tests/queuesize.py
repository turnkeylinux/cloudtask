#!/usr/bin/python
from multiprocessing import Process, Semaphore, Condition
from multiprocessing.queues import Queue

import time

class BetterQueue(Queue):
    """Queue that uses a semaphore to reliably count items in it"""
    def __init__(self, maxsize=0):
        self.sem_items = Semaphore(0)
        self.cond_empty = Condition()

        Queue.__init__(self, maxsize)

    def __getstate__(self):
        return Queue.__getstate__(self) + (self.sem_items, self.cond_empty)

    def __setstate__(self, state):
        Queue.__setstate__(self, state[:-2])
        self.sem_items, self.cond_empty = state[-2:]

    def put(self, obj, block=True, timeout=None):
        Queue.put(self, obj, block, timeout)
        self.sem_items.release()

    def get(self, block=True, timeout=None):
        ret = Queue.get(self, block, timeout)
        self.sem_items.acquire()
        if self.sem_items.get_value() == 0:
            self.cond_empty.acquire()
            try:
                self.cond_empty.notify_all()
            finally:
                self.cond_empty.release()

        return ret

    def qsize(self):
        return self.sem_items.get_value()

    def wait(self):
        """Wait for all items to be read"""
        self.cond_empty.acquire()
        try:
            if self.qsize():
                self.cond_empty.wait()
        finally:
            self.cond_empty.release()

def worker(input):
    for sleep in iter(input.get, 'STOP'):
        print "sleeping for %d seconds" % sleep
        time.sleep(sleep)

def test():
    bq = BetterQueue()
    for i in range(3):
        bq.put(3)

    p = Process(target=worker, args=(bq,))
    p.start()

    return p, bq
