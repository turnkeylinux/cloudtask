#!/usr/bin/python

import os
import signal
from multiprocessing import Process, Event, Semaphore

from multiprocessing import Semaphore, Condition
from multiprocessing.queues import Queue

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

    def wait(self, timeout=None):
        """Wait for all items to be got"""
        self.cond_empty.acquire()
        try:
            if self.qsize():
                self.cond_empty.wait(timeout)
        finally:
            self.cond_empty.release()

class Worker(Process):
    class Terminated(Exception):
        pass

    @classmethod
    def worker(cls, idle, q_input, q_output, func):
        def raise_exception(s, f):
            signal.signal(s, signal.SIG_IGN)
            raise cls.Terminated

        signal.signal(signal.SIGTERM, raise_exception)
        signal.signal(signal.SIGINT, raise_exception)

        class UNDEFINED:
            pass

        try:
            while True:

                retval = UNDEFINED
                input = q_input.get()

                try:

                    if not isinstance(input, tuple):
                        args = (input,)
                    else:
                        args = input

                    idle.clear()
                    try:
                        retval = func(*args)
                        q_output.put(retval)
                    finally:
                        idle.set()

                except:
                    if retval is UNDEFINED:
                        q_input.put(input)
                    raise

        except cls.Terminated:
            pass # just exit peacefully

    def __init__(self, q_input, q_output, func):
        self.idle = Event()
        self.idle.set()

        Process.__init__(self, 
                         target=self.worker, 
                         args=(self.idle, q_input, q_output, func))

    def is_busy(self):
        return not self.idle.is_set()

    def wait(self, timeout=None):
        """wait until Worker is idle"""
        return self.idle.wait(timeout)

    def stop(self):
        """signal worker to stop working"""
        if not self.is_alive():
            return

        os.kill(self.pid, signal.SIGTERM)

class WorkerPool:
    def __init__(self, size, func):
        input = BetterQueue()
        output = Queue()

        self.workers = []

        for i in range(size):
            worker = Worker(input, output, func)
            worker.start()

            self.workers.append(worker)

        self.input = input
        self.output = output

    def wait(self):
        """wait for all input to be processed"""
        while True:
            self.input.wait()
            for worker in self.workers:
                if worker.is_busy():
                    worker.wait()
                    continue # worker may put stuff into the input

            # only reached when there was no input and no active workers
            return

    def join(self):
        self.wait()
        for worker in self.workers:
            worker.stop()
            worker.join()

import time
def sleeper(seconds):
    print "%d: sleeping for %d seconds" % (os.getpid(), seconds)
    time.sleep(seconds)
    print "%d: done sleeping" % os.getpid()

    return seconds

def test():
    i= Queue()
    o = Queue()

    w = Worker(i, o, sleeper)
    w.start()

    return i, o, w


def test2():
    pool = WorkerPool(3, sleeper)

    for i in range(5):
        pool.input.put(3)

    return pool

if __name__ == "__main__":
    test()
