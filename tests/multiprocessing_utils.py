import os
import signal

from multiprocessing import Process, Event, Semaphore

from multiprocessing import Semaphore, Condition
from multiprocessing.queues import Queue, Empty

from threadloop import ThreadLoop

class WaitableQueue(Queue):
    """Queue that uses a semaphore to reliably count items in it"""
    def __init__(self, maxsize=0):
        self.sem_items = Semaphore(0)

        self.cond_empty = Condition()
        self.cond_notempty = Condition()

        Queue.__init__(self, maxsize)

    def __getstate__(self):
        return Queue.__getstate__(self) + (self.sem_items, self.cond_empty, self.cond_notempty)

    def __setstate__(self, state):
        Queue.__setstate__(self, state[:-2])
        self.sem_items, self.cond_empty, self.cond_notempty = state[-2:]

    def put(self, obj, block=True, timeout=None):
        Queue.put(self, obj, block, timeout)
        self.sem_items.release()

        if self.sem_items.get_value() != 0:
            self.cond_notempty.acquire()
            try:
                self.cond_notempty.notify_all()
            finally:
                self.cond_notempty.release()

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

    def wait_empty(self, timeout=None):
        """Wait for all items to be got"""
        self.cond_empty.acquire()
        try:
            if self.qsize():
                self.cond_empty.wait(timeout)
        finally:
            self.cond_empty.release()

    def wait_notempty(self, timeout=None):
        """Wait for all items to be got"""
        self.cond_notempty.acquire()
        try:
            if self.qsize() == 0:
                self.cond_notempty.wait(timeout)
        finally:
            self.cond_notempty.release()

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
    """
    High-level worker pool.

    1) Reads from an input queue.
    2) result = func(*input) 
    
       (if there's an exception input is put back into the input queue)

    3) Writes result back to the output queue.


    """
    def __init__(self, size, func):
        input = WaitableQueue()
        output = WaitableQueue()

        self.workers = []

        for i in range(size):
            worker = Worker(input, output, func)
            worker.start()

            self.workers.append(worker)

        self.input = input
        self.results = []

        def get_results():
            output.wait_notempty(0.1)

            while True:
                try:
                    result = output.get(False)
                    self.results.append(result)
                except Empty:
                    break

        self._results_getter = ThreadLoop(get_results)

    def wait(self):
        """wait for all input to be processed"""
        while True:
            self.input.wait_empty()
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

        self._results_getter.stop()

