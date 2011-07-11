import os
import signal

import time
from multiprocessing import Process, Event, Semaphore

from multiprocessing import Semaphore, Condition, Value
from multiprocessing.queues import Queue, Empty

from threadloop import ThreadLoop

class WaitableQueue(Queue):
    """Queue that uses a semaphore to reliably count items in it"""
    def __init__(self, maxsize=0):
        self.cond_empty = Condition()
        self.cond_notempty = Condition()
        self._put_counter = Value('i', 0)

        Queue.__init__(self, maxsize)

    def put(self, obj, block=True, timeout=None):
        Queue.put(self, obj, block, timeout)
        self._put_counter.value += 1

        if self.qsize() != 0:
            self.cond_notempty.acquire()
            try:
                self.cond_notempty.notify_all()
            finally:
                self.cond_notempty.release()

    @property
    def put_counter(self):
        return self._put_counter.value

    def get(self, block=True, timeout=None):
        ret = Queue.get(self, block, timeout)
        if self.qsize() == 0:
            self.cond_empty.acquire()
            try:
                self.cond_empty.notify_all()
            finally:
                self.cond_empty.release()

        return ret

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

class Parallelize:
    class Worker(Process):
        class Terminated(Exception):
            pass

        @classmethod
        def worker(cls, done, idle, q_input, q_output, func):
            def raise_exception(s, f):
                signal.signal(s, signal.SIG_IGN)
                raise cls.Terminated

            signal.signal(signal.SIGTERM, raise_exception)
            signal.signal(signal.SIGINT, raise_exception)

            class UNDEFINED:
                pass

            try:
                while True:
                    if done.is_set():
                        return

                    retval = UNDEFINED
                    try:
                        input = q_input.get(timeout=0.1)
                    except Empty:
                        continue

                    idle.clear()

                    try:
                        retval = func(*input)
                        q_output.put(retval)
                    except:
                        if retval is UNDEFINED:
                            q_input.put(input)
                        raise

                    finally:
                        idle.set()

            except cls.Terminated:
                pass # just exit peacefully

        def __init__(self, q_input, q_output, func):
            self.idle = Event()
            self.done = Event()

            self.idle.set()

            Process.__init__(self, 
                             target=self.worker, 
                             args=(self.done, self.idle, q_input, q_output, func))

        def is_busy(self):
            return self.is_alive() and not self.idle.is_set()

        def wait(self, timeout=None):
            """wait until Worker is idle"""
            return self.idle.wait(timeout)

        def stop(self):
            """let worker finish what it was doing and join"""
            if not self.is_alive():
                return

            self.done.set()

    def __init__(self, size, func):
        input = WaitableQueue()
        output = WaitableQueue()

        self.workers = []

        for i in range(size):
            worker = self.Worker(input, output, func)
            worker.start()

            self.workers.append(worker)

        self.size = size

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
        def find_busy_worker():
            for worker in self.workers:
                if worker.is_busy():
                    return worker

        while True:
            self.input.wait_empty(0.1)

            saved_put_counter = self.input.put_counter

            worker = find_busy_worker()
            if worker:
                worker.wait()
                continue

            time.sleep(0.1)

            # workers may have written to the input Queue
            if self.input.put_counter != saved_put_counter:
                continue

            # only reached when there was no input and no active workers
            return

    def stop(self):

        for worker in self.workers:
            worker.stop()

        for worker in self.workers:
            worker.join()

        self._results_getter.stop()

    def __call__(self, *args):
        self.input.put(args)

def test():
    import time
    def sleeper(seconds):
        print os.getpid()
        time.sleep(seconds)
        return seconds

    sleeper = Parallelize(100, sleeper)
    try:
        for i in range(10000):
            sleeper(1)
        sleeper.wait()
    finally:
        sleeper.stop()

    print "len(pool.results) = %d" % len(sleeper.results)

if __name__ == "__main__":
    test()
