#!/usr/bin/python
from __future__ import with_statement

import threading
import time
import signal

class LoopThread(threading.Thread):
    def __init__(self, func):
        """
        func can be a regular function or a generator function.

        Regular functions loop forever until they return False.

        Generator functions iterate until they yield False.
        """
        self._done = threading.Event()
        self._func = func

        threading.Thread.__init__(self)

    def run(self):
        while True:
            ret = self._func()

            if ret is False:
                self._done.set()

            if self._done.isSet():
                return

            # special treatment for generator functions
            if hasattr(ret, 'next'): 
                iterable = ret

                for ret in iterable:
                    if ret is False:
                        self._done.set()

                    if self._done.isSet():
                        return

                self._done.set()
                return

    def stop(self):
        self._done.set()
        self.join()

    @property
    def done(self):
        return self._done.isSet()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, tb):
        self.stop()

def test():
    def hello1():
        print "hello world"
        time.sleep(1)
        return True

    def hello2():
        while True:
            print "hello world"
            time.sleep(1)
            yield True

    def hello3():
        for i in range(3):
            print "hello world %d" % i
            time.sleep(1)
            yield True

        print "done"

    with LoopThread(hello3) as loop:
        while True:
            if loop.done:
                break

            time.sleep(1)

    loop = LoopThread(hello1)
    loop.start()
    try:
        for i in range(3):
            time.sleep(1)
    finally:
        loop.stop()

if __name__ == "__main__":
    test()
