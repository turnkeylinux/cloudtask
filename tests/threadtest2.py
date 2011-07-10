#!/usr/bin/python
from __future__ import with_statement

import threading
import time
import signal

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
    for i in range(5):
        print "hello world %d" % i
        time.sleep(1)
        yield True

    print "done"
    yield False

class Foo:
    def __init__(self):
        done = threading.Event()
        t = threading.Thread(target=hello, args=(done,))
        t.start()

        self.done = done
        self.t = t

    def stop(self):
        self.done.set()
        self.t.join()

class ThreadLoop(threading.Thread):
    def __init__(self, func):
        self.done = threading.Event()
        self.func = func

        threading.Thread.__init__(self)

    def run(self):
        while True:
            ret = self.func()
            if self.done.isSet() or ret is False:
                return

            # special treatment for generator functions
            if hasattr(ret, 'next'): 
                iterable = ret

                for ret in iterable:
                    if self.done.isSet() or ret is False:
                        return

                return

    def stop(self):
        self.done.set()
        self.join()

def test():
    loop = ThreadLoop(hello1)
    loop.start()
    try:
        while True:
            time.sleep(1)

    finally:
        loop.stop()

if __name__ == "__main__":
    test()
