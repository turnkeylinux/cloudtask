#!/usr/bin/python
import threading
import time
import signal

def hello(done):
    while not done.isSet():
        print "hello world"
        time.sleep(1)

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

def test():
    foo = Foo()
    try:
        while True:
            time.sleep(1)
    finally:
        foo.stop()

if __name__ == "__main__":
    test()
