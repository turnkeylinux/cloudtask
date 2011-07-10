#!/usr/bin/python
import threading
import time

def hello(done):
    while not done.isSet():
        print "hello world"
        time.sleep(1)

def test():
    done = threading.Event()
    t = threading.Thread(target=hello, args=(done,))
    t.start()
    time.sleep(3)
    print "done"
    done.set()
    t.join()

if __name__ == "__main__":
    test()
