#!/usr/bin/python

import os
import signal
import time

def test():
    class A:
        pass

    a = A()
    a.count = 0
    def handler(sig, action):
        a.count += 1

    signal.signal(signal.SIGUSR1, handler)

    for i in range(1000):
        os.kill(os.getpid(), signal.SIGUSR1)

    print a.count

if __name__ == "__main__":
    test()
