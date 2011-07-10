#!/usr/bin/python

import os
import signal

from multiprocessing_utils import WorkerPool

import time
def sleeper(seconds):
    print "%d: sleeping for %d seconds" % (os.getpid(), seconds)
    time.sleep(seconds)
    print "%d: done sleeping" % os.getpid()

    return seconds

def test():
    pool = WorkerPool(250, sleeper)

    try:
        for i in range(250):
            pool.input.put(1)

        pool.wait()
        pool.join()
    finally:
        pool.join()

    print len(pool.results)
    return pool

if __name__ == "__main__":
    test()
