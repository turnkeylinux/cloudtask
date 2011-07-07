#!/usr/bin/python

import os
import time
from multiprocessing import Process, Queue

MAGIC_STOP = '__STOP__'

def worker(input):
    
    for sleep in iter(input.get, MAGIC_STOP):
        print "%d: sleeping for %d seconds" % (os.getpid(), sleep)
        time.sleep(sleep)
        print "%d: finished sleeping" % (os.getpid())

def test():

    q = Queue()
    procs = []
    for i in range(3):
        proc = Process(target=worker, args=(q,))
        proc.start()
        procs.append(proc)

    for i in range(9):
        q.put(1)

    for proc in procs:
        q.put(MAGIC_STOP)

    for proc in procs:
        proc.join()

if __name__ == "__main__":
    test()
