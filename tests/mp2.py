#!/usr/bin/python

import sys
import time
from multiprocessing import Process, Lock

if __name__ == '__main__':
    l = Lock()

    def f(i, locking=True):
        if locking:
            l.acquire()
        s = 'hello world ' + str(i)
        for i in range(len(s)):
            sys.stdout.write(s[i])
            sys.stdout.flush()
            time.sleep(0.00001)
        print
        sys.stdout.flush()

        if locking:
            l.release()

    procs = []
    print "WITHOUT LOCKING"
    for num in range(10):
        p = Process(target=f, args=(num,False))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    print "WITH LOCKING"
    for num in range(10):
        Process(target=f, args=(num,True)).start()
