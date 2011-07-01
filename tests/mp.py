#!/usr/bin/python

import os
import time

from multiprocessing import Process

def f(name, seconds):
    print 'from %d hello %s' % (os.getpid(), name)
    time.sleep(seconds)

def main():
    print "parent pid: %d" % (os.getpid())
    procs = []
    for i in range(10):
        p = Process(target=f, args=('bob', i))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    print "after join"

if __name__ == '__main__':
    main()
