#!/usr/bin/python

import os
import time

from multiprocessing import Process

def f(name):
    print 'from %d hello %s' % (os.getpid(), name)
    time.sleep(10)

def main():
    print "parent pid: %d" % (os.getpid())
    p = Process(target=f, args=('bob',))
    p.start()
    p.join()
    print "after join"

if __name__ == '__main__':
    main()
