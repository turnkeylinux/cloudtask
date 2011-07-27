#!/usr/bin/python

import os
import time
import random
import signal
from multiprocessing_utils import *

class Terminated(Exception):
    pass

class ExampleExecutor:
    def __init__(self, name):

        self.name = name
        self.pid = os.getpid()

        print "%s.__init__: pid %d" % (self.name, self.pid)

    def __call__(self, *args):
        print "%s.__call__(%s)" % (self.name, `args`)
        time.sleep(random.randrange(1, 10))
        return args

    def __del__(self):
        if os.getpid() == self.pid:
            print "destroying %d" % self.pid

def test():
    deferred = []
    for i in range(10):
        deferred_executor = Deferred(ExampleExecutor, i)
        deferred.append(deferred_executor)

    def raise_exception(s, f):
        signal.signal(s, signal.SIG_IGN)
        raise Terminated

    signal.signal(signal.SIGINT, raise_exception)

    p = Parallelize(deferred)
    try:
        print "len(p.executors) = %d" % len(p.executors)

        for executor in p.executors:
            print executor.pid

        for i in range(2):
            p(i)

        p.wait(keepalive=False, keepalive_spares=1)
        print "p.results: " + `p.results`
    finally:
        aborted = p.stop()
        print "after stop: " + `aborted`

if __name__ == "__main__":
    test()
