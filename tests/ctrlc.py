#!/usr/bin/python

import time
from multiprocessing import Process
import signal
import sys
import random

from command import Command

procs = []

def worker():

    c = Command('for i in $(seq 30); do echo $i; sleep 1; done', setpgrp=True)

    def sigint(s, f):
        print "terminating %d" % c.pid
        c.terminate()
        sys.exit(1)

    signal.signal(signal.SIGINT, sigint)

    def cb(c, buf):
        if buf:
            sys.stdout.write(buf)
            sys.stdout.flush()

        return True

    c.read(cb)
    c.wait()

for i in range(100):
    p = Process(target=worker)
    p.start()
    procs.append(p)

# the code below is sufficient
signal.signal(signal.SIGINT, SIG_IGN)

while True:
    running = 0
    for proc in procs:
        if proc.is_alive():
            running += 1
        else:
            proc.join()

    print "RUNNING: %d" % running
    if not running:
        break

    time.sleep(1)
