#!/usr/bin/python

import time
import signal

class CaughtSignal(Exception):
    pass

def foo():
    try:
        while True:
            time.sleep(1)
    except CaughtSignal:
        print "caught signal"

def handler(s, f):
    raise CaughtSignal

signal.signal(signal.SIGINT, handler)
foo()
