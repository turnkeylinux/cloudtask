#!/usr/bin/python
import time
import signal

flag = [ True ]

def handler(s, f):
    print "handler"
    flag[0] = False

signal.signal(signal.SIGINT, handler)

def callback():
    print "flag: " + `flag[0]`
    return flag[0]

while True:
    if not callback():
        print "callback returned False"
        break

    time.sleep(1)
