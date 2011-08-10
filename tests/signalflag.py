#!/usr/bin/python
import time
import signal

class Bool:
    value = True

flag = Bool()

def handler(s, f):
    print "handler"
    flag.value = False

signal.signal(signal.SIGINT, handler)

def callback():
    print "flag: " + `flag.value`
    return flag.value

while True:
    if not callback():
        print "callback returned False"
        break

    time.sleep(1)
