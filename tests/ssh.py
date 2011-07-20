#!/usr/bin/python
import sys
from command import Command

command = 'echo HOME=$HOME; echo pid=$$; (while true; do sleep 1; echo hello; done)'
args = ('ssh',
        'd',
        command)
c = Command(args, setpgrp=True)
def cb(c, buf):
    if buf:
        sys.stdout.write(buf)
        sys.stdout.flush()
    return True
    
c.read(cb)
c.wait()
