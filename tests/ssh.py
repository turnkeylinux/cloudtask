#!/usr/bin/python
import sys
from command import Command

class SSHCommand(Command):
    def __init__(self, address, command):
        opts = ('StrictHostKeyChecking=no',
                'PasswordAuthentication=no')

        argv = ['ssh', '-t']
        for opt in opts:
            argv += [ "-o", opt ]

        argv += [ address, command ]
        Command.__init__(self, argv, setpgrp=True)

command = 'echo HOME=$HOME; echo pid=$$; (while true; do sleep 1; echo hello; done)'

c = SSHCommand('d', command)
def cb(c, buf):
    if buf:
        sys.stdout.write(buf)
        sys.stdout.flush()
    return True
    
c.read(cb)
c.wait()
