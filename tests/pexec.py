#!/usr/bin/python
import sys
import shlex
import getopt
import time

from command import Command, fmt_argv

def iter_stdin_args():
    for line in sys.stdin.readlines():
        yield shlex.split(line)



class Timeout:
    def __init__(self, seconds=None):
        """If seconds is None, timeout never expires"""
        self.seconds = seconds
        self.started = time.time()

    def expired(self):
        if self.seconds and time.time() - self.started > self.seconds:
            return True
        return False

def usage(e=None):
    if e:
        print >> sys.stderr, "error: " + str(e)

    print >> sys.stderr, "syntax: %s [ --timeout=SECS ] [ command ]" % sys.argv[0]
    sys.exit(1)

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   'h', ['help', 'timeout='])
    except getopt.GetoptError, e:
        usage(e)

    opt_timeout = None

    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage()

        if opt == '--timeout':
            opt_timeout = float(val)

    command = args
    if len(command) == 1:

        if len(shlex.split(command[0])) > 1:
            command = command[0]

    for args in iter_stdin_args():
        def join(command, args):
            if isinstance(command, str):
                return command + ' ' + fmt_argv(args)

            return command + args

        c = Command(join(command, args), setpgrp=True)
        print "# EXECUTING: " + str(c)

        timeout = Timeout(opt_timeout)
        while True:

            try:
                output = c.fromchild.read_nonblock()
            except c.fromchild.EOF:
                c.wait()
                break

            sys.stdout.write(output)

            if not c.running:
                break

            if timeout.expired():
                c.terminate()
                print "# TIMED OUT"
                break

        if c.exitcode:
            print "# NON-ZERO EXITCODE: %d" % c.exitcode

        print


if __name__ == "__main__":
    main()
