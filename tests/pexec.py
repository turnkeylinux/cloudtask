#!/usr/bin/python
import sys
import shlex
from subprocess import Popen
import re
import commands

def fmt_command(argv):
    if not argv:
        return ""

    args = argv[1:]

    for i, arg in enumerate(args):
        if re.search(r"[\s'\"]", arg):
            args[i] = commands.mkarg(arg)
        else:
            args[i] = " " + arg

    return argv[0] + "".join(args)

def iter_stdin_args():
    for line in sys.stdin.readlines():
        yield shlex.split(line)

def main():
    command = sys.argv[1:]

    for args in iter_stdin_args():
        print "# EXECUTING: " + fmt_command(command + args)
        proc = Popen(command + args)
        exitcode = proc.wait()

        if exitcode != 0:
            print "# NON-ZERO EXITCODE: %d" % exitcode

        print


if __name__ == "__main__":
    main()
