#!/usr/bin/env python
# 
# Copyright (c) 2012 Liraz Siri <liraz@turnkeylinux.org>
# 
# This file is part of CloudTask.
# 
# CloudTask is open source software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 3 of the License, or (at your
# option) any later version.
# 

"""
Analyze session logs

"""

from os.path import *
import sys
import getopt

from cloudtask import logalyzer

def usage(e=None):
    if e:
        print >> sys.stderr, "error: " + str(e)

    print >> sys.stderr, "Usage: %s path/to/session [ path/to/outputs/ ]" % sys.argv[0]
    print >> sys.stderr, __doc__.strip()
    sys.exit(1)

def fatal(e):
    print >> sys.stderr, "error: " + str(e)
    sys.exit(1)

def main():

    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], 
                                       'h', [ 'help' ])
    except getopt.GetoptError, e:
        usage(e)

    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage()

    if len(args) < 1:
        usage()

    session_path = args[0]

    if not isdir(session_path):
        fatal("not a directory '%s'" % session_path)

    if len(args) > 1:
        outputs_dir = args[1]
        if not isdir(outputs_dir):
            fatal("not a directory '%s'" % outputs_dir)

    else:
        outputs_dir = None

    print logalyzer.logalyzer(session_path, outputs_dir)

if __name__ == "__main__":
    main()
