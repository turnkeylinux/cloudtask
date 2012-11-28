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
Analyze log output

"""

from cloudtask.session import Session

import os
from os.path import *
import sys
import getopt

import re

from StringIO import StringIO

def usage(e=None):
    if e:
        print >> sys.stderr, "error: " + str(e)

    print >> sys.stderr, "Usage: %s [ -opts ] path/to/session" % sys.argv[0]
    print >> sys.stderr, __doc__.strip()
    sys.exit(1)

def fatal(e):
    print >> sys.stderr, "error: " + str(e)
    sys.exit(1)

def fmt_elapsed(seconds):
    hours = seconds / 3600
    minutes = (seconds % 3600) / 60
    seconds = (seconds % 3600) % 60

    return "%02d:%02d:%02d" % (hours, minutes, seconds)

def logalyzer(session_path):
    session_paths = Session.Paths(session_path)

    conf = eval(file(session_paths.conf).read())
    log = file(session_paths.log).read()

    m = re.search(r'^session (\d+): (\d+) jobs in (\d+) seconds \((\d+) succeeded, (\d+) failed\)', log, 
                  re.MULTILINE)
    assert m

    summary = {}
    vals = map(int, m.groups())
    for i, attrname in enumerate(('id', 'jobs', 'elapsed', 'completed', 'failed')):
        summary[attrname] = vals[i]

    s = summary
    header =  "session %d: %s elapsed, %d jobs - %d failed, %d succeeded" % (s['id'],
                                                                            fmt_elapsed(s['elapsed']),
                                                                            s['jobs'],
                                                                            s['failed'],
                                                                            s['completed'])

    c = conf
    workers = "%d x (%s)" % (c['split'],
                             " : ".join([ c[attr] 
                                       for attr in ('ec2_region', 'ec2_size', 'ec2_type', 'ami_id') 
                                       if c[attr] ]))


    fields = conf
    fields['workers'] = workers

    sio = StringIO()

    print >> sio, header
    print >> sio, "=" * len(header)
    print >> sio
    print >> sio, "Configuration:"
    print >> sio

    for field in ('command', 'workers', 'backup_id', 'overlay', 'timeout', 'report'):
        print >> sio, "    %-16s %s" % (field.replace('_', '-'), fields[field])

    print >> sio

    return sio.getvalue()

def main():

    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], 
                                       'h', [ 'help' ])
    except getopt.GetoptError, e:
        usage(e)

    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage()

    if len(args) != 1:
        usage()

    session_path = args[0]
    if not isdir(session_path):
        fatal("not a directory '%s'" % session_path)

    print logalyzer(session_path)

if __name__ == "__main__":
    main()
