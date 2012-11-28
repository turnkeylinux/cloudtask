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

from cloudtask.session import Session

import os
from os.path import *
import sys
import getopt

import re

from StringIO import StringIO
from datetime import datetime

def fmt_elapsed(seconds):
    hours = seconds / 3600
    minutes = (seconds % 3600) / 60
    seconds = (seconds % 3600) % 60

    return "%02d:%02d:%02d" % (hours, minutes, seconds)

class WorkersLog:
    class LogEntry:
        TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"
        def __init__(self, timestamp, title):
            self.timestamp = datetime.strptime(timestamp, self.TIMESTAMP_FMT)
            self.title = title
            self.body = None

        def __repr__(self):
            return "LogEntry%s" % `datetime.strftime(self.timestamp, self.TIMESTAMP_FMT), self.title`

    @classmethod
    def parse_worker_log(cls, fpath):
        entries = []
        body = ""
        for line in file(fpath).readlines():
            m = re.match(r'^# (\d{4}-\d+-\d+ \d\d:\d\d:\d\d) \[.*?\] (.*)', line)
            if not m:
                body += line
                continue
            else:
                body = body.strip()
                if entries and body:
                    entries[-1].body = body
                body = ""
                timestamp, title = m.groups()
                entries.append(cls.LogEntry(timestamp, title))

        return entries

    class Job:
        def __init__(self, worker_id, name, result, timestamp, elapsed, output):
            self.worker_id = worker_id
            self.name = name
            self.result = result
            self.timestamp = timestamp
            self.elapsed = elapsed
            self.output = output

        def __repr__(self):
            return "Job%s" % `self.worker_id, self.name, self.result, self.elapsed`

    @classmethod
    def get_jobs(cls, log_entries, worker_id, command):
        pat = re.compile(r'^(.*?) # %s (.*)' % command)

        jobs = []
        for i, entry in enumerate(log_entries):
            m = pat.match(entry.title)
            if m:
                result, name = m.groups()
                started = log_entries[i-1]
                elapsed = (entry.timestamp - started.timestamp).seconds
                jobs.append(cls.Job(worker_id, name, result, started.timestamp, elapsed, started.body))

        return jobs

    def __init__(self, dpath, command):
        jobs = {}
        for fname in os.listdir(dpath):
            worker_id = int(fname)
            fpath = join(dpath, fname)
            log_entries = self.parse_worker_log(fpath)
            worker_jobs = self.get_jobs(log_entries, worker_id, command)

            for job in worker_jobs:
                name = job.name
                if name in jobs:
                    conflicting_job = jobs[name]
                    if job.timestamp > conflicting_job.timestamp:
                        jobs[name] = job
                else:
                    jobs[name] = job

        self.jobs = jobs.values()

def fmt_table(rows, title=[], groupby=None):
    col_widths = []
    for col_index in range(len(rows[0])):
        col = [ str(row[col_index]) for row in rows ]
        col_width = max(map(len, col)) + 5
        col_widths.append(col_width)

    row_fmt = " ".join([ '%%-%ds' % width for width in col_widths ])
    sio = StringIO()
    if title:
        title = title[:]
        title[0] = "# " + title[0]
        print >> sio, row_fmt % tuple(title)
        print >> sio

    for i, row in enumerate(rows):
        if groupby and i and groupby(rows[i]) != groupby(rows[i-1]):
            print >> sio

        print >> sio, row_fmt % row

    return sio.getvalue()

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

    sio = StringIO()

    def header(level, s):
        c = "=-"[level]
        return s + "\n" + c * len(s) + "\n"

    print >> sio, header(0, "session %d: %s elapsed, %d jobs - %d failed, %d completed" % 
                            (s['id'], fmt_elapsed(s['elapsed']), s['jobs'], s['failed'], s['completed']))

    print >> sio, "Configuration:"
    print >> sio

    c = conf
    workers = "%d x (%s)" % (c['split'] if c['split'] else 1,
                             " : ".join([ c[attr] 
                                       for attr in ('ec2_region', 'ec2_size', 'ec2_type', 'ami_id') 
                                       if attr in c and c[attr] ]))

    fields = conf
    fields['workers'] = workers

    for field in ('command', 'workers', 'backup_id', 'overlay', 'post', 'pre', 'timeout', 'report'):
        if field in fields and fields[field]:
            print >> sio, "    %-16s %s" % (field.replace('_', '-'), fields[field])

    print >> sio

    wl = WorkersLog(session_paths.workers, conf['command'])

    jobs = wl.jobs[:]
    jobs.sort(lambda a,b: cmp((a.worker_id, b.elapsed), (b.worker_id, a.elapsed)))

    failures = [ job for job in jobs if job.result != 'exit 0' ]

    if failures:
        single_failure = (len(failures) == 1)

        print >> sio, header(0, "Failed %d jobs" % len(failures))
        if not single_failure:
            print >> sio, header(1, "Summary")

        rows = [ (job.name, fmt_elapsed(job.elapsed), job.result, job.worker_id)
                  for job in failures ]

        fmted_table = fmt_table(rows, ["NAME", "ELAPSED", "RESULT", "WORKER"], 
                                groupby=lambda a:a[3])
        print >> sio, fmted_table
        fmted_rows = [ line for line in fmted_table.splitlines()[2:] if line ]

        if not single_failure:
            print >> sio, header(1, "Last output")

        def indent(depth, buf):
            return "\n".join([ " " * depth + line for line in buf.splitlines() ])

        for i, fmted_row in enumerate(fmted_rows):
            if not single_failure:
                print >> sio, fmted_row
                print >> sio
            print >> sio, indent(4, "\n".join(failures[i].output.splitlines()[-5:]))
            print >> sio

    completed = [ job for job in jobs if job.result == 'exit 0' ]
    print >> sio, header(0, "Completed %d jobs" % len(completed))

    rows = [ (job.name, fmt_elapsed(job.elapsed), job.worker_id)
              for job in completed ]
    fmted_table = fmt_table(rows, ["NAME", "ELAPSED", "WORKER"], 
                            groupby=lambda a:a[2])
    print >> sio, fmted_table

    return sio.getvalue()

def usage(e=None):
    if e:
        print >> sys.stderr, "error: " + str(e)

    print >> sys.stderr, "Usage: %s [ -opts ] path/to/session" % sys.argv[0]
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

    if len(args) != 1:
        usage()

    session_path = args[0]
    if not isdir(session_path):
        fatal("not a directory '%s'" % session_path)

    print logalyzer(session_path)

if __name__ == "__main__":
    main()
