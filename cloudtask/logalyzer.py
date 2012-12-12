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
import os
from os.path import *
from cloudtask.session import Session

import re

from StringIO import StringIO
from datetime import datetime

def fmt_elapsed(seconds):
    units = {}
    for unit, unit_seconds in (('days', 86400), ('hours', 3600), ('minutes', 60), ('seconds', 1)):
        units[unit] = seconds / unit_seconds
        seconds = seconds % unit_seconds


    if units['days']:
        formatted = "%d days " % units['days']
    else:
        formatted = ""

    formatted += "%02d:%02d:%02d" % (units['hours'], units['minutes'], units['seconds'])
    return formatted

class Error(Exception):
    pass

class WorkersLog:
    class LogEntry:
        TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"
        def __init__(self, timestamp, title):
            self.timestamp = datetime.strptime(timestamp, self.TIMESTAMP_FMT)
            self.title = title
            self.body = ""

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

    class Instance:
        def __init__(self, worker_id, instance_id, seconds):
            self.worker_id = worker_id
            self.instance_id = instance_id
            self.seconds = seconds

        def __repr__(self):
            return "Instance%s" % `self.worker_id, self.instance_id, self.seconds`

    @classmethod
    def get_jobs(cls, log_entries, command):
        pat = re.compile(r'^(.*?) # %s (.*)' % command)

        jobs = []
        for i, entry in enumerate(log_entries):
            m = pat.match(entry.title)
            if m:
                result, name = m.groups()
                started = log_entries[i-1]
                elapsed = (entry.timestamp - started.timestamp).seconds
                jobs.append((name, result, started.timestamp, elapsed, started.body))

        return jobs

    @classmethod
    def get_instance_elapsed(cls, log_entries):
        launched = None
        destroyed = None

        for log_entry in log_entries:
            m = re.match(r'launched worker (.*)', log_entry.title)
            if m:
                instanceid = m.group(1)
                launched = (log_entry.timestamp, instanceid)
                continue

            m = re.match(r'destroyed worker (.*)', log_entry.title)
            if m:
                instanceid = m.group(1)
                destroyed = (log_entry.timestamp, instanceid)

        if not launched:
            return None, None

        if launched and destroyed:
            if destroyed[1] != launched[1]:
                destroyed = None

        if launched and not destroyed:
            instanceid = launched[1]
            return instanceid, None

        return launched[1], (destroyed[0] - launched[0]).seconds

    def __init__(self, dpath, command):
        jobs = {}
        instances = []

        for fname in os.listdir(dpath):
            worker_id = int(fname)
            fpath = join(dpath, fname)
            log_entries = self.parse_worker_log(fpath)

            instance_id, seconds = self.get_instance_elapsed(log_entries)
            if instance_id:
                instances.append(self.Instance(worker_id, instance_id, seconds))

            worker_jobs = [ self.Job(worker_id, *job_args) for 
                            job_args in self.get_jobs(log_entries, command) ]

            for job in worker_jobs:
                name = job.name
                if name in jobs:
                    conflicting_job = jobs[name]
                    if job.timestamp > conflicting_job.timestamp:
                        jobs[name] = job
                else:
                    jobs[name] = job

        self.jobs = jobs.values()
        self.instances = instances

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

    m = re.search(r'session (\d+).*(\d+) seconds', log, re.MULTILINE)
    if not m:
        raise Error("couldn't find session summary")

    id, elapsed = map(int, m.groups())

    jobs = Session.Jobs(session_paths.jobs)

    # calculate stats
    
    results = [ result for command, result in jobs.finished ]

    class stats:
        pending = len(jobs.pending)
        finished = len(jobs.finished)
        total = pending + finished
        succeeded = results.count('EXIT=0')

        failures = len(results) - succeeded
        failures_timeouts = results.count('TIMEOUT')
        failures_errors = len(results) - succeeded - failures_timeouts

    sio = StringIO()

    def header(level, s):
        c = "=-"[level]
        return s + "\n" + c * len(s) + "\n"

    print >> sio, header(0, "session %d: %s elapsed, %d jobs - %d pending, %d failed, %d completed" % 
                            (id, fmt_elapsed(elapsed), stats.total, stats.pending, stats.failures, stats.succeeded))

    print >> sio, "Configuration:"
    print >> sio

    c = conf
    workers = "%d x (%s)" % (c['split'] if c['split'] else 1,
                             " : ".join([ c[attr] 
                                       for attr in ('ec2_region', 'ec2_size', 'ec2_type', 'ami_id', 'snapshot_id') 
                                       if attr in c and c[attr] ]))

    fields = conf
    fields['workers'] = workers

    for field in ('command', 'workers', 'backup_id', 'overlay', 'post', 'pre', 'timeout', 'report'):
        if field in fields and fields[field]:
            print >> sio, "    %-16s %s" % (field.replace('_', '-'), fields[field])

    print >> sio

    wl = WorkersLog(session_paths.workers, conf['command'])

    if stats.pending:
        print >> sio, header(0, "%d pending jobs" % stats.pending)
        print >> sio, " ".join([ job[len(conf['command']):].strip() for job in jobs.pending ])
        print >> sio

    jobs = wl.jobs[:]
    jobs.sort(lambda a,b: cmp((a.worker_id, b.elapsed), (b.worker_id, a.elapsed)))
    
    if stats.failures:
        single_failure = (stats.failures == 1)
        print >> sio, header(0, "%d failed - errors: %d, timeout: %d" % (stats.failures, 
                                                                              stats.failures_errors,
                                                                              stats.failures_timeouts))
        if not single_failure:
            print >> sio, header(1, "Summary")

        failures = [ job for job in jobs if job.result != 'exit 0' ]
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

            if failures[i].output:
                print >> sio, indent(4, "\n".join(failures[i].output.splitlines()[-5:]))
                print >> sio

    if stats.succeeded:
        print >> sio, header(0, "%d succeeded" % stats.succeeded)

        completed = [ job for job in jobs if job.result == 'exit 0' ]
        rows = [ (job.name, fmt_elapsed(job.elapsed), job.worker_id)
                  for job in completed ]
        print stats.succeeded
        fmted_table = fmt_table(rows, ["NAME", "ELAPSED", "WORKER"], 
                                groupby=lambda a: a[2])
        print >> sio, fmted_table

    return sio.getvalue()


