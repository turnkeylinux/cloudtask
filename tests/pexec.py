#!/usr/bin/python
"""
Run commands in parallel

Options:

    --timeout=SECS        How long to wait before giving up
    --split=N             How many processes to execute in parallel
    --split-log=DIR       Path to directory where we save logs
                          Required with --split
"""
import os
import sys
import shlex
import getopt
import time

import errno

from command import Command, fmt_argv
from multiprocessing import Process, Queue

def makedirs(path):
    try:
        os.makedirs(path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise

class Timeout:
    def __init__(self, seconds=None):
        """If seconds is None, timeout never expires"""
        self.seconds = seconds
        self.started = time.time()

    def expired(self):
        if self.seconds and time.time() - self.started > self.seconds:
            return True
        return False

class DatedLog:
    def __init__(self, fh):
        self.fh = fh

    def __call__(self, msg):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        print >> self.fh, "# %s: %s" % (timestamp, msg)

        if self.fh != sys.stdout:
            print >> sys.stdout, "%d: %s" % (os.getpid(), msg)

def run_command(command, outfh, timeout=None):
    log = DatedLog(outfh)

    command = Command(command, setpgrp=True)
    log(str(command))

    timeout = Timeout(timeout)
    def handler(command, buf):
        if buf:
            outfh.write(buf)
            outfh.flush()

        if command.running and timeout.expired():
            command.terminate()
            log("timeout %d # %s" % (timeout.seconds, command))

        return True

    out = command.read(handler)
    if command.exitcode is not None:
        log("exit %d # %s" % (command.exitcode, command))

    print >> outfh

    return command.exitcode

def worker(jobs_todo, jobs_done, logdir, timeout):
    fh = file(os.path.join(logdir, "%d" % os.getpid()), "w")

    for command in iter(jobs_todo.get, 'STOP'):
        exitcode = run_command(command, fh, timeout)
        jobs_done.put((command, exitcode))

def error(e):
    print >> sys.stderr, "error: " + str(e)
    sys.exit(1)

def usage(e=None):
    if e:
        print >> sys.stderr, "error: " + str(e)

    print >> sys.stderr, "syntax: %s [ -opts ] [ command ]" % sys.argv[0]
    print >> sys.stderr, __doc__.strip()
    sys.exit(1)

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   'h', ['help', 
                                         'timeout=',
                                         'split=',
                                         'split-log='])
    except getopt.GetoptError, e:
        usage(e)

    opt_timeout = None
    opt_split = None
    opt_split_log = None

    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage()

        if opt == '--timeout':
            opt_timeout = float(val)

        if opt == '--split':
            opt_split = int(val)
            if opt_split < 1:
                usage("bad --split value '%s'" % val)

        if opt == '--split-log':
            opt_split_log = val
            if os.path.isfile(opt_split_log):
                error("'%s' is a file, not a directory" % val)

            makedirs(opt_split_log)
            if not os.access(opt_split_log, os.W_OK):
                error("not allowed to write to '%s'" % opt_split_log)

    if (opt_split and not opt_split_log) or \
       (opt_split_log and not opt_split):
        usage("--split and --split-log go together")


    jobs_todo = Queue()
    jobs_done = Queue()

    procs = []
    if opt_split:

        for i in range(opt_split):
            proc = Process(target=worker, args=(jobs_todo, jobs_done, opt_split_log, opt_timeout))

            proc.start()
            procs.append(proc)

        print "initialized %d workers: %s" % (len(procs), 
                                              " ".join([ str(proc.pid) 
                                                         for proc in procs ]))

    command = args
    if len(command) == 1:
        if len(shlex.split(command[0])) > 1:
            command = command[0]

    for line in sys.stdin.readlines():
        args = shlex.split(line)

        def join(command, args):
            if isinstance(command, str):
                return command + ' ' + fmt_argv(args)

            return command + args

        job = join(command, args)

        if not opt_split:
            run_command(job, sys.stdout, opt_timeout)
        else:
            jobs_todo.put(job)

    if opt_split:
        for i in range(opt_split):
            jobs_todo.put('STOP')

        for proc in procs:
            proc.join()
            
if __name__ == "__main__":
    main()
