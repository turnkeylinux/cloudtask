#!/usr/bin/python
"""
Run commands in parallel

Options:

    --timeout=SECS        How long to wait before giving up
    --split=N             How many processes to execute in parallel
    --split-logs=DIR       Path to directory where we save logs
                          Required with --split
"""
import os
import sys
import shlex
import getopt
import time

import errno

from command import Command, fmt_argv
from multiprocessing import Process, Queue, Manager

import random

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

class CommandExecutor:
    MAGIC_STOP = '__STOP__'

    class Error(Exception):
        pass

    @staticmethod
    def _execute(command, outfh, timeout=None):
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

    def _subprocess(self):
        fh = file(os.path.join(self.split_logs, "%d" % os.getpid()), "w")

        for job in iter(self.q_todo.get, self.MAGIC_STOP):
            result = self._execute(job, fh, self.timeout)
            self.results.append((job, result))

        fh.close()

    def __init__(self, split=None, split_logs=None, timeout=None):
        self.results = []
        self.split = None

        if (split and not split_logs) or (split_logs and not split):
            raise self.Error("--split and --split-logs go together")

        self.split = split
        self.timeout = timeout

        if not split:
            return

        self.results = Manager().list()

        if split < 2:
            raise self.Error("bad split (%d) minimum is 2" % split)

        self.split_logs = split_logs
        self.q_todo = Queue()

        procs = []

        for i in range(self.split):
            proc = Process(target=self._subprocess)
            proc.start()
            procs.append(proc)

        print "initialized %d workers: %s" % (len(procs), 
                                              " ".join([ str(proc.pid) 
                                                         for proc in procs ]))
        self.procs = procs

    def __call__(self, job):
        if not self.split:
            result = self._execute(job, sys.stdout, self.timeout)
            self.results.append((job, result))
        else:
            self.q_todo.put(job)

    def join(self):
        if not self.split:
            return

        for i in range(self.split):
            self.q_todo.put(self.MAGIC_STOP)

        for proc in self.procs:
            proc.join()

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
                                         'split-logs='])
    except getopt.GetoptError, e:
        usage(e)

    opt_timeout = None
    opt_split = None
    opt_split_logs = None

    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage()

        if opt == '--timeout':
            opt_timeout = float(val)

        if opt == '--split':
            opt_split = int(val)
            if opt_split < 1:
                usage("bad --split value '%s'" % val)

        if opt == '--split-logs':
            opt_split_logs = val
            if os.path.isfile(opt_split_logs):
                error("'%s' is a file, not a directory" % val)

            makedirs(opt_split_logs)
            if not os.access(opt_split_logs, os.W_OK):
                error("not allowed to write to '%s'" % opt_split_logs)

    command = args
    if len(command) == 1:
        if len(shlex.split(command[0])) > 1:
            command = command[0]

    try:
        executor = CommandExecutor(opt_split, opt_split_logs, opt_timeout)
    except CommandExecutor.Error, e:
        usage(e)

    for line in sys.stdin.readlines():
        args = shlex.split(line)

        def join(command, args):
            if isinstance(command, str):
                return command + ' ' + fmt_argv(args)

            return command + args

        executor(join(command, args))

    executor.join()

    exitcodes = [ exitcode for command, exitcode in executor.results ]

    succeeded = exitcodes.count(0)
    failed = len(exitcodes) - ok

    print "%d commands executed (%d succeeded, %d failed)" % (len(exitcodes),
                                                              succeeded, failed)

if __name__ == "__main__":
    main()
