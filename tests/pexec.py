#!/usr/bin/python
"""
Run commands in parallel

Options:

    --timeout=SECS        How long to wait before giving up
    --split=N             How many processes to execute in parallel
    --sessions=PATH       Path to location where sessions are stored
                          environment: PEXEC_SESSIONS
                          default: $HOME/.pexec/sessions/

Usage:

    seq 10 | pexec echo
    seq 10 | pexec --split=3 echo
    pexec --resume=1 --timeout=6 --split=3


"""
import os
from os.path import *
import sys
import shlex
import getopt
import time

import errno

from command import Command, fmt_argv
from multiprocessing import Process, Queue
from multiprocessing.queues import Empty

from paths import Paths

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

class Session:
    class SessionPaths(Paths):
        files = ['workers', 'log', 'jobs']

    def __init__(self, sessions_path):
        if not exists(sessions_path):
            makedirs(sessions_path)

        if not isdir(sessions_path):
            raise Error("sessions path is not a directory: " + sessions_path)

        session_ids = [ int(fname) for fname in os.listdir(sessions_path) 
                        if fname.isdigit() ]

        if session_ids:
            new_session_id = max(map(int, session_ids)) + 1
        else:
            new_session_id = 1

        path = join(sessions_path, "%d" % new_session_id)
        makedirs(path)

        self.paths = self.SessionPaths(path)
        self.id = new_session_id

class CommandExecutor:
    """
    Execute commands serially or in parallel.

    Features:

        - output from each subprocess sent in realtime to <logs>/<pid>
        - commands may be string or tuples
        - optional timeout

    Usage::

        executor = CommandExecutor(2, "logs/", timeout=10)
        for command in commands:
            executor(command)

        executor.join()
        for command, exitcode in executor.results:
            print "%d: %s" % (exitcode, 

    """

    MAGIC_STOP = '__STOP__'

    class Error(Exception):
        pass

    @staticmethod
    def _execute(command, log, timeout=None):
        def status(msg):
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            print >> log, "# %s: %s" % (timestamp, msg)

            if log != sys.stdout:
                print >> sys.stdout, "%d: %s" % (os.getpid(), msg)

        command = Command(command, setpgrp=True)
        status(str(command))

        timeout = Timeout(timeout)
        def handler(command, buf):
            if buf:
                log.write(buf)
                log.flush()

            if command.running and timeout.expired():
                command.terminate()
                status("timeout %d # %s" % (timeout.seconds, command))

            return True

        out = command.read(handler)
        if command.exitcode is not None:
            status("exit %d # %s" % (command.exitcode, command))

        print >> log
        return command.exitcode

    def _worker(self):
        makedirs(self.session.paths.workers)
        log_path = join(self.session.paths.workers, "%d" % os.getpid())
        log = file(log_path, "w")

        for job in iter(self.q_jobs.get, self.MAGIC_STOP):
            result = self._execute(job, log, self.timeout)
            self.q_results.put((job, result))

        log.close()

    def __init__(self, sessions_path, split=None, timeout=None):
        self.results = []
        self.split = None

        self.split = split
        self.timeout = timeout

        self.session = Session(sessions_path)

        if not split:
            print "session %d: serial" % self.session.id
            return

        if split < 2:
            raise self.Error("bad split (%d) minimum is 2" % split)

        self.q_jobs = Queue()
        self.q_results = Queue()

        procs = []

        for i in range(self.split):
            proc = Process(target=self._worker)
            proc.start()
            procs.append(proc)

        print "session %d: split %d workers = %s" % (self.session.id,
                                                     len(procs), 
                                                     " ".join([ str(proc.pid) 
                                                       for proc in procs ]))
        print
        self.procs = procs

    def __call__(self, job):
        if not self.split:
            result = self._execute(job, sys.stdout, self.timeout)
            self.results.append((job, result))
        else:
            self.q_jobs.put(job)

    def join(self):
        if not self.split:
            return

        for i in range(self.split):
            self.q_jobs.put(self.MAGIC_STOP)

        def qgetall(q):
            vals = []
            while True:
                try:
                    val = q.get(False)
                    vals.append(val)
                except Empty:
                    break

            return vals

        while True:
            running = 0
            for proc in self.procs:
                if proc.is_alive():
                    running += 1
                else:
                    proc.join()

            self.results += qgetall(self.q_results)

            if not running:
                break

            time.sleep(0.1)

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
    opt_sessions = os.environ.get('PEXEC_SESSIONS', 
                                  join(os.environ['HOME'], '.pexec', 'sessions'))
    opt_split = None
    opt_timeout = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   'h', ['help', 
                                         'timeout=',
                                         'split=',
                                         'sessions=',
                                         ])
    except getopt.GetoptError, e:
        usage(e)

    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage()

        if opt == '--timeout':
            opt_timeout = float(val)

        if opt == '--split':
            opt_split = int(val)
            if opt_split < 1:
                usage("bad --split value '%s'" % val)

        if opt == '--sessions':
            opt_sessions = val

    command = args
    if len(command) == 1:
        if len(shlex.split(command[0])) > 1:
            command = command[0]

    try:
        executor = CommandExecutor(opt_sessions, opt_split, opt_timeout)
    except CommandExecutor.Error, e:
        usage(e)

    for line in sys.stdin.readlines():
        args = shlex.split(line)

        def command_join(command, args):
            if isinstance(command, str):
                return command + ' ' + fmt_argv(args)

            return command + args

        executor(command_join(command, args))

    executor.join()

    exitcodes = [ exitcode for command, exitcode in executor.results ]

    succeeded = exitcodes.count(0)
    failed = len(exitcodes) - succeeded

    print "%d commands executed (%d succeeded, %d failed)" % (len(exitcodes),
                                                              succeeded, failed)

if __name__ == "__main__":
    main()
