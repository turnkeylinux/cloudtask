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
import signal

from command import Command, fmt_argv
from multiprocessing import Process, Queue
from multiprocessing.queues import Empty

from paths import Paths

class SigTerminate(Exception):
    def __init__(self, msg, sig):
        self.sig = sig
        Exception.__init__(self, msg)

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

    class WorkerLog:
        def __init__(self, path):
            self.path = path
            self.fh = None

        def __getattr__(self, attr):
            if not self.fh:
                self.fh = file(join(self.path, str(os.getpid())), "w", 1)

            return getattr(self.fh, attr)

    class ManagerLog:
        def __init__(self, path):
            self.fh = file(path, "w", 1)

        def write(self, buf):
            self.fh.write(buf)
            sys.stdout.write(buf)
            sys.stdout.flush()

        def __getattr__(self, attr):
            return getattr(self.fh, attr)

    def __init__(self, sessions_path, opt_split):
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

        if opt_split:
            makedirs(self.paths.workers)
            self.wlog = self.WorkerLog(self.paths.workers)
            self.mlog = self.ManagerLog(self.paths.log)
                     
        else:
            self.wlog = self.ManagerLog(self.paths.log)
            self.mlog = self.wlog

    def save(self, jobs, results):
        fh = file(self.paths.jobs, "w")

        states = []
        for job, result in results:
            if result is None:
                state = "TIMEOUT"
            else:
                state = "EXIT=%s" % result

            states.append((job, state))

        pending = set(jobs) - set([ job for job, result in results ])

        for job in pending:
            states.append((job, "PENDING"))

        for job, state in states:
            print >> fh, "%s\t%s" % (state, job)

        fh.close()

class CommandExecutor:
    """
    Execute commands serially or in parallel.

    Features:

        - output from each subprocess sent in realtime to <logs>/<pid>
        - commands may be string or tuples
        - optional timeout

    Usage::

        executor = CommandExecutor(2, timeout=10)
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
    def _execute(command, timeout=None, wlog=None, mlog=None):
        def status(msg):
            if wlog:
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                print >> wlog, "# %s: %s" % (timestamp, msg)

            if mlog and mlog != wlog:
                mlog.write("%d: %s" % (os.getpid(), msg) + "\n")

        command = Command(command, pty=True)
        status(str(command))

        timeout = Timeout(timeout)
        def handler(command, buf):
            if buf and wlog:
                wlog.write(buf)

            if command.running and timeout.expired():
                command.terminate()
                status("timeout %d # %s" % (timeout.seconds, command))

            return True

        try:
            out = command.read(handler)
        except SigTerminate:
            command.terminate()
            status("terminated # %s" % command)
            raise

        if command.exitcode is not None:
            status("exit %d # %s" % (command.exitcode, command))

        if wlog:
            print >> wlog

        return command.exitcode

    def _worker(self):
        for job in iter(self.q_jobs.get, self.MAGIC_STOP):
            try:
                result = self._execute(job, self.timeout, self.wlog, self.mlog)
            except SigTerminate:
                self.q_jobs.put(job)
                sys.exit(1)

            self.q_results.put((job, result))

        if self.wlog:
            self.wlog.close()

    def __init__(self, split=None, timeout=None, wlog=None, mlog=None):
        self.results = []
        self.split = None

        self.split = split
        self.timeout = timeout

        self.wlog = wlog
        self.mlog = mlog

        if not split:
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

        self.procs = procs

    def __call__(self, job):
        if not self.split:
            result = self._execute(job, self.timeout, self.wlog)
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

    def killall(self, sig=signal.SIGTERM):
        if not self.split:
            return

        for proc in self.procs:
            if not proc.is_alive():
                continue

            os.kill(proc.pid, sig)

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

    session = Session(opt_sessions, opt_split)

    def terminate(sig, f):
        signal.signal(sig, signal.SIG_IGN)
        raise SigTerminate("caught signal (%d) to terminate" % sig, sig)

    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGTERM, terminate)

    try:
        executor = CommandExecutor(opt_split, opt_timeout, 
                                   session.wlog, session.mlog)
    except CommandExecutor.Error, e:
        usage(e)

    if opt_split:
        pids = [ proc.pid for proc in executor.procs ]
        print >> session.mlog, "session %d: split %d workers = %s" % (session.id, len(pids), 
                                                     " ".join(map(str, pids)))

    else:
        print >> session.mlog, "session %d: serial" % session.id


    jobs = []

    for line in sys.stdin.readlines():
        args = shlex.split(line)

        if isinstance(command, str):
            job = command + ' ' + fmt_argv(args)
        else:
            job = fmt_argv(command + args)

        jobs.append(job)

    try:
        for job in jobs:
            executor(job)

        executor.join()

    except SigTerminate, e:
        print >> session.mlog, str(e)
        executor.killall(e.sig)
        executor.join()

        print >> session.mlog, "session %d: terminated (%d finished, %d pending)" % (session.id,
                                                                                     len(executor.results),
                                                                                     len(jobs) - len(executor.results))

        session.save(jobs, executor.results)
        sys.exit(1)

    session.save(jobs, executor.results)

    exitcodes = [ exitcode for command, exitcode in executor.results ]

    succeeded = exitcodes.count(0)
    failed = len(exitcodes) - succeeded

    print >> session.mlog, "session %d: %d commands executed (%d succeeded, %d failed)" % \
                            (session.id, len(exitcodes), succeeded, failed)

if __name__ == "__main__":
    main()
