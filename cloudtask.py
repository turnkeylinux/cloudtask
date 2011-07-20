#!/usr/bin/python
"""
Execute tasks in the cloud

Options:

    --workers=<workers>      List of pre-launched workers to use
        
        <workers> := path/to/file | host1,host2,...hostN

    --timeout=SECS           How long to wait before giving up
    --split=N                How many processes to execute in parallel
    --sessions=PATH          Path to location where sessions are stored
                             environment: CLOUDTASK_SESSIONS
                             default: $HOME/.cloudtask/sessions

    --resume=ID              Resume session

Usage:

    seq 10 | cloudtask echo
    seq 10 | cloudtask --split=3 echo
    cloudtask --timeout=6 --split=3 --resume=1


"""
import os
from os.path import *
import sys
import shlex
import getopt
import time
import copy

import signal

from command import Command, fmt_argv
from session import Session

from multiprocessing import Event
from multiprocessing_utils import Parallelize, Deferred

class SigTerminate(Exception):
    def __init__(self, msg, sig):
        self.sig = sig
        Exception.__init__(self, msg)

class AttrDict(dict):
    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError("no such attribute '%s'" % name)

    def __setattr__(self, name, val):
        self[name] = val

class TaskConf(AttrDict):
    def __init__(self, apikey=None, command=None, overlay=None, pre=None, post=None, timeout=None):

        self.apikey = apikey
        self.command = command
        self.overlay = overlay
        self.pre = pre
        self.post = post
        self.timeout = timeout

def hub_launch(apikey, howmany):
    raise Exception("not implemented")

def hub_destroy(apikey, addresses):
    raise Exception("not implemented")

class Timeout:
    def __init__(self, seconds=None):
        """If seconds is None, timeout never expires"""
        self.seconds = seconds
        self.started = time.time()

    def expired(self):
        if self.seconds and time.time() - self.started > self.seconds:
            return True
        return False

class SSHCommand(Command):
    def __init__(self, address, command):
        opts = ('StrictHostKeyChecking=no',
                'PasswordAuthentication=no')

        argv = ['ssh']
        for opt in opts:
            argv += [ "-o", opt ]

        argv += [ address, command ]
        Command.__init__(self, argv, setpgrp=True)

class CloudWorker:
    def __init__(self, session, taskconf, address=None, destroy=None, event_stop=None):

        self.event_stop = event_stop

        self.wlog = session.wlog
        self.mlog = session.mlog
        self.session_key = session.key

        self.timeout = taskconf.timeout

        if destroy is None:
            if address:
                destroy = False
            else:
                destroy = True

        self.destroy = destroy

        if not address:
            address = hub_launch(taskconf.apikey, 1)[0]

        self.address = address
        self.pid = os.getpid()

    def __getstate__(self):
        return (self.address, self.pid)

    def __setstate__(self, state):
        (self.address, self.pid) = state

    def __call__(self, command):
        if self.event_stop:
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        timeout = self.timeout

        wlog = self.wlog
        mlog = self.mlog
        
        def status(msg):
            if wlog:
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                print >> wlog, "# %s: %s" % (timestamp, msg)

            if mlog and mlog != wlog:
                mlog.write("%d: %s" % (os.getpid(), msg) + "\n")

        def handle_stop():
            if not self.event_stop:
                return

            if self.event_stop.is_set():
                raise Parallelize.Worker.Terminated

        handle_stop()

        command = SSHCommand(self.address, command)
        status(str(command))

        timeout = Timeout(timeout)
        def handler(command, buf):
            if buf and wlog:
                wlog.write(buf)

            if command.running and timeout.expired():
                command.terminate()
                status("timeout %d # %s" % (timeout.seconds, command))

            handle_stop()
            return True

        try:
            out = command.read(handler)

        # SigTerminate raised in serial mode, the other in Parallelized mode
        except (SigTerminate, Parallelize.Worker.Terminated):
            command.terminate()
            status("terminated # %s" % command)
            raise

        if command.exitcode is not None:
            status("exit %d # %s" % (command.exitcode, command))

        if wlog:
            print >> wlog

        return (str(command), command.exitcode)

    def __del__(self):
        if os.getpid() != self.pid:
            return

        if self.destroy:
            hub_destroy(self.taskconf.apikey, [ self.address ])

class CloudExecutor:
    class Error(Exception):
        pass

    def __init__(self, session, taskconf, split=None, addresses=[]):
        self.session = session
        self.taskconf = taskconf
        self.split = split

        if not split:
            if addresses:
                address = addresses[0]
            else:
                address = None
            self._execute = CloudWorker(session, taskconf, address)
            self.results = []

        else:
            if split < 2:
                raise self.Error("bad split (%d) minimum is 2" % split)
            
            addresses = copy.copy(addresses)

            workers = []
            event_stop = Event()
            for i in range(split):
                if addresses:
                    address = addresses.pop(0)
                else:
                    address = None

                worker = Deferred(CloudWorker, session, taskconf, address, event_stop=event_stop)

                workers.append(worker)

            self._execute = Parallelize(workers)
            self.results = self._execute.results
            self.event_stop = event_stop

    def __call__(self, job):
        result = self._execute(job)
        if not self.split:
            self.results.append(result)

    def stop(self):
        if not self.split:
            return

        self.event_stop.set()
        time.sleep(0.1)
        self._execute.stop()

    def join(self):
        if self.split:
            self._execute.wait()
            self._execute.stop()

def error(e):
    print >> sys.stderr, "error: " + str(e)
    sys.exit(1)

def usage(e=None):
    if e:
        print >> sys.stderr, "error: " + str(e)

    print >> sys.stderr, "syntax: %s [ -opts ] [ command ]" % sys.argv[0]
    print >> sys.stderr, "syntax: %s [ -opts ] --resume=SESSION_ID" % sys.argv[0]
    print >> sys.stderr, __doc__.strip()
    sys.exit(1)

def main():
    opt_sessions = os.environ.get('CLOUDTASK_SESSIONS', 
                                  join(os.environ['HOME'], '.cloudtask', 'sessions'))

    opt_split = None
    opt_timeout = None
    opt_resume = None
    opt_workers = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   'h', ['help', 
                                         'timeout=',
                                         'split=',
                                         'sessions=',
                                         'resume=',
                                         'workers=',
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

        if opt == '--workers':
            if isfile(val):
                opt_workers = file(val).read().splitlines()
            else:
                opt_workers = [ worker.strip() for worker in val.split(',') ]

        if opt == '--resume':
            try:
                opt_resume = int(val)
            except ValueError:
                usage("--resume session id must be an integer")


    if not args:
        usage()

    command = args

    if len(command) == 1:
        if len(shlex.split(command[0])) > 1:
            command = command[0]

    if opt_resume:
        if command:
            usage("--resume incompatible with a command")

        session = Session(opt_sessions, opt_split, id=opt_resume)
        jobs = session.jobs.pending

        if not jobs:
            print "session %d finished" % session.id
            sys.exit(0)
        else:
            print >> session.mlog, "session %d: resuming (%d pending, %d finished)" % (session.id, len(session.jobs.pending), len(session.jobs.finished))

    else:
        session = Session(opt_sessions, opt_split)
        jobs = []
        for line in sys.stdin.readlines():
            args = shlex.split(line)

            if isinstance(command, str):
                job = command + ' ' + fmt_argv(args)
            else:
                job = fmt_argv(command + args)

            jobs.append(job)

    taskconf = TaskConf(timeout=opt_timeout)

    try:
        executor = CloudExecutor(session, taskconf, opt_split, opt_workers)
    except CloudExecutor.Error, e:
        usage(e)

    print >> session.mlog, "session %d (pid %d)" % (session.id, os.getpid())

    def terminate(sig, f):
        signal.signal(sig, signal.SIG_IGN)
        raise SigTerminate("caught signal (%d) to terminate" % sig, sig)

    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGTERM, terminate)

    try:
        for job in jobs:
            executor(job)

        executor.join()

    except SigTerminate, e:
        print >> session.mlog, str(e)
        executor.stop()

        session.jobs.update(jobs, executor.results)
        print >> session.mlog, "session %d: terminated (%d finished, %d pending)" % \
                                (session.id, 
                                 len(session.jobs.finished), 
                                 len(session.jobs.pending))

        sys.exit(1)

    session.jobs.update(jobs, executor.results)

    exitcodes = [ exitcode for command, exitcode in executor.results ]

    succeeded = exitcodes.count(0)
    failed = len(exitcodes) - succeeded

    print >> session.mlog, "session %d: %d commands in %d seconds (%d succeeded, %d failed)" % \
                            (session.id, len(exitcodes), session.elapsed, succeeded, failed)


if __name__ == "__main__":
    main()
