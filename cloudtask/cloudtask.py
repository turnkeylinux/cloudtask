#!/usr/bin/python
"""
Execute tasks in the cloud

Options:

    --user=USERNAME          Username to execute commands as (default: root)
                
    --pre=COMMAND            Worker setup command
    --post=COMMAND           Worker cleanup command

    --overlay=PATH           Path to worker filesystem overlay
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
import signal
import traceback

from session import Session

from executor import CloudExecutor, CloudWorker
from command import fmt_argv

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
    def __init__(self, apikey=None, command=None, overlay=None, pre=None, post=None, timeout=None, user=None):

        self.apikey = apikey
        self.command = command
        self.overlay = overlay
        self.pre = pre
        self.post = post
        self.timeout = timeout

        self.user = user

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

class Task:
    USER = 'root'

    COMMAND = None
    PRE = None
    POST = None
    OVERLAY = None
    SPLIT = None
    TIMEOUT = None
    WORKERS = None

    @classmethod
    def main(cls):
        opt_sessions = os.environ.get('CLOUDTASK_SESSIONS', 
                                      join(os.environ['HOME'], '.cloudtask', 'sessions'))

        opt_pre = cls.PRE
        opt_post = cls.POST
        opt_overlay = cls.OVERLAY
        opt_split = cls.SPLIT
        opt_timeout = cls.TIMEOUT
        opt_workers = cls.WORKERS
        opt_user = cls.USER

        opt_resume = None

        try:
            opts, args = getopt.getopt(sys.argv[1:], 
                                       'h', ['help', 
                                             'overlay=',
                                             'pre=',
                                             'post=',
                                             'timeout=',
                                             'user=',
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

            if opt == '--pre':
                opt_pre = val

            if opt == '--post':
                opt_post = val

            if opt == '--overlay':
                if not isdir(val):
                    usage("overlay '%s' not a directory" % val)

                opt_overlay = val

            if opt == '--timeout':
                opt_timeout = float(val)

            if opt == '--user':
                opt_user = val


            if opt == '--split':
                opt_split = int(val)
                if opt_split < 1:
                    usage("bad --split value '%s'" % val)

                if opt_split == 1:
                    opt_split = None

            if opt == '--sessions':
                opt_sessions = val

            if opt == '--workers':
                opt_workers = val

            if opt == '--resume':
                try:
                    opt_resume = int(val)
                except ValueError:
                    usage("--resume session id must be an integer")


        if opt_workers:
            if isinstance(opt_workers, str):
                if isfile(opt_workers):
                    opt_workers = file(opt_workers).read().splitlines()
                else:
                    opt_workers = [ worker.strip() for worker in opt_workers.split(',') ]
            else:
                opt_workers = list(opt_workers)

        if cls.COMMAND:
            command = [ cls.COMMAND ] + args
        else:
            if not args:
                usage()

            command = args

        if len(command) == 1:
            # treat command as a string if it looks complex
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

        taskconf = TaskConf(overlay=opt_overlay,
                            pre=opt_pre,
                            post=opt_post,
                            timeout=opt_timeout,
                            user=opt_user)

        print >> session.mlog, "session %d (pid %d)" % (session.id, os.getpid())

        def terminate(sig, f):
            signal.signal(sig, signal.SIG_IGN)
            raise SigTerminate("caught signal (%d) to terminate" % sig, sig)

        signal.signal(signal.SIGINT, terminate)
        signal.signal(signal.SIGTERM, terminate)

        executor = None

        try:
            executor = CloudExecutor(session, taskconf, opt_split, opt_workers)

            for job in jobs:
                executor(job)

            executor.join()

        except Exception, e:
            if not isinstance(e, CloudWorker.Error):
                traceback.print_exc(file=session.mlog)

            if executor:
                executor.stop()
                results = executor.results
            else:
                results = []

            session.jobs.update(jobs, results)
            print >> session.mlog, "session %d: terminated (%d finished, %d pending)" % \
                                    (session.id, 
                                     len(session.jobs.finished), 
                                     len(session.jobs.pending))

            sys.exit(1)

        session.jobs.update(jobs, executor.results)

        exitcodes = [ exitcode for command, exitcode in executor.results ]

        succeeded = exitcodes.count(0)
        failed = len(exitcodes) - succeeded

        print >> session.mlog, "session %d: %d jobs in %d seconds (%d succeeded, %d failed)" % \
                                (session.id, len(exitcodes), session.elapsed, succeeded, failed)

        if session.jobs.pending:
            print >> session.mlog, "session %d: no workers left alive, %d jobs pending" % (session.id, len(session.jobs.pending))
            sys.exit(1)

main = Task.main

if __name__ == "__main__":
    main()

