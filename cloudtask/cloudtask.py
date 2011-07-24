#!/usr/bin/python
"""
Execute commands in the cloud

Resolution order for options:
1) command line (highest precedence)
2) task-level default
3) environment variable (lowest precedence)

Options:

    --user=USERNAME          Username to execute commands as (default: root)
                             environment: CLOUDTASK_USER
                
    --pre=COMMAND            Worker setup command
                             environment: CLOUDTASK_PRE

    --post=COMMAND           Worker cleanup command
                             environment: CLOUDTASK_POST

    --overlay=PATH           Path to worker filesystem overlay
                             environment: CLOUDTASK_OVERLAY

    --workers=<workers>      List of pre-launched workers to use
                             environment: CLOUDTASK_WORKERS
        
        <workers> := path/to/file | host1,host2,...hostN

    --timeout=SECS           How long to wait before giving up
                             environment: CLOUDTASK_TIMEOUT

    --split=N                How many processes to execute in parallel
                             environment: CLOUDTASK_SPLIT

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

class TaskConf:
    user = 'root'

    pre = None
    post = None
    overlay = None

    apikey = None
    command = None
    timeout = None

    split = None
    workers = None

    __all__ = [ attr for attr in dir() if not attr.startswith("_") ]
    
    def __getitem__(self, name):
        return getattr(self, name)

    def __setitem__(self, name, val):
        return setattr(self, name, val)

    def __repr__(self):
        d = {}
        for attr in self.__all__:
            d[attr] = getattr(self, attr)
        return `d`

class Task:

    DESCRIPTION = None

    @classmethod
    def usage(cls, e=None):
        if e:
            print >> sys.stderr, "error: " + str(e)

        if not cls.COMMAND:
            print >> sys.stderr, "syntax: %s [ -opts ] [ command ]" % sys.argv[0]
        else:
            print >> sys.stderr, "syntax: %s [ -opts ] [ extra args ]" % sys.argv[0]

        print >> sys.stderr, "syntax: %s [ -opts ] --resume=SESSION_ID" % sys.argv[0]
        if cls.DESCRIPTION:
            print >> sys.stderr, cls.DESCRIPTION.strip()
            print >> sys.stderr, "\n".join(__doc__.strip().splitlines()[1:])
        else:
            print >> sys.stderr, __doc__.strip()

        sys.exit(1)

    @classmethod
    def main(cls):
        usage = cls.usage

        # allow taskconf values to be overwritten from inherited classes
        taskconf = TaskConf()
        for attr in taskconf.__all__:
            taskconf[attr] = getattr(cls, attr.upper())

        opt_resume = None
        opt_sessions = os.environ.get('CLOUDTASK_SESSIONS', 
                                      join(os.environ['HOME'], '.cloudtask', 'sessions'))

        try:
            opts, args = getopt.getopt(sys.argv[1:], 
                                       'h', ['help', 
                                             'sessions=',
                                             'resume=' ] +
                                            [ attr + '=' 
                                              for attr in taskconf.__all__ ])
        except getopt.GetoptError, e:
            usage(e)

        for opt, val in opts:
            if opt in ('-h', '--help'):
                usage()

            elif opt == '--sessions':
                opt_sessions = val

            elif opt == '--resume':
                try:
                    opt_resume = int(val)
                except ValueError:
                    usage("--resume session id must be an integer")

            elif opt == '--overlay':
                if not isdir(val):
                    usage("overlay '%s' not a directory" % val)

                taskconf.overlay = val

            elif opt == '--timeout':
                taskconf.timeout = float(val)

            elif opt == '--split':
                taskconf.split = int(val)
                if taskconf.split < 1:
                    usage("bad --split value '%s'" % val)

                if taskconf.split == 1:
                    taskconf.split = None

            else:
                opt = opt[2:]
                taskconf[opt] = val

        if taskconf.workers:
            if isinstance(taskconf.workers, str):
                if isfile(taskconf.workers):
                    taskconf.workers = file(taskconf.workers).read().splitlines()
                else:
                    taskconf.workers = [ worker.strip() for worker in taskconf.workers.split(',') ]
            else:
                taskconf.workers = list(taskconf.workers)

        if cls.COMMAND:
            command = [ cls.COMMAND ] + args
        else:
            command = args

        if len(command) == 1:
            # treat command as a string if it looks complex
            if len(shlex.split(command[0])) > 1:
                command = command[0]

        if opt_resume:
            if command:
                usage("--resume incompatible with a command")

            session = Session(opt_sessions, taskconf.split, id=opt_resume)
            jobs = session.jobs.pending

            if not jobs:
                print "session %d finished" % session.id
                sys.exit(0)
            else:
                print >> session.mlog, "session %d: resuming (%d pending, %d finished)" % (session.id, len(session.jobs.pending), len(session.jobs.finished))

        else:
            if os.isatty(sys.stdin.fileno()):
                usage()

            session = Session(opt_sessions, taskconf.split)
            jobs = []
            for line in sys.stdin.readlines():
                args = shlex.split(line)

                if isinstance(command, str):
                    job = command + ' ' + fmt_argv(args)
                else:
                    job = fmt_argv(command + args)

                jobs.append(job)

        print >> session.mlog, "session %d (pid %d)" % (session.id, os.getpid())

        def terminate(sig, f):
            signal.signal(sig, signal.SIG_IGN)
            raise CloudWorker.Terminated("caught signal (%d) to terminate" % sig, sig)

        signal.signal(signal.SIGINT, terminate)
        signal.signal(signal.SIGTERM, terminate)

        executor = None

        try:
            executor = CloudExecutor(session, taskconf)
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

# set default class values to TaskConf defaults
for attr in TaskConf.__all__:
    setattr(Task, 
            attr.upper(), 
            os.environ.get('CLOUDTASK_' + attr.upper(), 
                           getattr(TaskConf, attr)))

main = Task.main

if __name__ == "__main__":
    main()

