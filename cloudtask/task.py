#!/usr/bin/python
"""
Execute commands in the cloud

Resolution order for options:
1) command line (highest precedence)
2) task-level default
3) CLOUDTASK_{PARAM_NAME} environment variable (lowest precedence)

Options:

    --hub-apikey=   Hub API KEY (required if launching workers)
    
    --ec2-region=   Region for instance launch (default: us-east-1)
    --ec2-size=     Instance launch size (default: m1.small)
    --ec2-type=     Instance launch type <s3|ebs> (default: s3)

    --sessions=     Path where sessions are stored (default: $HOME/.cloudtask)

    --user=         Username to execute commands as (default: root)
    --pre=          Worker setup command
    --post=         Worker cleanup command
    --overlay=      Path to worker filesystem overlay
    --timeout=      How many seconds to wait before giving up
    --split=        Number of workers to execute jobs in parallel

    --workers=      List of pre-launched workers to use
        
                    path/to/file | host-1 ... host-N

    --report=       Task reporting hook, examples:

                    sh: command || py: file || py: code
                    mail: from@foo.com to@bar.com 

Usage:

    seq 10 | cloudtask echo
    seq 10 | cloudtask --split=3 echo

    # resume session 1 while overriding timeout
    cloudtask --resume=1 --timeout=6


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

from taskconf import TaskConf
from reporter import Reporter

class Task:

    DESCRIPTION = None
    SESSIONS = None

    @staticmethod
    def error(e=None):
        print >> sys.stderr, "error: " + str(e)
        sys.exit(1)

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
        error = cls.error

        try:
            opts, args = getopt.getopt(sys.argv[1:], 
                                       'h', ['help', 
                                             'resume=',
                                             'sessions='] +
                                            [ attr.replace('_', '-') + '=' 
                                              for attr in TaskConf.__all__ ])
        except getopt.GetoptError, e:
            usage(e)

        opt_resume = None

        if cls.SESSIONS:
            opt_sessions = cls.SESSIONS
            if not opt_sessions.startswith('/'):
                opt_sessions = join(dirname(sys.argv[0]), opt_sessions)

        else:
            opt_sessions = os.environ.get('CLOUDTASK_SESSIONS',
                                          join(os.environ['HOME'], '.cloudtask'))

        for opt, val in opts:
            if opt in ('-h', '--help'):
                usage()

            elif opt == '--resume':
                try:
                    opt_resume = int(val)
                except ValueError:
                    error("--resume session id must be an integer")

            elif opt == '--sessions':
                if not isdir(val):
                    error("--sessions path '%s' is not a directory" % val)

                opt_sessions = val

        if opt_resume:
            session = Session(opt_sessions, id=opt_resume)
            taskconf = session.taskconf
        else:
            session = None

            # allow taskconf values to be overwritten from inherited classes
            taskconf = TaskConf()
            for attr in taskconf.__all__:
                taskconf[attr] = getattr(cls, attr.upper())

            if not taskconf.hub_apikey:
                taskconf.hub_apikey = os.environ.get('HUB_APIKEY')

            if taskconf.overlay and not taskconf.overlay.startswith('/'):
                taskconf.overlay = abspath(join(dirname(sys.argv[0]), taskconf.overlay))

        for opt, val in opts:
            if opt in ('--resume', '--sessions'):
                continue

            if opt == '--overlay':
                if not isdir(val):
                    error("overlay '%s' not a directory" % val)

                taskconf.overlay = abspath(val)

            elif opt == '--timeout':
                taskconf.timeout = int(val)

            elif opt == '--split':
                taskconf.split = int(val)
                if taskconf.split < 1:
                    error("bad --split value '%s'" % val)

                if taskconf.split == 1:
                    taskconf.split = None

            else:
                opt = opt[2:]
                taskconf[opt.replace('-', '_')] = val

        if taskconf.workers:
            if isinstance(taskconf.workers, str):
                if isfile(taskconf.workers):
                    taskconf.workers = file(taskconf.workers).read().splitlines()
                else:
                    taskconf.workers = [ worker.strip() for worker in taskconf.workers.split(',') ]
            else:
                taskconf.workers = list(taskconf.workers)

        if taskconf.report:
            try:
                reporter = Reporter(taskconf.report)
            except Reporter.Error, e:
                error(e)

        split = taskconf.split if taskconf.split else 1
        if len(taskconf.workers) < split and not taskconf.hub_apikey:
            error("please provide a HUB APIKEY or more pre-launched workers")

        if opt_resume:
            if args:
                error("--resume incompatible with a command")

            jobs = session.jobs.pending

            if not jobs:
                print "session %d finished" % session.id
                sys.exit(0)
            else:
                print >> session.mlog, "session %d: resuming (%d pending, %d finished)" % (session.id, len(session.jobs.pending), len(session.jobs.finished))

        else:
            if cls.COMMAND:
                command = [ cls.COMMAND ] + args
            else:
                command = args

            if len(command) == 1:
                # treat command as a string if it looks complex
                if len(shlex.split(command[0])) > 1:
                    command = command[0]

            taskconf.command = (fmt_argv(command) 
                                if isinstance(command, list) 
                                else command)

            if os.isatty(sys.stdin.fileno()):
                usage()

            jobs = []
            for line in sys.stdin.readlines():
                args = shlex.split(line)

                if isinstance(command, str):
                    job = command + ' ' + fmt_argv(args)
                else:
                    job = fmt_argv(command + args)

                jobs.append(job)

        if split > len(jobs):
            split = len(jobs)

        if not session:
            session = Session(opt_sessions)

        session.taskconf = taskconf

        print >> session.mlog, "session %d (pid %d)" % (session.id, os.getpid())

        def terminate(sig, f):
            signal.signal(sig, signal.SIG_IGN)
            raise CloudWorker.Terminated("caught signal (%d) to terminate" % sig, sig)

        signal.signal(signal.SIGINT, terminate)
        signal.signal(signal.SIGTERM, terminate)

        executor = None

        try:
            executor = CloudExecutor(split, session, taskconf)
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

        reporter.report(session)

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

