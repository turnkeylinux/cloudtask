#!/usr/bin/python
# 
# Copyright (c) 2010-2012 Liraz Siri <liraz@turnkeylinux.org>
# 
# This file is part of CloudTask.
# 
# CloudTask is open source software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 3 of the License, or (at your
# option) any later version.
# 

"""
Execute commands in the cloud

Resolution order for options:
1) command line (highest precedence)
2) task-level default
3) CLOUDTASK_{PARAM_NAME} environment variable (lowest precedence)

Options:
    --force          Don't ask for confirmation

    --hub-apikey=    Hub API KEY (required if launching workers)

    --snapshot-id=   Launch instance from a snapshot ID
    --backup-id=     TurnKey Backup ID to restore on launch
    --ami-id=        Force launch a specific AMI ID (default is the latest Core)
    
    --ec2-region=    Region for instance launch (default: us-east-1)
    --ec2-size=      Instance launch size (default: m1.small)
    --ec2-type=      Instance launch type <s3|ebs> (default: s3)

    --sessions=      Path where sessions are stored (default: $HOME/.cloudtask)

    --timeout=       How many seconds to wait for a job before failing (default: 3600)
    --retries=       How many times to retry a failed job (default: 0)
    --strikes=       How many consecutive failures before we dismiss worker (default: 0 - disabled)

    --user=          Username to execute commands as (default: root)
    --pre=           Worker setup command
    --post=          Worker cleanup command
    --overlay=       Path to worker filesystem overlay
    --split=         Number of workers to execute jobs in parallel

    --workers=       List of pre-launched workers to use
        
                     path/to/file | host-1 ... host-N

    --report=        Task reporting hook, examples:

                     sh: command || py: file || py: code
                     mail: from@foo.com to@bar.com

Usage:

    seq 10 | cloudtask echo
    seq 10 | cloudtask --split=3 echo

    # resume session 1 while overriding timeout
    cloudtask --resume=1 --timeout=6

    # retry failed jobs in session 2 while overriding the split
    cloudtask --retry=2 --split=1


"""
import os
from os.path import isdir
import sys
import shlex
import getopt
import signal
import traceback
import re
import time

from session import Session

from executor import CloudExecutor, CloudWorker
from command import fmt_argv

from taskconf import TaskConf
from reporter import Reporter
from watchdog import Watchdog

class Task:

    COMMAND = None
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
            print >> sys.stderr, "syntax: cat jobs | %s [ -opts ] [ command ]" % sys.argv[0]
        else:
            print >> sys.stderr, "syntax: %s [ -opts ] [ extra args ]" % sys.argv[0]

        print >> sys.stderr, "syntax: %s [ -opts ] --resume=SESSION_ID" % sys.argv[0]
        print >> sys.stderr, "syntax: %s [ -opts ] --retry=SESSION_ID" % sys.argv[0]
        if cls.DESCRIPTION:
            print >> sys.stderr, cls.DESCRIPTION.strip()
            print >> sys.stderr, "\n".join(__doc__.strip().splitlines()[1:])
        else:
            print >> sys.stderr, __doc__.strip()

        sys.exit(1)

    @classmethod
    def confirm(cls, taskconf, split, jobs):
        def filter(job):
            job = re.sub('^\s*', '', job[len(taskconf.command):])
            return job

        job_first = filter(jobs[0])
        job_last = filter(jobs[-1])

        job_range = ("%s .. %s" % (job_first, job_last) 
                     if job_first != job_last else "%s" % job_first)

        print >> sys.stderr, "About to launch %d cloud server%s to execute %d jobs (%s):" % (split, 
                                                                                             "s" if split and split > 1 else "",
                                                                                             len(jobs), job_range)

        print >> sys.stderr, "\n" + taskconf.fmt()

        orig_stdin = sys.stdin 
        sys.stdin = os.fdopen(sys.stderr.fileno(), 'r')
        while True:
            answer = raw_input("Is this really what you want? [yes/no] ")
            if answer:
                break
        sys.stdin = orig_stdin

        if answer.lower() != "yes":
            print >> sys.stderr, "You didn't answer 'yes'. Aborting!"
            sys.exit(1)

    @classmethod
    def main(cls):
        usage = cls.usage
        error = cls.error

        try:
            opts, args = getopt.getopt(sys.argv[1:], 
                                       'h', ['help', 
                                             'force',
                                             'resume=',
                                             'retry=',
                                             'sessions='] +
                                            [ attr.replace('_', '-') + '=' 
                                              for attr in TaskConf.__all__ ])
        except getopt.GetoptError, e:
            usage(e)

        opt_resume = None
        opt_retry = None
        opt_force = False

        if cls.SESSIONS:
            opt_sessions = cls.SESSIONS
            if not opt_sessions.startswith('/'):
                opt_sessions = os.path.join(dirname(sys.argv[0]), opt_sessions)

        else:
            opt_sessions = os.environ.get('CLOUDTASK_SESSIONS',
                                          os.path.join(os.environ['HOME'], '.cloudtask'))

        for opt, val in opts:
            if opt in ('-h', '--help'):
                usage()

            elif opt == '--resume':
                try:
                    opt_resume = int(val)
                except ValueError:
                    error("--resume session id must be an integer")

            elif opt == '--retry':
                try:
                    opt_retry = int(val)
                except ValueError:
                    error("--retry session id must be an integer")

            elif opt == '--sessions':
                if not isdir(val):
                    error("--sessions path '%s' is not a directory" % val)

                opt_sessions = val

            elif opt == '--force':
                opt_force = True

        if opt_resume and opt_retry:
            error("--retry and --resume can't be used together, different modes")

        if opt_resume:
            session = Session(opt_sessions, id=opt_resume)
            taskconf = session.taskconf
        elif opt_retry:
            session = Session(opt_sessions, id=opt_retry)
            session.jobs.update_retry_failed()
            if not session.jobs.pending:
                error("no failed jobs to retry")
            taskconf = session.taskconf
        else:
            session = None

            # allow taskconf values to be overwritten from inherited classes
            taskconf = TaskConf()
            for attr in taskconf.__all__:
                taskconf[attr] = getattr(cls, attr.upper())

            if taskconf.overlay and not taskconf.overlay.startswith('/'):
                taskconf.overlay = abspath(join(dirname(sys.argv[0]), taskconf.overlay))

        if not taskconf.hub_apikey:
            taskconf.hub_apikey = os.environ.get('HUB_APIKEY')

        for opt, val in opts:
            if opt in ('--resume', '--sessions', '--force'):
                continue

            if opt == '--overlay':
                if not isdir(val):
                    error("overlay '%s' not a directory" % val)

                taskconf.overlay = abspath(val)

            elif opt == '--split':
                taskconf.split = int(val)
                if taskconf.split < 1:
                    error("bad --split value '%s'" % val)

                if taskconf.split == 1:
                    taskconf.split = None

            elif opt == '--backup-id':
                try:
                    taskconf.backup_id = int(val)
                except ValueError:
                    error("--backup-id '%s' is not an integer " % val)

                if taskconf.backup_id < 1:
                    error("--backup-id can't be smaller than 1")

            elif opt[2:] in ('timeout', 'retries', 'strikes'):
                setattr(taskconf, opt[2:], int(val))

            else:
                opt = opt[2:]
                taskconf[opt.replace('-', '_')] = val

        if taskconf.workers:
            if isinstance(taskconf.workers, str):
                if isfile(taskconf.workers):
                    taskconf.workers = file(taskconf.workers).read().splitlines()
                else:
                    taskconf.workers = [ worker.strip() for worker in re.split('\s+', taskconf.workers) ]
            else:
                taskconf.workers = list(taskconf.workers)

        if taskconf.report:
            try:
                reporter = Reporter(taskconf.report)
            except Reporter.Error, e:
                error(e)
        else:
            reporter = None

        if opt_resume or opt_retry:
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
                line = re.sub('#.*', '', line)
                line = line.strip()
                if not line:
                    continue
                args = shlex.split(line)

                if isinstance(command, str):
                    job = command + ' ' + fmt_argv(args)
                else:
                    job = fmt_argv(command + args)

                jobs.append(job)

            if not jobs:
                error("no jobs, nothing to do")

        split = taskconf.split if taskconf.split else 1
        if split > len(jobs):
            split = len(jobs)

        if len(taskconf.workers) < split and not taskconf.hub_apikey:
            error("please provide a HUB APIKEY or more pre-launched workers")

        if os.isatty(sys.stderr.fileno()) and not opt_force :
            cls.confirm(taskconf, split, jobs)

        if not session:
            session = Session(opt_sessions)

        session.taskconf = taskconf

        ok = cls.work(jobs, split, session, taskconf)
        
        if reporter:
            reporter.report(session)

        if not ok:
            sys.exit(1)

    @classmethod
    def work(cls, jobs, split, session, taskconf):

        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        print >> session.mlog, "%s :: session %d (pid %d)\n" % (timestamp, session.id, os.getpid())

        class CaughtSignal(CloudWorker.Terminated):
            pass

        def terminate(sig, f):
            signal.signal(sig, signal.SIG_IGN)
            sigs = dict([ ( getattr(signal, attr), attr) 
                            for attr in dir(signal) if attr.startswith("SIG") ])

            raise CaughtSignal("caught %s termination signal" % sigs[sig], sig)

        watchdog = Watchdog(session, taskconf)

        signal.signal(signal.SIGINT, terminate)
        signal.signal(signal.SIGTERM, terminate)

        executor = None

        try:
            executor = CloudExecutor(split, session, taskconf)
            for job in jobs:
                executor(job)

            executor.join()
            results = executor.results

        except Exception, e:
            if isinstance(e, CaughtSignal):
                print >> session.mlog,  "# " + str(e[0])

            elif not isinstance(e, (CloudWorker.Error, CloudWorker.Terminated)):
                traceback.print_exc(file=session.mlog)

            if executor:
                executor.stop()
                results = executor.results
            else:
                results = []

        watchdog.terminate()
        watchdog.join()

        session.jobs.update(jobs, results)

        session_results = [ result for job, result in session.jobs.finished ]
        pending = len(session.jobs.pending)

        succeeded = session_results.count("EXIT=0")
        pending = len(session.jobs.pending)
        timeouts = session_results.count("TIMEOUT")
        errors = len(session_results) - succeeded - timeouts

        total = len(session.jobs.finished) + len(session.jobs.pending)

        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        print >> session.mlog, "\n%s :: session %d (%d seconds): %d/%d !OK - %d pending, %d timeouts, %d errors, %d OK" % \
                (timestamp, session.id, session.elapsed, 
                 total - succeeded, total, len(session.jobs.pending), timeouts, errors, succeeded)

        return (total - succeeded == 0)

# set default class values to TaskConf defaults
for attr in TaskConf.__all__:
    setattr(Task, 
            attr.upper(), 
            os.environ.get('CLOUDTASK_' + attr.upper(), 
                           getattr(TaskConf, attr)))

main = Task.main

if __name__ == "__main__":
    main()

