#!/usr/bin/python
# 
# Copyright (c) 2010-2011 Liraz Siri <liraz@turnkeylinux.org>
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
    --backup-id=     TurnKey Backup ID to restore on launch
    --ec2-region=    Region for instance launch (default: us-east-1)
    --ec2-size=      Instance launch size (default: m1.small)
    --ec2-type=      Instance launch type <s3|ebs> (default: s3)

    --sessions=      Path where sessions are stored (default: $HOME/.cloudtask)

    --timeout=       How many seconds to wait before giving up (default: 3600)
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


"""
import os
from os.path import *
import sys
import shlex
import getopt
import signal
import traceback
import re

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
            print >> sys.stderr, "syntax: cat jobs | %s [ -opts ] [ command ]" % sys.argv[0]
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
    def confirm(cls, taskconf, split, jobs):
        print >> sys.stderr, "About to launch %d cloud server%s to execute the following task:" % (split, "s" if split and split > 1 else "")

        def filter(job):
            job = re.sub('^\s*', '', job[len(taskconf.command):])
            return job

        job_first = filter(jobs[0])
        job_last = filter(jobs[-1])

        job_range = ("%s .. %s" % (job_first, job_last) 
                     if job_first != job_last else "%s" % job_first)

        table = [ ('jobs', '%d (%s)' % (len(jobs), job_range)) ]

        for attr in ('split', 'command', 'hub-apikey', 
                     'ec2-region', 'ec2-size', 'ec2-type', 
                     'user', 'backup-id', 'workers', 
                     'overlay', 'post', 'pre', 'timeout', 'report'):

            val = taskconf[attr.replace('-', '_')]
            if isinstance(val, list):
                val = " ".join(val)
            if not val:
                val = "-"
            table.append((attr, val))

        print >> sys.stderr
        print >> sys.stderr, "  Parameter       Value"
        print >> sys.stderr, "  ---------       -----"
        print >> sys.stderr
        for row in table:
            print >> sys.stderr, "  %-15s %s" % (row[0], row[1])

        print >> sys.stderr

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
                                             'sessions='] +
                                            [ attr.replace('_', '-') + '=' 
                                              for attr in TaskConf.__all__ ])
        except getopt.GetoptError, e:
            usage(e)

        opt_resume = None
        opt_force = False

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

            elif opt == '--force':
                opt_force = True

        if opt_resume:
            session = Session(opt_sessions, id=opt_resume)
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

            elif opt == '--timeout':
                taskconf.timeout = int(val)

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
        else:
            reporter = None

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

        if reporter:
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

