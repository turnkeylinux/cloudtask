#!/usr/bin/python
"""
Execute tasks in the cloud

Options:

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
import time
import copy
import traceback

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

class SSH:
    class Error(Exception):
        pass

    class Command(Command):
        TIMEOUT = 30

        OPTS = ('StrictHostKeyChecking=no',
                'PasswordAuthentication=no')

        class TimeoutError(Command.Error):
            pass

        @classmethod
        def argv(cls, identity_file=None, *args):
            argv = ['ssh']
            if identity_file:
                argv += [ '-i', identity_file ]

            for opt in cls.OPTS:
                argv += [ "-o", opt ]

            argv += args
            return argv

        def __init__(self, address, command, 
                     identity_file=None, callback=None):
            self.address = address
            self.command = command
            self.callback = callback

            argv = self.argv(identity_file, address, command)
            Command.__init__(self, argv, setpgrp=True)

        def __str__(self):
            return "ssh %s %s" % (self.address, `self.command`)

        def close(self, timeout=TIMEOUT):
            finished = self.wait(timeout, callback=self.callback)
            if not finished:
                self.terminate()
                raise self.TimeoutError("ssh timed out after %d seconds" % timeout)

            if self.exitcode != 0:
                raise self.Error(self.output)

    def __init__(self, address, identity_file=None, callback=None):
        self.address = address
        self.identity_file = identity_file
        self.callback = callback

        if not self.is_alive():
            raise self.Error("%s is not alive " % address)

    def is_alive(self, timeout=Command.TIMEOUT):
        command = self.command('true')
        try:
            command.close(timeout)
        except command.TimeoutError:
            return False

        except command.Error:
            raise self.Error("unexpected error")

        return True

    def command(self, command):
        return self.Command(self.address, command, 
                            identity_file=self.identity_file,
                            callback=self.callback)

    def copy_id(self, key_path):
        if not key_path.endswith(".pub"):
            key_path += ".pub"

        command = 'mkdir -p $HOME/.ssh; cat >> $HOME/.ssh/authorized_keys'

        command = self.command(command)
        command.tochild.write(file(key_path).read())
        command.tochild.close()

        try:
            command.close()
        except command.Error, e:
            raise self.Error("can't add id to authorized keys: " + str(e))
        
    def remove_id(self, key_path):
        if not key_path.endswith(".pub"):
            key_path += ".pub"

        vals = file(key_path).read().split()
        if not vals[0].startswith('ssh'):
            raise self.Error("invalid public key in " + key_path)
        id = vals[-1]

        command = 'sed -i "/%s/d" $HOME/.ssh/authorized_keys' % id
        command = self.command(command)

        try:
            command.close()
        except command.Error, e:
            raise self.Error("can't remove id from authorized-keys: " + str(e))

    def apply_overlay(self, overlay_path):
        ssh_command = " ".join(self.Command.argv(self.identity_file))
        argv = [ 'rsync', '--timeout=%d' % self.Command.TIMEOUT, '-rHEL', '-e', ssh_command,
                overlay_path.rstrip('/') + '/', "%s:/" % self.address ]

        command = Command(argv, setpgrp=True)
        command.wait(callback=self.callback)

        if command.exitcode != 0:
            raise self.Error("rsync failed: " + command.output)

class CloudWorker:
    def __init__(self, session, taskconf, address=None, destroy=None, event_stop=None):

        self.pid = os.getpid()

        if event_stop:
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        self.event_stop = event_stop

        self.wlog = session.wlog
        self.mlog = session.mlog
        self.session_key = session.key

        self.timeout = taskconf.timeout
        self.cleanup_command = taskconf.post

        if destroy is None:
            if address:
                destroy = False
            else:
                destroy = True

        self.destroy = destroy
        self.address = address

        if not self.address:
            self.address = hub_launch(taskconf.apikey, 1)[0]
            self.status("hub launched worker")
        else:
            self.status("using existing worker")

        self.ssh = None
        try:
            self.ssh = SSH(address, 
                           identity_file=self.session_key.path, 
                           callback=self.handle_stop)
        except SSH.Error, e:
            self.status("ssh error: " + str(e))
            traceback.print_exc(file=self.wlog)

            raise

        try:
            self.ssh.copy_id(self.session_key.public)

            if taskconf.overlay:
                self.ssh.apply_overlay(taskconf.overlay)

            if taskconf.pre:
                self.ssh.command(taskconf.pre).close()

        except Exception:
            self.status("setup failed")
            traceback.print_exc(file=self.wlog)

            raise

    def _cleanup(self):
        if not self.ssh:
            return

        if self.cleanup_command:
            self.ssh.command(self.cleanup_command).close()

        self.ssh.remove_id(self.session_key.public)

    def __getstate__(self):
        return (self.address, self.pid)

    def __setstate__(self, state):
        (self.address, self.pid) = state

    def status(self, msg):
        wlog = self.wlog
        mlog = self.mlog

        if wlog:
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            print >> wlog, "# %s [%s] %s" % (timestamp, self.address, msg)

        if mlog and mlog != wlog:
            mlog.write("%s (%d): %s" % (self.address, os.getpid(), msg) + "\n")

    def handle_stop(self):
        if not self.event_stop:
            return

        if self.event_stop.is_set():
            raise Parallelize.Worker.Terminated

    def __call__(self, command):
        timeout = self.timeout

        self.handle_stop()

        self.status(str(command))
        ssh_command = self.ssh.command(command)

        timeout = Timeout(timeout)
        def handler(ssh_command, buf):
            if buf and self.wlog:
                self.wlog.write(buf)

            if ssh_command.running and timeout.expired():
                ssh_command.terminate()
                self.status("timeout %d # %s" % (timeout.seconds, command))

            self.handle_stop()
            return True

        try:
            out = ssh_command.read(handler)

        # SigTerminate raised in serial mode, the other in Parallelized mode
        except (SigTerminate, Parallelize.Worker.Terminated):
            ssh_command.terminate()
            self.status("terminated # %s" % command)
            raise

        if ssh_command.exitcode is not None:
            self.status("exit %d # %s" % (ssh_command.exitcode, command))

        if self.wlog:
            print >> self.wlog

        return (str(command), ssh_command.exitcode)

    def __del__(self):
        if os.getpid() != self.pid:
            return

        self._cleanup()

        if self.destroy and self.address:
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
            addresses = copy.copy(addresses)

            workers = []
            self.event_stop = Event()
            for i in range(split):
                if addresses:
                    address = addresses.pop(0)
                else:
                    address = None

                worker = Deferred(CloudWorker, session, taskconf, address, event_stop=self.event_stop)

                workers.append(worker)

            self._execute = Parallelize(workers)
            self.results = self._execute.results

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

    opt_pre = None
    opt_post = None
    opt_overlay = None
    opt_split = None
    opt_timeout = None
    opt_resume = None
    opt_workers = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   'h', ['help', 
                                         'overlay=',
                                         'pre=',
                                         'post=',
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

        if opt == '--split':
            opt_split = int(val)
            if opt_split < 1:
                usage("bad --split value '%s'" % val)

            if opt_split == 1:
                opt_split = None

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

    taskconf = TaskConf(overlay=opt_overlay,
                        pre=opt_pre,
                        post=opt_post,
                        timeout=opt_timeout)

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
        print >> session.mlog, str(e)

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

    print >> session.mlog, "session %d: %d commands in %d seconds (%d succeeded, %d failed)" % \
                            (session.id, len(exitcodes), session.elapsed, succeeded, failed)


if __name__ == "__main__":
    main()
