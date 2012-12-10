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

import os
import time
import traceback
import copy
import signal
import re

from multiprocessing import Event, Queue
from multiprocessing_utils import Parallelize, Deferred

import sighandle

from ssh import SSH
from _hub import Hub

import threading

class Timeout:
    def __init__(self, seconds=None):
        """If seconds is None, timeout never expires"""
        self.seconds = seconds
        self.started = time.time()

    def expired(self):
        if self.seconds and time.time() - self.started > self.seconds:
            return True
        return False

    def reset(self):
        self.started = time.time()

class Job:
    class Retry(Parallelize.Worker.Retry):
        pass

    def __init__(self, command, retry_limit):
        self.command = command
        self.retry = 0
        self.retry_limit = retry_limit

class CloudWorker:
    SSH_PING_RETRIES = 3

    Terminated = Parallelize.Worker.Terminated

    class Error(Terminated):
        pass

    @classmethod
    def _stop_handler(cls, event_stop):
        def func():
            if not event_stop:
                return

            if event_stop.is_set():
                raise cls.Terminated

        return func

    def __init__(self, session, taskconf, ipaddress=None, destroy=None, event_stop=None, launchq=None):

        self.pid = os.getpid()

        if event_stop:
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        self.event_stop = event_stop

        self.wlog = session.wlog
        self.mlog = session.mlog
        self.session_key = session.key

        self.strikes = taskconf.strikes
        self.strike = 0

        self.timeout = taskconf.timeout
        self.cleanup_command = taskconf.post
        self.user = taskconf.user

        self.ipaddress = ipaddress
        self.instanceid = None

        self.hub = None
        self.ssh = None

        if destroy is None:
            if ipaddress:
                destroy = False
            else:
                destroy = True
        self.destroy = destroy

        if not ipaddress:
            if not taskconf.hub_apikey:
                raise self.Error("can't auto launch a worker without a Hub API KEY")
            self.hub = Hub(taskconf.hub_apikey)

            if launchq:
                with sighandle.sigignore(signal.SIGINT, signal.SIGTERM):
                    instance = launchq.get()
            else:
                class Bool:
                    value = False
                stopped = Bool()

                def handler(s, f):
                    stopped.value = True

                with sighandle.sighandle(handler, signal.SIGINT, signal.SIGTERM):
                    def callback():
                        return not stopped.value

                    instance = list(self.hub.launch(1, VerboseLog(session.mlog), callback, **taskconf.ec2_opts))[0]

            if not instance or (event_stop and event_stop.is_set()):
                raise self.Terminated

            self.ipaddress, self.instanceid = instance

            self.status("launched worker %s" % self.instanceid)

        else:
            self.status("using existing worker")

        self.handle_stop = self._stop_handler(event_stop)

        try:
            self.ssh = SSH(self.ipaddress, 
                           identity_file=self.session_key.path, 
                           login_name=taskconf.user,
                           callback=self.handle_stop)
        except SSH.Error, e:
            self.status("unreachable via ssh: " + str(e))
            traceback.print_exc(file=self.wlog)

            raise self.Error(e)

        try:
            self.ssh.copy_id(self.session_key.public)

            if taskconf.overlay:
                self.ssh.apply_overlay(taskconf.overlay)

            if taskconf.pre:
                self.ssh.command(taskconf.pre).close()

        except Exception, e:
            self.status("setup failed")
            traceback.print_exc(file=self.wlog)

            raise self.Error(e)

    def _cleanup(self):
        if self.ssh:
            try:
                self.ssh.callback = None
                if self.cleanup_command:
                    self.ssh.command(self.cleanup_command).close()

                self.ssh.remove_id(self.session_key.public)
            except:
                pass

        if self.destroy and self.ipaddress and self.hub:
            try:
                destroyed = [ (ipaddress, instanceid) 
                              for ipaddress, instanceid in self.hub.destroy(self.ipaddress) 
                              if ipaddress == self.ipaddress ]
                
                if destroyed:
                    ipaddress, instanceid = destroyed[0]
                    self.status("destroyed worker %s" % instanceid)
                else:
                    raise self.Error("Hub didn't destroy worker instance as requested!")
            except:
                self.status("failed to destroy worker %s" % self.instanceid)
                traceback.print_exc(file=self.wlog)
                raise

    def __getstate__(self):
        return (self.ipaddress, self.pid)

    def __setstate__(self, state):
        (self.ipaddress, self.pid) = state

    def status(self, msg, after_output=False):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

        c = "\n" if after_output else ""
        self.wlog.status.write(c + "# %s [%s] %s\n" % (timestamp, self.ipaddress, msg))
        self.mlog.write("%s (%d): %s\n" % (self.ipaddress, os.getpid(), msg))

    def __call__(self, job):
        command = job.command
        timeout = self.timeout

        self.handle_stop()

        self.status(str(command))
        ssh_command = self.ssh.command(command, pty=True)

        timeout = Timeout(timeout)
        read_timeout = Timeout(self.ssh.TIMEOUT)

        class CommandTimeout(Exception):
            pass

        class WorkerDied(Exception):
            pass

        def handler(ssh_command, buf):
            if buf:
                read_timeout.reset()
                self.wlog.write(buf)

            if ssh_command.running and timeout.expired():
                raise CommandTimeout

            if read_timeout.expired():
                for retry in range(self.SSH_PING_RETRIES):
                    try:
                        self.ssh.ping()
                        break
                    except self.ssh.Error, e:
                        pass
                else:
                    raise WorkerDied(e)

                read_timeout.reset()

            self.handle_stop()
            return True

        try:
            out = ssh_command.read(handler)

        # SigTerminate raised in serial mode, the other in Parallelized mode
        except self.Terminated:
            self.status("terminated # %s" % command, True)
            raise

        except WorkerDied, e:
            self.status("worker died (%s) # %s" % (e, command), True)
            raise self.Error(e)

        except CommandTimeout:
            self.status("timeout # %s" % command, True)
            exitcode = None

        else:
            if ssh_command.exitcode == 255 and re.match(r'^ssh: connect to host.*:.*$', ssh_command.output):
                self.status("worker unreachable # %s" % command)
                self.wlog.write("%s\n" % ssh_command.output)
                raise self.Error(SSH.Error(ssh_command.output))

            self.status("exit %d # %s" % (ssh_command.exitcode, command), True)
            exitcode = ssh_command.exitcode

        finally:
            ssh_command.terminate()

        if ssh_command.exitcode != 0:
            self.strike += 1
            if self.strikes and self.strike >= self.strikes:
                self.status("terminating worker after %d strikes" % self.strikes)
                raise self.Error

            if job.retry < job.retry_limit:
                job.retry += 1
                self.status("will retry (%d of %d)" % (job.retry, job.retry_limit))

                raise job.Retry
        else:
            self.strike = 0

        return (str(command), exitcode)

    def __del__(self):
        if os.getpid() != self.pid:
            return

        self._cleanup()

class VerboseLog:
    def __init__(self, fh):
        self.fh = fh

    def write(self, s):
        self.fh.write("# " + s)

class CloudExecutor:
    class Error(Exception):
        pass

    def __init__(self, split, session, taskconf):
        ipaddresses = taskconf.workers
        if split == 1:
            split = False

        if not split:
            if ipaddresses:
                ipaddress = ipaddresses[0]
            else:
                ipaddress = None
            self._execute = CloudWorker(session, taskconf, ipaddress)
            self.results = []

        else:
            ipaddresses = copy.copy(ipaddresses)

            workers = []
            self.event_stop = Event()
            
            launchq = None

            new_workers = split - len(ipaddresses)
            if new_workers > 0:
                if not taskconf.hub_apikey:
                    raise self.Error("need API KEY to launch %d new workers" % new_workers)

                launchq = Queue()
                def thread():

                    def callback():
                        return not self.event_stop.is_set()

                    hub = Hub(taskconf.hub_apikey)
                    i = None
                    try:
                        for i, instance in enumerate(hub.launch(new_workers, VerboseLog(session.mlog), callback, **taskconf.ec2_opts)):
                            launchq.put(instance)
                    except Exception, e:
                        unlaunched_workers = new_workers - (i + 1) \
                                             if i is not None \
                                             else new_workers

                        for i in range(unlaunched_workers):
                            launchq.put(None)

                        if not isinstance(e, hub.Stopped):
                            traceback.print_exc(file=session.mlog)

                threading.Thread(target=thread).start()

            for i in range(split):
                if ipaddresses:
                    ipaddress = ipaddresses.pop(0)
                else:
                    ipaddress = None

                worker = Deferred(CloudWorker, session, taskconf, ipaddress, 
                                  event_stop=self.event_stop, launchq=launchq)

                workers.append(worker)

            self._execute = Parallelize(workers)
            self.results = self._execute.results

        self.split = split
        self.job_retry_limit = taskconf.retries

    def __call__(self, job):
        if not isinstance(job, Job):
            job = Job(job, self.job_retry_limit)

        if self.split:
            return self._execute(job)

        try:
            result = self._execute(job)
        except job.Retry:
            return self(job)

        self.results.append(result)

    def stop(self):
        if not self.split:
            self._execute = None
            return

        self.event_stop.set()
        time.sleep(0.1)
        self._execute.stop()

    def join(self):
        if self.split:
            self._execute.wait(keepalive=False, keepalive_spares=1)

        self.stop()
