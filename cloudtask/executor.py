from __future__ import with_statement

import os
import time
import traceback
import copy
import signal
import re

from multiprocessing import Event
from multiprocessing_utils import Parallelize, Deferred

from sigignore import sigignore

from ssh import SSH
from _hub import Hub

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

class CloudWorker:
    Terminated = Parallelize.Worker.Terminated

    class Error(Exception):
        pass

    @classmethod
    def _stop_handler(cls, event_stop):
        def func():
            if not event_stop:
                return

            if event_stop.is_set():
                raise cls.Terminated

        return func

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
        self.user = taskconf.user

        self.address = address
        self.hub = None
        self.ssh = None

        if destroy is None:
            if address:
                destroy = False
            else:
                destroy = True
        self.destroy = destroy

        if not address:
            if not taskconf.hub_apikey:
                raise self.Error("can't auto launch a worker without a Hub API KEY")
            self.hub = Hub(taskconf.hub_apikey)

            with sigignore(signal.SIGINT, signal.SIGTERM):
                kwargs = dict([ (attr[4:], taskconf[attr]) 
                                for attr in taskconf.__all__ 
                                if attr.startswith('ec2_') ])
                kwargs['label'] = 'cloudtask: ' + taskconf.command

                self.address = self.hub.launch(1, **kwargs)[0]

            if event_stop and event_stop.is_set():
                raise self.Terminated

            self.status("launched new worker")

        else:
            self.status("using existing worker")

        self.handle_stop = self._stop_handler(event_stop)

        try:
            self.ssh = SSH(self.address, 
                           identity_file=self.session_key.path, 
                           login_name=taskconf.user,
                           callback=self.handle_stop)
        except SSH.Error, e:
            self.status("ssh error: " + str(e))
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

        if self.destroy and self.address and self.hub:
            destroyed = self.hub.destroy(self.address)
            if self.address in destroyed:
                self.status("destroyed worker")
            else:
                self.status("failed to destroy worker")

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

    def __call__(self, command):
        timeout = self.timeout

        self.handle_stop()

        self.status(str(command))
        ssh_command = self.ssh.command(command)

        timeout = Timeout(timeout)
        read_timeout = Timeout(self.ssh.TIMEOUT)
        def handler(ssh_command, buf):
            if buf and self.wlog:
                self.wlog.write(buf)
                read_timeout.reset()

            if ssh_command.running and timeout.expired():
                ssh_command.terminate()
                self.status("timeout %d # %s" % (timeout.seconds, command))
                return

            if read_timeout.expired():
                if not self.ssh.is_alive():
                    ssh_command.terminate()
                    self.status("worker died # %s" % command)
                    raise SSH.TimeoutError

                read_timeout.reset()

            self.handle_stop()
            return True

        try:
            out = ssh_command.read(handler)

        # SigTerminate raised in serial mode, the other in Parallelized mode
        except self.Terminated:
            ssh_command.terminate()
            self.status("terminated # %s" % command)
            raise

        if ssh_command.exitcode is not None:
            if ssh_command.exitcode == 255 and re.match(r'^ssh: connect to host.*:.*$', ssh_command.output):
                self.status("worker unreachable # %s" % command)
                raise SSH.Error(ssh_command.output)

            self.status("exit %d # %s" % (ssh_command.exitcode, command))

        if self.wlog:
            print >> self.wlog

        return (str(command), ssh_command.exitcode)

    def __del__(self):
        if os.getpid() != self.pid:
            return

        self._cleanup()

class CloudExecutor:
    class Error(Exception):
        pass

    def __init__(self, session, taskconf):
        self.session = session
        self.taskconf = taskconf

        split = taskconf.split
        addresses = taskconf.workers

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
        if not self.taskconf.split:
            self.results.append(result)

    def stop(self):
        if not self.taskconf.split:
            return

        self.event_stop.set()
        time.sleep(0.1)
        self._execute.stop()

    def join(self):
        if self.taskconf.split:
            self._execute.wait()
            self._execute.stop()
