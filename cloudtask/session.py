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
from os.path import *
import sys

import paths
import errno
import time

from temp import TempFile
import uuid

import executil

from taskconf import TaskConf
import pprint

import re

def makedirs(path, mode=0750):
    try:
        os.makedirs(path, mode)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise

class TempSessionKey(TempFile):
    def __init__(self):
        TempFile.__init__(self, prefix='key_')
        os.remove(self.path)

        self.uuid = uuid.uuid4()
        executil.getoutput("ssh-keygen -N '' -f %s -C %s" % (self.path, self.uuid))

    @property
    def public(self):
        return self.path + ".pub"

    def __del__(self):
        if os.getpid() == self.pid:
            os.remove(self.public)

        TempFile.__del__(self)

class UNDEFINED:
    pass

class Session(object):
    class Error(Exception):
        pass

    class Paths(paths.Paths):
        files = ['conf', 'workers', 'log', 'jobs']

    class Jobs:
        def __init__(self, path):
            self.pending = []
            self.finished = []
            self.path = path

            if not exists(path):
                return

            for line in file(path).readlines():
                line = line.strip()
                state, command = line.split('\t', 1)
                if state == "PENDING":
                    self.pending.append(command)
                else:
                    self.finished.append((command, state))

        def save(self):
            fh = file(self.path, "w")

            for job, result in self.finished:
                print >> fh, "%s\t%s" % (result, job)

            for job in self.pending:
                print >> fh, "PENDING\t%s" % job

            fh.close()

        def update(self, jobs=[], results=[]):
            for job, result in results:
                if result is None:
                    state = "TIMEOUT"
                else:
                    state = "EXIT=%s" % result

                self.finished.append((job, state))

            self.pending = list((set(self.pending) | set(jobs)) - \
                                 set([ job for job, result in results ]))

            self.save()

        def update_retry_failed(self):
            finished = set(self.finished)
            failed = set([(job, result) for job, result in finished if result != 'EXIT=0'])
            ok = finished - failed

            self.finished = list(ok)
            self.pending += [ job for job, result in failed ]

            self.save()

    class WorkerLog(object):
        def fh(self):
            if not self._fh:
                self._fh = file(join(self.path, str(os.getpid())), "a", 1)

            return self._fh
        status = fh = property(fh)


        def __init__(self, path, tee=False):
            self._fh = None
            self.path = path
            self.tee = tee

        @staticmethod
        def _filter(buf):
            buf = re.sub(r'Connection to \S+ closed\.\r+\n', '', buf)
            buf = re.sub(r'\r[^\r\n]+$', '', buf)
            buf = re.sub(r'.*\r(?![\r\n])','', buf)
            buf = re.sub(r'\r+\n', '\n', buf)

            return buf

        def write(self, buf):
            if self.tee:
                sys.stdout.write(buf)
                sys.stdout.flush()

            # filter progress bars and other return-carriage crap
            buf = self._filter(buf)

            if buf:
                self.fh.write(buf)

        def __getattr__(self, attr):
            return getattr(self.fh, attr)

    class ManagerLog:
        def __init__(self, path):
            self.fh = file(path, "a", 1)

        def write(self, buf):
            self.fh.write(buf)
            sys.stdout.write(buf)
            sys.stdout.flush()

        def __getattr__(self, attr):
            return getattr(self.fh, attr)

    @staticmethod
    def new_session_id(sessions_path):
        while True:
            session_ids = [ int(fname) for fname in os.listdir(sessions_path) 
                            if fname.isdigit() ]

            if session_ids:
                new_session_id = max(map(int, session_ids)) + 1
            else:
                new_session_id = 1

            id = new_session_id
            try:
                os.mkdir(join(sessions_path, "%d" % id))
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise
                continue

            return id

    def __init__(self, sessions_path, id=None):
        if not exists(sessions_path):
            makedirs(sessions_path)

        if not isdir(sessions_path):
            raise self.Error("sessions path is not a directory: " + sessions_path)

        if not id:
            id = self.new_session_id(sessions_path)

        path = join(sessions_path, "%d" % id)
        if not isdir(path):
            raise self.Error("no such session '%s'" % id)

        self.paths = Session.Paths(path)
        self.jobs = self.Jobs(self.paths.jobs)

        self._wlog = None
        self._mlog = None

        self.started = time.time()
        self.key = TempSessionKey()

        self.id = id

    def taskconf(self, val=UNDEFINED):
        path = self.paths.conf

        if val is UNDEFINED:
            return TaskConf.fromdict(eval(file(path).read()))
        else:
            d = val.dict()
            del d['hub_apikey']
            print >> file(path, "w"), pprint.pformat(d)
    taskconf = property(taskconf, taskconf)

    @property
    def elapsed(self):
        return time.time() - self.started 

    @property
    def wlog(self):
        if self._wlog:
            return self._wlog

        makedirs(self.paths.workers)
        wlog = self.WorkerLog(self.paths.workers, False if self.taskconf.split else True) 

        self._wlog = wlog
        return wlog

    @property
    def mlog(self):
        if self._mlog:
            return self._mlog

        mlog = self.ManagerLog(self.paths.log) 

        self._mlog = mlog
        return mlog
