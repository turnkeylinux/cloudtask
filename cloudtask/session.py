import os
from os.path import *
import sys

from paths import Paths
import errno
import time

from temp import TempFile
import uuid

import executil

def makedirs(path):
    try:
        os.makedirs(path)
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

class Session:
    class Error(Exception):
        pass

    class Paths(Paths):
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

        def update(self, jobs=[], results=[]):
            for job, result in results:
                if result is None:
                    state = "TIMEOUT"
                else:
                    state = "EXIT=%s" % result

                self.finished.append((job, state))

            states = self.finished[:]

            self.pending = list((set(self.pending) | set(jobs)) - \
                                 set([ job for job, result in results ]))

            for job in self.pending:
                states.append((job, "PENDING"))

            fh = file(self.path, "w")
            for job, state in states:
                print >> fh, "%s\t%s" % (state, job)
            fh.close()

    class WorkerLog:
        def __init__(self, path):
            self.path = path
            self.fh = None

        def __getattr__(self, attr):
            if not self.fh:
                self.fh = file(join(self.path, str(os.getpid())), "a", 1)

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

    def __init__(self, taskconf, id=None):
        sessions_path = taskconf.sessions
        if not exists(sessions_path):
            makedirs(sessions_path)

        if not isdir(sessions_path):
            raise self.Error("sessions path is not a directory: " + sessions_path)

        session_ids = [ int(fname) for fname in os.listdir(sessions_path) 
                        if fname.isdigit() ]

        new_session = False if id else True
        if new_session:
            if session_ids:
                new_session_id = max(map(int, session_ids)) + 1
            else:
                new_session_id = 1

            self.id = new_session_id
        else:
            if id not in session_ids:
                raise self.Error("no such session '%s'" % `id`)

            self.id = id

        path = join(sessions_path, "%d" % self.id)
        self.paths = Session.Paths(path)

        if new_session:
            makedirs(path)
            taskconf.save(self.paths.conf)

        self.jobs = self.Jobs(self.paths.jobs)

        if taskconf.split:
            makedirs(self.paths.workers)
            self.wlog = self.WorkerLog(self.paths.workers)
            self.mlog = self.ManagerLog(self.paths.log)
                     
        else:
            self.wlog = self.ManagerLog(self.paths.log)
            self.mlog = self.wlog

        self.started = time.time()
        self.key = TempSessionKey()

        self.taskconf = taskconf

    @property
    def elapsed(self):
        return time.time() - self.started 

