import os
from os.path import isdir, isfile, join

import time
from multiprocessing import Process

import re

class Error(Exception):
    pass

def pid_exists(pid):
    return isdir("/proc/%d" % pid)

def get_ppid(pid):
    if not pid_exists(pid):
        raise Error("pid %d does not exist" % pid)
    status = file("/proc/%d/status" % pid).read()

    status_dict = dict([ (key.lower(),val) 
                          for key,val in [  re.split(r':\t\s*', line) for line in status.splitlines() ]])
    return int(status_dict['ppid'])


class Watchdog:
    @staticmethod
    def watchdog(session, taskconf):

        print "watchdog pid: %d, ppid: %d" % (os.getpid(), os.getppid())

        session_pid = os.getppid()
        workers_path = session.paths.workers

        def get_active_worker_mtimes():
            """returns list of tuples (worker_id, worker_log_mtime)"""

            if not isdir(workers_path):
                return

            for fname in os.listdir(workers_path):
                fpath = join(workers_path, fname)
                if not isfile(fpath):
                    continue

                try:
                    worker_id = int(fname)
                except ValueError:
                    continue

                if not pid_exists(worker_id) or get_ppid(worker_id) != session_pid:
                    continue

                mtime = os.stat(fpath).st_mtime
                yield worker_id, mtime


        while True:
            time.sleep(1)

            if not pid_exists(session_pid):
                break

            mtimes = [ mtime for worker_id, mtime in get_active_worker_mtimes() ]
            if not mtimes:
                continue

            session_idle = time.time() - max(mtimes)
            print "session_idle = %d" % session_idle

    @classmethod
    def run(cls, session, taskconf):
        try:
            cls.watchdog(session, taskconf)
        except KeyboardInterrupt:
            pass

    def __init__(self, session, taskconf):
        self.session = session
        self.taskconf = taskconf

        self.process = Process(target=self.run, args=(self.session, self.taskconf))
        self.process.start()

    def terminate(self):
        self.process.terminate()

    def join(self):
        self.process.join()

