import os
from os.path import isdir, isfile, join

import time
import signal
from multiprocessing import Process

import traceback
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

class SessionWatcher:
    def __init__(self, session_pid, workers_path):
        self.session_pid = session_pid
        self.workers_path = workers_path

    def get_active_workers(self):
        """returns list of tuples (worker_id, worker_log_mtime)"""

        if not isdir(self.workers_path):
            return []

        active_workers = []
        for fname in os.listdir(self.workers_path):
            fpath = join(self.workers_path, fname)
            if not isfile(fpath):
                continue

            try:
                worker_id = int(fname)
            except ValueError:
                continue

            if not pid_exists(worker_id) or (self.session_pid not in (worker_id, get_ppid(worker_id))):
                continue

            mtime = os.stat(fpath).st_mtime
            active_workers.append((worker_id, mtime))

        return active_workers

class Watchdog:
    SIGTERM_TIMEOUT = 300
    SIGTERM_TIMEOUT = 3

    @classmethod
    def watchdog(cls, session, taskconf):

        def terminate(s, t):
            print "watchdog received termination signal"

        #signal.signal(signal.SIGTERM, terminate)

        print "watchdog pid: %d, ppid: %d" % (os.getpid(), os.getppid())

        session_pid = os.getppid()
        workers_path = session.paths.workers

        watcher = SessionWatcher(session_pid, workers_path)

        def log(s):
            session.mlog.write("# watchdog: %s\n" % s)

        watchdog_timeout = taskconf.timeout * 2
        session_idle = 0

        # wait while the session exists and is not idle
        while pid_exists(session_pid) and session_idle < watchdog_timeout:
            time.sleep(1)

            mtimes = [ mtime for worker_id, mtime in watcher.get_active_workers() ]
            if not mtimes:
                continue

            session_idle = time.time() - max(mtimes)
            print "session_idle = %d" % session_idle

        if session_idle >= watchdog_timeout:
            log("session idle after %d seconds" % watchdog_timeout)

            # SIGTERM active workers
            for worker_id, worker_mtime in watcher.get_active_workers():
                try:
                    log("kill -TERM %d" % worker_id)
                    os.kill(worker_id, signal.SIGTERM)
                except:
                    traceback.print_exc(file=session.mlog)

            # wait up to SIGTERM_TIMEOUT for them to terminate
            started = time.time()
            while time.time() - started < cls.SIGTERM_TIMEOUT:
                time.sleep(1)
                active_workers = watcher.get_active_workers()
                if not active_workers:
                    break

            # no more Mr. Nice Guy: SIGKILL workers that are still alive
            for worker_id, worker_mtime in watcher.get_active_workers():
                try:
                    log("kill -KILL %d" % worker_id)
                    os.kill(worker_id, signal.SIGKILL)
                except:
                    traceback.print_exc(file=session.mlog)

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

