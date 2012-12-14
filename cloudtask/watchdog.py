import os
from os.path import isdir, isfile, join

import time
import signal
from multiprocessing import Process

import traceback
import re

import logalyzer
from _hub import Hub

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

class SessionWatcher(object):
    class Worker:
        def __init__(self, pid, mtime):
            self.pid = pid
            self.mtime = mtime

    def __init__(self, session_pid, workers_path):
        self.session_pid = session_pid
        self.workers_path = workers_path

    def active_workers(self):
        """returns list of tuples (worker_pid, worker_log_mtime)"""

        if not isdir(self.workers_path):
            return []

        active_workers = []
        for fname in os.listdir(self.workers_path):
            fpath = join(self.workers_path, fname)
            if not isfile(fpath):
                continue

            try:
                worker_pid = int(fname)
            except ValueError:
                continue

            if not pid_exists(worker_pid) or (self.session_pid not in (worker_pid, get_ppid(worker_pid))):
                continue

            mtime = os.stat(fpath).st_mtime
            active_workers.append(self.Worker(worker_pid, mtime))

        return active_workers
    active_workers = property(active_workers)

    def idletime(self):
        mtimes = [ worker.mtime for worker in self.active_workers ]
        if not mtimes:
            return None

        return time.time() - max(mtimes)
    idletime = property(idletime)

class Retrier:
    def __init__(self, timeout, errorsleep, errorlog=None):
        self.timeout = timeout
        self.errorsleep = errorsleep
        self.errorlog = errorlog

    def __call__(self, callable, *args, **kwargs):
        started = time.time()

        while (time.time() - started) < self.timeout:
            try:
                return callable(*args, **kwargs)

            except KeyboardInterrupt:
                break

            except:
                if self.errorlog:
                    traceback.print_exc(file=self.errorlog)

            time.sleep(self.errorsleep)

        raise

class Watchdog:
    SIGTERM_TIMEOUT = 300

    DESTROY_ERROR_TIMEOUT = 3600*3
    DESTROY_ERROR_SLEEP = 300

    @staticmethod
    def log(fh, s):
        fh.write("# watchdog: %s\n" % s)

    @classmethod
    def watch(cls, workers_path, logfh, timeout):

        session_pid = os.getppid()
        watcher = SessionWatcher(session_pid, workers_path)

        def log(s):
            cls.log(logfh, s)

        idletime = None
        
        # wait while the session exists and is not idle so long we consider it stuck
        while pid_exists(session_pid):
            time.sleep(1)

            idletime = watcher.idletime
            if idletime is None:
                continue

            if idletime > timeout:
                break


        if idletime and idletime > timeout:
            log("session idle after %d seconds" % idletime)

            # SIGTERM active workers
            for worker in watcher.active_workers:
                try:
                    log("kill -TERM %d" % worker.pid)
                    os.kill(worker.pid, signal.SIGTERM)
                except:
                    traceback.print_exc(file=session.mlog)

            # wait up to SIGTERM_TIMEOUT for them to terminate
            started = time.time()
            while time.time() - started < cls.SIGTERM_TIMEOUT:
                time.sleep(1)
                active_workers = watcher.active_workers
                if not active_workers:
                    break

            # no more Mr. Nice Guy: SIGKILL workers that are still alive
            for worker in watcher.active_workers:
                try:
                    log("kill -KILL %d" % worker.pid)
                    os.kill(worker.pid, signal.SIGKILL)
                except:
                    traceback.print_exc(file=session.mlog)

    @classmethod
    def run(cls, session, taskconf):
        class Stopped(Exception):
            pass

        # SIGTERM sent to us when parent process has finished
        def stop(s, t):
            raise Stopped
        signal.signal(signal.SIGTERM, stop)

        # we stop watching because the session ended or because it idled
        workers_path = session.paths.workers
        try:
            cls.watch(session.paths.workers, session.mlog, taskconf.timeout * 2)

        except KeyboardInterrupt:
            return

        except Stopped:
            pass

        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        cls.cleanup(session, taskconf)

    @classmethod
    def cleanup(cls, session, taskconf):

        def log(s):
            cls.log(session.mlog, s)

        def get_zombie_instances():
            wl = logalyzer.WorkersLog(session.paths.workers, taskconf.command)
            for worker in wl.workers:
                if worker.instanceid and not worker.instancetime:
                    yield worker.instanceid

        zombie_instances = list(get_zombie_instances())
        if not zombie_instances:
            return

        zombie_instances.sort()
        log("destroying zombie instances: " + " ".join(sorted(zombie_instances)))
        hub = Hub(taskconf.hub_apikey)
        retrier = Retrier(cls.DESTROY_ERROR_TIMEOUT, cls.DESTROY_ERROR_SLEEP, session.mlog)
        destroyed = retrier(hub.destroy, *zombie_instances)
        log("destroyed zombie instances: " + " ".join(sorted([ instanceid for ipaddress, instanceid in destroyed ])))


    def __init__(self, session, taskconf):
        self.session = session
        self.taskconf = taskconf

        self.process = Process(target=self.run, args=(self.session, self.taskconf))
        self.process.start()

    def terminate(self):
        self.process.terminate()

    def join(self):
        self.process.join()

