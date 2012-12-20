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

    def log(self, s):
        self.logfh.write("# watchdog: %s\n" % s)

    def watch(self):
        timeout = self.taskconf.timeout * 2

        session_pid = os.getppid()
        watcher = SessionWatcher(session_pid, self.path_workers)

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
            self.log("session idle after %d seconds" % idletime)

            # SIGTERM active workers
            for worker in watcher.active_workers:
                try:
                    self.log("kill -TERM %d" % worker.pid)
                    os.kill(worker.pid, signal.SIGTERM)
                except:
                    traceback.print_exc(file=self.logfh)

            # wait up to SIGTERM_TIMEOUT for them to terminate
            started = time.time()
            while time.time() - started < self.SIGTERM_TIMEOUT:
                time.sleep(1)
                active_workers = watcher.active_workers
                if not active_workers:
                    break

            # no more Mr. Nice Guy: SIGKILL workers that are still alive
            for worker in watcher.active_workers:
                try:
                    self.log("kill -KILL %d" % worker.pid)
                    os.kill(worker.pid, signal.SIGKILL)
                except:
                    traceback.print_exc(file=self.logfh)

    def run(self):
        class Stopped(Exception):
            pass

        # SIGINT should raise KeyboardInterrupt
        signal.signal(signal.SIGINT, signal.default_int_handler)

        # SIGTERM sent to us when parent process has finished
        def stop(s, t):
            raise Stopped
        signal.signal(signal.SIGTERM, stop)

        try:
            self.watch()

        except KeyboardInterrupt:
            return

        except Stopped:
            pass

        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        self.cleanup()

    def cleanup(self):

        def get_zombie_instances():
            wl = logalyzer.WorkersLog(self.path_workers, self.taskconf.command)
            for worker in wl.workers:
                if worker.instanceid and not worker.instancetime:
                    yield worker

        zombie_workers = list(get_zombie_instances())
        if not zombie_workers:
            return

        zombie_instances = [ worker.instanceid for worker in zombie_workers ]
        self.log("destroying zombie instances: " + " ".join(sorted(zombie_instances)))
        hub = Hub(self.taskconf.hub_apikey)
        retrier = Retrier(self.DESTROY_ERROR_TIMEOUT, self.DESTROY_ERROR_SLEEP, self.logfh)
        destroyed_instances = [ instanceid for ipaddress, instanceid in retrier(hub.destroy, *zombie_instances) ]
        self.log("destroyed zombie instances: " + " ".join(sorted(destroyed_instances)))

        # log destruction to the respective worker logs
        for zombie_worker in zombie_workers:
            if zombie_worker.instanceid not in destroyed_instances:
                continue
            worker_log = file("%s/%d" % (self.path_workers, zombie_worker.worker_id), "a")
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            print >> worker_log, "\n# %s [watchdog] destroyed worker %s" % (timestamp, zombie_worker.instanceid)
            worker_log.close()

    def __init__(self, logfh, path_workers, taskconf):
        self.logfh = logfh
        self.path_workers = path_workers
        self.taskconf = taskconf

        self.process = Process(target=self.run)
        self.process.start()

    def terminate(self):
        self.process.terminate()

    def join(self):
        self.process.join()

