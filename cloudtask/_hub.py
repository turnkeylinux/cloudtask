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

from hub import Hub as _Hub
from hub.utils import HubAPIError

import time

from StringIO import StringIO

class Hub:
    WAIT_FIRST = 30
    WAIT_STATUS = 10
    WAIT_RETRY = 5

    RETRIES = 2

    PENDING_TIMEOUT = 300

    class Error(Exception):
        pass

    class Stopped(Error):
        pass

    def __init__(self, apikey, wait_first=WAIT_FIRST, wait_status=WAIT_STATUS, wait_retry=WAIT_RETRY, retries=RETRIES):

        self.apikey = apikey
        self.wait_first = wait_first
        self.wait_status = wait_status
        self.wait_retry = wait_retry
        self.retries = retries

    def retry(self, callable, *args, **kwargs):
        for i in range(self.retries + 1):
            try:
                return callable(*args, **kwargs)
            except HubAPIError, e:
                if e.name == 'HubAccount.InvalidApiKey':
                    raise self.Error(e)

                if e.name == 'BackupRecord.NotFound':
                    raise self.Error(e)

                time.sleep(self.wait_retry)

        raise self.Error(e)

    def launch(self, howmany, callback=None, **kwargs):
        """launch <howmany> workers, wait until booted and return their public IP addresses.

        Invoke callback every frequently. If callback returns False, we terminate launching.
        """

        retry = self.retry
        hub = _Hub(self.apikey)

        pending_ids = set()
        yielded_ids = set()

        time_started = time.time()
        kwargs.update(sec_updates='SKIP')

        name = kwargs.pop('backup_id', None)
        if not name:
            name = 'core'

        def get_pending_servers():
            return [ server 
                     for server in retry(hub.servers.get, refresh_cache=True)
                     if server.instanceid in (pending_ids - yielded_ids) ]

        stopped = None
        while True:

            if callback and not stopped:
                if callback() is False:
                    stopped = time.time()

            if stopped:

                servers = [ server for server in get_pending_servers() ]

                if not servers:
                    raise self.Stopped

                for server in servers:
                    if server.status == 'running':
                        retry(server.destroy, auto_unregister=True)
                        pending_ids.remove(server.instanceid)

                    if server.status == 'pending' and \
                       (time.time() - stopped > self.PENDING_TIMEOUT):
                        raise self.Error("stuck pending instance")

                time.sleep(self.wait_status)
                continue

            if len(pending_ids) < howmany:
                server = retry(hub.servers.launch, name, **kwargs)
                pending_ids.add(server.instanceid)

            if time.time() - time_started < self.wait_first:
                continue

            for server in get_pending_servers():
                if server.status != 'running' or server.boot_status != 'booted':
                    continue

                yielded_ids.add(server.instanceid)
                yield server.ipaddress

            if len(yielded_ids) == howmany:
                break

            time.sleep(self.wait_status)

    def destroy(self, *addresses):
        if not addresses:
            return

        hub = _Hub(self.apikey)
        retry = self.retry

        destroyable = [ server
                        for server in retry(hub.servers.get, refresh_cache=True)
                        if server.ipaddress in addresses ]

        for server in destroyable:
            retry(server.destroy, auto_unregister=True)

        return [ server.ipaddress for server in destroyable ]
