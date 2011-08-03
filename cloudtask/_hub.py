from hub import Hub as _Hub
from hub.utils import HubAPIError

import time

from StringIO import StringIO

class Hub:
    class Error(Exception):
        pass

    def __init__(self, apikey, wait_first=5, wait_status=0, wait_retry=5, retries=2):

        self.apikey = apikey
        self.wait_first = wait_first
        self.wait_status = wait_status
        self.retries = retries

    def retry(self, callable, *args, **kwargs):
        for i in range(self.retries + 1):
            try:
                return callable(*args, **kwargs)
            except HubAPIError, e:
                if e.name == 'HubAccount.InvalidApiKey':
                    raise self.Error(e)

                time.sleep(self.wait_retry)

        raise self.Error(e)

    def launch(self, howmany, **kwargs):
        """launch <howmany> workers, wait until booted and return their public IP addresses"""

        retry = self.retry
        hub = _Hub(self.apikey)

        pending_ids = set()
        yielded_ids = set()

        time_started = time.time()
        kwargs.update(sec_updates='SKIP')
        while True:
            if len(pending_ids) < howmany:
                server = retry(hub.servers.launch, 'core', **kwargs)
                pending_ids.add(server.instanceid)

            if time.time() - time_started < self.wait_first:
                continue

            servers = [ server 
                        for server in retry(hub.servers.get, refresh_cache=True)
                        if server.instanceid in (pending_ids - yielded_ids) ]

            for server in servers:
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
