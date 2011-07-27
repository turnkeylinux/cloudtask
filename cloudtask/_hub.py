from hub import Hub as _Hub
from hub.utils import HubAPIError

import time

from StringIO import StringIO
import traceback

def get_traceback():
    sio = StringIO()
    traceback.print_exc(file=sio)
    return sio.getvalue()

class Hub:
    class Error(Exception):
        pass

    #def __init__(self, apikey, wait_first=30, wait_interval=15):
    def __init__(self, apikey, wait_first=1, wait_interval=1, retries=1):
        self.apikey = apikey
        self.wait_first = wait_first
        self.wait_interval = wait_interval
        self.retries = retries

    def _launch(self, howmany, **kwargs):
        """launch <howmany> workers, wait until booted and return their public IP addresses"""

        hub = _Hub(self.apikey)

        errors_tb = set()
        errors = 0

        pending_ids = set()
        yielded_ids = set()

        time_started = time.time()
        while True:
            if errors > (howmany * self.retries):
                raise self.Error("Too many API errors:\n" + "\n".join(errors_tb))

            if len(pending_ids) < howmany:
                try:
                    server = hub.servers.launch('core', **kwargs)
                except HubAPIError, e:
                    if e.name == 'HubAccount.InvalidApiKey':
                        raise self.Error(e)

                    errors_tb.add(get_traceback())
                    errors += 1

                    continue
                pending_ids.add(server.instanceid)

            if time.time() - time_started < self.wait_first:
                continue

            try:
                servers = [ server 
                            for server in hub.servers.get(refresh_cache=True)
                            if server.instanceid in (pending_ids - yielded_ids) ]
            except HubAPIError:
                # ignoring hopefully temporary error
                continue

            for server in servers:
                if server.status != 'running' or server.boot_status != 'booted':
                    continue

                yielded_ids.add(server.instanceid)
                yield server.ipaddress

            if len(yielded_ids) == howmany:
                break

            time.sleep(self.wait_interval)

    def launch(self, howmany, **kwargs):
        if howmany == 1:
            return list(self._launch(howmany, **kwargs))[0]
        else:
            return self._launch(howmany, **kwargs)

    def destroy(self, *addresses):
        if not addresses:
            return

        hub = _Hub(self.apikey)

        destroyable = [ server
                        for server in hub.servers.get(refresh_cache=True)
                        if server.ipaddress in addresses ]

        addresses =  dict([ (server.instanceid, server.ipaddress) 
                             for server in destroyable ])

        for server in destroyable:
            server.destroy()

        server_ids = set([ server.instanceid for server in destroyable ])

        time.sleep(self.wait_first)

        addresses_destroyed = []
        while True:
            servers = [ server 
                        for server in hub.servers.get(refresh_cache=True)
                        if server.instanceid in server_ids ]

            done = True
            for server in servers:
                if server.status == 'terminated':
                    server.unregister()
                    addresses_destroyed.append(addresses[server.instanceid])

                else:
                    done = False

            if done:
                return addresses_destroyed
            else:
                time.sleep(self.wait_interval)

