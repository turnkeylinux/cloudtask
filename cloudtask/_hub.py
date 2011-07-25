from hub import Hub as _Hub
import time

class Hub:
    #def __init__(self, apikey, wait_first=30, wait_interval=15):
    def __init__(self, apikey, wait_first=0, wait_interval=0):
        self.apikey = apikey
        self.wait_first = wait_first
        self.wait_interval = wait_interval

    def launch(self, howmany, **kwargs):
        """launch <howmany> workers, wait until booted and return their public IP addresses"""

        hub = _Hub(self.apikey)

        pending = []
        for i in range(howmany):
            server = hub.servers.launch('core', **kwargs)
            pending.append(server)

        pending_ids = set([ server.instanceid for server in pending ])

        time.sleep(self.wait_first)

        while True:
            servers = [ server 
                        for server in hub.servers.get(refresh_cache=True)
                        if server.instanceid in pending_ids ]

            for server in servers:
                if server.status != 'running' or server.boot_status != 'booted':
                    break
            else:
                return [ server.ipaddress for server in servers ]

            time.sleep(self.wait_interval)

    def destroy(self, addresses):
        if not addresses:
            return

        hub = _Hub(self.apikey)

        servers = [ server
                    for server in hub.servers.get(refresh_cache=True)
                    if server.ipaddress in addresses ]

        addresses =  dict([ (server.instanceid, server.ipaddress) 
                             for server in servers ])

        for server in servers:
            server.destroy()

        server_ids = set([ server.instanceid for server in servers ])

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

