from hub import Hub
import time

LAUNCH_WAIT_FIRST = 30
LAUNCH_WAIT_INTERVAL = 15

def launch(apikey, howmany, **kwargs):
    """launch <howmany> workers, wait until booted and return their public IP addresses"""

    hub = Hub(apikey)

    pending = []
    for i in range(howmany):
        server = hub.servers.launch('core', **kwargs)
        pending.append(server)

    pending_ids = set([ server.instanceid for server in pending ])

    time.sleep(LAUNCH_WAIT_FIRST)

    while True:
        servers = [ server 
                    for server in hub.servers.get(refresh_cache=True)
                    if server.instanceid in pending_ids ]

        for server in servers:
            if server.status != 'running' or server.boot_status != 'booted':
                break
        else:
            return [ server.ipaddress for server in servers ]

        time.sleep(LAUNCH_WAIT_INTERVAL)

def destroy(apikey, addresses):
    raise Exception("not implemented")

