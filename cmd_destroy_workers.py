#!/usr/bin/env python
# 
# Copyright (c) 2010-2012 Liraz Siri <liraz@turnkeylinux.org>
# 
# This file is part of CloudTask.
# 
# CloudTask is open source software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 3 of the License, or (at your
# option) any later version.
# 

"""
Destroy + unregister cloud workers and remove their addresses from file listing

Options:

    --hub-apikey  Hub APIKEY
                  Environment: HUB_APIKEY

Return codes:

    0   destroyed all workers
    1   fatal error
    2   couldn't destroy some workers

Usage example:

    cloudtask-destroy-workers worker-ips.txt
    cat workers-ips | cloudtask-destroy-workers -

"""

import os
import sys
import getopt

from cloudtask import Hub

def usage(e=None):
    if e:
        print >> sys.stderr, "error: " + str(e)

    print >> sys.stderr, "Usage: %s [ -opts ] ( path/to/list-of-ips | - )" % sys.argv[0]
    print >> sys.stderr, __doc__.strip()
    sys.exit(1)

def fatal(e):
    print >> sys.stderr, "error: " + str(e)
    sys.exit(1)

def main():
    hub_apikey = os.environ.get('HUB_APIKEY', os.environ.get('CLOUDTASK_HUB_APIKEY'))
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   'h', [ 'help',
                                          'hub-apikey=' ])
    except getopt.GetoptError, e:
        usage(e)

    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage()

        if opt == '--hub-apikey':
            hub_apikey = val

    if not len(args) == 1:
        usage()

    if not hub_apikey:
        fatal("missing required HUB_APIKEY")

    input = args[0]
    if input == '-':
        fh = sys.stdin
    else:
        fh = file(input)

    ip_addresses = fh.read().splitlines()
    if not ip_addresses:
        print "no workers to destroy"
        return
    
    destroyed = Hub(hub_apikey).destroy(*ip_addresses)
    if not destroyed:
        fatal("couldn't destroy any workers")
    
    ip_addresses_left = list(set(ip_addresses) - set([ ip_address for ip_address, instanceid in destroyed ]))
    if ip_addresses_left:
        print >> sys.stderr, "warning: can't destroy " + " ".join(ip_addresses_left)

        ip_addresses_left.sort()
        if input != '-':
            fh = file(input, "w")
            for ip_address in ip_addresses_left:
                print >> fh, ip_address
            fh.close()

        sys.exit(2)

    if not ip_addresses_left:
        if input != '-':
            os.remove(input)

        sys.exit(0)

if __name__ == "__main__":
    main()
