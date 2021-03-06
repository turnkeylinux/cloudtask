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
Launch new cloud workers and write list of addresses to a file

Cloudtask can launch and destroy cloud workers automatically when needed, but
sometimes it can be desirable to launch a persistent pool of workers and manage
it by hand.

Options:

    --hub-apikey       Hub APIKEY
                       Environment: HUB_APIKEY

    --snapshot-id      Launch instance from a snapshot ID
    --backup-id        TurnKey Backup ID to restore on launch
    --ami-id           Force launch a specific AMI ID (default is the latest Core)

    --region           Region for instance launch (default: us-east-1)
                       Regions:

                         us-east-1 (Virginia, USA)
                         us-west-1 (California, USA)
                         eu-west-1 (Ireland, Europe)
                         ap-southeast-1 (Singapore, Asia)

    --size             Instance size (default: m1.small)
                       Sizes:

                         t1.micro (1 CPU core, 613M RAM, no tmp storage)
                         m1.small (1 CPU core, 1.7G RAM, 160G tmp storage)
                         c1.medium (2 CPU cores, 1.7G RAM, 350G tmp storage)

    --type             Instance type <s3|ebs> (default: s3)
    --label            Hub description label for all launched servers

    --install-updates  Install security updates (by default they're skipped)

Usage examples:

    # create workers.txt file with list of new worker addresses
    cloudtask-launch-workers 10 workers.txt

    # append list of worker addresses to a file
    cloudtask-launch-workers 10 - >> workers.txt

"""

import os
from os.path import *
import sys
import getopt

import signal
from sighandle import sighandle

from cloudtask import Hub
from lazyclass import lazyclass

def usage(e=None):
    if e:
        print >> sys.stderr, "error: " + str(e)

    print >> sys.stderr, "Usage: %s [ -opts ] howmany ( path/to/list-of-ips | - )" % sys.argv[0]
    print >> sys.stderr, __doc__.strip()
    sys.exit(1)

def fatal(e):
    print >> sys.stderr, "error: " + str(e)
    sys.exit(1)

def main():
    kwargs = {
        'region': "us-east-1",
        'size': "m1.small",
        'type': "s3",
        'label': "Cloudtask worker",
        'backup_id': None,
        'ami_id': None,
        'snapshot_id': None,
    }

    hub_apikey = os.environ.get('HUB_APIKEY', os.environ.get('CLOUDTASK_HUB_APIKEY'))
    
    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], 
                                       'h', [ 'help',
                                              'install-updates',
                                              'hub-apikey=' ] + 
                                            [ key.replace('_', '-') + '=' 
                                              for key in kwargs ])
    except getopt.GetoptError, e:
        usage(e)

    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage()

        if opt == '--hub-apikey':
            hub_apikey = val

        if opt == '--install-updates':
            kwargs['sec_updates'] = 'INSTALL'

        for key in kwargs:
            if opt == '--' + key.replace('_', '-'):
                kwargs[key] = val
                break

    if len(args) < 2:
        usage()

    if not hub_apikey:
        fatal("missing required HUB_APIKEY")

    howmany, output = args

    try:
        howmany = int(howmany)
        if howmany < 1:
            raise ValueError
    except ValueError:
        usage("illegal howmany value '%s'" % howmany)

    if output != '-':
        if exists(output):
            fatal("'%s' already exists, refusing to overwrite" % output)

        output = lazyclass(file)(output, "w")
    else:
        output = sys.stdout

    class Bool:
        value = False
    stopped = Bool()

    def handler(s, f):
        print >> sys.stderr, "caught SIGINT, stopping launch"
        stopped.value = True

    with sighandle(handler, signal.SIGINT):
        def callback():
            return not stopped.value

        for ipaddress, instanceid in Hub(hub_apikey).launch(howmany, logfh=sys.stderr, callback=callback, **kwargs):
            print >> output, ipaddress
            output.flush()

if __name__ == "__main__":
    main()
