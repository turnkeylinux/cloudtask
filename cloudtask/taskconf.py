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

from StringIO import StringIO

class TaskConf:
    user = 'root'

    command = None

    pre = None
    post = None
    overlay = None

    timeout = 3600
    retries = 0
    strikes = 0

    split = None
    workers = []

    hub_apikey = None
    backup_id = None
    ami_id = None
    snapshot_id = None

    ssh_identity = None

    ec2_region = 'us-east-1'
    ec2_size = 't2.small'
    ec2_type = 's3'

    report = None

    __all__ = [ attr for attr in dir() if not attr.startswith("_") ]

    def __getitem__(self, name):
        return getattr(self, name)

    def __setitem__(self, name, val):
        return setattr(self, name, val)

    def __repr__(self):
        return `self.dict()`

    def dict(self):
        d = {}
        for attr in self.__all__:
            d[attr] = getattr(self, attr)
        return d

    @classmethod
    def fromdict(cls, d):
        taskconf = cls()
        for attr in d:
            taskconf[attr] = d[attr]
        return taskconf

    @property
    def ec2_opts(self):
        opts = dict([ (attr[4:], self[attr])
                       for attr in self.__all__
                       if attr.startswith('ec2_') ])
        opts['label'] = 'Cloudtask: ' + self.command

        for attrname in ('backup_id', 'ami_id', 'snapshot_id'):
            val = getattr(self, attrname)
            if val:
                opts[attrname] = val

        return opts

    def fmt(self):
        sio = StringIO()

        table = []
        for attr in ('split', 'command', 'ssh-identity', 'hub-apikey',
                     'ec2-region', 'ec2-size', 'ec2-type',
                     'user', 'backup-id', 'ami-id', 'snapshot-id', 'workers',
                     'overlay', 'post', 'pre', 'timeout', 'report'):

            val = self[attr.replace('-', '_')]
            if isinstance(val, list):
                val = " ".join(val)
            if not val:
                continue
            table.append((attr, val))

        print >> sio, "  Parameter       Value"
        print >> sio, "  ---------       -----"
        print >> sio
        for row in table:
            print >> sio, "  %-15s %s" % (row[0], row[1])

        return sio.getvalue()
