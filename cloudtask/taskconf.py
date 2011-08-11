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

class TaskConf:
    user = 'root'

    command = None

    pre = None
    post = None
    overlay = None

    timeout = 3600

    split = None
    workers = []

    hub_apikey = None
    backup_id = None
    ec2_region = 'us-east-1'
    ec2_size = 'm1.small'
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
        if self.backup_id:
            opts['backup_id'] = self.backup_id
        return opts
