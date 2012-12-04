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

from hub import Spawner

class Error(Exception):
    pass

class Hub(Spawner):
    def launch(self, howmany, logfh=None, callback=None, **kwargs):
        """launch <howmany> workers, wait until booted and return their public IP addresses.

        Invoke callback every frequently. If callback returns False, we terminate launching.
        """

        if 'sec_updates' not in kwargs:
            kwargs.update(sec_updates='SKIP')

        snapshot_id = kwargs.pop('snapshot_id', None)
        ami_id = kwargs.pop('ami_id', None)

        if snapshot_id and ami_id:
            raise Error("can't force together unrelated ami and snapshot")

        name = snapshot_id or ami_id or 'core'
        return Spawner.launch(self, name, howmany, logfh, callback, **kwargs)
