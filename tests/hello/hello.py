#!/usr/bin/python
"""Print hello world"""

from cloudtask import Task

class T(Task):
    DESCRIPTION = __doc__
    TIMEOUT = 3
    COMMAND = "/usr/local/bin/hello"
    OVERLAY = 'overlay/'
    WORKERS = '/tmp/workers'
    SPLIT = 2
    SESSIONS = 'sessions/'

if __name__ == "__main__":
    T.main()
