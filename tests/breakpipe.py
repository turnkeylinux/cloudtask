#!/usr/bin/python

def breakpipe():
    import sys
    from forked import forkpipe
    import time

    pid, r, w = forkpipe()
    if pid == 0:
        sys.exit(1)
        time.sleep(10)
    else:
        w.write("test")

breakpipe()
