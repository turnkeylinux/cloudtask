#!/usr/bin/python

import os
import time
import signal

def sigignore(*sigs):
    def decorate(method):
        def wrapper(*args, **kwargs):
            orig_handlers = []

            for sig in sigs:
                orig_handlers.append(signal.getsignal(sig))
                signal.signal(sig, signal.SIG_IGN)

            try:
                return method(*args, **kwargs)
            finally:

                for (i, sig) in enumerate(sigs):
                    signal.signal(sig, orig_handlers[i])

        wrapper.__name__ = method.__name__
        wrapper.__doc__ = method.__doc__

        return wrapper
    return decorate

class Sleep:
    @sigignore(signal.SIGINT, signal.SIGTERM)
    def sleep(self, seconds):
        print "%s.foo(%s): before sleep" % (self.__class__, `seconds`)
        time.sleep(seconds)
        print "%s.foo(%s): after sleep" % (self.__class__, `seconds`)

@sigignore(signal.SIGINT, signal.SIGTERM)
def sleep(seconds):
    """ this is foo """

    print "before sleep"
    time.sleep(seconds)
    print "after sleep"

def test():
    print os.getpid()

    def handler(sig, frame):
        print "caught sig %d" % sig

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    Sleep().sleep(10)
    sleep(10)

    time.sleep(10)

if __name__ == "__main__":
    test()
