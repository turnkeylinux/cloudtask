#!/usr/bin/python

import time
import signal

def sigignore(sig):
    def decorate(method):
        def wrapper(*args, **kwargs):
            orig_handler = signal.getsignal(sig)
            signal.signal(sig, signal.SIG_IGN)
            try:
                return method(*args, **kwargs)
            finally:
                signal.signal(sig, orig_handler)

        wrapper.__name__ = method.__name__
        wrapper.__doc__ = method.__doc__

        return wrapper
    return decorate

class Sleep:
    @sigignore(signal.SIGINT)
    def sleep(self, seconds):
        print "%s.foo(%s): before sleep" % (self.__class__, `seconds`)
        time.sleep(seconds)
        print "%s.foo(%s): after sleep" % (self.__class__, `seconds`)

@sigignore(signal.SIGINT)
def sleep(seconds):
    """ this is foo """

    print "before sleep"
    time.sleep(3)
    print "after sleep"

def test():
    def handler(sig, frame):
        print "caught sig %d" % sig

    signal.signal(signal.SIGINT, handler)
    Sleep().sleep(3)

    sleep(3)

    time.sleep(10)

if __name__ == "__main__":
    test()
