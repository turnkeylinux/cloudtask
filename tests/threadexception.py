import time
import threading

class Error(Exception):
    pass

def foo():
    time.sleep(1)
    raise Error

threading.Thread(target=foo).start()

print "DONE"
