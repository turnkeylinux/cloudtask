import time
import threading

class Error(Exception):
    pass

def foo():
    time.sleep(1)
    raise Error

thread = threading.Thread(target=foo)
thread.start()
thread.join()

time.sleep(3)
print "DONE"
