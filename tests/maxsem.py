#!/usr/bin/python

from multiprocessing import Semaphore

sem = Semaphore()
i = 0
while True:
    i += 1
    if i % 10000 == 0:
        print sem.get_value()

    try:
        sem.release()
    except:
        print `sem.get_value()`
        raise
