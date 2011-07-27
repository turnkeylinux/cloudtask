#!/usr/bin/python

import os
import sys
import time
import random

#sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

args = sys.argv[1:]
if args:
    max = int(args[0])
else:
    max = 10

rmax = random.randint(1, max)
print "sleeping for an aggregate of %d seconds" % rmax
for i in range(rmax):
    print i
    time.sleep(1)

sys.exit(0)
