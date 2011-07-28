#!/usr/bin/python

import os
import sys

input = sys.stdin.read()

print "INPUT: "
print input,

sys.stdin = os.fdopen(sys.stderr.fileno(), 'r')
answer = raw_input("?")

print "Your answer: " + answer

