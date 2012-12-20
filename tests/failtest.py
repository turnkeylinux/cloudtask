#!/usr/bin/python
import sys
import random

def randbool(probability):
    r = random.random() * 100
    if r <= probability:
        return True
    return False

def main():
    args = sys.argv[1:]
    if not args:
        print >> sys.stderr, "syntax: %s probability-of-failure" % sys.argv[0]
        sys.exit(1)

    probability = float(args[0])

    failed = randbool(probability)
    if failed:
        print "failed"
        sys.exit(1)
    else:
        print "ok"
        sys.exit(0)

if __name__ == "__main__":
    main()
