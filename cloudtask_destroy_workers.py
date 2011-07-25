#!/usr/bin/python
"""
Destroy cloud workers

Options:

    --apikey      Hub APIKEY
                  Environment: HUB_APIKEY

Usage example:

    cloudtask-destroy-workers 10 workers.txt

"""

import os
import sys
import getopt

from cloudtask import Hub

def usage(e=None):
    if e:
        print >> sys.stderr, "error: " + str(e)

    print >> sys.stderr, "Usage: %s [ -opts ] ( path/to/file | - )" % sys.argv[0]
    print >> sys.stderr, __doc__.strip()
    sys.exit(1)

def fatal(e):
    print >> sys.stderr, "error: " + str(e)
    sys.exit(1)

def main():
    apikey = os.environ.get('HUB_APIKEY', os.environ.get('CLOUDTASK_APIKEY'))
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   'h', [ 'help',
                                          'apikey=' ])
    except getopt.GetoptError, e:
        usage(e)

    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage()

        if opt == '--apikey':
            apikey = val

    if not apikey:
        fatal("missing required APIKEY")

    if not len(args) == 1:
        usage()

    input = args[0]
    if input == '-':
        input = sys.stdin
    else:
        input = file(input)

    addresses = input.read().splitlines()

    Hub(apikey).destroy(addresses)

if __name__ == "__main__":
    main()
