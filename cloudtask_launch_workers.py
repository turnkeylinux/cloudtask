#!/usr/bin/python
"""
Launch new cloud workers

Cloudtask can launch and destroy cloud workers automatically when needed, but
sometimes it can be desirable to launch a persistent pool of workers and manage
it by hand.

Options:

    --apikey            Hub APIKEY
                        Environment: HUB_APIKEY

    --ec2-region        Region for instance launch (default: us-east-1)
    --ec2-size          Instance size (default: m1.small)
    --ec2-type          Instance type <s3|ebs> (default: s3)

    --label             Optional server description

Usage example:

    cloudtask-launch-workers 10 workers.txt

"""

import sys
import getopt

from cloudtask import hub

def usage(e=None):
    if e:
        print >> sys.stderr, "error: " + str(e)

    print >> sys.stderr, "Usage: %s [ -opts ] howmany ( path/to/file | - )" % sys.argv[0]
    print >> sys.stderr, __doc__.strip()
    sys.exit(1)

def main():
    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], 
                                       'h', [ 'help',
                                              'apikey=',
                                              'ec2-region=',
                                              'ec2-size=',
                                              'ec2-type=',
                                              'label=' ])
    except getopt.GetoptError, e:
        usage(e)

    for opt, val in opts:
        if opt in ('-h', '--help'):
            usage()

    if len(args) < 2:
        usage()

if __name__ == "__main__":
    main()
