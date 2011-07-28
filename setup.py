import os
import sys
from distutils.core import setup
from executil import getoutput
from os.path import *

def get_version():
    if not exists("debian/changelog"):
        return None

    output = getoutput("dpkg-parsechangelog")
    version = [ line.split(" ")[1]
                for line in output.split("\n")
                if line.startswith("Version:") ][0]
    return version

def parse_control(control):
    """parse control fields -> dict"""
    d = {}
    for line in control.split("\n"):
        if not line or line[0] == " ":
            continue
        line = line.strip()
        i = line.index(':')
        key = line[:i]
        val = line[i + 2:]
        d[key] = val

    return d

def get_packages():
    packages = []
    source_path = abspath(dirname(sys.argv[0]))
    for fname in os.listdir(source_path):
        fpath = join(source_path, fname)
        if isdir(fpath) and exists(join(fpath, '__init__.py')):
            packages.append(fname)

    return packages

def main():
    control_fields = parse_control(file("debian/control").read())

    setup(packages = get_packages(),
          # non-essential meta-data
          name=control_fields['Source'],
          version=get_version(),
          description=control_fields['Description'])

if __name__=="__main__":
    main()
