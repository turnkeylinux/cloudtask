#!/usr/bin/python

from __future__ import with_statement
from contextlib import closing, contextmanager

@contextmanager
def foo():
    print "started foo()"
    try:
        yield
        print "foo(): no exception"
    finally:
        print "stopped foo()"

class FooCtx:
    def __enter__(self):
        print "FooCtx.__enter__"

    def __exit__(self, type, value, tb):
        print "FooCtx.__exit__"
        if tb is None:
            print "FooCtx.__exit__: no exception"

class ClosableFoo:
    def __init__(self):
        print "Foo.__init__()"

    def close(self):
        print "Foo.closed()"

def test_ClosableFoo():
    with closing(ClosableFoo()):
        print "test with Foo()"
        raise Exception("evil")

def test_foo():
    with foo():
        print "test with foo()"
        raise Exception("evil")

def test_FooCtx():
    with FooCtx():
        print "test with FooCtx"

test_ClosableFoo()
