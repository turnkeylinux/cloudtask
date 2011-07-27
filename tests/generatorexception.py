class Error(Exception):
    pass

def foo():
    for i in reversed(range(10)):
        if i == 5:
            raise Error

        yield i

i = 0
try:
    for (i, val) in enumerate(foo()):
        print i
except Error:
    print "caught exception at i = %d" % i
    raise
