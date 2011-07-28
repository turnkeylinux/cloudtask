#!/usr/bin/python
import sys

def usage(): 
    print >> sys.stderr, "syntax: %s 'python-expression'" % sys.argv[0]
    sys.exit(1)

def error(e):
    print >> sys.stderr, "error: " + str(e)
    sys.exit(1)

def execute(code):
    vars = {'name': 'Liraz',
            'age': 30}

    return eval(code, {}, vars)

def main():
    args = sys.argv[1:]
    if not args:
        usage()

    expr = args[0]
    print "Expression: " + `expr`

    try:
        code = compile(expr, '<test>', 'exec')
    except SyntaxError, e:
        raise

    retval = execute(code)
    if retval is not None:
        print "return " + `retval`

if __name__ == "__main__":
    main()
