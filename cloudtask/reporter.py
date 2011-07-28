import re

def report(session):
    taskconf = session.taskconf
    if not taskconf.report:
        return

    hook = taskconf.report.strip()

class PythonHandler:
    def __init__(self, expr):
        self.code = compile(expr, '--report', 'exec')

    def __call__(self, session):
        taskconf = session.taskconf

        vars = {'session': session,
                'jobs': session.jobs,
                'taskconf': session.taskconf,
                'command': taskconf.command,
                'split': taskconf.split}

        eval(self.code, {}, vars)

class DummyHandler:
    def __init__(self, expr):
        self.expr = expr
    
    def __call__(self, session):
        print self.expr

class Reporter:
    class Error(Exception):
        pass

    handlers = {
        'py': PythonHandler,
        'dummy':  DummyHandler
    }

    def __init__(self, hook):

        handlers = self.handlers

        m = re.match(r'(.*?):\s*(.*)', hook)
        if not m:
            raise self.Error("can't parser reporting hook '%s'" % hook)

        handler, expr = m.groups()

        if handler not in handlers:
            raise self.Error("no '%s' in supported handlers (%s)" % (handler, ", ".join(handlers)))


        handler = handlers[handler]
        try:
            self.handler = handler(expr)
        except Exception, e:
            raise self.Error(e)

    def report(self, session):
        self.handler(session)
