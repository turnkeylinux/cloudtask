# 
# Copyright (c) 2010-2011 Liraz Siri <liraz@turnkeylinux.org>
# 
# This file is part of CloudTask.
# 
# CloudTask is open source software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 3 of the License, or (at your
# option) any later version.
# 

import os
from os.path import *
import re

import shlex
from email.Message import Message
from command import Command

class Error(Exception):
    pass

class PythonHandler:
    def __init__(self, expr):
        if isfile(expr):
            expr = file(expr).read()

        self.code = compile(expr, '--report', 'exec')

    def __call__(self, session):
        taskconf = session.taskconf

        vars = {'session': session,
                'jobs': session.jobs,
                'taskconf': session.taskconf,
                'command': taskconf.command,
                'split': taskconf.split}

        eval(self.code, {}, vars)

class MailHandler:
    class Sendmail:
        SENDMAIL_PATH = "/usr/sbin/sendmail"
        def __init__(self):
            if not exists(self.SENDMAIL_PATH):
                raise Error("can't use mail handler: missing " + self.SENDMAIL_PATH)

        def __call__(self, sender, recipient, subject, body):
            
            msg = Message()
            msg.add_header("Subject", subject)
            msg.add_header("To", str(recipient))
            msg.add_header("From", str(sender))
            msg.set_payload(body)

            command = [ self.SENDMAIL_PATH, '-i', '-f', sender.address, recipient.address ] 
            command = Command(command)
            command.tochild.write(msg.as_string())
            command.tochild.close()
            command.wait()

            if command.exitcode != 0:
                raise Error("sendmail failed (%d): %s" % (command.exitcode,
                                                          command.output))

    class Email:
        def __init__(self, email):
            self.email = email
            m = re.search(r'<(.*)>', email)
            if m:
                address = m.group(1)
            else:
                address = email

            if not re.match(r'^[\w\.]+\@[\w\.]+$', address):
                raise Error("illegal email address '%s'" % email)

            self.address = address

        def __repr__(self):
            return '<Email(%s)>' % `str(self)`

        def __str__(self):
            return self.email

    def __init__(self, expr):
        args = [ self.Email(arg) for arg in shlex.split(expr) ]
        if len(args) < 2:
            raise Error("mail handler needs at least 1 recipient (in addition to the sender's address)")

        self.sender = args[0]
        self.recipients = args[1:]
        self.sendmail = self.Sendmail()

    def __call__(self, session):
        mlog = file(session.paths.log).read()
        taskconf = session.taskconf

        for recipient in self.recipients:

            subject = "[Cloudtask] " + taskconf.command

            self.sendmail(self.sender, recipient, 
                          subject, mlog)


class ShellHandler:
    ENV_WHITELIST = ('HOME', 'PATH', 'USER', 'SHELL')
    def __init__(self, expr):
        self.command = expr

    def __call__(self, session):
        os.chdir(session.paths.path)

        for var in os.environ.keys():
            if var not in self.ENV_WHITELIST:
                del os.environ[var]

        taskconf = session.taskconf
        for attr in taskconf.__all__:
            if attr == 'workers':
                continue

            if taskconf[attr]:
                os.environ['CLOUDTASK_' + attr.upper()] = str(taskconf[attr])

        if taskconf.workers:
            os.environ['CLOUDTASK_WORKERS'] = " ".join(taskconf.workers)

        os.environ

        os.system(self.command)

class Reporter:
    Error = Error

    handlers = {
        'py': PythonHandler,
        'sh': ShellHandler,
        'mail': MailHandler
    }

    def __init__(self, hook):

        handlers = self.handlers

        m = re.match(r'(.*?):\s*(.*)', hook)
        if not m:
            raise self.Error("can't parser reporting hook '%s'" % hook)

        handler, expr = m.groups()

        if handler not in handlers:
            raise self.Error("no '%s' in supported reporting handlers (%s)" % (handler, ", ".join(handlers)))


        handler = handlers[handler]
        try:
            self.handler = handler(expr)
        except Exception, e:
            raise self.Error(e)

    def report(self, session):
        self.handler(session)
