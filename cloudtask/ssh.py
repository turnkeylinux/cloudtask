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

from os.path import *
from command import Command, fmt_argv

class SSH:
    class Error(Exception):
        pass

    class Command(Command):
        TIMEOUT = 30

        OPTS = ('StrictHostKeyChecking=no',
                'PasswordAuthentication=no')

        class TimeoutError(Command.Error):
            pass

        @classmethod
        def argv(cls, identity_file=None, login_name=None, pty=False, *args):
            argv = ['ssh']

            if pty:
                argv += [ '-t' ]

            if identity_file:
                argv += [ '-i', identity_file ]

            if login_name:
                argv += [ '-l', login_name ]

            for opt in cls.OPTS:
                argv += [ "-o", opt ]

            argv += args

            return argv

        def __init__(self, address, command, 
                     identity_file=None, 
                     login_name=None,
                     callback=None,
                     pty=False):
            self.address = address
            self.command = command
            self.callback = callback

            argv = self.argv(identity_file, login_name, pty, address, command)
            Command.__init__(self, argv, pty=pty, setpgrp=True)

        def __str__(self):
            return "ssh %s %s" % (self.address, `self.command`)

        def close(self, timeout=TIMEOUT):
            finished = self.wait(timeout, callback=self.callback)
            if not finished:
                self.terminate()
                raise self.TimeoutError("ssh timed out after %d seconds" % timeout)

            if self.exitcode != 0:
                raise self.Error(self.output)

    TimeoutError = Command.TimeoutError
    TIMEOUT = Command.TIMEOUT

    def __init__(self, address, 
                 identity_file=None, login_name=None, callback=None):
        self.address = address
        self.identity_file = identity_file
        self.login_name = login_name
        self.callback = callback

        self.ping()

    def ping(self, timeout=TIMEOUT):
        command = self.command('true')
        try:
            command.close(timeout)
        except (command.TimeoutError, command.Error), e:
            raise self.Error(str(e).strip())

    def command(self, command, pty=False):
        return self.Command(self.address, command, 
                            identity_file=self.identity_file,
                            login_name=self.login_name,
                            callback=self.callback,
                            pty=pty)

    def copy_id(self, key_path):
        if not key_path.endswith(".pub"):
            key_path += ".pub"

        command = 'mkdir -p $HOME/.ssh; cat >> $HOME/.ssh/authorized_keys'

        command = self.command(command)
        command.tochild.write(file(key_path).read())
        command.tochild.close()

        try:
            command.close()
        except command.Error, e:
            raise self.Error("can't add id to authorized keys: " + str(e))
        
    def remove_id(self, key_path):
        if not key_path.endswith(".pub"):
            key_path += ".pub"

        vals = file(key_path).read().split()
        if not vals[0].startswith('ssh'):
            raise self.Error("invalid public key in " + key_path)
        id = vals[-1]

        command = 'sed -i "/%s/d" $HOME/.ssh/authorized_keys' % id
        command = self.command(command)

        try:
            command.close()
        except command.Error, e:
            raise self.Error("can't remove id from authorized-keys: " + str(e))

    def apply_overlay(self, overlay_path):
        if not isdir(overlay_path):
            raise self.Error("overlay path '%s' is not a directory" % overlay_path)

        ssh_command = " ".join(self.Command.argv(self.identity_file, 
                                                 self.login_name))
        argv = [ 'rsync', '--timeout=%d' % self.TIMEOUT, '-rHEL', '-e', ssh_command,
                overlay_path.rstrip('/') + '/', "%s:/" % self.address ]

        command = Command(argv, setpgrp=True)
        command.wait(callback=self.callback)

        if command.exitcode != 0:
            raise self.Error("rsync failed: " + command.output)

