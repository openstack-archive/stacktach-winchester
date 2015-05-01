# Copyright (c) 2014 Dark Secret Software Inc.
# Copyright (c) 2015 Rackspace
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from alembic import command
from alembic import config
from alembic import util
import argparse
import inspect


class AlembicCommandLine(object):
    prog = None
    description = None
    allowed_commands = None

    def __init__(self, prog=None, description=None, allowed_commands=None):
        if prog is not None:
            self.prog = prog
        if description is not None:
            self.description = description
        if allowed_commands is not None:
            self.allowed_commands = allowed_commands

        self.parser = self.generate_options()

    def add_command_options(self, parser, positional, kwargs):
        if 'template' in kwargs:
            parser.add_argument("-t", "--template",
                                default='generic',
                                type=str,
                                help="Setup template for use with 'init'")
        if 'message' in kwargs:
            parser.add_argument("-m", "--message",
                                type=str,
                                help="Message string to use with 'revision'")
        if 'sql' in kwargs:
            parser.add_argument("--sql",
                                action="store_true",
                                help="Don't emit SQL to database - dump to "
                                     "standard output/file instead")
        if 'tag' in kwargs:
            parser.add_argument("--tag",
                                type=str,
                                help="Arbitrary 'tag' name - can be used by "
                                     "custom env.py scripts.")
        if 'autogenerate' in kwargs:
            parser.add_argument("--autogenerate",
                                action="store_true",
                                help="Populate revision script with "
                                     "candidate migration operations, based "
                                     "on comparison of database to model.")
        # "current" command
        if 'head_only' in kwargs:
            parser.add_argument("--head-only",
                                action="store_true",
                                help="Only show current version and "
                                "whether or not this is the head revision.")

        if 'rev_range' in kwargs:
            parser.add_argument("-r", "--rev-range",
                                action="store",
                                help="Specify a revision range; "
                                "format is [start]:[end]")

        positional_help = {
            'directory': "location of scripts directory",
            'revision': "revision identifier"
        }
        for arg in positional:
            parser.add_argument(arg, help=positional_help.get(arg))

    def add_options(self, parser):
        parser.add_argument("-c", "--config",
                            type=str,
                            default="alembic.ini",
                            help="Alternate config file")
        parser.add_argument("-n", "--name",
                            type=str,
                            default="alembic",
                            help="Name of section in .ini file to "
                                    "use for Alembic config")
        parser.add_argument("-x", action="append",
                            help="Additional arguments consumed by "
                            "custom env.py scripts, e.g. -x "
                            "setting1=somesetting -x setting2=somesetting")

    def generate_options(self):
        parser = argparse.ArgumentParser(prog=self.prog)
        self.add_options(parser)
        subparsers = parser.add_subparsers()

        for fn, name, doc, positional, kwarg in self.get_commands():
            subparser = subparsers.add_parser(name, help=doc)
            self.add_command_options(subparser, positional, kwarg)
            subparser.set_defaults(cmd=(fn, positional, kwarg))
        return parser

    def get_commands(self):
        cmds = []
        for fn in [getattr(command, n) for n in dir(command)]:
            if (inspect.isfunction(fn) and
                    fn.__name__[0] != '_' and
                    fn.__module__ == 'alembic.command'):

                if (self.allowed_commands and
                        fn.__name__ not in self.allowed_commands):
                    continue

                spec = inspect.getargspec(fn)
                if spec[3]:
                    positional = spec[0][1:-len(spec[3])]
                    kwarg = spec[0][-len(spec[3]):]
                else:
                    positional = spec[0][1:]
                    kwarg = []
                cmds.append((fn, fn.__name__, fn.__doc__, positional, kwarg))
        return cmds

    def get_config(self, options):
        return config.Config(file_=options.config,
                             ini_section=options.name,
                             cmd_opts=options)

    def run_cmd(self, config, options):
        fn, positional, kwarg = options.cmd

        try:
            fn(config, *[getattr(options, k) for k in positional],
               **dict((k, getattr(options, k)) for k in kwarg))
        except util.CommandError as e:
            util.err(str(e))

    def main(self, argv=None):
        options = self.parser.parse_args(argv)
        if not hasattr(options, "cmd"):
            # see http://bugs.python.org/issue9253, argparse
            # behavior changed incompatibly in py3.3
            self.parser.error("too few arguments")
        else:
            self.run_cmd(self.get_config(options), options)


if __name__ == '__main__':
    cmdline = AlembicCommandLine()
    cmdline.main()
