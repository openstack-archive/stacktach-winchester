# Copyright (c) 2014 Dark Secret Software Inc.
# Copyright (c) 2015 Rackspace
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import alembic
import logging

from winchester.db.alembic_command import AlembicCommandLine


logger = logging.getLogger(__name__)


class DBAdminCommandLine(AlembicCommandLine):
    description = "Winchester DB admin commandline tool."

    def add_options(self, parser):
        parser.add_argument('--config', '-c',
                            default='winchester.yaml',
                            type=str,
                            help='The name of the winchester config file')

    def get_config(self, options):
        alembic_cfg = alembic.config.Config()
        alembic_cfg.set_main_option("winchester_config", options.config)
        alembic_cfg.set_main_option("script_location",
                                    "winchester.db:migrations")
        return alembic_cfg


def main():
    cmd = DBAdminCommandLine(allowed_commands=['upgrade', 'downgrade',
                                               'current', 'history', 'stamp'])
    cmd.main()


if __name__ == '__main__':
    main()
