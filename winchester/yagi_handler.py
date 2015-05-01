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

import logging

import yagi.config
from yagi.handler import BaseHandler

from winchester.config import ConfigManager
from winchester import time_sync
from winchester.trigger_manager import TriggerManager


logger = logging.getLogger(__name__)


with yagi.config.defaults_for('winchester') as default:
    default("config_file", "winchester.yaml")


class WinchesterHandler(BaseHandler):
    CONFIG_SECTION = "winchester"
    AUTO_ACK = True

    def __init__(self, app=None, queue_name=None):
        super(WinchesterHandler, self).__init__(app=app, queue_name=queue_name)
        conf_file = self.config_get("config_file")
        config = ConfigManager.load_config_file(conf_file)
        self.time_sync = time_sync.TimeSync(config, publishes=True)
        self.trigger_manager = TriggerManager(config, time_sync=self.time_sync)

    def handle_messages(self, messages, env):
        for notification in self.iterate_payloads(messages, env):
            tyme = notification['timestamp']
            self.time_sync.publish(tyme)
            self.trigger_manager.add_notification(notification)

    def on_idle(self, num_messages, queue_name):
        self.trigger_manager._log_statistics()
