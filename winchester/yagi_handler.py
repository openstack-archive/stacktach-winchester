import logging

from yagi.handler import BaseHandler
import yagi.config

from winchester.trigger_manager import TriggerManager
from winchester.config import ConfigManager
from winchester import time_sync


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
