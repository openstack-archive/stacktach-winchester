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

import datetime
import logging
import simport
from stackdistiller import condenser
from stackdistiller import distiller

from winchester.config import ConfigItem
from winchester.config import ConfigManager
from winchester.config import ConfigSection
from winchester.db import DBInterface
from winchester.db import DuplicateError
from winchester import debugging
from winchester.definition import TriggerDefinition
from winchester import time_sync as ts

logger = logging.getLogger(__name__)


class EventCondenser(condenser.CondenserBase):

    def __init__(self, dbi):
        self.dbi = dbi
        self.clear()

    def clear(self):
        self.traits = dict()
        self.event_type = None
        self.message_id = None
        self.timestamp = None

    def add_trait(self, name, trait_type, value):
        if isinstance(value, datetime.datetime):
            value = self._fix_time(value)
        self.traits[name] = value

    def add_envelope_info(self, event_type, message_id, when):
        self.event_type = event_type
        self.message_id = message_id
        self.timestamp = when

    def get_event(self):
        event = self.traits.copy()
        event['message_id'] = self.message_id
        event['timestamp'] = self._fix_time(self.timestamp)
        event['event_type'] = self.event_type
        return event

    def _fix_time(self, dt):
        """Stackdistiller converts all times to utc.

        We store timestamps as utc datetime. However, the explicit
        UTC timezone on incoming datetimes causes comparison issues
        deep in sqlalchemy. We fix this by converting all datetimes
        to naive utc timestamps
        """
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt

    def validate(self):
        if self.event_type is None:
            return False
        if self.message_id is None:
            return False
        if self.timestamp is None:
            return False
        if not self.traits:
            return False
        return True


class TriggerManager(object):

    @classmethod
    def config_description(cls):
        return dict(
            config_path=ConfigItem(
                help="Path(s) to find additional config files",
                multiple=True, default='.'),
            distiller_config=ConfigItem(
                required=False,
                help="Name of distiller config file "
                     "describing what to extract from the "
                     "notifications"),
            distiller_trait_plugins=ConfigItem(
                help="dictionary of trait plugins to load "
                     "for stackdistiller. Classes specified with "
                     "simport syntax. See stackdistiller and "
                     "simport docs for more info", default=dict()),
            time_sync_endpoint=ConfigItem(
                help="URL of time sync service for use with"
                     " replying old events.",
                default=None),
            catch_all_notifications=ConfigItem(
                help="Store basic info for all notifications,"
                     " even if not listed in distiller config",
                default=False),
            statistics_period=ConfigItem(
                help="Emit stats on event counts, etc every "
                     "this many seconds", default=10),
            database=ConfigSection(
                help="Database connection info.",
                config_description=DBInterface.config_description()),
            trigger_definitions=ConfigItem(
                required=False,
                help="Name of trigger definitions file "
                     "defining trigger conditions and what events to "
                     "process for each stream"),
        )

    def __init__(self, config, db=None, stackdistiller=None, trigger_defs=None,
                 time_sync=None):
        config = ConfigManager.wrap(config, self.config_description())
        self.config = config
        self.debug_manager = debugging.DebugManager()
        self.trigger_definitions = []
        config.check_config()
        config.add_config_path(*config['config_path'])
        if time_sync is None:
            time_sync = ts.TimeSync()
        self.time_sync = time_sync

        if db is not None:
            self.db = db
        else:
            self.db = DBInterface(config['database'])
        if stackdistiller is not None:
            self.distiller = stackdistiller
        else:
            # distiller_config is optional
            if config.has_key('distiller_config'):
                dist_config = config.load_file(config['distiller_config'])
                plugmap = self._load_plugins(config['distiller_trait_plugins'],
                                             distiller.DEFAULT_PLUGINMAP)
                self.distiller = distiller.Distiller(
                    dist_config,
                    trait_plugin_map=plugmap,
                    catchall=config['catch_all_notifications'])
        if trigger_defs is not None:
            self.trigger_definitions = trigger_defs
            for t in self.trigger_definitions:
                t.set_debugger(self.debug_manager)
        else:
            # trigger_definition config file is optional
            if config.has_key('trigger_definitions'):
                defs = config.load_file(config['trigger_definitions'])
                self.trigger_definitions = [
                    TriggerDefinition(conf, self.debug_manager)
                    for conf in defs]
        # trigger_map is used to quickly access existing trigger_defs
        self.trigger_map = dict(
            (tdef.name, tdef) for tdef in self.trigger_definitions)
        self.saved_events = 0
        self.received = 0
        self.last_status = self.current_time()

    @classmethod
    def _load_plugins(cls, plug_map, defaults=None):
        plugins = dict()
        if defaults is not None:
            plugins.update(defaults)
        for name, cls_string in plug_map.items():
            try:
                plugins[name] = simport.load(cls_string)
            except simport.ImportFailed as e:
                logger.error("Could not load plugin %s: Import failed. %s" % (
                    name, e))
            except (simport.MissingMethodOrFunction,
                    simport.MissingModule,
                    simport.BadDirectory) as e:
                logger.error("Could not load plugin %s: Not found. %s" % (
                    name, e))
        return plugins

    def current_time(self):
        # here so it's easily overridden.
        return self.time_sync.current_time()

    def save_event(self, event):
        traits = event.copy()
        try:
            message_id = traits.pop('message_id')
            timestamp = traits.pop('timestamp')
            event_type = traits.pop('event_type')
        except KeyError as e:
            logger.warning("Received invalid event: %s" % e)
            return False
        try:
            self.db.create_event(message_id, event_type,
                                 timestamp, traits)
            self.saved_events += 1
            return True
        except DuplicateError:
            logger.info("Received duplicate event %s, Ignoring." % message_id)
        return False

    def convert_notification(self, notification_body):
        cond = EventCondenser(self.db)
        cond.clear()
        self.received += 1
        if self.distiller.to_event(notification_body, cond):
            if cond.validate():
                return cond.get_event()
            else:
                logger.warning("Received invalid event")
        else:
            event_type = notification_body.get('event_type',
                                               '**no event_type**')
            message_id = notification_body.get('message_id', '**no id**')
            logger.info("Dropping unconverted %s notification %s"
                        % (event_type, message_id))
        return None

    def _log_statistics(self):
        logger.info("Received %s notifications. Saved %s events." % (
                    self.received, self.saved_events))
        self.received = 0
        self.saved_events = 0
        self.last_status = self.current_time()

        self.debug_manager.dump_debuggers()

    def _add_or_create_stream(self, trigger_def, event, dist_traits):
        stream = self.db.get_active_stream(trigger_def.name, dist_traits,
                                           self.current_time())
        if stream is None:
            trigger_def.debugger.bump_counter("New stream")
            stream = self.db.create_stream(trigger_def.name, event,
                                           dist_traits,
                                           trigger_def.expiration)
            logger.debug("Created New stream %s for %s: distinguished by %s"
                         % (stream.id, trigger_def.name, str(dist_traits)))
        else:
            self.db.add_event_stream(stream, event, trigger_def.expiration)
        return stream

    def _ready_to_fire(self, stream, trigger_def):
        timestamp = trigger_def.get_fire_timestamp(self.current_time())
        self.db.stream_ready_to_fire(stream, timestamp)
        trigger_def.debugger.bump_counter("Ready to fire")
        logger.debug("Stream %s ready to fire at %s" % (stream.id, timestamp))

    def add_trigger_definition(self, list_of_triggerdefs, debugger=None):
        if debugger is None:
            debugger = self.debug_manager
        for td in list_of_triggerdefs:
            if self.trigger_map.has_key(td['name']) is False:
                # Only add if name is unique
                tdef = TriggerDefinition(td, debugger)
                self.trigger_definitions.append(tdef)
                self.trigger_map[td['name']] = tdef

    def delete_trigger_definition(self, trigger_def_name):
        if self.trigger_map.has_key(trigger_def_name):
            self.trigger_definitions.remove(
                self.trigger_map.get(trigger_def_name))
            del self.trigger_map[trigger_def_name]

    def add_event(self, event):
        if self.save_event(event):
            for trigger_def in self.trigger_definitions:
                matched_criteria = trigger_def.match(event)
                if matched_criteria:
                    dist_traits = trigger_def.get_distinguishing_traits(
                        event, matched_criteria)
                    stream = self._add_or_create_stream(trigger_def, event,
                                                        dist_traits)
                    trigger_def.debugger.bump_counter("Added events")
                    if stream.fire_timestamp is None:
                        if trigger_def.should_fire(self.db.get_stream_events(
                                stream)):
                            self._ready_to_fire(stream, trigger_def)

    def add_notification(self, notification_body):
        event = self.convert_notification(notification_body)
        if event:
            self.add_event(event)
