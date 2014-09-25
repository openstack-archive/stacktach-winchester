import datetime
import logging
from stackdistiller import distiller, condenser
import simport

from winchester.config import ConfigManager, ConfigSection, ConfigItem
from winchester import debugging
from winchester.db import DBInterface, DuplicateError
from winchester.definition import TriggerDefinition


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
        to naive utc timestamps"""
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
        return dict(config_path=ConfigItem(
                            help="Path(s) to find additional config files",
                                 multiple=True, default='.'),
                    distiller_config=ConfigItem(required=True,
                                       help="Name of distiller config file "
                                       "describing what to extract from the "
                                       "notifications"),
                    distiller_trait_plugins=ConfigItem(
                        help="dictionary of trait plugins to load "
                             "for stackdistiller. Classes specified with "
                             "simport syntax. See stackdistiller and "
                             "simport docs for more info", default=dict()),
                    catch_all_notifications=ConfigItem(
                        help="Store basic info for all notifications,"
                             " even if not listed in distiller config",
                             default=False),
                    statistics_period=ConfigItem(
                        help="Emit stats on event counts, etc every "
                             "this many seconds", default=10),
                    database=ConfigSection(help="Database connection info.",
                            config_description=DBInterface.config_description()),
                    trigger_definitions=ConfigItem(required=True,
                               help="Name of trigger definitions file "
                               "defining trigger conditions and what events to "
                               "process for each stream"),
               )

    def __init__(self, config, db=None, stackdistiller=None, trigger_defs=None):
        config = ConfigManager.wrap(config, self.config_description())
        self.config = config
        self.debug_manager = debugging.DebugManager()
        config.check_config()
        config.add_config_path(*config['config_path'])

        if db is not None:
            self.db = db
        else:
            self.db = DBInterface(config['database'])
        if stackdistiller is not None:
            self.distiller = stackdistiller
        else:
            dist_config = config.load_file(config['distiller_config'])
            plugmap = self._load_plugins(config['distiller_trait_plugins'],
                                         distiller.DEFAULT_PLUGINMAP)
            self.distiller = distiller.Distiller(dist_config,
                                                 trait_plugin_map=plugmap,
                                                 catchall=config['catch_all_notifications'])
        if trigger_defs is not None:
            self.trigger_definitions = trigger_defs
            for t in self.trigger_definitions:
                t.set_debugger(self.debug_manager)
        else:
            defs = config.load_file(config['trigger_definitions'])
            self.trigger_definitions = [TriggerDefinition(conf,
                                        self.debug_manager)
                                            for conf in defs]
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
                log.error("Could not load plugin %s: Import failed. %s" % (
                          name, e))
            except (simport.MissingMethodOrFunction,
                    simport.MissingModule,
                    simport.BadDirectory) as e:
                log.error("Could not load plugin %s: Not found. %s" % (
                          name, e))
        return plugins

    def current_time(self):
        # here so it's easily overridden.
        return datetime.datetime.utcnow()

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
            event_type = notification_body.get('event_type', '**no event_type**')
            message_id = notification_body.get('message_id', '**no id**')
            logger.info("Dropping unconverted %s notification %s" % (event_type, message_id))
        return None

    def _log_statistics(self):
        logger.info("Received %s notifications. Saved %s events." % (
                    self.received, self.saved_events))
        self.received = 0
        self.saved_events = 0
        self.last_status = self.current_time()

        self.debug_manager.dump_debuggers()

    def _add_or_create_stream(self, trigger_def, event, dist_traits):
        stream = self.db.get_active_stream(trigger_def.name, dist_traits, self.current_time())
        if stream is None:
            trigger_def.debugger.bump_counter("New stream")
            stream = self.db.create_stream(trigger_def.name, event, dist_traits,
                                           trigger_def.expiration)
            logger.debug("Created New stream %s for %s: distinguished by %s" % (
                          stream.id, trigger_def.name, str(dist_traits)))
        else:
            self.db.add_event_stream(stream, event, trigger_def.expiration)
        return stream

    def _ready_to_fire(self, stream, trigger_def):
        timestamp = trigger_def.get_fire_timestamp(self.current_time())
        self.db.stream_ready_to_fire(stream, timestamp)
        trigger_def.debugger.bump_counter("Ready to fire")
        logger.debug("Stream %s ready to fire at %s" % (
                      stream.id, timestamp))

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
