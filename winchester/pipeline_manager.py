import datetime
import time
import logging
import random
import simport
import six

from winchester.db import DBInterface, DuplicateError, LockError
from winchester.config import ConfigManager, ConfigSection, ConfigItem
from winchester.definition import TriggerDefinition
from winchester.models import StreamState

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    pass


class PipelineExecutionError(PipelineError):
    def __init__(self, msg="", cause=None):
        super(PipelineExecutionError, self).__init__("%s: caused by %s" % (msg, repr(cause)))
        self.cause = cause


class PipelineConfigError(PipelineError):
    pass


class Pipeline(object):

    @classmethod
    def check_handler_config(cls, conf, handler_map):
        if isinstance(conf, six.string_types):
            conf = dict(name=conf, params=dict())
        if 'name' not in conf:
            raise PipelineConfigError("Handler name not in config! %s" % str(conf))
        if 'params' not in conf:
            conf['params'] = {}
        if conf['name'] not in handler_map:
            raise PipelineConfigError("Unknown handler in pipeline config %s" % conf['name'])
        return conf

    def __init__(self, name, config, handler_map):
        self.name = name
        self.handlers = []
        self.env = dict()
        for handler_conf in config:
            name = handler_conf['name']
            params = handler_conf['params']
            handler_class = handler_map[name]
            try:
                handler = handler_class(**params)
            except Exception as e:
                logger.exception("Error initalizing handler %s for pipeline %s" %
                                 handler_class, self.name)
                raise PipelineExecutionError("Error loading pipeline", e)
            self.handlers.append(handler)

    def handle_events(self, events):
        event_ids = set(e['message_id'] for e in events)
        try:
            for handler in self.handlers:
                events = handler.handle_events(events, self.env)
        except Exception as e:
            logger.exception("Error processing pipeline %s" % self.name)
            self.rollback()
            raise PipelineExecutionError("Error in pipeline", e)
        new_events = [e for e in events if e['message_id'] not in event_ids]
        self.commit()
        return new_events

    def commit(self):
        for handler in self.handlers:
            try:
                handler.commit()
            except:
                logger.exception("Commit error on handler in pipeline %s" % self.name)

    def rollback(self):
        for handler in self.handlers:
            try:
                handler.rollback()
            except:
                logger.exception("Rollback error on handler in pipeline %s" % self.name)


class PipelineManager(object):

    @classmethod
    def config_description(cls):
        return dict(config_path=ConfigItem(help="Path(s) to find additional config files",
                                           multiple=True, default='.'),
                    pipeline_handlers=ConfigItem(required=True,
                                                 help="dictionary of pipeline handlers to load "
                                                       "Classes specified with simport syntax. "
                                                       "simport docs for more info"),
                    statistics_period=ConfigItem(help="Emit stats on event counts, etc every "
                                                      "this many seconds", default=10),
                    pipeline_worker_batch_size=ConfigItem(help="Number of streams for pipeline "
                                                          "worker(s) to load at a time", default=1000),
                    pipeline_worker_delay=ConfigItem(help="Number of seconds for pipeline worker to sleep "
                                                          "when it finds no streams to process", default=10),
                    database=ConfigSection(help="Database connection info.",
                                config_description=DBInterface.config_description()),
                    trigger_definitions=ConfigItem(required=True,
                                       help="Name of trigger definitions file "
                                       "defining trigger conditions and what events to "
                                       "process for each stream"),
                    pipeline_config=ConfigItem(required=True,
                                       help="Name of pipeline config file "
                                            "defining the handlers for each pipeline."),
                   )

    def __init__(self, config, db=None, pipeline_handlers=None, pipeline_config=None, trigger_defs=None):
        logger.debug("PipelineManager: Using config: %s" % str(config))
        config = ConfigManager.wrap(config, self.config_description())
        self.config = config
        config.check_config()
        config.add_config_path(*config['config_path'])
        if db is not None:
            self.db = db
        else:
            self.db = DBInterface(config['database'])

        if pipeline_handlers is not None:
            self.pipeline_handlers = pipeline_handlers
        else:
            self.pipeline_handlers = self._load_plugins(config['pipeline_handlers'])
        logger.debug("Pipeline handlers: %s" % str(self.pipeline_handlers))

        if pipeline_config is not None:
            self.pipeline_config = pipeline_config
        else:
            self.pipeline_config = config.load_file(config['pipeline_config'])

        logger.debug("Pipeline config: %s" % str(self.pipeline_config))
        for pipeline, handler_configs in self.pipeline_config.items():
            self.pipeline_config[pipeline] = [Pipeline.check_handler_config(conf,
                                                self.pipeline_handlers)
                                              for conf in handler_configs]

        if trigger_defs is not None:
            self.trigger_definitions = trigger_defs
        else:
            defs = config.load_file(config['trigger_definitions'])
            logger.debug("Loaded trigger definitions %s" % str(defs))
            self.trigger_definitions = [TriggerDefinition(conf) for conf in defs]
        self.trigger_map = dict((tdef.name, tdef) for tdef in self.trigger_definitions)

        self.pipeline_worker_batch_size = config['pipeline_worker_batch_size']
        self.pipeline_worker_delay = config['pipeline_worker_delay']
        self.statistics_period = config['statistics_period']
        self.streams_fired = 0
        self.streams_expired = 0
        self.streams_loaded = 0
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

    def _log_statistics(self):
        logger.info("Loaded %s streams. Fired %s, Expired %s." % (
                    self.streams_loaded, self.streams_fired, self.streams_expired))
        self.streams_fired = 0
        self.streams_expired = 0
        self.streams_loaded = 0
        self.last_status = self.current_time()

    def add_new_events(self, events):
        pass

    def _run_pipeline(self, stream, trigger_def, pipeline_name, pipeline_config):
        events = self.db.get_stream_events(stream)
        try:
            pipeline = Pipeline(pipeline_name, pipeline_config, self.pipeline_handlers)
            new_events = pipeline.handle_events(events)
        except PipelineExecutionError:
            logger.error("Exception in pipeline %s handling stream %s" % (
                          pipeline_name, stream.id))
            return False
        if new_events:
            self.add_new_events(new_events)
        return True

    def _complete_stream(self, stream):
        self.db.set_stream_state(stream, StreamState.completed)

    def _error_stream(self, stream):
        self.db.set_stream_state(stream, StreamState.error)

    def _expire_error_stream(self, stream):
        self.db.set_stream_state(stream, StreamState.expire_error)

    def fire_stream(self, stream):
        try:
            stream = self.db.set_stream_state(stream, StreamState.firing)
        except LockError:
            logger.debug("Stream %s locked. Moving on..." % stream.id)
            return False
        logger.debug("Firing Stream %s." % stream.id)
        trigger_def = self.trigger_map.get(stream.name)
        if trigger_def is None:
            logger.error("Stream %s has unknown trigger definition %s" % (
                         stream.id, stream.name))
            self._error_stream(stream)
            return False
        pipeline = trigger_def.fire_pipeline
        if pipeline is not None:
            pipe_config = self.pipeline_config.get(pipeline)
            if pipe_config is None:
                logger.error("Trigger %s for stream %s has unknown pipeline %s" % (
                            stream.name, stream.id, pipeline))
                self._error_stream(stream)
            if not self._run_pipeline(stream, trigger_def, pipeline, pipe_config):
                self._error_stream(stream)
                return False
        else:
            logger.debug("No fire pipeline for stream %s. Nothing to do." % (
                         stream.id))
        self._complete_stream(stream)
        self.streams_fired +=1
        return True

    def expire_stream(self, stream):
        try:
            stream = self.db.set_stream_state(stream, StreamState.expiring)
        except LockError:
            logger.debug("Stream %s locked. Moving on..." % stream.id)
            return False
        logger.debug("Expiring Stream %s." % stream.id)
        trigger_def = self.trigger_map.get(stream.name)
        if trigger_def is None:
            logger.error("Stream %s has unknown trigger definition %s" % (
                         stream.id, stream.name))
            self._expire_error_stream(stream)
            return False
        pipeline = trigger_def.expire_pipeline
        if pipeline is not None:
            pipe_config = self.pipeline_config.get(pipeline)
            if pipe_config is None:
                logger.error("Trigger %s for stream %s has unknown pipeline %s" % (
                            stream.name, stream.id, pipeline))
                self._expire_error_stream(stream)
            if not self._run_pipeline(stream, trigger_def, pipeline, pipe_config):
                self._expire_error_stream(stream)
                return False
        else:
            logger.debug("No expire pipeline for stream %s. Nothing to do." % (
                         stream.id))
        self._complete_stream(stream)
        self.streams_expired +=1
        return True

    def process_ready_streams(self, batch_size, expire=False):
        streams = self.db.get_ready_streams(batch_size, self.current_time(),
                                            expire=expire)
        stream_ct = len(streams)
        if expire:
            logger.debug("Loaded %s streams to expire." % stream_ct)
        else:
            logger.debug("Loaded %s streams to fire." % stream_ct)

        random.shuffle(streams)
        for stream in streams:
            if expire:
                self.expire_stream(stream)
            else:
                self.fire_stream(stream)
        self.streams_loaded += stream_ct
        return stream_ct

    def run(self):
        while True:
            fire_ct = self.process_ready_streams(self.pipeline_worker_batch_size)
            expire_ct = self.process_ready_streams(self.pipeline_worker_batch_size,
                                                   expire=True)

            if (self.current_time() - self.last_status).seconds > self.statistics_period:
                self._log_statistics()

            if not fire_ct and not expire_ct:
                logger.debug("No streams to fire or expire. Sleeping...")
                time.sleep(self.pipeline_worker_delay)
