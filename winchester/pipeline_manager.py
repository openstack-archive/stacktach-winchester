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
import random
import simport
import six
import time

from winchester.config import ConfigItem
from winchester.config import ConfigManager
from winchester.db import DBInterface
from winchester.db import LockError
from winchester.definition import TriggerDefinition
from winchester.models import StreamState
from winchester import time_sync as ts
from winchester.trigger_manager import TriggerManager


logger = logging.getLogger(__name__)


class PipelineError(Exception):
    pass


class PipelineExecutionError(PipelineError):
    def __init__(self, msg="", cause=None):
        super(PipelineExecutionError, self).__init__(
            "%s: caused by %s" % (msg, repr(cause)))
        self.cause = cause


class PipelineConfigError(PipelineError):
    pass


class Pipeline(object):
    @classmethod
    def check_handler_config(cls, conf, handler_map):
        if isinstance(conf, six.string_types):
            conf = dict(name=conf, params=dict())
        if 'name' not in conf:
            raise PipelineConfigError(
                "Handler name not in config! %s" % str(conf))
        if 'params' not in conf:
            conf['params'] = {}
        if conf['name'] not in handler_map:
            raise PipelineConfigError(
                "Unknown handler in pipeline config %s" % conf['name'])
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
                logger.exception(
                    "Error initalizing handler %s for pipeline %s" %
                    (handler_class, self.name))
                raise PipelineExecutionError("Error loading pipeline", e)
            self.handlers.append(handler)

    def handle_events(self, events, stream, debugger):
        self.env['stream_id'] = stream.id
        event_ids = set(e['message_id'] for e in events)
        try:
            for handler in self.handlers:
                events = handler.handle_events(events, self.env)
            debugger.bump_counter("Pre-commit successful")
        except Exception as err:
            logger.exception("Error processing pipeline %s" % self.name)
            debugger.bump_counter("Pipeline error")
            self.rollback(debugger)
            raise PipelineExecutionError("Error in pipeline", err)
        new_events = [e for e in events if e['message_id'] not in event_ids]
        self.commit(debugger)
        return new_events

    def commit(self, debugger):
        for handler in self.handlers:
            try:
                handler.commit()
                debugger.bump_counter("Commit successful")
            except Exception:
                debugger.bump_counter("Commit error")
                logger.exception(
                    "Commit error on handler in pipeline %s" % self.name)

    def rollback(self, debugger):
        for handler in self.handlers:
            try:
                handler.rollback()
                debugger.bump_counter("Rollback successful")
            except Exception:
                debugger.bump_counter("Rollback error")
                logger.exception(
                    "Rollback error on handler in pipeline %s" % self.name)


class PipelineManager(object):
    @classmethod
    def config_description(cls):
        configs = TriggerManager.config_description()
        configs.update(dict(
            pipeline_handlers=ConfigItem(
                required=True,
                help="dictionary of pipeline handlers to load "
                     "Classes specified with simport syntax. "
                     "simport docs for more info"),
            pipeline_worker_batch_size=ConfigItem(
                help="Number of streams for pipeline "
                     "worker(s) to load at a time",
                default=1000),
            pipeline_worker_delay=ConfigItem(
                help="Number of seconds for pipeline worker "
                     "to sleep when it finds no streams to "
                     "process", default=10),
            pipeline_config=ConfigItem(required=True,
                                       help="Name of pipeline config file "
                                            "defining the handlers for each "
                                            "pipeline."),
            purge_completed_streams=ConfigItem(
                help="Delete successfully proccessed "
                     "streams when finished?",
                default=True),
        ))
        return configs

    def __init__(self, config, db=None, pipeline_handlers=None,
                 pipeline_config=None, trigger_defs=None, time_sync=None,
                 proc_name='pipeline_worker'):
        # name used to distinguish worker processes in logs
        self.proc_name = proc_name

        logger.debug("PipelineManager(%s): Using config: %s"
                     % (self.proc_name, str(config)))
        config = ConfigManager.wrap(config, self.config_description())
        self.config = config
        config.check_config()
        config.add_config_path(*config['config_path'])
        if time_sync is None:
            time_sync = ts.TimeSync()
        self.time_sync = time_sync

        if db is not None:
            self.db = db
        else:
            self.db = DBInterface(config['database'])

        if pipeline_handlers is not None:
            self.pipeline_handlers = pipeline_handlers
        else:
            self.pipeline_handlers = self._load_plugins(
                config['pipeline_handlers'])
        logger.debug("Pipeline handlers: %s" % str(self.pipeline_handlers))

        if pipeline_config is not None:
            self.pipeline_config = pipeline_config
        else:
            self.pipeline_config = config.load_file(config['pipeline_config'])

        logger.debug("Pipeline config: %s" % str(self.pipeline_config))
        for pipeline, handler_configs in self.pipeline_config.items():
            self.pipeline_config[pipeline] = [
                Pipeline.check_handler_config(conf,
                                              self.pipeline_handlers)
                for conf in handler_configs]

        if trigger_defs is not None:
            self.trigger_definitions = trigger_defs
        else:
            defs = config.load_file(config['trigger_definitions'])
            logger.debug("Loaded trigger definitions %s" % str(defs))
            self.trigger_definitions = [TriggerDefinition(conf, None) for conf
                                        in defs]
        self.trigger_map = dict(
            (tdef.name, tdef) for tdef in self.trigger_definitions)

        self.trigger_manager = TriggerManager(
            self.config, db=self.db,
            trigger_defs=self.trigger_definitions,
            time_sync=time_sync)

        self.pipeline_worker_batch_size = config['pipeline_worker_batch_size']
        self.pipeline_worker_delay = config['pipeline_worker_delay']
        self.statistics_period = config['statistics_period']
        self.purge_completed_streams = config['purge_completed_streams']
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

    def _log_statistics(self):
        logger.info("Loaded %s streams. Fired %s, Expired %s." % (
            self.streams_loaded, self.streams_fired, self.streams_expired))
        self.streams_fired = 0
        self.streams_expired = 0
        self.streams_loaded = 0
        self.last_status = self.current_time()

        self.trigger_manager.debug_manager.dump_debuggers()

    def add_new_events(self, events):
        for event in events:
            self.trigger_manager.add_event(event)

    def _run_pipeline(self, stream, trigger_def, pipeline_name,
                      pipeline_config):
        events = self.db.get_stream_events(stream)
        debugger = trigger_def.debugger
        try:
            pipeline = Pipeline(pipeline_name, pipeline_config,
                                self.pipeline_handlers)
            new_events = pipeline.handle_events(events, stream, debugger)
        except PipelineExecutionError:
            logger.error("Exception in pipeline %s handling stream %s" % (
                pipeline_name, stream.id))
            return False
        if new_events:
            self.add_new_events(new_events)
        return True

    def _complete_stream(self, stream):
        if self.purge_completed_streams:
            self.db.purge_stream(stream)
        else:
            try:
                self.db.set_stream_state(stream, StreamState.completed)
            except LockError:
                logger.error(
                    "Stream %s locked while trying to set 'complete' state! "
                    "This should not happen." % stream.id)

    def _error_stream(self, stream):
        try:
            self.db.set_stream_state(stream, StreamState.error)
        except LockError:
            logger.error("Stream %s locked while trying to set 'error' state! "
                         "This should not happen." % stream.id)

    def _expire_error_stream(self, stream):
        try:
            self.db.set_stream_state(stream, StreamState.expire_error)
        except LockError:
            logger.error(
                "Stream %s locked while trying to set 'expire_error' state! "
                "This should not happen." % stream.id)

    def safe_get_debugger(self, trigger_def):
        return trigger_def.debugger if trigger_def is not None else \
            self.trigger_manager.debug_manager.get_debugger(None)

    def fire_stream(self, stream):
        trigger_def = self.trigger_map.get(stream.name)
        debugger = self.safe_get_debugger(trigger_def)
        try:
            stream = self.db.set_stream_state(stream, StreamState.firing)
        except LockError:
            logger.debug("Stream %s locked. Moving on..." % stream.id)
            debugger.bump_counter("Locked")
            return False
        logger.debug("Firing Stream %s." % stream.id)
        if trigger_def is None:
            debugger.bump_counter("Unknown trigger def '%s'" % stream.name)
            logger.error("Stream %s has unknown trigger definition %s" % (
                         stream.id, stream.name))
            self._error_stream(stream)
            return False
        pipeline = trigger_def.fire_pipeline
        if pipeline is not None:
            pipe_config = self.pipeline_config.get(pipeline)
            if pipe_config is None:
                debugger.bump_counter("Unknown pipeline '%s'" % pipeline)
                logger.error("Trigger %s for stream %s has unknown "
                             "pipeline %s" % (stream.name, stream.id,
                                              pipeline))
                self._error_stream(stream)
            if not self._run_pipeline(stream, trigger_def, pipeline,
                                      pipe_config):
                self._error_stream(stream)
                return False
        else:
            logger.debug("No fire pipeline for stream %s. Nothing to do." % (
                         stream.id))
            debugger.bump_counter("No fire pipeline for '%s'" % stream.name)
        self._complete_stream(stream)
        debugger.bump_counter("Streams fired")
        self.streams_fired += 1
        return True

    def expire_stream(self, stream):
        trigger_def = self.trigger_map.get(stream.name)
        debugger = self.safe_get_debugger(trigger_def)
        try:
            stream = self.db.set_stream_state(stream, StreamState.expiring)
        except LockError:
            debugger.bump_counter("Locked")
            logger.debug("Stream %s locked. Moving on..." % stream.id)
            return False
        logger.debug("Expiring Stream %s." % stream.id)
        if trigger_def is None:
            debugger.bump_counter("Unknown trigger def '%s'" % stream.name)
            logger.error("Stream %s has unknown trigger definition %s" % (
                stream.id, stream.name))
            self._expire_error_stream(stream)
            return False
        pipeline = trigger_def.expire_pipeline
        if pipeline is not None:
            pipe_config = self.pipeline_config.get(pipeline)
            if pipe_config is None:
                debugger.bump_counter("Unknown pipeline '%s'" % pipeline)
                logger.error(
                    "Trigger %s for stream %s has unknown pipeline %s" % (
                        stream.name, stream.id, pipeline))
                self._expire_error_stream(stream)
            if not self._run_pipeline(stream, trigger_def, pipeline,
                                      pipe_config):
                self._expire_error_stream(stream)
                return False
        else:
            logger.debug("No expire pipeline for stream %s. Nothing to do." % (
                stream.id))
            debugger.bump_counter("No expire pipeline for '%s'" % stream.name)
        self._complete_stream(stream)
        debugger.bump_counter("Streams expired")
        self.streams_expired += 1
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
            fire_ct = self.process_ready_streams(
                self.pipeline_worker_batch_size)
            expire_ct = self.process_ready_streams(
                self.pipeline_worker_batch_size,
                expire=True)

            if ((self.current_time() - self.last_status).seconds
                    > self.statistics_period):
                self._log_statistics()

            if not fire_ct and not expire_ct:
                logger.debug("No streams to fire or expire. Sleeping...")
                time.sleep(self.pipeline_worker_delay)
