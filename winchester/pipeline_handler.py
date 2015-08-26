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

import abc
import collections
import datetime
import fnmatch
import json
import logging
import six
import time
import uuid

from notabene import kombu_driver as driver
import requests


logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class PipelineHandlerBase(object):
    """Base class for Pipeline handlers.

       Pipeline handlers perform the actual processing on a set of events
       captured by a stream. The handlers are chained together, each handler
       in a pipeline is called in order, and receives the output of the
       previous handler.

       Once all of the handlers in a pipeline have successfully processed the
       events (with .handle_events() ), each handler's .commit() method will be
       called. If any handler in the chain raises an exception, processing of
       events will stop, and each handler's .rollback() method will be called.

    """

    def __init__(self, **kw):
        """Setup the pipeline handler.

           A new instance of each handler for a pipeline is used for each
           stream (set of events) processed.

           :param kw: The parameters listed in the pipeline config file for
                      this handler (if any).
         """

    @abc.abstractmethod
    def handle_events(self, events, env):
        """This method handles the actual event processing.

        This method receives a list of events and should return a list of
        events as well. The return value of this method will be passed to
        the next handler's .handle_events() method. Handlers can add new
        events simply by adding them to the list they return. New events
        (those with unrecognized message_id's), will be saved to the
        database if all handlers in this pipeline complete successfully.
        Likewise, handlers can omit events from the list they return to
        act as a filter for downstream handlers.

        Care should be taken to avoid any operation with side-effects in
        this method. Pipelines can be re-tried if a handler throws an
        error. If you need to perform such operations, such as interacting
        with an external system, save the needed information in an instance
        variable, and perform the operation in the .commit() method.

        :param events: A list of events.
        :param env:  Just a dictionary, it's passed to each handler, and
                     can act as a shared scratchpad.

        :returns: A list of events.
        """

    @abc.abstractmethod
    def commit(self):
        """Called when each handler in this pipeline successfully completes.

        If you have operations with side effects, preform them here.
        Exceptions raised here will be logged, but otherwise ignored.
        """

    @abc.abstractmethod
    def rollback(self):
        """Called if error in any handler while processing a list of events.

        If you need to perform some kind of cleanup, do it here.
        Exceptions raised here will be logged, but otherwise ignored.
        """


class LoggingHandler(PipelineHandlerBase):
    def handle_events(self, events, env):
        emsg = ', '.join("%s: %s" % (event['event_type'], event['message_id'])
                         for event in events)
        logger.info("Received %s events: \n%s" % (len(events), emsg))
        return events

    def commit(self):
        pass

    def rollback(self):
        pass


class NotabeneException(Exception):
    pass


class ConnectionManager(object):
    def __init__(self):
        # {connection_properties:
        #   {exchange_properties: (connection, exchange)}}
        self.pool = {}

    def _extract_params(self, kw):
        host = kw.get('host', 'localhost')
        user = kw.get('user', 'guest')
        password = kw.get('password', 'guest')
        port = kw.get('port', 5672)
        vhost = kw.get('vhost', '/')
        library = kw.get('library', 'librabbitmq')
        exchange_name = kw.get('exchange')
        exchange_type = kw.get('exchange_type', 'topic')

        if exchange_name is None:
            raise NotabeneException("No 'exchange' name provided")

        connection_dict = {'host': host, 'port': port,
                           'user': user, 'password': password,
                           'library': library, 'vhost': vhost}
        connection_tuple = tuple(sorted(connection_dict.items()))

        exchange_dict = {'exchange_name': exchange_name,
                         'exchange_type': exchange_type}
        exchange_tuple = tuple(sorted(exchange_dict.items()))

        return (connection_dict, connection_tuple,
                exchange_dict, exchange_tuple)

    def get_connection(self, properties, queue_name):
        (connection_dict, connection_tuple,
         exchange_dict, exchange_tuple) = self._extract_params(properties)
        connection_info = self.pool.get(connection_tuple)
        if connection_info is None:
            connection = driver.create_connection(connection_dict['host'],
                                                  connection_dict['port'],
                                                  connection_dict['user'],
                                                  connection_dict['password'],
                                                  connection_dict['library'],
                                                  connection_dict['vhost'])
            connection_info = (connection, {})
            self.pool[connection_tuple] = connection_info
        connection, exchange_pool = connection_info
        exchange = exchange_pool.get(exchange_tuple)
        if exchange is None:
            exchange = driver.create_exchange(exchange_dict['exchange_name'],
                                              exchange_dict['exchange_type'])
            exchange_pool[exchange_tuple] = exchange

            # Make sure the queue exists so we don't lose events.
            queue = driver.create_queue(queue_name, exchange, queue_name,
                                        channel=connection.channel())
            queue.declare()

        return (connection, exchange)


# Global ConnectionManager. Shared by all Handlers.
connection_manager = ConnectionManager()


class NotabeneHandler(PipelineHandlerBase):
    # Handlers are created per stream, so we have to be smart about
    # things like connections to databases and queues.
    # We don't want to create too many connections, and we have to
    # remember that stream processing has to occur quickly, so
    # we want to avoid round-trips where possible.
    def __init__(self, **kw):
        super(NotabeneHandler, self).__init__(**kw)
        global connection_manager

        self.queue_name = kw.get('queue_name')
        if self.queue_name is None:
            raise NotabeneException("No 'queue_name' provided")
        self.connection, self.exchange = connection_manager.get_connection(
            kw, self.queue_name)

        self.env_keys = kw.get('env_keys', [])

    def handle_events(self, events, env):
        keys = [key for key in self.env_keys]
        self.pending_notifications = []
        for key in keys:
            self.pending_notifications.extend(env.get(key, []))
        return events

    def commit(self):
        for notification in self.pending_notifications:
            logger.info("Publishing '%s' to '%s' with routing_key '%s'" %
                        (notification['event_type'], self.exchange,
                         self.queue_name))
            try:
                driver.send_notification(notification, self.queue_name,
                                         self.connection, self.exchange)
            except Exception as e:
                logger.exception(e)

    def rollback(self):
        pass


class UsageException(Exception):
    def __init__(self, code, message):
        super(UsageException, self).__init__(message)
        self.code = code


class UsageHandler(PipelineHandlerBase):
    def __init__(self, **kw):
        super(UsageHandler, self).__init__(**kw)
        self.warnings = []

    def _get_audit_period(self, event):
        apb = event.get('audit_period_beginning')
        ape = event.get('audit_period_ending')
        return apb, ape

    def _is_exists(self, event):
        return event['event_type'] == 'compute.instance.exists'

    def _is_non_EOD_exists(self, event):
        # For non-EOD .exists, we just check that the APB and APE are
        # not 24hrs apart. We could check that it's not midnight, but
        # could be possible (though unlikely).
        # And, if we don't find any extras, don't error out ...
        # we'll do that later.
        apb, ape = self._get_audit_period(event)
        return (self._is_exists(event) and apb and ape
                and ape.date() != (apb.date() + datetime.timedelta(days=1)))

    def _is_EOD_exists(self, event):
        # We could have several .exists records, but only the
        # end-of-day .exists will have audit_period_* time of
        # 00:00:00 and be 24hrs apart.
        apb, ape = self._get_audit_period(event)
        return (self._is_exists(event) and apb and ape
                and apb.time() == datetime.time(0, 0, 0)
                and ape.time() == datetime.time(0, 0, 0)
                and ape.date() == (apb.date() + datetime.timedelta(days=1)))

    def _extract_launched_at(self, exists):
        if not exists.get('launched_at'):
            raise UsageException("U1", ".exists has no launched_at value.")
        return exists['launched_at']

    def _extract_interesting_events(self, events, interesting):
        return [event for event in events
                if event['event_type'] in interesting]

    def _find_deleted_events(self, events):
        interesting = ['compute.instance.delete.end']
        return self._extract_interesting_events(events, interesting)

    def _verify_fields(self, this, that, fields):
        for field in fields:
            if field not in this and field not in that:
                continue
            if this[field] != that[field]:
                raise UsageException("U2",
                                     "Conflicting '%s' values ('%s' != '%s')"
                                     % (field, this[field], that[field]))

    def _confirm_delete(self, exists, delete_events, fields):
        deleted_at = exists.get('deleted_at')
        state = exists.get('state')
        apb, ape = self._get_audit_period(exists)

        if not deleted_at and delete_events:
            raise UsageException("U6", ".deleted events found but .exists "
                                       "has no deleted_at value.")

        if deleted_at and state != "deleted":
            raise UsageException("U3", ".exists state not 'deleted' but "
                                       ".exists deleted_at is set.")

        if deleted_at and not delete_events:
            # We've already confirmed it's in the "deleted" state.
            launched_at = exists.get('launched_at')
            if deleted_at < launched_at:
                raise UsageException(
                    "U4",
                    ".exists deleted_at < .exists launched_at.")

            # Is the deleted_at within this audit period?
            if (apb and ape and deleted_at >= apb and deleted_at <= ape):
                raise UsageException("U5",
                                     ".exists deleted_at in audit "
                                     "period, but no matching .delete "
                                     "event found.")

        if len(delete_events) > 1:
            raise UsageException("U7", "Multiple .delete.end events")

        if delete_events:
            self._verify_fields(exists, delete_events[-1],  fields)

    def _confirm_launched_at(self, block, exists):
        if exists.get('state') != 'active':
            return

        apb, ape = self._get_audit_period(exists)

        # Does launched_at have a value within this audit period?
        # If so, we should have a related event. Otherwise, this
        # instance was created previously.
        launched_at = self._extract_launched_at(exists)
        if apb and ape and apb <= launched_at <= ape and len(block) == 0:
            raise UsageException("U8", ".exists launched_at in audit "
                                       "period, but no related events found.")

            # TODO(sandy): Confirm the events we got set launched_at
            # properly.

    def _get_core_fields(self):
        """Broken out so derived classes can define their own trait list."""
        return ['launched_at', 'instance_flavor_id', 'tenant_id',
                'os_architecture', 'os_version', 'os_distro']

    def _do_checks(self, block, exists):
        interesting = ['compute.instance.rebuild.end',
                       'compute.instance.finish_resize.end',
                       'compute.instance.rescue.end']

        self._confirm_launched_at(block, exists)

        fields = self._get_core_fields()
        last_interesting = None
        for event in block:
            if event['event_type'] in interesting:
                last_interesting = event
        if last_interesting:
            self._verify_fields(last_interesting, exists, fields)
        elif self._is_non_EOD_exists(exists):
            self.warnings.append("Non-EOD .exists found "
                                 "(msg_id: %s) "
                                 "with no operation event." %
                                 exists['message_id'])

        deleted = self._find_deleted_events(block)
        delete_fields = ['launched_at', 'deleted_at']
        self._confirm_delete(exists, deleted, delete_fields)

    def _base_notification(self, exists):
        basen = exists.copy()
        if 'bandwidth_in' not in basen:
            basen['bandwidth_in'] = 0
        if 'bandwidth_out' not in basen:
            basen['bandwidth_out'] = 0
        if 'rax_options' not in basen:
            basen['rax_options'] = '0'
        basen['original_message_id'] = exists.get('message_id', '')
        return basen
#       apb, ape = self._get_audit_period(exists)
#       return {
#           'payload': {
#               'audit_period_beginning': str(apb),
#               'audit_period_ending': str(ape),
#               'launched_at': str(exists.get('launched_at', '')),
#               'deleted_at': str(exists.get('deleted_at', '')),
#               'instance_id': exists.get('instance_id', ''),
#               'tenant_id': exists.get('tenant_id', ''),
#               'display_name': exists.get('display_name', ''),
#               'instance_type': exists.get('instance_flavor', ''),
#               'instance_flavor_id': exists.get('instance_flavor_id', ''),
#               'state': exists.get('state', ''),
#               'state_description': exists.get('state_description', ''),
#               'bandwidth': {'public': {
#                   'bw_in': exists.get('bandwidth_in', 0),
#                   'bw_out': exists.get('bandwidth_out', 0)}},
#               'image_meta': {
#                   'org.openstack__1__architecture': exists.get(
#                       'os_architecture', ''),
#                   'org.openstack__1__os_version': exists.get('os_version',
#                                                              ''),
#                   'org.openstack__1__os_distro': exists.get('os_distro', ''),
#                   'org.rackspace__1__options': exists.get('rax_options', '0')
#               }
#           },
#           'original_message_id': exists.get('message_id', '')}

    def _generate_new_id(self, original_message_id, event_type):
        # Generate message_id for new events deterministically from
        # the original message_id and event type using uuid5 algo.
        # This will allow any dups to be caught by message_id. (mdragon)
        if original_message_id:
            oid = uuid.UUID(original_message_id)
            return uuid.uuid5(oid, event_type)
        else:
            logger.error("Generating %s, but origional message missing"
                         " origional_message_id." % event_type)
            return uuid.uuid4()

    def _process_block(self, block, exists):
        error = None
        try:
            self._do_checks(block, exists)
            event_type = "compute.instance.exists.verified"
        except UsageException as e:
            error = e
            event_type = "compute.instance.exists.failed"
            logger.warn("Stream %s UsageException: (%s) %s" %
                        (self.stream_id, e.code, e))
            apb, ape = self._get_audit_period(exists)
            logger.warn("Stream %s deleted_at: %s, launched_at: %s, "
                        "state: %s, APB: %s, APE: %s, #events: %s" %
                        (self.stream_id, exists.get("deleted_at"),
                         exists.get("launched_at"), exists.get("state"),
                         apb, ape, len(block)))

        if len(block) > 1:
            logger.warn("%s - events (stream: %s)"
                        % (event_type, self.stream_id))
            for event in block:
                logger.warn("^Event: %s - %s" %
                            (event['timestamp'], event['event_type']))

        events = []
        # We could have warnings, but a valid event list.
        if self.warnings:
            instance_id = exists.get('instance_id', 'n/a')
            warning_event = {'event_type': 'compute.instance.exists.warnings',
                             'publisher_id': 'stv3',
                             'message_id': str(uuid.uuid4()),
                             'timestamp': exists.get(
                                 'timestamp',
                                 datetime.datetime.utcnow()),
                             'stream_id': int(self.stream_id),
                             'instance_id': instance_id,
                             'warnings': ', '.join(self.warnings)}
            events.append(warning_event)

        new_event = self._base_notification(exists)
        new_event['message_id'] = self._generate_new_id(
            new_event['original_message_id'], event_type)
        new_event.update({'event_type': event_type,
                          'publisher_id': 'stv3',
                          'timestamp': exists.get('timestamp',
                                                  datetime.datetime.utcnow()),
                          'stream_id': int(self.stream_id),
                          'error': str(error),
                          'error_code': error and error.code})
        events.append(new_event)
        return events

    def handle_events(self, events, env):
        self.env = env
        self.stream_id = env['stream_id']
        self.warnings = []

        new_events = []
        block = []
        for event in events:
            if self._is_exists(event):
                new_events.extend(self._process_block(block, event))
                block = []
            else:
                block.append(event)

        # Final block should be empty.
        if block:
            new_event = {
                'event_type': "compute.instance.exists.failed",
                'message_id': str(uuid.uuid4()),
                'timestamp': block[0].get('timestamp',
                                          datetime.datetime.utcnow()),
                'stream_id': int(self.stream_id),
                'instance_id': block[0].get('instance_id'),
                'error': "Notifications, but no .exists "
                         "notification found.",
                'error_code': "U0"
            }
            new_events.append(new_event)

        return events + new_events

    def commit(self):
        pass

    def rollback(self):
        pass


class AtomPubException(Exception):
    pass


cuf_template = ("""<event xmlns="http://docs.rackspace.com/core/event" """
                """xmlns:nova="http://docs.rackspace.com/event/nova" """
                """version="1" """
                """id="%(message_id)s" resourceId="%(instance_id)s" """
                """resourceName="%(display_name)s" """
                """dataCenter="%(data_center)s" """
                """region="%(region)s" tenantId="%(tenant_id)s" """
                """startTime="%(start_time)s" endTime="%(end_time)s" """
                """type="USAGE"><nova:product version="1" """
                """serviceCode="CloudServersOpenStack" """
                """resourceType="SERVER" """
                """flavorId="%(instance_flavor_id)s" """
                """flavorName="%(instance_flavor)s" """
                """status="%(status)s" %(options)s """
                """bandwidthIn="%(bandwidth_in)s" """
                """bandwidthOut="%(bandwidth_out)s"/></event>""")


class AtomPubHandler(PipelineHandlerBase):
    auth_token_cache = None

    def __init__(self, url, event_types=None, extra_info=None,
                 content_format='json', title=None, categories=None,
                 auth_user='', auth_key='', auth_server='',
                 wait_interval=30, max_wait=600, http_timeout=120, **kw):
        super(AtomPubHandler, self).__init__(**kw)
        self.events = []
        self.included_types = []
        self.excluded_types = []
        self.url = url
        self.auth_user = auth_user
        self.auth_key = auth_key
        self.auth_server = auth_server
        self.wait_interval = wait_interval
        self.max_wait = max_wait
        self.http_timeout = http_timeout
        self.content_format = content_format
        self.title = title
        self.categories = categories
        if extra_info:
            self.extra_info = extra_info
        else:
            self.extra_info = {}

        if event_types:
            if isinstance(event_types, six.string_types):
                event_types = [event_types]
            for t in event_types:
                if t.startswith('!'):
                    self.excluded_types.append(t[1:])
                else:
                    self.included_types.append(t)
        else:
            self.included_types.append('*')
        if self.excluded_types and not self.included_types:
            self.included_types.append('*')

    def _included_type(self, event_type):
        return any(fnmatch.fnmatch(event_type, t) for t in self.included_types)

    def _excluded_type(self, event_type):
        return any(fnmatch.fnmatch(event_type, t) for t in self.excluded_types)

    def match_type(self, event_type):
        return (self._included_type(event_type)
                and not self._excluded_type(event_type))

    def handle_events(self, events, env):
        for event in events:
            event_type = event['event_type']
            if self.match_type(event_type):
                self.events.append(event)
        logger.debug("Matched %s events." % len(self.events))
        return events

    def commit(self):
        for event in self.events:
            event_type = event.get('event_type', '')
            message_id = event.get('message_id', '')
            try:
                status = self.publish_event(event)
                logger.debug("Sent %s event %s. Status %s" % (event_type,
                                                              message_id,
                                                              status))
            except Exception:
                original_message_id = event.get('original_message_id', '')
                logger.exception("Error publishing %s event %s "
                                 "(original id: %s)!" % (event_type,
                                                         message_id,
                                                         original_message_id))

    def publish_event(self, event):
        content, content_type = self.format(event)
        event_type = self.get_event_type(event.get('event_type'))
        atom = self.generate_atom(event, event_type, content, content_type,
                                  title=self.title, categories=self.categories)

        logger.debug("Publishing event: %s" % atom)
        return self._send_event(atom)

    def generate_atom(self, event, event_type, content, content_type,
                      categories=None, title=None):
        template = ("""<atom:entry xmlns:atom="http://www.w3.org/2005/Atom">"""
                    """<atom:id>urn:uuid:%(message_id)s</atom:id>"""
                    """%(categories)s"""
                    """<atom:title type="text">%(title)s</atom:title>"""
                    """<atom:content type="%(content_type)s">%(content)s"""
                    """</atom:content></atom:entry>""")
        if title is None:
            title = event_type
        if categories is None:
            cats = []
        else:
            cats = categories[:]
        cats.append(event_type)
        original_message_id = event.get('original_message_id')
        if original_message_id is not None:
            cats.append('original_message_id:%s' % original_message_id)
        cattags = ''.join("""<atom:category term="%s" />""" % cat
                          for cat in cats)
        info = dict(message_id=event.get('message_id'),
                    original_message_id=original_message_id,
                    event=event,
                    event_type=event_type,
                    content=content,
                    categories=cattags,
                    title=title,
                    content_type=content_type)
        return template % info

    def get_event_type(self, event_type):
        etf = getattr(self, 'event_type_%s' % self.content_format, None)
        if etf:
            return etf(event_type)
        return event_type

    def event_type_cuf_xml(self, event_type):
        return event_type + ".cuf"

    def format(self, event):
        eff = getattr(self, 'format_%s' % self.content_format, None)
        if eff is None:
            eff = getattr(self, 'format_json')
        return eff(event)

    def format_json(self, event):
        c = json.dumps(event)
        return (c, 'application/json')

    def format_cuf_xml(self, event):
        tvals = collections.defaultdict(lambda: '')
        tvals.update(event)
        tvals.update(self.extra_info)
        start_time, end_time = self._get_times(event)
        tvals['start_time'] = self._format_time(start_time)
        tvals['end_time'] = self._format_time(end_time)
        tvals['status'] = self._get_status(event)
        tvals['options'] = self._get_options(event)
        c = cuf_template % tvals
        return (c, 'application/xml')

    def _get_options(self, event):
        opt = int(event.get('rax_options', 0))
        flags = [bool(opt & (2**i)) for i in range(8)]
        os = 'LINUX'
        app = None
        if flags[0]:
            os = 'RHEL'
        if flags[2]:
            os = 'WINDOWS'
        if flags[6]:
            os = 'VYATTA'
        if flags[3]:
            app = 'MSSQL'
        if flags[5]:
            app = 'MSSQL_WEB'
        if app is None:
            return 'osLicenseType="%s"' % os
        else:
            return 'osLicenseType="%s" applicationLicense="%s"' % (os, app)

    def _get_status(self, event):
        state = event.get('state')
        state_description = event.get('state_description')
        status = 'UNKNOWN'
        status_map = {
            "building": 'BUILD',
            "stopped": 'SHUTOFF',
            "paused": 'PAUSED',
            "suspended": 'SUSPENDED',
            "rescued": 'RESCUE',
            "error": 'ERROR',
            "deleted": 'DELETED',
            "soft-delete": 'SOFT_DELETED',
            "shelved": 'SHELVED',
            "shelved_offloaded": 'SHELVED_OFFLOADED',
        }
        if state in status_map:
            status = status_map[state]
        if state == 'resized':
            if state_description == 'resize_reverting':
                status = 'REVERT_RESIZE'
            else:
                status = 'VERIFY_RESIZE'
        if state == 'active':
            active_map = {
                "rebooting": 'REBOOT',
                "rebooting_hard": 'HARD_REBOOT',
                "updating_password": 'PASSWORD',
                "rebuilding": 'REBUILD',
                "rebuild_block_device_mapping": 'REBUILD',
                "rebuild_spawning": 'REBUILD',
                "migrating": 'MIGRATING',
                "resize_prep": 'RESIZE',
                "resize_migrating": 'RESIZE',
                "resize_migrated": 'RESIZE',
                "resize_finish": 'RESIZE',
            }
            status = active_map.get(state_description, 'ACTIVE')
        if status == 'UNKNOWN':
            logger.error("Unknown status for event %s: state %s (%s)" % (
                         event.get('message_id'), state, state_description))
        return status

    def _get_times(self, event):
        audit_period_beginning = event.get('audit_period_beginning')
        audit_period_ending = event.get('audit_period_ending')
        launched_at = event.get('launched_at')
        terminated_at = event.get('terminated_at')
        if not terminated_at:
            terminated_at = event.get('deleted_at')

        start_time = max(launched_at, audit_period_beginning)
        if not terminated_at:
            end_time = audit_period_ending
        else:
            end_time = min(terminated_at, audit_period_ending)
        if start_time > end_time:
            start_time = audit_period_beginning
        return (start_time, end_time)

    def _format_time(self, dt):
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        if dt:
            return datetime.datetime.strftime(dt, time_format)
        else:
            return ''

    def _get_auth(self, force=False, headers=None):
        if headers is None:
            headers = {}
        if force or not AtomPubHandler.auth_token_cache:
            auth_body = {"auth": {
                         "RAX-KSKEY:apiKeyCredentials": {
                             "username": self.auth_user,
                             "apiKey": self.auth_key,
                         }}}
            auth_headers = {"User-Agent": "Winchester",
                            "Accept": "application/json",
                            "Content-Type": "application/json"}
            logger.debug("Contacting  auth server %s" % self.auth_server)
            res = requests.post(self.auth_server,
                                data=json.dumps(auth_body),
                                headers=auth_headers)
            res.raise_for_status()
            token = res.json()["access"]["token"]["id"]
            logger.debug("Token received: %s" % token)
            AtomPubHandler.auth_token_cache = token
        headers["X-Auth-Token"] = AtomPubHandler.auth_token_cache
        return headers

    def _send_event(self, atom):
        headers = {"Content-Type": "application/atom+xml"}
        headers = self._get_auth(headers=headers)
        attempts = 0
        status = 0
        while True:
            try:
                res = requests.post(self.url,
                                    data=atom,
                                    headers=headers,
                                    timeout=self.http_timeout)
                status = res.status_code
                if status >= 200 and status < 300:
                    break
                if status == 401:
                    logger.info("Auth expired, reauthorizing...")
                    headers = self._get_auth(headers=headers, force=True)
                    continue
                if status == 409:
                    # they already have this. No need to retry. (mdragon)
                    logger.debug("Duplicate message: \n%s" % atom)
                    break
                if status == 400:
                    # AtomPub server won't accept content.
                    logger.error("Invalid Content: Server rejected content: "
                                 "\n%s" % atom)
                    break
            except requests.exceptions.ConnectionError:
                logger.exception("Connection error talking to %s" % self.url)
            except requests.exceptions.Timeout:
                logger.exception("HTTP timeout talking to %s" % self.url)
            except requests.exceptions.HTTPError:
                logger.exception("HTTP protocol error talking to "
                                 "%s" % self.url)
            except requests.exceptions.RequestException:
                logger.exception("Unknown exeption talking to %s" % self.url)
            # If we got here, something went wrong
            attempts += 1
            wait = min(attempts * self.wait_interval, self.max_wait)
            logger.error("Message delivery failed, going to sleep, will "
                         "try again in %s seconds" % str(wait))
            time.sleep(wait)
        return status

    def rollback(self):
        pass
