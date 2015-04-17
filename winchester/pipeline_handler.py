import abc
import datetime
import logging
import six
import uuid

from notabene import kombu_driver as driver


logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class PipelineHandlerBase(object):
    """Base class for Pipeline handlers.

       Pipeline handlers perform the actual processing on a set of events
       captured by a stream. The handlers are chained together, each handler
       in a pipeline is called in order, and receives the output of the previous
       handler.

       Once all of the handlers in a pipeline have successfully processed the
       events (with .handle_events() ), each handler's .commit() method will be
       called. If any handler in the chain raises an exception, processing of
       events will stop, and each handler's .rollback() method will be called."""

    def __init__(self, **kw):
       """Setup the pipeline handler.

          A new instance of each handler for a pipeline is used for each
          stream (set of events) processed.

          :param kw: The parameters listed in the pipeline config file for
                     this handler (if any).
        """

    @abc.abstractmethod
    def handle_events(self, events, env):
        """ This method handles the actual event processing.

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
        """ Called when each handler in this pipeline has successfully
            completed.

            If you have operations with side effects, preform them here.
            Exceptions raised here will be logged, but otherwise ignored.
        """

    @abc.abstractmethod
    def rollback(self):
        """ Called if there is an error for any handler while processing a list
        of events.

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
        connection_dict, connection_tuple, \
        exchange_dict, exchange_tuple = self._extract_params(properties)
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
                raise UsageException("U4",
                              ".exists deleted_at < .exists launched_at.")

            # Is the deleted_at within this audit period?
            if (apb and ape and deleted_at >= apb and deleted_at <= ape):
                raise UsageException("U5", ".exists deleted_at in audit "
                    "period, but no matching .delete event found.")

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
        if (apb and ape and
            launched_at >= apb and launched_at <= ape and
            len(block) == 0):
                raise UsageException("U8", ".exists launched_at in audit "
                    "period, but no related events found.")

        # TODO(sandy): Confirm the events we got set launched_at
        # properly.

    def _get_core_fields(self):
        """Broken out so derived classes can define their
           own trait list."""
        return ['launched_at', 'instance_flavor_id', 'tenant_id',
                'os_architecture', 'os_version', 'os_distro']

    def _do_checks(self, block, exists):
        interesting = ['compute.instance.rebuild.end',
                       'compute.instance.resize.prep.end',
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
        apb, ape = self._get_audit_period(exists)
        return {
            'payload': {
              'audit_period_beginning': str(apb),
              'audit_period_ending': str(ape),
              'launched_at': str(exists.get('launched_at', '')),
              'deleted_at': str(exists.get('deleted_at', '')),
              'instance_id': exists.get('instance_id', ''),
              'tenant_id': exists.get('tenant_id', ''),
              'display_name': exists.get('display_name', ''),
              'instance_type': exists.get('instance_flavor', ''),
              'instance_flavor_id': exists.get('instance_flavor_id', ''),
              'state': exists.get('state', ''),
              'state_description': exists.get('state_description', ''),
              'bandwidth': {'public': {
                              'bw_in': exists.get('bandwidth_in', 0),
                              'bw_out': exists.get('bandwidth_out', 0)}},
              'image_meta': {
                'org.openstack__1__architecture':
                                            exists.get('os_architecture', ''),
                'org.openstack__1__os_version': exists.get('os_version', ''),
                'org.openstack__1__os_distro': exists.get('os_distro', ''),
                'org.rackspace__1__options': exists.get('rax_options', '0')
              }},
            'original_message_id': exists.get('message_id', '')}

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
                             'timestamp': exists.get('timestamp',
                                                  datetime.datetime.utcnow()),
                             'stream_id': int(self.stream_id),
                             'instance_id': exists.get('instance_id'),
                             'warnings': self.warnings}
            events.append(warning_event)

        new_event = self._base_notification(exists)
        new_event.update({'event_type': event_type,
                          'message_id': str(uuid.uuid4()),
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
            new_event = {'event_type': "compute.instance.exists.failed",
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

        env['usage_notifications'] = new_events
        return events

    def commit(self):
        pass

    def rollback(self):
        pass
