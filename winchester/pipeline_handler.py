import abc
import datetime
import logging
import six
import uuid


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


class UsageException(Exception):
    def __init__(self, code, message):
        super(UsageException, self).__init__(message)
        self.code = code


class UsageHandler(PipelineHandlerBase):
    def _find_exists(self, events):
        exists = None

        # We could have several .exists records, but only the
        # end-of-day .exists will have audit_period_* time of
        # 00:00:00 and be 24hrs apart.
        for event in events:
            apb = event.get('audit_period_beginning')
            ape = event.get('audit_period_ending')
            if (event['event_type'] == 'compute.instance.exists'
                and apb and ape and apb.time() == datetime.time(0, 0, 0)
                and ape.time() == datetime.time(0, 0, 0)
                and ape.date() == (apb.date() + datetime.timedelta(days=1))):
                exists = event
                self.audit_beginning = apb
                self.audit_ending = ape
                break

        if not exists:
            raise UsageException("U0", "No .exists record.")

        return exists

    def _extract_launched_at(self, exists):
        if not exists.get('launched_at'):
            raise UsageException("U1", ".exists has no launched_at value.")
        return exists['launched_at']

    def _extract_interesting_events(self, events, interesting):
        return [event for event in events
                        if event['event_type'] in interesting]

    def _find_events(self, events):
        interesting = ['compute.instance.rebuild.start',
                       'compute.instance.resize.prep.start',
                       'compute.instance.resize.revert.start',
                       'compute.instance.rescue.start',
                       'compute.instance.create.end',
                       'compute.instance.rebuild.end',
                       'compute.instance.resize.finish.end',
                       'compute.instance.resize.revert.end',
                       'compute.instance.rescue.end']

        # We could easily end up with no events in final_set if
        # there were no operations performed on an instance that day.
        # We'll still get a .exists for every active instance though.

        return self._extract_interesting_events(events, interesting)

    def _find_deleted_events(self, events):
        interesting = ['compute.instance.delete.end']
        return self._extract_interesting_events(events, interesting)

    def _verify_fields(self, exists, launch, fields):
        for field in fields:
            if field not in exists and field not in launch:
                continue
            if exists[field] != launch[field]:
                raise UsageException("U2",
                                "Conflicting '%s' values ('%s' != '%s')"
                                % (field, exists[field], launch[field]))

    def _confirm_delete(self, exists, deleted, fields):
        deleted_at = exists.get('deleted_at')
        state = exists.get('state')

        if deleted_at and state != "deleted":
            raise UsageException("U3", ".exists state not 'deleted' but "
                             "deleted_at is set.")

        if deleted_at and not deleted:
            # We've already confirmed it's in the "deleted" state.
            launched_at = exists.get('launched_at')
            if deleted_at < launched_at:
                raise UsageException("U4",
                                    ".exists deleted_at < launched_at.")

            # Is the deleted_at within this audit period?
            if (deleted_at >= self.audit_beginning
                                and deleted_at <= self.audit_ending):
                raise UsageException("U5", ".exists deleted_at in audit "
                    "period, but no matching .delete event found.")

        if not deleted_at and deleted:
            raise UsageException("U6", ".deleted events found but .exists "
                                "has no deleted_at value.")

        if len(deleted) > 1:
            raise UsageException("U7", "Multiple .delete.end events")

        if deleted:
            self._verify_fields(exists, deleted[0],  fields)

    def _confirm_launched_at(self, exists, events):
        if exists.get('state') != 'active':
            return

        # Does launched_at have a value within this audit period?
        # If so, we should have a related event. Otherwise, this
        # instance was created previously.
        launched_at = exists['launched_at']
        if (launched_at >= self.audit_beginning
                and launched_at <= self.audit_ending and len(events) == 1):
            raise UsageException("U8", ".exists launched_at in audit "
                "period, but no related events found.")

        # TODO(sandy): Confirm the events we got set launched_at
        # properly.

    def _get_core_fields(self):
        """Broken out so derived classes can define their
           own trait list."""
        return ['launched_at', 'instance_type_id', 'tenant_id',
                'os_architecture', 'os_version', 'os_distro']

    def _do_checks(self, exists, events):
        core_fields = self._get_core_fields()
        delete_fields = ['launched_at', 'deleted_at']

        self._extract_launched_at(exists)
        deleted = self._find_deleted_events(events)
        for c in self._find_events(events):
            self._verify_fields(exists, c, core_fields)

        self._confirm_launched_at(exists, events)
        self._confirm_delete(exists, deleted, delete_fields)

    def handle_events(self, events, env):
        self.env = env
        self.stream_id = env['stream_id']

        exists = None
        error = None
        try:
            exists = self._find_exists(events)
            self._do_checks(exists, events)
            event_type = "compute.instance.exists.verified"
        except UsageException as e:
            error = e
            event_type = "compute.instance.exists.failed"
            logger.warn("Stream %s UsageException: (%s) %s" %
                                            (self.stream_id, e.code, e))
            if exists:
                logger.warn("Stream %s deleted_at: %s, launched_at: %s, "
                            "state: %s, APB: %s, APE: %s, #events: %s" %
                            (self.stream_id, exists.get("deleted_at"),
                             exists.get("launched_at"), exists.get("state"),
                             exists.get("audit_period_beginning"),
                             exists.get("audit_period_ending"), len(events)))

        if len(events) > 1:
            logger.warn("Events for Stream: %s" % self.stream_id)
            for event in events:
                logger.warn("^Event: %s - %s" %
                                    (event['timestamp'], event['event_type']))

        if exists:
            new_event = {'event_type': event_type,
                         'message_id': str(uuid.uuid4()),
                         'timestamp': exists.get('timestamp',
                                                datetime.datetime.utcnow()),
                         'stream_id': int(self.stream_id),
                         'instance_id': exists.get('instance_id'),
                         'error': str(error),
                         'error_code': error and error.code
                        }
            events.append(new_event)
        else:
            logger.debug("No .exists record")
        return events

    def commit(self):
        pass

    def rollback(self):
        pass
