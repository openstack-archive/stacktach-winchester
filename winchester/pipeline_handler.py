import abc
import logging
import six


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


