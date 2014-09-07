winchester
==========

An OpenStack notification event processing library based on persistant streams.

Winchester is designed to process event streams, such as those produced from
OpenStack notifications. Events are represented as simple python dictionaries.
They should be flat dictionaries (not nested), with a minimum of three keys:

    "message_id":   A unique identifier for this event, such as a uuid.
    "event_type":   A string identifying the event's type. Usually a hierarchical dotted name like "foo.bar.baz"
    "timestamp":    Time the event occurred (a python datetime, in UTC)

The individual keys of the event dictionary are called *traits* and can be
strings, integers, floats or datetimes. For processing of the (often large)
notifications that come out of OpenStack, winchester uses the
[StackDistiller library](https://github.com/StackTach/stackdistiller) to
extract flattened events from the notifications, that only contain the data
you actually need for processing.

Winchester's processing is done through *triggers* and *pipelines*.

A *trigger* is composed of a *match_criteria* which is like a
persistant query, collecting events you want to process into a
persistant *stream* (stored in a sql database), a set of distinguishing
traits, which can separate your list of events into distinct streams,
similar to a **GROUP BY** clause in an SQL query, and a *fire_criteria*,
which specifies the conditions a given *stream* has to match for the
trigger to fire. When it does, the events in the *stream* are sent to
a *pipeline* listed as the *fire_pipeline* for processing as a batch.
Also listed is an *expire_timestamp*. If a given stream does not meet
the *fire_criteria* by that time, it is expired, and can be sent to
an *expire_pipeline* for alternate processing. Both *fire_pipeline*
and *expire_pipeline* are optional, but at least one of them must
be specified.

A *pipeline* is simply a list of simple *handlers*. Each *handler*
in the pipeline receives the list of events in a given stream,
sorted by timestamp, in turn. *Handlers* can filter events from the list,
or add new events to it. These changes will be seen by *handlers* further
down the pipeline. *Handlers* should avoid operations with side-effects,
other than modifying the list of events, as pipeline processing can be
re-tried later if there is an error. Instead, if all handlers process the
list of events without raising an exception, a *commit* call is made on
each handler, giving it the chance to perform actions, like sending data
to external systems. *Handlers* are simple to write, as pretty much any
object that implements the appropriate *handle_events*, *commit* and
*rollback* methods can be a *handler*.

## Installing and running.

Winchster is installable as a simple python package.
Once installed, and the appropriate database url is specified in the
*winchester.yaml* config file (example included in the *etc* directory),
you can create the appropriate database schema with:

    winchester_db -c <path_to_your_config_files>/winchester.yaml upgrade head

If you need to run the SQL by hand, or just want to look at the schema, the
following will print out the appropriate table creation SQL:

    winchester_db -c <path_to_your_config_files>/winchester.yaml upgrade --sql head

Once you have done that, and configured the appropriate *triggers.yaml*,
*pipelines.yaml*, and, if using StackDistiller, *event_definitions.yaml* configs
(again, examples are in *etc* in the winchester codebase), you can add events
into the system by calling the *add_event* method of Winchester's TriggerManager.
If you are processing OpenStack notifications, you can call *add_notification*,
which will pare down the notification into an event with StackDistiller, and
then call *add_event* with that. If you are reading OpenStack notifications off
of a RabbitMQ queue, there is a plugin for the
[Yagi](https://github.com/rackerlabs/yagi) notification processor included with
Winchester. Simply add "winchester.yagi\_handler.WinchesterHandler" to the "apps"
line in your *yagi.conf* section for the queues you want to listen to, and add a:

    [winchester]
    config_file = <path_to_your_config_files>/winchester.yaml

section to the *yagi.conf*.

To run the actual pipeline processing, which is run as a separate daemon, run:

    pipeline_worker -c <path_to_your_config_files>/winchester.yaml

You can pass the *-d* flag to the *pipeline_worker* to tell it to run as a background
daemon.

Winchester uses an optimistic locking scheme in the database to coordinate firing,
expiring, and processing of streams, so you can run as many processes (like
Yagi's *yagi-event* daemon) feeding TriggerManagers as you need to handle the
incoming events, and as many *pipeline_worker*s as you need to handle the resulting
processing load, scaling the system horizontally.
