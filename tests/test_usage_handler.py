import unittest2 as unittest

import datetime
import mock

from winchester import pipeline_handler


class TestUsageHandler(unittest.TestCase):
    def setUp(self):
        super(TestUsageHandler, self).setUp()
        self.handler = pipeline_handler.UsageHandler()

    def test_find_exists_happyday(self):
        start = datetime.datetime(2014, 12, 31, 0, 0, 0)
        end = start + datetime.timedelta(days=1)
        events = [{'event_type': 'event_1'},
                  {'event_type': 'event_2'},
                  {'event_type': 'compute.instance.exists',
                   'audit_period_beginning': start,
                   'audit_period_ending': end}]

        exists = self.handler._find_exists(events)
        self.assertEquals(exists, events[2])

    def test_find_exists_none(self):
        events = [{'event_type': 'event_1'},
                  {'event_type': 'event_2'}]

        with self.assertRaises(pipeline_handler.UsageException):
            self.handler._find_exists(events)

    def test_find_exists_midday(self):
        start = datetime.datetime(2014, 12, 31, 1, 1, 1)
        end = datetime.datetime(2014, 12, 31, 1, 1, 2)
        events = [{'event_type': 'event_1'},
                  {'event_type': 'event_2'},
                  {'event_type': 'compute.instance.exists',
                   'audit_period_beginning': start,
                   'audit_period_ending': end}]

        with self.assertRaises(pipeline_handler.UsageException):
            self.handler._find_exists(events)

    def test_find_exists_long(self):
        start = datetime.datetime(2014, 12, 31, 0, 0, 0)
        end = start + datetime.timedelta(days=2)
        events = [{'event_type': 'event_1'},
                  {'event_type': 'event_2'},
                  {'event_type': 'compute.instance.exists',
                   'audit_period_beginning': start,
                   'audit_period_ending': end}]

        with self.assertRaises(pipeline_handler.UsageException):
            self.handler._find_exists(events)

    def test_find_exists_no_audit_periods(self):
        events = [{'event_type': 'event_1'},
                  {'event_type': 'event_2'},
                  {'event_type': 'compute.instance.exists'}]

        with self.assertRaises(pipeline_handler.UsageException):
            self.handler._find_exists(events)

    def test_extract_launched_at(self):
        with self.assertRaises(pipeline_handler.UsageException):
            self.handler._extract_launched_at({})
        self.assertEquals("foo", self.handler._extract_launched_at(
                                                    {'launched_at': 'foo'}))

    def test_extract_interesting(self):
        interesting = ["a", "b", "c"]
        e1 = {'event_type': 'a'}
        e2 = {'event_type': 'b'}
        e3 = {'event_type': 'c'}
        e4 = {'event_type': 'd'}
        e5 = {'event_type': 'e'}
        self.assertEquals([e1, e2, e3],
                          self.handler._extract_interesting_events(
                            [e4, e1, e2, e3, e5], interesting))

    def test_verify_fields_no_match(self):
        exists = {'a': 1, 'b': 2, 'c': 3}
        launched = exists
        self.handler._verify_fields(exists, launched, ['d', 'e', 'f'])

    def test_verify_fields_happyday(self):
        exists = {'a': 1, 'b': 2, 'c': 3}
        launched = exists
        self.handler._verify_fields(exists, launched, ['a', 'b', 'c'])

    def test_verify_fields_mismatch(self):
        exists = {'a': 1, 'b': 2, 'c': 3}
        launched = {'a': 10, 'b': 20, 'c': 30}
        with self.assertRaises(pipeline_handler.UsageException):
            self.handler._verify_fields(exists, launched, ['a', 'b', 'c'])

    def test_confirm_delete_no_delete_events(self):
        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_delete({'deleted_at': 'now',
                                          'state': 'active'}, [], [])
            self.assertEquals("U3", e.code)

        deleted_at = datetime.datetime(2014, 12, 31, 1, 0, 0)
        launched_at = datetime.datetime(2014, 12, 31, 2, 0, 0)
        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_delete({'deleted_at': deleted_at,
                                          'launched_at': launched_at,
                                          'state': 'deleted'}, [], [])
            self.assertEquals("U4", e.code)

        self.handler.audit_beginning = datetime.datetime(2014, 12, 30, 0, 0, 0)
        self.handler.audit_ending = datetime.datetime(2014, 12, 31, 0, 0, 0)
        deleted_at = datetime.datetime(2014, 12, 30, 2, 0, 0)
        launched_at = datetime.datetime(2014, 12, 30, 1, 0, 0)
        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_delete({'deleted_at': deleted_at,
                                          'launched_at': launched_at,
                                          'state': 'deleted'}, [], [])
            self.assertEquals("U5", e.code)

        # Test the do-nothing scenario
        self.handler._confirm_delete({}, [], [])

    def test_confirm_delete_delete_events(self):
        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_delete({}, [{}], [])
            self.assertEquals("U6", e.code)

        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_delete({'deleted_at': 'now'}, [{}, {}], [])
            self.assertEquals("U7", e.code)

        with mock.patch.object(self.handler, "_verify_fields") as v:
            exists = {'deleted_at': 'now', 'state': 'deleted'}
            deleted = {'foo': 1}
            self.handler._confirm_delete(exists, [deleted], ['a'])
            v.assert_called_with(exists, deleted, ['a'])

    def test_confirm_launched_at(self):
        self.handler._confirm_launched_at({'state': 'deleted'}, [])

        self.handler.audit_beginning = datetime.datetime(2014, 12, 30, 0, 0, 0)
        self.handler.audit_ending = datetime.datetime(2014, 12, 31, 0, 0, 0)
        launched_at = datetime.datetime(2014, 12, 30, 1, 0, 0)
        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_launched_at({'state': 'active',
                                               'launched_at': launched_at},
                                              [{}])
            self.assertEquals("U8", e.code)

    def test_handle_events_no_exists(self):
        env = {'stream_id': 'stream'}
        with mock.patch.object(self.handler, "_find_exists") as c:
            c.side_effect = pipeline_handler.UsageException("UX", "Error")
            events = self.handler.handle_events([], env)
            self.assertEquals(0, len(events))

    def test_handle_events_exists(self):
        env = {'stream_id': 'stream'}
        with mock.patch.object(self.handler, "_find_exists") as ex:
            ex.return_value = {'timestamp':'now', 'instance_id':'inst'}
            with mock.patch.object(self.handler, "_do_checks") as c:
                events = self.handler.handle_events([], env)
                self.assertEquals(1, len(events))
                f = events[0]
                self.assertEquals("compute.instance.exists.verified",
                                  f['event_type'])
                self.assertEquals("now", f['timestamp'])
                self.assertEquals("stream", f['stream_id'])
                self.assertEquals("inst", f['instance_id'])
                self.assertEquals("None", f['error'])
                self.assertIsNone(f['error_code'])

    def test_handle_events_bad(self):
        env = {'stream_id': 'stream'}
        with mock.patch.object(self.handler, "_find_exists") as ex:
            ex.return_value = {'timestamp':'now', 'instance_id':'inst'}
            with mock.patch.object(self.handler, "_do_checks") as c:
                c.side_effect = pipeline_handler.UsageException("UX", "Error")
                events = self.handler.handle_events([], env)
                self.assertEquals(1, len(events))
                f = events[0]
                self.assertEquals("compute.instance.exists.failed",
                                  f['event_type'])
                self.assertEquals("now", f['timestamp'])
                self.assertEquals("stream", f['stream_id'])
                self.assertEquals("inst", f['instance_id'])
                self.assertEquals("Error", f['error'])
                self.assertEquals("UX", f['error_code'])


    @mock.patch.object(pipeline_handler.UsageHandler, '_get_core_fields')
    @mock.patch.object(pipeline_handler.UsageHandler, '_extract_launched_at')
    @mock.patch.object(pipeline_handler.UsageHandler, '_find_events')
    @mock.patch.object(pipeline_handler.UsageHandler, '_confirm_launched_at')
    @mock.patch.object(pipeline_handler.UsageHandler, '_confirm_delete')
    def test_do_check(self, cd, cla, fe, ela, gcf):
        fe.return_value = [1,2,3]
        self.handler._do_checks({}, [])
