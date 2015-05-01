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

import unittest2 as unittest

import datetime
import mock

from winchester import pipeline_handler


class TestUsageHandler(unittest.TestCase):
    def setUp(self):
        super(TestUsageHandler, self).setUp()
        self.handler = pipeline_handler.UsageHandler()

    def test_get_audit_period(self):
        event = {}
        apb, ape = self.handler._get_audit_period(event)
        self.assertIsNone(apb)
        self.assertIsNone(ape)

        event = {'audit_period_beginning': "beginning"}
        apb, ape = self.handler._get_audit_period(event)
        self.assertEqual(apb, "beginning")
        self.assertIsNone(ape)

        event = {'audit_period_ending': "ending"}
        apb, ape = self.handler._get_audit_period(event)
        self.assertIsNone(apb)
        self.assertEqual(ape, "ending")

    def test_is_exists(self):
        event = {'event_type': None}
        self.assertFalse(self.handler._is_exists(event))

        event = {'event_type': 'foo'}
        self.assertFalse(self.handler._is_exists(event))

        event = {'event_type': 'compute.instance.exists'}
        self.assertTrue(self.handler._is_exists(event))

    def test_is_non_EOD_exists(self):
        start = datetime.datetime(2014, 12, 31, 0, 0, 0)
        end = start + datetime.timedelta(days=1)
        eod = {'event_type': 'compute.instance.exists',
               'audit_period_beginning': start,
               'audit_period_ending': end}
        self.assertFalse(self.handler._is_non_EOD_exists(eod))

        start = datetime.datetime(2014, 12, 31, 1, 0, 0)
        end = start + datetime.timedelta(hours=1)
        eod = {'event_type': 'compute.instance.exists',
               'audit_period_beginning': start,
               'audit_period_ending': end}
        self.assertTrue(self.handler._is_non_EOD_exists(eod))

        eod = {'event_type': 'compute.instance.foo',
               'audit_period_beginning': start,
               'audit_period_ending': end}
        self.assertFalse(self.handler._is_non_EOD_exists(eod))

        eod = {'event_type': 'compute.instance.foo',
               'audit_period_ending': end}
        self.assertFalse(self.handler._is_non_EOD_exists(eod))

    def test_is_EOD_exists(self):
        start = datetime.datetime(2014, 12, 31, 1, 0, 0)
        end = start + datetime.timedelta(days=1)
        eod = {'event_type': 'compute.instance.exists',
               'audit_period_beginning': start,
               'audit_period_ending': end}
        self.assertFalse(self.handler._is_EOD_exists(eod))

        start = datetime.datetime(2014, 12, 31, 0, 0, 0)
        end = start + datetime.timedelta(hours=1)
        eod = {'event_type': 'compute.instance.exists',
               'audit_period_beginning': start,
               'audit_period_ending': end}
        self.assertFalse(self.handler._is_EOD_exists(eod))

        start = datetime.datetime(2014, 12, 31, 0, 0, 0)
        end = start + datetime.timedelta(days=1)
        eod = {'event_type': 'compute.instance.exists',
               'audit_period_beginning': start,
               'audit_period_ending': end}
        self.assertTrue(self.handler._is_EOD_exists(eod))

        eod = {'event_type': 'compute.instance.foo',
               'audit_period_beginning': start,
               'audit_period_ending': end}
        self.assertFalse(self.handler._is_EOD_exists(eod))

        eod = {'event_type': 'compute.instance.foo',
               'audit_period_ending': end}
        self.assertFalse(self.handler._is_EOD_exists(eod))

    def test_extract_launched_at(self):
        with self.assertRaises(pipeline_handler.UsageException):
            self.handler._extract_launched_at({})
        self.assertEqual("foo", self.handler._extract_launched_at(
            {'launched_at': 'foo'}))

    def test_extract_interesting(self):
        interesting = ["a", "b", "c"]
        e1 = {'event_type': 'a'}
        e2 = {'event_type': 'b'}
        e3 = {'event_type': 'c'}
        e4 = {'event_type': 'd'}
        e5 = {'event_type': 'e'}
        self.assertEqual([e1, e2, e3],
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
            self.assertEqual("U3", e.code)

        deleted_at = datetime.datetime(2014, 12, 31, 1, 0, 0)
        launched_at = datetime.datetime(2014, 12, 31, 2, 0, 0)
        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_delete({'deleted_at': deleted_at,
                                          'launched_at': launched_at,
                                          'state': 'deleted'}, [], [])
            self.assertEqual("U4", e.code)

        apb = datetime.datetime(2014, 12, 30, 0, 0, 0)
        ape = datetime.datetime(2014, 12, 31, 0, 0, 0)
        deleted_at = datetime.datetime(2014, 12, 30, 2, 0, 0)
        launched_at = datetime.datetime(2014, 12, 30, 1, 0, 0)
        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_delete({'deleted_at': deleted_at,
                                          'launched_at': launched_at,
                                          'audit_period_beginning': apb,
                                          'audit_period_ending': ape,
                                          'state': 'deleted'}, [], [])
            self.assertEqual("U5", e.code)

        # Test the do-nothing scenario
        self.handler._confirm_delete({}, [], [])

    def test_confirm_delete_with_delete_events(self):
        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_delete({}, [{}], [])
            self.assertEqual("U6", e.code)

        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_delete({'deleted_at': 'now'}, [{}, {}], [])
            self.assertEqual("U7", e.code)

        with mock.patch.object(self.handler, "_verify_fields") as v:
            exists = {'deleted_at': 'now', 'state': 'deleted'}
            deleted = {'foo': 1}
            self.handler._confirm_delete(exists, [deleted], ['a'])
            v.assert_called_with(exists, deleted, ['a'])

    def test_confirm_launched_at(self):
        self.handler._confirm_launched_at([], {'state': 'deleted'})

        apb = datetime.datetime(2014, 12, 30, 0, 0, 0)
        ape = datetime.datetime(2014, 12, 31, 0, 0, 0)
        launched_at = datetime.datetime(2014, 12, 30, 1, 0, 0)
        with self.assertRaises(pipeline_handler.UsageException) as e:
            self.handler._confirm_launched_at([],
                                              {'state': 'active',
                                               'audit_period_beginning': apb,
                                               'audit_period_ending': ape,
                                               'launched_at': launched_at})
            self.assertEqual("U8", e.code)

    def test_process_block_exists(self):
        exists = {'event_type': 'compute.instance.exists', 'timestamp': 'now',
                  'instance_id': 'inst'}
        self.handler.stream_id = 123
        with mock.patch.object(self.handler, "_do_checks"):
            events = self.handler._process_block([], exists)
            self.assertEqual(1, len(events))
            f = events[0]
            self.assertEqual("compute.instance.exists.verified",
                             f['event_type'])
            self.assertEqual("now", f['timestamp'])
            self.assertEqual(123, f['stream_id'])
            self.assertEqual("inst", f['payload']['instance_id'])
            self.assertEqual("None", f['error'])
            self.assertIsNone(f['error_code'])

    def test_process_block_bad(self):
        exists = {'event_type': 'compute.instance.exists', 'timestamp': 'now',
                  'instance_id': 'inst'}
        self.handler.stream_id = 123
        with mock.patch.object(self.handler, "_do_checks") as c:
            c.side_effect = pipeline_handler.UsageException("UX", "Error")
            events = self.handler._process_block([], exists)
            self.assertEqual(1, len(events))
            f = events[0]
            self.assertEqual("compute.instance.exists.failed",
                             f['event_type'])
            self.assertEqual("now", f['timestamp'])
            self.assertEqual(123, f['stream_id'])
            self.assertEqual("inst", f['payload']['instance_id'])
            self.assertEqual("Error", f['error'])
            self.assertEqual("UX", f['error_code'])

    def test_process_block_warnings(self):
        self.handler.warnings = ['one', 'two']
        exists = {'event_type': 'compute.instance.exists',
                  'timestamp': 'now', 'instance_id': 'inst'}
        self.handler.stream_id = 123
        with mock.patch.object(self.handler, "_do_checks"):
            events = self.handler._process_block([], exists)
            self.assertEqual(2, len(events))
            self.assertEqual("compute.instance.exists.warnings",
                             events[0]['event_type'])
            self.assertEqual("compute.instance.exists.verified",
                             events[1]['event_type'])

    @mock.patch.object(pipeline_handler.UsageHandler, '_confirm_launched_at')
    @mock.patch.object(pipeline_handler.UsageHandler, '_get_core_fields')
    @mock.patch.object(pipeline_handler.UsageHandler, '_verify_fields')
    @mock.patch.object(pipeline_handler.UsageHandler, '_is_non_EOD_exists')
    @mock.patch.object(pipeline_handler.UsageHandler, '_find_deleted_events')
    @mock.patch.object(pipeline_handler.UsageHandler, '_confirm_delete')
    def test_do_check_no_interesting_EOD_exists(self, cd, fde, inee, vf,
                                                gcf, cla):
        block = []
        exists = {'event_type': 'compute.instance.exists',
                  'message_id': 2}
        inee.return_value = False
        self.handler._do_checks(block, exists)
        self.assertTrue(len(self.handler.warnings) == 0)
        self.assertFalse(vf.called)

    @mock.patch.object(pipeline_handler.UsageHandler, '_confirm_launched_at')
    @mock.patch.object(pipeline_handler.UsageHandler, '_get_core_fields')
    @mock.patch.object(pipeline_handler.UsageHandler, '_verify_fields')
    @mock.patch.object(pipeline_handler.UsageHandler, '_is_non_EOD_exists')
    @mock.patch.object(pipeline_handler.UsageHandler, '_find_deleted_events')
    @mock.patch.object(pipeline_handler.UsageHandler, '_confirm_delete')
    def test_do_check_no_interesting_non_EOD(self, cd, fde, inee, vf,
                                             gcf, cla):
        block = []
        exists = {'event_type': 'compute.instance.exists',
                  'message_id': 2}
        inee.return_value = True
        self.handler._do_checks(block, exists)
        self.assertTrue(len(self.handler.warnings) == 1)
        self.assertFalse(vf.called)

    @mock.patch.object(pipeline_handler.UsageHandler, '_confirm_launched_at')
    @mock.patch.object(pipeline_handler.UsageHandler, '_get_core_fields')
    @mock.patch.object(pipeline_handler.UsageHandler, '_verify_fields')
    @mock.patch.object(pipeline_handler.UsageHandler, '_is_non_EOD_exists')
    @mock.patch.object(pipeline_handler.UsageHandler, '_find_deleted_events')
    @mock.patch.object(pipeline_handler.UsageHandler, '_confirm_delete')
    def test_do_check_interesting(self, cd, fde, inee, vf, gcf, cla):
        block = [{'event_type': 'compute.instance.rebuild.end',
                  'message_id': 1}]
        exists = {'event_type': 'compute.instance.exists',
                  'message_id': 2}
        self.handler._do_checks(block, exists)
        self.assertTrue(len(self.handler.warnings) == 0)
        self.assertTrue(vf.called)

    def test_handle_events_no_data(self):
        env = {'stream_id': 123}
        events = self.handler.handle_events([], env)
        self.assertEqual(0, len(events))

    def test_handle_events_no_exists(self):
        env = {'stream_id': 123}
        raw = [{'event_type': 'foo'}]
        events = self.handler.handle_events(raw, env)
        self.assertEqual(1, len(events))
        notifications = env['usage_notifications']
        self.assertEqual(1, len(notifications))
        self.assertEqual("compute.instance.exists.failed",
                         notifications[0]['event_type'])

    @mock.patch.object(pipeline_handler.UsageHandler, '_process_block')
    def test_handle_events_exists(self, pb):
        env = {'stream_id': 123}
        raw = [{'event_type': 'foo'},
               {'event_type': 'compute.instance.exists'}]
        events = self.handler.handle_events(raw, env)
        self.assertEqual(2, len(events))
        self.assertTrue(pb.called)

    @mock.patch.object(pipeline_handler.UsageHandler, '_process_block')
    def test_handle_events_dangling(self, pb):
        env = {'stream_id': 123}
        raw = [
            {'event_type': 'foo'},
            {'event_type': 'compute.instance.exists'},
            {'event_type': 'foo'},
        ]
        events = self.handler.handle_events(raw, env)
        self.assertEqual(3, len(events))
        notifications = env['usage_notifications']
        self.assertEqual(1, len(notifications))
        self.assertEqual("compute.instance.exists.failed",
                         notifications[0]['event_type'])
        self.assertTrue(pb.called)
