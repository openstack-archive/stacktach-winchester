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

# for Python2.6 compatability.
import unittest2 as unittest

import datetime
import timex

from winchester import debugging
from winchester import definition


class TestCriterion(unittest.TestCase):

    def setUp(self):
        super(TestCriterion, self).setUp()
        self.fake_group = debugging.NoOpGroup()
        self.fake_debugger = debugging.NoOpDebugger()

    def test_basic_criterion(self):
        c = definition.Criterion(3, 'foo')
        self.assertTrue(c.match({'foo': 3}, self.fake_group))
        self.assertFalse(c.match({'foo': 5}, self.fake_group))
        self.assertFalse(c.match({'bar': 5}, self.fake_group))
        self.assertFalse(c.match({'foo': "booga"}, self.fake_group))

    def test_numeric_criterion(self):
        c = definition.NumericCriterion("3", 'foo')
        self.assertTrue(c.match({'foo': 3}, self.fake_group))
        self.assertFalse(c.match({'foo': 5}, self.fake_group))
        self.assertFalse(c.match({'bar': 5}, self.fake_group))
        self.assertFalse(c.match({'foo': "booga"}, self.fake_group))
        c = definition.NumericCriterion("> 3", 'foo')
        self.assertFalse(c.match({'foo': 3}, self.fake_group))
        self.assertTrue(c.match({'foo': 5}, self.fake_group))
        c = definition.NumericCriterion("< 3", 'foo')
        self.assertFalse(c.match({'foo': 3}, self.fake_group))
        self.assertFalse(c.match({'foo': 5}, self.fake_group))
        self.assertTrue(c.match({'foo': 1}, self.fake_group))
        with self.assertRaises(definition.DefinitionError):
            c = definition.NumericCriterion("zazz", "foo")
        with self.assertRaises(definition.DefinitionError):
            c = definition.NumericCriterion("", "foo")

    def test_float_criterion(self):
        c = definition.FloatCriterion("3.14", 'foo')
        self.assertTrue(c.match({'foo': 3.14}, self.fake_group))
        self.assertFalse(c.match({'foo': 5.2}, self.fake_group))
        self.assertFalse(c.match({'bar': 5.2}, self.fake_group))
        self.assertFalse(c.match({'foo': "booga"}, self.fake_group))
        c = definition.FloatCriterion("> 3.14", 'foo')
        self.assertFalse(c.match({'foo': 3.14}, self.fake_group))
        self.assertTrue(c.match({'foo': 5.2}, self.fake_group))
        c = definition.FloatCriterion("< 3.14", 'foo')
        self.assertFalse(c.match({'foo': 3.14}, self.fake_group))
        self.assertFalse(c.match({'foo': 3.5}, self.fake_group))
        self.assertTrue(c.match({'foo': 3.02}, self.fake_group))
        with self.assertRaises(definition.DefinitionError):
            c = definition.FloatCriterion("zazz", "foo")
        with self.assertRaises(definition.DefinitionError):
            c = definition.FloatCriterion("", "foo")

    def test_time_criterion(self):
        c = definition.TimeCriterion("day", "foo")
        e = dict(timestamp=datetime.datetime(2014, 8, 1, 7, 52, 31, 2),
                 foo=datetime.datetime(2014, 8, 1, 1, 2, 0, 0))
        self.assertTrue(c.match(e, self.fake_group))
        e = dict(timestamp=datetime.datetime(2014, 8, 1, 7, 52, 31, 2),
                 foo=datetime.datetime(2014, 8, 2, 1, 2, 0, 0))
        self.assertFalse(c.match(e, self.fake_group))
        e = dict(timestamp=datetime.datetime(2014, 8, 1, 7, 52, 31, 2),
                 bar=datetime.datetime(2014, 8, 1, 1, 2, 0, 0))
        self.assertFalse(c.match(e, self.fake_group))
        e = dict(timestamp=datetime.datetime(2014, 8, 1, 7, 52, 31, 2),
                 message_id='1234-5678',
                 quux=4,
                 foo=datetime.datetime(2014, 8, 1, 1, 2, 0, 0))
        self.assertTrue(c.match(e, self.fake_group))


class TestCriteria(unittest.TestCase):

    def setUp(self):
        super(TestCriteria, self).setUp()
        self.fake_group = debugging.NoOpGroup()
        self.fake_debugger = debugging.NoOpDebugger()

    def test_defaults(self):
        criteria = definition.Criteria({})
        self.assertEqual(len(criteria.included_types), 1)
        self.assertEqual(len(criteria.excluded_types), 0)
        self.assertEqual(criteria.included_types[0], '*')
        self.assertEqual(criteria.number, 1)
        self.assertIsNone(criteria.timestamp)
        self.assertEqual(len(criteria.map_distinguished_by), 0)
        self.assertEqual(len(criteria.traits), 0)

    def test_event_type_configs(self):
        config = dict(event_type="test.foo.bar")
        criteria = definition.Criteria(config)
        self.assertEqual(len(criteria.included_types), 1)
        self.assertEqual(len(criteria.excluded_types), 0)
        self.assertEqual(criteria.included_types[0], 'test.foo.bar')
        config = dict(event_type="!test.foo.bar")
        criteria = definition.Criteria(config)
        self.assertEqual(len(criteria.included_types), 1)
        self.assertEqual(len(criteria.excluded_types), 1)
        self.assertEqual(criteria.included_types[0], '*')
        self.assertEqual(criteria.excluded_types[0], 'test.foo.bar')
        config = dict(event_type=["test.foo.bar", "!test.wakka.wakka"])
        criteria = definition.Criteria(config)
        self.assertEqual(len(criteria.included_types), 1)
        self.assertEqual(len(criteria.excluded_types), 1)
        self.assertEqual(criteria.included_types[0], 'test.foo.bar')
        self.assertEqual(criteria.excluded_types[0], 'test.wakka.wakka')

    def test_match_type(self):
        config = dict(event_type=["test.foo.bar", "!test.wakka.wakka"])
        criteria = definition.Criteria(config)
        self.assertTrue(criteria.match_type('test.foo.bar'))
        self.assertFalse(criteria.match_type('test.wakka.wakka'))
        self.assertFalse(criteria.match_type('test.foo.baz'))
        config = dict(event_type=["test.foo.*", "!test.wakka.*"])
        criteria = definition.Criteria(config)
        self.assertTrue(criteria.match_type('test.foo.bar'))
        self.assertTrue(criteria.match_type('test.foo.baz'))
        self.assertFalse(criteria.match_type('test.wakka.wakka'))

    def test_match_for_type(self):
        config = dict(event_type=["test.foo.*", "!test.wakka.*"])
        criteria = definition.Criteria(config)
        event1 = dict(event_type="test.foo.zazz")
        event2 = dict(event_type="test.wakka.zazz")
        event3 = dict(event_type="test.boingy")
        self.assertTrue(criteria.match(event1, self.fake_group))
        self.assertFalse(criteria.match(event2, self.fake_group))
        self.assertFalse(criteria.match(event3, self.fake_group))

    def test_match_for_timestamp(self):
        config = dict(timestamp='day($launched_at)')
        criteria = definition.Criteria(config)
        event1 = dict(event_type='test.thing',
                      timestamp=datetime.datetime(2014, 8, 1, 17, 16, 15, 14),
                      launched_at=datetime.datetime(2014, 8, 1, 1, 2, 3, 4))
        event2 = dict(event_type='test.thing',
                      timestamp=datetime.datetime(2014, 8, 2, 17, 16, 15, 14),
                      launched_at=datetime.datetime(2014, 8, 1, 1, 2, 3, 4))
        event3 = dict(event_type='test.thing',
                      timestamp=datetime.datetime(2014, 8, 2, 17, 16, 15, 14))
        self.assertTrue(criteria.match(event1, self.fake_group))
        self.assertFalse(criteria.match(event2, self.fake_group))
        self.assertFalse(criteria.match(event3, self.fake_group))

    def test_match_for_traits(self):
        config = dict(traits=dict(some_trait="test",
                                  launched_at={'datetime': "day"},
                                  memory_mb={'int': "> 2048"},
                                  test_weight={'float': "< 4.02"},
                                  other_trait={'string': 'text here'}))
        criteria = definition.Criteria(config)
        event1 = dict(event_type='test.thing',
                      timestamp=datetime.datetime(2014, 8, 1, 17, 16, 15, 14),
                      launched_at=datetime.datetime(2014, 8, 1, 1, 2, 3, 4),
                      some_trait='test',
                      other_trait='text here',
                      memory_mb=4096,
                      test_weight=3.1415)
        event2 = dict(event_type='test.thing',
                      timestamp=datetime.datetime(2014, 8, 1, 17, 16, 15, 14),
                      launched_at=datetime.datetime(2014, 8, 1, 1, 2, 3, 4),
                      some_trait='foo',
                      other_trait='text here',
                      memory_mb=4096,
                      test_weight=3.1415)
        event3 = dict(event_type='test.thing',
                      timestamp=datetime.datetime(2014, 8, 1, 17, 16, 15, 14),
                      launched_at=datetime.datetime(2014, 8, 1, 1, 2, 3, 4),
                      other_trait='text here',
                      memory_mb=4096,
                      test_weight=3.1415)
        event4 = dict(event_type='test.thing',
                      timestamp=datetime.datetime(2014, 8, 1, 17, 16, 15, 14),
                      launched_at=datetime.datetime(2014, 8, 2, 1, 2, 3, 4),
                      some_trait='test',
                      other_trait='text here',
                      memory_mb=4096,
                      test_weight=3.1415)
        event5 = dict(event_type='test.thing',
                      timestamp=datetime.datetime(2014, 8, 1, 17, 16, 15, 14),
                      launched_at=datetime.datetime(2014, 8, 1, 1, 2, 3, 4),
                      some_trait='test',
                      other_trait='text here',
                      memory_mb=1024,
                      test_weight=3.1415)
        event6 = dict(event_type='test.thing',
                      timestamp=datetime.datetime(2014, 8, 1, 17, 16, 15, 14),
                      launched_at=datetime.datetime(2014, 8, 1, 1, 2, 3, 4),
                      some_trait='test',
                      other_trait='text here',
                      memory_mb=4096,
                      test_weight=6.283)
        self.assertTrue(criteria.match(event1, self.fake_group))
        self.assertFalse(criteria.match(event2, self.fake_group))
        self.assertFalse(criteria.match(event3, self.fake_group))
        self.assertFalse(criteria.match(event4, self.fake_group))
        self.assertFalse(criteria.match(event5, self.fake_group))
        self.assertFalse(criteria.match(event6, self.fake_group))


class TestTriggerDefinition(unittest.TestCase):
    def setUp(self):
        super(TestTriggerDefinition, self).setUp()
        self.debug_manager = debugging.DebugManager()

    def test_config_error_check_and_defaults(self):
        with self.assertRaises(definition.DefinitionError):
            definition.TriggerDefinition(dict(), self.debug_manager)
        with self.assertRaises(definition.DefinitionError):
            definition.TriggerDefinition(dict(name='test_trigger'),
                                         self.debug_manager)
        with self.assertRaises(definition.DefinitionError):
            definition.TriggerDefinition(dict(name='test_trigger',
                                              expiration='$last + 1d'),
                                         self.debug_manager)
        with self.assertRaises(definition.DefinitionError):
            definition.TriggerDefinition(dict(name='test_trigger',
                                              expiration='$last + 1d',
                                              fire_pipeline='test_pipeline'),
                                         self.debug_manager)
        with self.assertRaises(definition.DefinitionError):
            definition.TriggerDefinition(
                dict(name='test_trigger',
                     expiration='$last + 1d',
                     fire_pipeline='test_pipeline',
                     fire_criteria=[dict(event_type='test.thing')]),
                self.debug_manager)
        tdef = definition.TriggerDefinition(
            dict(name='test_trigger',
                 expiration='$last + 1d',
                 fire_pipeline='test_pipeline',
                 fire_criteria=[dict(event_type='test.thing')],
                 match_criteria=[dict(event_type='test.*')]),
            self.debug_manager)
        self.assertEqual(len(tdef.distinguished_by), 0)
        self.assertEqual(len(tdef.fire_criteria), 1)
        self.assertIsInstance(tdef.fire_criteria[0], definition.Criteria)
        self.assertEqual(len(tdef.match_criteria), 1)
        self.assertIsInstance(tdef.match_criteria[0], definition.Criteria)
        self.assertEqual(tdef.fire_delay, 0)
        self.assertEqual(len(tdef.load_criteria), 0)

    def test_match_for_criteria(self):
        config = dict(name='test_trigger',
                      expiration='$last + 1d',
                      fire_pipeline='test_pipeline',
                      fire_criteria=[dict(event_type='test.thing')],
                      match_criteria=[dict(event_type='test.*')])
        tdef = definition.TriggerDefinition(config, self.debug_manager)
        event1 = dict(event_type='test.thing')
        event2 = dict(event_type='other.thing')
        self.assertTrue(tdef.match(event1))
        self.assertFalse(tdef.match(event2))
        config = dict(name='test_trigger',
                      expiration='$last + 1d',
                      fire_pipeline='test_pipeline',
                      fire_criteria=[dict(event_type='test.thing')],
                      match_criteria=[dict(event_type='test.*'),
                                      dict(event_type='other.*')])
        tdef = definition.TriggerDefinition(config, self.debug_manager)
        self.assertTrue(tdef.match(event1))
        self.assertTrue(tdef.match(event2))

    def test_match_for_distinguished_traits(self):
        config = dict(name='test_trigger',
                      expiration='$last + 1d',
                      distinguished_by=['instance_id'],
                      fire_pipeline='test_pipeline',
                      fire_criteria=[dict(event_type='test.thing')],
                      match_criteria=[dict(event_type='test.*')])
        tdef = definition.TriggerDefinition(config, self.debug_manager)
        event1 = dict(event_type='test.thing', instance_id='foo')
        event2 = dict(event_type='test.thing')
        self.assertTrue(tdef.match(event1))
        self.assertFalse(tdef.match(event2))

    def test_get_distinguished_traits(self):
        config = dict(name='test_trigger',
                      expiration='$last + 1d',
                      distinguished_by=['instance_id'],
                      fire_pipeline='test_pipeline',
                      fire_criteria=[dict(event_type='test.thing')],
                      match_criteria=[dict(event_type='test.*')])
        event1 = dict(event_type='test.thing', instance_id='foo')
        tdef = definition.TriggerDefinition(config, self.debug_manager)
        mcriteria = tdef.match(event1)
        dt = tdef.get_distinguishing_traits(event1, mcriteria)
        self.assertEqual(len(dt), 1)
        self.assertIn('instance_id', dt)
        self.assertEqual(dt['instance_id'], 'foo')

    def test_get_distinguished_traits_with_timeexpression(self):
        config = dict(name='test_trigger',
                      expiration='$last + 1d',
                      distinguished_by=['instance_id', dict(timestamp='day')],
                      fire_pipeline='test_pipeline',
                      fire_criteria=[dict(event_type='test.thing')],
                      match_criteria=[dict(event_type='test.*')])
        event1 = dict(event_type='test.thing', instance_id='foo',
                      timestamp=datetime.datetime(2014, 8, 1, 20, 4, 23, 444))
        tdef = definition.TriggerDefinition(config, self.debug_manager)
        mcriteria = tdef.match(event1)
        dt = tdef.get_distinguishing_traits(event1, mcriteria)
        self.assertEqual(len(dt), 2)
        self.assertIn('instance_id', dt)
        self.assertEqual(dt['instance_id'], 'foo')
        timerange = timex.TimeRange(datetime.datetime(2014, 8, 1, 0, 0, 0, 0),
                                    datetime.datetime(2014, 8, 2, 0, 0, 0, 0))
        self.assertIn('timestamp', dt)
        self.assertIsInstance(dt['timestamp'], timex.TimeRange)
        self.assertEqual(dt['timestamp'].begin, timerange.begin)
        self.assertEqual(dt['timestamp'].end, timerange.end)

    def test_get_distinguished_traits_with_map(self):
        config = dict(name='test_trigger',
                      expiration='$last + 1d',
                      distinguished_by=['instance_id'],
                      fire_pipeline='test_pipeline',
                      fire_criteria=[dict(event_type='test.thing')],
                      match_criteria=[dict(event_type='test.*',
                                           map_distinguished_by=dict(
                                               instance_id='other_id'))])
        event1 = dict(event_type='test.thing', instance_id='foo',
                      other_id='bar')
        tdef = definition.TriggerDefinition(config, self.debug_manager)
        mcriteria = tdef.match(event1)
        dt = tdef.get_distinguishing_traits(event1, mcriteria)
        self.assertEqual(len(dt), 1)
        self.assertIn('instance_id', dt)
        self.assertEqual(dt['instance_id'], 'bar')

    def test_get_fire_timestamp(self):
        config = dict(name='test_trigger',
                      expiration='$last + 1d',
                      distinguished_by=['instance_id'],
                      fire_pipeline='test_pipeline',
                      fire_criteria=[dict(event_type='test.thing')],
                      match_criteria=[dict(event_type='test.*')])
        tdef = definition.TriggerDefinition(config, self.debug_manager)
        test_time = datetime.datetime(2014, 8, 1, 20, 4, 23, 444)
        test_time_plus_1hr = datetime.datetime(2014, 8, 1, 21, 4, 23, 444)
        ft = tdef.get_fire_timestamp(test_time)
        self.assertEqual(ft, test_time)
        config = dict(name='test_trigger',
                      expiration='$last + 1d',
                      fire_delay=3600,
                      distinguished_by=['instance_id'],
                      fire_pipeline='test_pipeline',
                      fire_criteria=[dict(event_type='test.thing')],
                      match_criteria=[dict(event_type='test.*')])
        tdef = definition.TriggerDefinition(config, self.debug_manager)
        ft = tdef.get_fire_timestamp(test_time)
        self.assertEqual(ft, test_time_plus_1hr)

    def test_should_fire(self):
        config = dict(name='test_trigger',
                      expiration='$last + 1d',
                      distinguished_by=['instance_id'],
                      fire_pipeline='test_pipeline',
                      fire_criteria=[dict(event_type='test.thing')],
                      match_criteria=[dict(event_type='test.*')])
        tdef = definition.TriggerDefinition(config, self.debug_manager)
        events1 = [dict(event_type='test.foobar'),
                   dict(event_type='test.thing'),
                   dict(event_type='test.thing')]
        events2 = [dict(event_type='test.foobar'),
                   dict(event_type='test.thing')]
        events3 = [dict(event_type='test.foobar'),
                   dict(event_type='test.whatsit')]
        self.assertTrue(tdef.should_fire(events1))
        self.assertTrue(tdef.should_fire(events2))
        self.assertFalse(tdef.should_fire(events3))
        config = dict(name='test_trigger',
                      expiration='$last + 1d',
                      distinguished_by=['instance_id'],
                      fire_pipeline='test_pipeline',
                      fire_criteria=[dict(event_type='test.thing', number=2)],
                      match_criteria=[dict(event_type='test.*')])
        tdef = definition.TriggerDefinition(config, self.debug_manager)
        self.assertTrue(tdef.should_fire(events1))
        self.assertFalse(tdef.should_fire(events2))
        self.assertFalse(tdef.should_fire(events3))
