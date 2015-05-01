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

import logging

import datetime
import timex
from winchester import db
from winchester import models

logging.basicConfig()

TEST_DATA = [
    {'event_type': [
        dict(id=1, desc='test.thing.begin'),
        dict(id=2, desc='test.thing.end'),
        dict(id=3, desc='test.otherthing.foo'),
    ]},
    {'event': [
        dict(id=1,
             message_id='1234-5678-001',
             generated=datetime.datetime(2014, 8, 1, 10, 20, 45, 453201),
             event_type_id=1, ),
        dict(id=2,
             message_id='1234-5678-002',
             generated=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             event_type_id=2, ),
        dict(id=3,
             message_id='1234-5678-003',
             generated=datetime.datetime(2014, 8, 1, 2, 10, 12, 0),
             event_type_id=3, ),
        dict(id=4,
             message_id='1234-5678-004',
             generated=datetime.datetime(2014, 8, 1, 4, 57, 55, 42),
             event_type_id=3, ),
    ]},
    {'trait': [
        dict(event_id=1, name='instance_id', type=int(models.Datatype.string),
             t_string='aaaa-bbbb-cccc-dddd'),
        dict(event_id=1, name='memory_mb', type=int(models.Datatype.int),
             t_int=1024),
        dict(event_id=1, name='test_weight', type=int(models.Datatype.float),
             t_float=20112.42),
        dict(event_id=1, name='launched_at',
             type=int(models.Datatype.datetime),
             t_datetime=datetime.datetime(2014, 7, 1, 2, 30, 45, 453201)),
    ]},
    {'stream': [
        dict(id=1, first_event=datetime.datetime(2014, 8, 1, 2, 10, 12, 0),
             last_event=datetime.datetime(2014, 8, 1, 4, 57, 55, 42),
             name='test_trigger',
             expire_timestamp=datetime.datetime(2014, 8, 2, 4, 57, 55, 42),
             state=int(models.StreamState.active),
             state_serial_no=0),
        dict(id=2,
             first_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             last_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             name='another_test_trigger',
             expire_timestamp=datetime.datetime(2014, 8, 2, 4, 57, 55, 42),
             state=int(models.StreamState.active),
             state_serial_no=0),
        dict(id=3,
             first_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             last_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             name='fire_test_trigger',
             fire_timestamp=datetime.datetime(2014, 8, 10, 6, 0, 0, 42),
             expire_timestamp=datetime.datetime(2014, 8, 15, 6, 0, 0, 42),
             state=int(models.StreamState.active),
             state_serial_no=0),
        dict(id=4,
             first_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             last_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             name='fire_test_trigger',
             fire_timestamp=datetime.datetime(2014, 8, 11, 6, 0, 0, 42),
             expire_timestamp=datetime.datetime(2014, 8, 16, 0, 0, 0, 42),
             state=int(models.StreamState.active),
             state_serial_no=0),
        dict(id=5,
             first_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             last_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             name='reset_test_trigger',
             fire_timestamp=datetime.datetime(2014, 8, 11, 6, 0, 0, 42),
             expire_timestamp=datetime.datetime(2014, 8, 16, 0, 0, 0, 42),
             state=int(models.StreamState.error),
             state_serial_no=0),
        dict(id=6,
             first_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             last_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             name='reset_test_trigger',
             fire_timestamp=datetime.datetime(2014, 8, 11, 6, 0, 0, 42),
             expire_timestamp=datetime.datetime(2014, 8, 16, 0, 0, 0, 42),
             state=int(models.StreamState.expire_error),
             state_serial_no=0),
        dict(id=7,
             first_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             last_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             name='reset_test_trigger',
             fire_timestamp=datetime.datetime(2014, 8, 11, 6, 0, 0, 42),
             expire_timestamp=datetime.datetime(2014, 8, 16, 0, 0, 0, 42),
             state=int(models.StreamState.retry_fire),
             state_serial_no=0),
        dict(id=8,
             first_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             last_event=datetime.datetime(2014, 8, 1, 15, 25, 45, 453201),
             name='reset_test_trigger',
             fire_timestamp=datetime.datetime(2014, 8, 11, 6, 0, 0, 42),
             expire_timestamp=datetime.datetime(2014, 8, 16, 0, 0, 0, 42),
             state=int(models.StreamState.retry_expire),
             state_serial_no=0),
    ]},
    {'streamevent': [
        dict(stream_id=1, event_id=3),
        dict(stream_id=1, event_id=4),
        dict(stream_id=2, event_id=2),
        dict(stream_id=3, event_id=2),
        dict(stream_id=3, event_id=1),
        dict(stream_id=4, event_id=2),
    ]},
    {'dist_trait': [
        dict(stream_id=1, name='instance_id', type=int(models.Datatype.string),
             dt_string='zzzz-xxxx-yyyy-wwww'),
        dict(stream_id=1, name='memory_mb', type=int(models.Datatype.int),
             dt_int=4096),
        dict(stream_id=1, name='test_weight', type=int(models.Datatype.float),
             dt_float=3.1415),
        dict(stream_id=1, name='launched_at',
             type=int(models.Datatype.datetime),
             dt_datetime=datetime.datetime(2014, 7, 8, 9, 40, 50, 77777)),
        dict(stream_id=1, name='timestamp',
             type=int(models.Datatype.timerange),
             dt_timerange_begin=datetime.datetime(2014, 7, 8, 0, 0, 0, 27),
             dt_timerange_end=datetime.datetime(2014, 7, 9, 0, 0, 0, 27)),
    ]},
]


def create_tables(dbi):
    # used for tests
    models.Base.metadata.create_all(dbi.engine)


def load_fixture_data(dbi, data):
    # Used for tests. This is fugly, refactor later (mdragon)
    for table in data:
        for table_name, rows in table.items():
            for row in rows:
                cols = []
                vals = []
                for col, val in row.items():
                    cols.append(col)
                    vals.append(val)

                q = ("INSERT into %(table)s (%(colnames)s) VALUES (%(qs)s)" %
                     dict(table=table_name,
                          colnames=','.join(cols),
                          qs=','.join(('?',) * len(vals)),))
                dbi.engine.execute(q, vals)


class TestDB(unittest.TestCase):

    def setUp(self):
        super(TestDB, self).setUp()
        self.db = db.DBInterface(dict(url='sqlite://'))
        create_tables(self.db)
        load_fixture_data(self.db, TEST_DATA)
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

        self.events = {}
        for event in TEST_DATA[1]['event']:
            self.events[event['message_id']] = event

    def tearDown(self):
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
        self.db.close()

    def test_get_event_type(self):
        t = self.db.get_event_type('test.thing.begin')
        self.assertEqual(t.id, 1)
        t = self.db.get_event_type('test.not_in_db')
        self.assertEqual(t.id, 4)  # next unused id.

    def test_create_event(self):
        message_id = '9876-0001-0001'
        event_type = 'test.thing.begin'
        timestamp = datetime.datetime(2014, 7, 4, 12, 7, 21, 4096)
        traits = dict(test_string='foobar',
                      test_number=42,
                      test_float=3.1415,
                      test_date=datetime.datetime(2014, 7, 1, 0, 0, 0, 0),
                      somevalue=u'A fine test string')
        self.db.create_event(message_id, event_type, timestamp, traits)
        event = self.db.get_event_by_message_id(message_id)
        self.assertEqual(len(event), 8)
        self.assertEqual(event['message_id'], message_id)
        self.assertEqual(event['event_type'], event_type)
        self.assertEqual(event['timestamp'], timestamp)
        for name, value in traits.items():
            self.assertEqual(event[name], value)
            if type(value) == str:
                t_value = unicode
            else:
                t_value = type(value)
            self.assertEqual(type(event[name]), t_value)

    def test_create_event_duplicate(self):
        message_id = '9876-0001-0001'
        event_type = 'test.thing.begin'
        timestamp = datetime.datetime(2014, 7, 4, 12, 7, 21, 4096)
        traits = dict(test_string='foobar',
                      test_number=42,
                      test_float=3.1415,
                      test_date=datetime.datetime(2014, 7, 1, 0, 0, 0, 0),
                      somevalue=u'A fine test string')
        self.db.create_event(message_id, event_type, timestamp, traits)
        with self.assertRaises(db.DuplicateError):
            self.db.create_event(message_id, event_type, timestamp, traits)

    def test_get_event_by_message_id(self):
        event = self.db.get_event_by_message_id('1234-5678-001')
        self.assertEqual(len(event), 7)
        expected = dict(message_id='1234-5678-001',
                        event_type='test.thing.begin',
                        timestamp=datetime.datetime(2014, 8, 1, 10, 20, 45,
                                                    453201),
                        instance_id='aaaa-bbbb-cccc-dddd',
                        memory_mb=1024,
                        test_weight=20112.42,
                        launched_at=datetime.datetime(2014, 7, 1, 2, 30, 45,
                                                      453201), )
        self.assertDictContainsSubset(expected, event)

    def test_get_stream_events(self):
        stream = self.db.get_stream_by_id(1)
        events = self.db.get_stream_events(stream)
        self.assertEqual(len(events), 2)
        self.assertIn('1234-5678-003', [e['message_id'] for e in events])
        self.assertIn('1234-5678-004', [e['message_id'] for e in events])

    def test_create_stream(self):
        event = dict(message_id='1234-5678-001',
                     event_type='test.thing.begin',
                     timestamp=datetime.datetime(2014, 8, 1, 10, 20, 45,
                                                 453201),
                     instance_id='aaaa-bbbb-cccc-dddd',
                     memory_mb=1024,
                     test_weight=20112.42,
                     launched_at=datetime.datetime(2014, 7, 1, 2, 30, 45,
                                                   453201), )
        timestamp = timex.TimeRange(datetime.datetime(2014, 8, 1, 0, 0, 0, 27),
                                    datetime.datetime(2014, 2, 2, 0, 0, 0, 27))
        dist_traits = dict(timestamp=timestamp,
                           instance_id='aaaa-bbbb-cccc-dddd')

        class MockTimestamp(object):
            pass

        mock_expire_value = datetime.datetime(2014, 8, 2, 12, 12, 12, 12)

        def mock_time_expr(first, last):
            self.assertEqual(first,
                             datetime.datetime(2014, 8, 1, 10, 20, 45, 453201))
            self.assertEqual(last,
                             datetime.datetime(2014, 8, 1, 10, 20, 45, 453201))
            t = MockTimestamp()
            t.timestamp = mock_expire_value
            return t

        stream = self.db.create_stream('test_create_stream', event,
                                       dist_traits,
                                       mock_time_expr)
        self.assertEqual(stream.name, 'test_create_stream')
        self.assertEqual(stream.first_event,
                         datetime.datetime(2014, 8, 1, 10, 20, 45, 453201))
        self.assertEqual(stream.last_event,
                         datetime.datetime(2014, 8, 1, 10, 20, 45, 453201))
        self.assertEqual(stream.expire_timestamp, mock_expire_value)
        self.assertIsNone(stream.fire_timestamp)
        self.assertEqual(stream.state, models.StreamState.active)
        self.assertEqual(stream.state_serial_no, 0)
        self.assertTrue(
            self.db.stream_has_dist_trait(stream.id, 'timestamp', timestamp))
        self.assertTrue(self.db.stream_has_dist_trait(stream.id,
                                                      'instance_id',
                                                      'aaaa-bbbb-cccc-dddd'))
        events = self.db.get_stream_events(stream)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['message_id'], '1234-5678-001')

    def test_add_event_stream(self):
        stream = self.db.get_stream_by_id(1)
        event = dict(message_id='1234-5678-001',
                     event_type='test.thing.begin',
                     timestamp=datetime.datetime(2014, 8, 1, 10, 20, 45,
                                                 453201),
                     instance_id='aaaa-bbbb-cccc-dddd',
                     memory_mb=1024,
                     test_weight=20112.42,
                     launched_at=datetime.datetime(2014, 7, 1, 2, 30, 45,
                                                   453201), )

        class MockTimestamp(object):
            pass

        mock_expire_value = datetime.datetime(2014, 8, 2, 12, 12, 12, 12)

        def mock_time_expr(first, last):
            self.assertEqual(first,
                             datetime.datetime(2014, 8, 1, 2, 10, 12, 0))
            self.assertEqual(last,
                             datetime.datetime(2014, 8, 1, 10, 20, 45, 453201))
            t = MockTimestamp()
            t.timestamp = mock_expire_value
            return t

        self.db.add_event_stream(stream, event, mock_time_expr)
        self.assertEqual(stream.expire_timestamp, mock_expire_value)
        self.assertEqual(stream.first_event,
                         datetime.datetime(2014, 8, 1, 2, 10, 12, 0))
        self.assertEqual(stream.last_event,
                         datetime.datetime(2014, 8, 1, 10, 20, 45, 453201))
        events = self.db.get_stream_events(stream)
        self.assertEqual(len(events), 3)
        self.assertIn('1234-5678-001', [e['message_id'] for e in events])
        self.assertIn('1234-5678-003', [e['message_id'] for e in events])
        self.assertIn('1234-5678-004', [e['message_id'] for e in events])

    def test_stream_dist_traits(self):
        with self.db.in_session() as session:
            stream = self.db.get_stream_by_id(1, session=session)
            dist_traits = stream.distinguished_by_dict
        self.assertEqual(len(dist_traits), 5)
        self.assertIn('instance_id', dist_traits)
        self.assertEqual(dist_traits['instance_id'], 'zzzz-xxxx-yyyy-wwww')
        self.assertEqual(type(dist_traits['instance_id']), unicode)
        self.assertIn('memory_mb', dist_traits)
        self.assertEqual(dist_traits['memory_mb'], 4096)
        self.assertEqual(type(dist_traits['memory_mb']), int)
        self.assertIn('test_weight', dist_traits)
        self.assertEqual(dist_traits['test_weight'], 3.1415)
        self.assertEqual(type(dist_traits['test_weight']), float)
        self.assertIn('launched_at', dist_traits)
        self.assertEqual(dist_traits['launched_at'],
                         datetime.datetime(2014, 7, 8, 9, 40, 50, 77777))
        self.assertEqual(type(dist_traits['launched_at']), datetime.datetime)
        self.assertIn('timestamp', dist_traits)
        timestamp = dist_traits['timestamp']
        self.assertEqual(type(timestamp), timex.TimeRange)
        self.assertEqual(timestamp.begin,
                         datetime.datetime(2014, 7, 8, 0, 0, 0, 27))
        self.assertEqual(timestamp.end,
                         datetime.datetime(2014, 7, 9, 0, 0, 0, 27))

    def test_stream_has_dist_trait(self):
        # this mostly tests that the polymorphic trait comparisons are working.
        dt = self.db.stream_has_dist_trait(1, 'instance_id',
                                           'zzzz-xxxx-yyyy-wwww')
        self.assertIsNotNone(dt)
        self.assertEqual(len(dt), 1)
        self.assertIn('instance_id', dt)
        self.assertEqual(dt['instance_id'], 'zzzz-xxxx-yyyy-wwww')

        dt = self.db.stream_has_dist_trait(1, 'memory_mb', 4096)
        self.assertIsNotNone(dt)
        self.assertEqual(len(dt), 1)
        self.assertIn('memory_mb', dt)
        self.assertEqual(dt['memory_mb'], 4096)

        dt = self.db.stream_has_dist_trait(1, 'test_weight', 3.1415)
        self.assertIsNotNone(dt)
        self.assertEqual(len(dt), 1)
        self.assertIn('test_weight', dt)
        self.assertEqual(dt['test_weight'], 3.1415)

        launched = datetime.datetime(2014, 7, 8, 9, 40, 50, 77777)
        dt = self.db.stream_has_dist_trait(1, 'launched_at', launched)
        self.assertIsNotNone(dt)
        self.assertEqual(len(dt), 1)
        self.assertIn('launched_at', dt)
        self.assertEqual(dt['launched_at'], launched)

        timestamp = timex.TimeRange(datetime.datetime(2014, 7, 8, 0, 0, 0, 27),
                                    datetime.datetime(2014, 7, 9, 0, 0, 0, 27))
        dt = self.db.stream_has_dist_trait(1, 'timestamp', timestamp)
        self.assertIsNotNone(dt)
        self.assertEqual(len(dt), 1)
        self.assertIn('timestamp', dt)
        self.assertEqual(dt['timestamp'].begin, timestamp.begin)
        self.assertEqual(dt['timestamp'].end, timestamp.end)

    def test_get_active_stream(self):
        timestamp = timex.TimeRange(datetime.datetime(2014, 7, 8, 0, 0, 0, 27),
                                    datetime.datetime(2014, 7, 9, 0, 0, 0, 27))
        dist_traits = dict(instance_id='zzzz-xxxx-yyyy-wwww',
                           memory_mb=4096,
                           test_weight=3.1415,
                           launched_at=datetime.datetime(2014, 7, 8, 9, 40, 50,
                                                         77777),
                           timestamp=timestamp)
        current_time = datetime.datetime(2014, 8, 2, 1, 0, 0, 2)
        stream = self.db.get_active_stream('test_trigger', dist_traits,
                                           current_time)
        self.assertIsNotNone(stream)
        self.assertEqual(stream.id, 1)
        current_time = datetime.datetime(2014, 8, 3, 1, 0, 0, 2)
        stream = self.db.get_active_stream('test_trigger', dist_traits,
                                           current_time)
        self.assertIsNone(stream)

    def test_stream_ready_to_fire(self):
        stream = self.db.get_stream_by_id(1)
        fire_time = datetime.datetime(2014, 8, 2, 12, 21, 2, 2)
        self.db.stream_ready_to_fire(stream, fire_time)
        stream = self.db.get_stream_by_id(1)
        self.assertEqual(stream.fire_timestamp, fire_time)

    def test_get_ready_streams_fire(self):
        current_time = datetime.datetime(2014, 8, 12, 0, 0, 0, 42)
        streams = self.db.get_ready_streams(10, current_time)
        self.assertEqual(len(streams), 3)
        stream_ids = [stream.id for stream in streams]
        self.assertIn(3, stream_ids)
        self.assertIn(4, stream_ids)
        self.assertIn(7, stream_ids)

        current_time = datetime.datetime(2014, 8, 10, 12, 0, 0, 42)
        streams = self.db.get_ready_streams(10, current_time)
        self.assertEqual(len(streams), 1)
        stream_ids = [stream.id for stream in streams]
        self.assertIn(3, stream_ids)

        current_time = datetime.datetime(2014, 8, 12, 0, 0, 0, 42)
        streams = self.db.get_ready_streams(1, current_time)
        self.assertEqual(len(streams), 1)

    def test_get_ready_streams_expire(self):
        current_time = datetime.datetime(2014, 8, 17, 0, 0, 0, 42)
        streams = self.db.get_ready_streams(10, current_time, expire=True)
        self.assertEqual(len(streams), 5)
        stream_ids = [stream.id for stream in streams]
        self.assertIn(1, stream_ids)
        self.assertIn(2, stream_ids)
        self.assertIn(3, stream_ids)
        self.assertIn(4, stream_ids)
        self.assertIn(8, stream_ids)

        current_time = datetime.datetime(2014, 8, 10, 12, 0, 0, 42)
        streams = self.db.get_ready_streams(10, current_time, expire=True)
        self.assertEqual(len(streams), 2)
        stream_ids = [stream.id for stream in streams]
        self.assertIn(1, stream_ids)
        self.assertIn(2, stream_ids)

        current_time = datetime.datetime(2014, 8, 17, 0, 0, 0, 42)
        streams = self.db.get_ready_streams(1, current_time, expire=True)
        self.assertEqual(len(streams), 1)

    def test_set_stream_state_sucess(self):
        stream = self.db.get_stream_by_id(1)
        old_serial = stream.state_serial_no
        new_stream = self.db.set_stream_state(stream,
                                              models.StreamState.firing)
        self.assertEqual(new_stream.state, models.StreamState.firing)
        self.assertEqual(new_stream.state_serial_no, old_serial + 1)

    def test_set_stream_state_locked(self):
        stream = self.db.get_stream_by_id(1)
        self.db.set_stream_state(stream, models.StreamState.firing)
        with self.assertRaises(db.LockError):
            self.db.set_stream_state(stream, models.StreamState.firing)

    def test_reset_stream_fire(self):
        stream = self.db.get_stream_by_id(5)
        stream = self.db.reset_stream(stream)
        self.assertEqual(stream.state, models.StreamState.retry_fire)

    def test_reset_stream_expire(self):
        stream = self.db.get_stream_by_id(6)
        stream = self.db.reset_stream(stream)
        self.assertEqual(stream.state, models.StreamState.retry_expire)

    def test_purge_stream(self):
        stream = self.db.get_stream_by_id(1)
        self.db.purge_stream(stream)
        with self.assertRaises(db.NoSuchStreamError):
            self.db.get_stream_by_id(1)

    def test_find_stream_count(self):
        count = self.db.find_streams(count=True)
        self.assertEqual([{'count': 8}], count)

    def test_find_stream_limit(self):
        streams = self.db.find_streams(limit=2)
        self.assertEqual(len(streams), 2)
        self.assertEqual(streams[0]['id'], 8)
        self.assertEqual(streams[1]['id'], 7)

    def test_find_stream_limit_asc(self):
        streams = self.db.find_streams(limit=2, mark='+')
        self.assertEqual(len(streams), 2)
        self.assertEqual(streams[0]['id'], 1)
        self.assertEqual(streams[1]['id'], 2)

    def test_find_stream_mark(self):
        streams = self.db.find_streams(mark='7')
        for stream in streams:
            self.assertIn('_mark', stream)
        self.assertEqual(streams[0]['id'], 6)
        self.assertEqual(streams[1]['id'], 5)

    def test_find_stream_mark_asc(self):
        streams = self.db.find_streams(mark='+2')
        for stream in streams:
            self.assertIn('_mark', stream)
        self.assertEqual(streams[0]['id'], 3)
        self.assertEqual(streams[1]['id'], 4)

    def test_find_events(self):
        events = self.db.find_events()
        self.assertEqual(4, len(events))
        for event in events:
            self.assertTrue(event['message_id'] in self.events)

    def test_find_event_count(self):
        count = self.db.find_events(count=True)
        self.assertEqual([{'count': 4}], count)

    def test_find_events_date_filter(self):
        _from = datetime.datetime(2014, 8, 1, 10)
        _to = datetime.datetime(2014, 8, 1, 16)
        events = self.db.find_events(from_datetime=_from, to_datetime=_to)
        self.assertEqual(2, len(events))
        msg_ids = [event['message_id'] for event in events]
        for good in ['1234-5678-001', '1234-5678-002']:
            self.assertTrue(good in msg_ids)

    def test_find_events_event_type(self):
        events = self.db.find_events(event_name='test.otherthing.foo')
        self.assertEqual(2, len(events))
        for event in events:
            self.assertTrue(event['event_type'], 'test.otherthing.foo')

    def test_find_events_traits(self):
        traits = {'memory_mb': 1024}
        events = self.db.find_events(traits=traits)
        self.assertEqual(1, len(events))
        self.assertTrue(events[0]['message_id'], '1234-5678-001')

    def test_find_events_limit(self):
        events = self.db.find_events(limit=2)
        self.assertEqual(2, len(events))

    def test_find_events_mark(self):
        events = self.db.find_events(limit=2)
        self.assertEqual(2, len(events))
        msg_ids = [event['message_id'] for event in events]

        events = self.db.find_events(limit=2, mark=events[-1]['_mark'])
        self.assertEqual(2, len(events))
        for event in events:
            self.assertTrue(event['message_id'] not in msg_ids)
