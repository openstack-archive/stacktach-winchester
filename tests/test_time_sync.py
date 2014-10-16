import unittest2 as unittest

import datetime
import mock

from winchester import time_sync


class TestTimeSyncNoEndpoint(unittest.TestCase):
    def setUp(self):
        super(TestTimeSyncNoEndpoint, self).setUp()
        self.time_sync = time_sync.TimeSync({})

    def test_should_update(self):
        now = datetime.datetime.utcnow()
        self.assertIsNone(self.time_sync.last_update)
        self.assertTrue(self.time_sync._should_update(now))

        short = now + datetime.timedelta(seconds=1)
        lng = now + datetime.timedelta(seconds=60)
        self.time_sync.last_update = now

        self.assertFalse(self.time_sync._should_update(short))
        self.assertTrue(self.time_sync._should_update(lng))

    def test_current_time(self):
        with mock.patch.object(self.time_sync, "_get_now") as w:
            w.return_value = "123"
            self.assertEqual(self.time_sync.current_time(), "123")

    def test_publish(self):
        with mock.patch.object(time_sync.dateutil.parser, "parse") as p:
            self.time_sync.publish("foo")
            self.assertEquals(0, p.call_count)


class BlowUp(Exception):
    pass


class TestTimeSyncEndpointPublisher(unittest.TestCase):
    def setUp(self):
        super(TestTimeSyncEndpointPublisher, self).setUp()
        self.time_sync = time_sync.TimeSync(
                    {"time_sync_endpoint":"example.com"}, publishes=True)

    def test_fetch_good(self):
        with mock.patch.object(time_sync.requests, "get") as r:
            response = mock.MagicMock()
            response.text = "now"
            r.return_value = response
            self.assertEquals("now", self.time_sync._fetch())

    def test_fetch_empty(self):
        with mock.patch.object(time_sync.time, "sleep") as t:
            with mock.patch.object(time_sync.requests, "get") as r:
                response = mock.MagicMock()
                response.text = ""
                r.return_value = response
                t.side_effect = BlowUp
                with self.assertRaises(BlowUp):
                    self.time_sync._fetch()

    def test_fetch_None(self):
        with mock.patch.object(time_sync.time, "sleep") as t:
            with mock.patch.object(time_sync.requests, "get") as r:
                response = mock.MagicMock()
                response.text = "None"
                r.return_value = response
                t.side_effect = BlowUp
                with self.assertRaises(BlowUp):
                    self.time_sync._fetch()

    def test_current_time(self):
        self.time_sync.last_tyme = "now"
        with mock.patch.object(self.time_sync, "_should_update") as u:
            self.assertEquals("now", self.time_sync.current_time())
            self.assertEquals(0, u.call_count)

    def test_publish(self):
        with mock.patch.object(time_sync.dateutil.parser, "parse") as p:
            p.return_value = "datetime object"
            with mock.patch.object(self.time_sync, "_should_update") as u:
                u.return_value = True

                with mock.patch.object(time_sync.requests, "post") as r:
                    r.return_value = ""

                    self.time_sync.publish("string datetime")

                    r.assert_called_once_with("example.com/time",
                                              data="string datetime")

    def test_publish_fails(self):
        with mock.patch.object(time_sync.dateutil.parser, "parse") as p:
            p.return_value = "datetime object"
            with mock.patch.object(self.time_sync, "_should_update") as u:
                u.return_value = True
                with mock.patch.object(time_sync.requests, "post") as r:
                    r.side_effect = BlowUp
                    with mock.patch.object(time_sync.logger, "exception") as e:
                        self.time_sync.publish("string datetime")
                        self.assertEquals(1, e.call_count)


class TestTimeSyncEndpointConsumer(unittest.TestCase):
    def setUp(self):
        super(TestTimeSyncEndpointConsumer, self).setUp()
        self.time_sync = time_sync.TimeSync(
                    {"time_sync_endpoint":"example.com"})

    def test_current_time(self):
         with mock.patch.object(self.time_sync, "_should_update") as u:
             u.return_value = True
             with mock.patch.object(time_sync.dateutil.parser, "parse") as p:
                p.return_value = "datetime object"
                with mock.patch.object(self.time_sync, "_fetch") as r:
                    r.return_value = "string datetime"

                    self.assertEquals(self.time_sync.current_time(),
                                      "datetime object")

    def test_current_time_fails(self):
        self.time_sync.last_tyme = "no change"
        with mock.patch.object(self.time_sync, "_should_update") as u:
            u.return_value = True
            with mock.patch.object(self.time_sync, "_fetch") as r:
                r.side_effect = BlowUp
                with mock.patch.object(time_sync.logger, "exception") as e:
                    self.assertEquals(self.time_sync.current_time(),
                                      "no change")
                    self.assertEquals(1, e.call_count)
