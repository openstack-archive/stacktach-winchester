import unittest2 as unittest

import datetime
import mock

from winchester import pipeline_handler


class TestConnectionManager(unittest.TestCase):
    def setUp(self):
        super(TestConnectionManager, self).setUp()
        self.mgr = pipeline_handler.ConnectionManager()

    def test_extract_params(self):
        with self.assertRaises(pipeline_handler.NotabeneException):
            self.mgr._extract_params({})

        cd, ct, ed, et = self.mgr._extract_params({'exchange': 'my_exchange'})

        self.assertEquals(cd, {'host': 'localhost',
                               'port': 5672,
                               'user': 'guest',
                               'password': 'guest',
                               'library': 'librabbitmq',
                               'vhost': '/'})

        self.assertEquals(ct, (('host', 'localhost'),
                               ('library', 'librabbitmq'),
                               ('password', 'guest'),
                               ('port', 5672),
                               ('user', 'guest'),
                               ('vhost', '/')))

        self.assertEquals(ed, {'exchange_name': 'my_exchange',
                               'exchange_type': 'topic'})

        self.assertEquals(et, (('exchange_name', 'my_exchange'),
                               ('exchange_type', 'topic')))


        kw = {'host': 'my_host', 'user': 'my_user', 'password': 'pwd',
              'port': 123, 'vhost': 'virtual', 'library': 'my_lib',
              'exchange': 'my_exchange', 'exchange_type': 'foo'}

        cd, ct, ed, et = self.mgr._extract_params(kw)

        self.assertEquals(cd, {'host': 'my_host',
                               'port': 123,
                               'user': 'my_user',
                               'password': 'pwd',
                               'library': 'my_lib',
                               'vhost': 'virtual'})

        self.assertEquals(ct, (('host', 'my_host'),
                               ('library', 'my_lib'),
                               ('password', 'pwd'),
                               ('port', 123),
                               ('user', 'my_user'),
                               ('vhost', 'virtual')))

        self.assertEquals(ed, {'exchange_name': 'my_exchange',
                               'exchange_type': 'foo'})

        self.assertEquals(et, (('exchange_name', 'my_exchange'),
                               ('exchange_type', 'foo')))


    @mock.patch.object(pipeline_handler.ConnectionManager, '_extract_params')
    @mock.patch.object(pipeline_handler.driver, 'create_connection')
    @mock.patch.object(pipeline_handler.driver, 'create_exchange')
    @mock.patch.object(pipeline_handler.driver, 'create_queue')
    def test_get_connection(self, cq, ce, cc, ep):
        conn = {'host': 'my_host', 'user': 'my_user', 'password': 'pwd',
              'port': 123, 'vhost': 'virtual', 'library': 'my_lib'}
        conn_set = tuple(sorted(conn.items()))
        exchange = {'exchange_name': 'my_exchange', 'exchange_type': 'foo'}
        exchange_set = tuple(sorted(exchange.items()))

        ep.return_value = (conn, conn_set, exchange, exchange_set)
        connection = mock.MagicMock("the connection")
        channel = mock.MagicMock("the channel")
        connection.channel = channel
        cc.return_value = connection
        mexchange = mock.MagicMock("the exchange")
        ce.return_value = mexchange
        queue = mock.MagicMock("the queue")
        queue.declare = mock.MagicMock()
        cq.return_value = queue

        final_connection, final_exchange = self.mgr.get_connection({}, "foo")

        self.assertEquals(final_connection, connection)
        self.assertEquals(final_exchange, mexchange)
        self.assertEquals(1, queue.declare.call_count)

        # Calling again should give the same results ...
        final_connection, final_exchange = self.mgr.get_connection({}, "foo")

        self.assertEquals(final_connection, connection)
        self.assertEquals(final_exchange, mexchange)
        self.assertTrue(queue.declare.called)
        self.assertEquals(1, queue.declare.call_count)

        # Change the exchange, and we should have same connection, but new
        # exchange object.
        exchange2 = {'exchange_name': 'my_exchange2', 'exchange_type': 'foo2'}
        exchange2_set = tuple(sorted(exchange2.items()))

        ep.return_value = (conn, conn_set, exchange2, exchange2_set)
        mexchange2 = mock.MagicMock("the exchange 2")
        ce.return_value = mexchange2

        final_connection, final_exchange = self.mgr.get_connection({}, "foo")

        self.assertEquals(final_connection, connection)
        self.assertEquals(final_exchange, mexchange2)
        self.assertEquals(2, queue.declare.call_count)

        # Change the connection, and we should have a new connection and new
        # exchange object.
        conn2 = {'host': 'my_host2', 'user': 'my_user2', 'password': 'pwd2',
              'port': 1234, 'vhost': 'virtual2', 'library': 'my_lib2'}
        conn2_set = tuple(sorted(conn2.items()))
        exchange3= {'exchange_name': 'my_exchange', 'exchange_type': 'foo'}
        exchange3_set = tuple(sorted(exchange3.items()))

        ep.return_value = (conn2, conn2_set, exchange3, exchange3_set)
        mexchange3 = mock.MagicMock("the exchange 3")
        ce.return_value = mexchange3

        connection2 = mock.MagicMock("the connection 2")
        channel2 = mock.MagicMock("the channel 2")
        connection2.channel = channel2
        cc.return_value = connection2

        final_connection, final_exchange = self.mgr.get_connection({}, "foo")

        self.assertEquals(final_connection, connection2)
        self.assertEquals(final_exchange, mexchange3)
        self.assertEquals(3, queue.declare.call_count)


class TestException(Exception):
    pass


class TestNotabeneHandler(unittest.TestCase):

    def test_constructor_no_queue(self):
        with self.assertRaises(pipeline_handler.NotabeneException) as e:
            pipeline_handler.NotabeneHandler()

    @mock.patch.object(pipeline_handler.connection_manager, 'get_connection')
    def test_constructor_queue(self, cm):
        cm.return_value = (1, 2)
        kw = {'queue_name': 'foo'}
        h = pipeline_handler.NotabeneHandler(**kw)
        self.assertIsNotNone(h.connection)
        self.assertIsNotNone(h.exchange)
        self.assertEquals(h.env_keys, [])

    @mock.patch.object(pipeline_handler.connection_manager, 'get_connection')
    def test_constructor_env_keys(self, cm):
        cm.return_value = (1, 2)
        kw = {'queue_name': 'foo', 'env_keys': ['x', 'y']}
        h = pipeline_handler.NotabeneHandler(**kw)
        self.assertIsNotNone(h.connection)
        self.assertIsNotNone(h.exchange)
        self.assertEquals(h.env_keys, ['x', 'y'])

    @mock.patch.object(pipeline_handler.connection_manager, 'get_connection')
    def test_handle_events(self, cm):
        cm.return_value = (1, 2)
        kw = {'queue_name': 'foo', 'env_keys': ['x', 'y', 'z']}
        h = pipeline_handler.NotabeneHandler(**kw)
        events = range(5)
        env = {'x': ['cat', 'dog'], 'y': ['fish']}
        ret = h.handle_events(events, env)
        self.assertEquals(ret, events)
        self.assertEquals(h.pending_notifications, ['cat', 'dog', 'fish'])

    @mock.patch.object(pipeline_handler.connection_manager, 'get_connection')
    def test_commit(self, cm):
        cm.return_value = (1, 2)
        kw = {'queue_name': 'foo'}
        h = pipeline_handler.NotabeneHandler(**kw)

        h.pending_notifications = [{'event_type': 'event1'},
                                   {'event_type': 'event2'}]
        with mock.patch.object(pipeline_handler.driver,
                                            'send_notification') as sn:
            h.commit()
            self.assertEquals(sn.call_count, 2)

    @mock.patch.object(pipeline_handler.connection_manager, 'get_connection')
    def test_commit(self, cm):
        cm.return_value = (1, 2)
        kw = {'queue_name': 'foo'}
        h = pipeline_handler.NotabeneHandler(**kw)

        h.pending_notifications = [{'event_type': 'event1'},
                                   {'event_type': 'event2'}]
        with mock.patch.object(pipeline_handler.driver,
                                            'send_notification') as sn:
            sn.side_effect = TestException
            with mock.patch.object(pipeline_handler.logger,
                                                    'exception') as ex:
                h.commit()
                self.assertEquals(ex.call_count, 2)
            self.assertEquals(sn.call_count, 2)
