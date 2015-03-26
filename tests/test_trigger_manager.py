import unittest2 as unittest

import mock

import datetime
import timex

from winchester import db as winch_db
from winchester import debugging
from winchester import definition
from winchester import trigger_manager


class TestTriggerManager(unittest.TestCase):

    def setUp(self):
        super(TestTriggerManager, self).setUp()
        self.debugger = debugging.NoOpDebugger()

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_save_event(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        event = dict(message_id='1234-test-5678',
                     timestamp=datetime.datetime(2014,8,1,10,9,8,77777),
                     event_type='test.thing',
                     test_trait="foobar",
                     other_test_trait=42)
        self.assertTrue(tm.save_event(event))
        tm.db.create_event.assert_called_once_with('1234-test-5678', 'test.thing',
            datetime.datetime(2014,8,1,10,9,8,77777),
            dict(test_trait='foobar', other_test_trait=42))

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_save_event_dup(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        tm.db.create_event.side_effect = winch_db.DuplicateError("test boom!")
        event = dict(message_id='1234-test-5678',
                     timestamp=datetime.datetime(2014,8,1,10,9,8,77777),
                     event_type='test.thing',
                     test_trait="foobar",
                     other_test_trait=42)
        self.assertFalse(tm.save_event(event))
        tm.db.create_event.assert_called_once_with('1234-test-5678', 'test.thing',
            datetime.datetime(2014,8,1,10,9,8,77777),
            dict(test_trait='foobar', other_test_trait=42))

    @mock.patch('winchester.trigger_manager.EventCondenser', autospec=True)
    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_convert_notification(self, mock_config_wrap, mock_condenser):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        tm.distiller = mock.MagicMock(spec=tm.distiller)
        test_event = "I'm a test event!"
        tm.distiller.to_event.return_value = True
        cond = mock_condenser.return_value
        cond.validate.return_value = True
        cond.get_event.return_value = test_event
        tm.save_event = mock.MagicMock()
        tm.save_event.return_value = True

        res = tm.convert_notification('test notification here')
        mock_condenser.assert_called_once_with(tm.db)
        cond.clear.assert_called_once_with()
        cond.validate.assert_called_once_with()
        tm.distiller.to_event.assert_called_once_with('test notification here', cond)
        self.assertEquals(res, test_event)

    @mock.patch('winchester.trigger_manager.EventCondenser', autospec=True)
    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_convert_notification_dropped(self, mock_config_wrap, mock_condenser):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        tm.distiller = mock.MagicMock(spec=tm.distiller)
        test_event = "I'm a test event!"
        tm.distiller.to_event.return_value = False
        cond = mock_condenser.return_value
        cond.validate.return_value = True
        cond.get_event.return_value = test_event
        tm.save_event = mock.MagicMock()
        tm.save_event.return_value = True

        test_notif = dict(event_type='test.notification.here', message_id='4242-4242')
        res = tm.convert_notification(test_notif)
        mock_condenser.assert_called_once_with(tm.db)
        cond.clear.assert_called_once_with()
        self.assertFalse(cond.validate.called)
        tm.distiller.to_event.assert_called_once_with(test_notif, cond)
        self.assertFalse(tm.save_event.called)
        self.assertIsNone(res)

    @mock.patch('winchester.trigger_manager.EventCondenser', autospec=True)
    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_convert_notification_invalid(self, mock_config_wrap, mock_condenser):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        tm.distiller = mock.MagicMock(spec=tm.distiller)
        test_event = "I'm a test event!"
        tm.distiller.to_event.return_value = True
        cond = mock_condenser.return_value
        cond.validate.return_value = False
        cond.get_event.return_value = test_event
        tm.save_event = mock.MagicMock()
        tm.save_event.return_value = True

        test_notif = dict(event_type='test.notification.here', message_id='4242-4242')
        res = tm.convert_notification(test_notif)
        mock_condenser.assert_called_once_with(tm.db)
        cond.clear.assert_called_once_with()
        cond.validate.assert_called_once_with()
        tm.distiller.to_event.assert_called_once_with(test_notif, cond)
        self.assertFalse(tm.save_event.called)
        self.assertIsNone(res)

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_add_or_create_stream(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        tm.db.get_active_stream.return_value = 'Existing Stream'
        tm.current_time = mock.MagicMock()
        trigger_def = mock.MagicMock()
        dist_traits = 'some traits'
        event = "eventful!"

        ret = tm._add_or_create_stream(trigger_def, event, dist_traits)
        tm.db.get_active_stream.assert_called_once_with(trigger_def.name,
                                    dist_traits, tm.current_time.return_value)
        self.assertFalse(tm.db.create_stream.called)
        tm.db.add_event_stream.assert_called_once_with(
                                        tm.db.get_active_stream.return_value,
                                        event, trigger_def.expiration)
        self.assertEqual(ret, tm.db.get_active_stream.return_value)

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_add_or_create_stream_create(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        tm.db.get_active_stream.return_value = None
        tm.current_time = mock.MagicMock()
        trigger_def = mock.MagicMock()
        dist_traits = 'some traits'
        event = "eventful!"

        ret = tm._add_or_create_stream(trigger_def, event, dist_traits)
        tm.db.get_active_stream.assert_called_once_with(trigger_def.name, dist_traits,
                                                        tm.current_time.return_value)
        tm.db.create_stream.assert_called_once_with(trigger_def.name, event, dist_traits,
                                                    trigger_def.expiration)
        self.assertFalse(tm.db.add_event_stream.called)
        self.assertEqual(ret, tm.db.create_stream.return_value)

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_ready_to_fire(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        tm.current_time = mock.MagicMock()
        trigger_def = mock.MagicMock()
        test_stream = mock.MagicMock()

        tm._ready_to_fire(test_stream, trigger_def)
        trigger_def.get_fire_timestamp.assert_called_once_with(tm.current_time.return_value)
        tm.db.stream_ready_to_fire.assert_called_once_with(test_stream,
            trigger_def.get_fire_timestamp.return_value)

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_add_notification(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.convert_notification = mock.MagicMock()
        tm.add_event = mock.MagicMock()

        tm.add_notification("test notification")
        tm.convert_notification.assert_called_once_with("test notification")
        tm.add_event.assert_called_once_with(tm.convert_notification.return_value)

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_add_notification_invalid_or_dropped(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.convert_notification = mock.MagicMock()
        tm.add_event = mock.MagicMock()
        tm.convert_notification.return_value = None

        tm.add_notification("test notification")
        tm.convert_notification.assert_called_once_with("test notification")
        self.assertFalse(tm.add_event.called)

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_add_event(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        tm.trigger_definitions = [mock.MagicMock() for n in range(3)]
        for d in tm.trigger_definitions:
            d.debugger = self.debugger
        m_def = tm.trigger_definitions[2]
        tm.trigger_definitions[0].match.return_value = None
        tm.trigger_definitions[1].match.return_value = None
        event = mock.MagicMock(name='event', spec=dict)
        tm.save_event = mock.MagicMock()
        tm._add_or_create_stream = mock.MagicMock()
        tm._add_or_create_stream.return_value.fire_timestamp = None
        tm._ready_to_fire = mock.MagicMock()
        m_def.should_fire.return_value = True

        tm.add_event(event)
        tm.save_event.assert_called_once_with(event)
        for td in tm.trigger_definitions:
            td.match.assert_called_once_with(event)
        m_def.get_distinguishing_traits.assert_called_once_with(event,
                                                    m_def.match.return_value)
        tm._add_or_create_stream.assert_called_once_with(m_def, event,
            m_def.get_distinguishing_traits.return_value)
        tm.db.get_stream_events.assert_called_once_with(
                                        tm._add_or_create_stream.return_value)
        m_def.should_fire.assert_called_once_with(
                                        tm.db.get_stream_events.return_value)
        tm._ready_to_fire.assert_called_once_with(
                                tm._add_or_create_stream.return_value, m_def)

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_add_event_on_ready_stream(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        tm.trigger_definitions = [mock.MagicMock() for n in range(3)]
        m_def = tm.trigger_definitions[2]
        tm.trigger_definitions[0].match.return_value = None
        tm.trigger_definitions[1].match.return_value = None
        event = mock.MagicMock(name='event', spec=dict)
        tm.save_event = mock.MagicMock()
        tm._add_or_create_stream = mock.MagicMock()
        tm._add_or_create_stream.return_value.fire_timestamp = "Fire!"
        tm._ready_to_fire = mock.MagicMock()
        m_def.should_fire.return_value = True

        tm.add_event(event)
        tm.save_event.assert_called_once_with(event)
        for td in tm.trigger_definitions:
            td.match.assert_called_once_with(event)
        m_def.get_distinguishing_traits.assert_called_once_with(event,
                                                    m_def.match.return_value)
        tm._add_or_create_stream.assert_called_once_with(m_def, event,
            m_def.get_distinguishing_traits.return_value)
        self.assertFalse(tm.db.get_stream_events.called)
        self.assertFalse(m_def.should_fire.called)
        self.assertFalse(tm._ready_to_fire.called)

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap')
    def test_add_event_no_match(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        tm.trigger_definitions = [mock.MagicMock() for n in range(3)]
        tm.trigger_definitions[0].match.return_value = None
        tm.trigger_definitions[1].match.return_value = None
        tm.trigger_definitions[2].match.return_value = None
        event = mock.MagicMock(name='event', spec=dict)
        tm.save_event = mock.MagicMock()
        tm._add_or_create_stream = mock.MagicMock()
        tm._add_or_create_stream.return_value.fire_timestamp = "Fire!"
        tm._ready_to_fire = mock.MagicMock()

        tm.add_event(event)
        tm.save_event.assert_called_once_with(event)
        for td in tm.trigger_definitions:
            td.match.assert_called_once_with(event)
        for td in tm.trigger_definitions:
            self.assertFalse(td.get_distinguishing_traits.called)
            self.assertFalse(td.should_fire.called)
        self.assertFalse(tm._add_or_create_stream.called)
        self.assertFalse(tm.db.get_stream_events.called)
        self.assertFalse(tm._ready_to_fire.called)

    @mock.patch.object(trigger_manager.ConfigManager, 'wrap') 
    def test_add__del_trigger_definition(self, mock_config_wrap):
        tm = trigger_manager.TriggerManager('test')
        tm.db = mock.MagicMock(spec=tm.db)
        td1 = dict(name='test_trigger1',
                 expiration='$last + 1d',
                 fire_pipeline='test_pipeline',
                 fire_criteria=[dict(event_type='test.thing')],
                 match_criteria=[dict(event_type='test.*')])
        tdlist = list()
        tdlist.append(td1)
        tm.add_trigger_definition(tdlist)
        self.assertTrue(tm.trigger_map.has_key('test_trigger1'))
        tm.delete_trigger_definition('test_trigger1')
        self.assertFalse(tm.trigger_map.has_key('test_trigger1'))
