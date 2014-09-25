import unittest2 as unittest

import mock

import datetime
import timex

from winchester import debugging
from winchester import db as winch_db
from winchester import pipeline_manager
from winchester.models import StreamState


class TestPipeline(unittest.TestCase):
    def setUp(self):
        super(TestPipeline, self).setUp()
        self.debugger = debugging.NoOpDebugger()

    def test_check_handler_config(self):

        handler_map = {'test_thing': "blah"}
        c = pipeline_manager.Pipeline.check_handler_config("test_thing", handler_map)
        self.assertIsInstance(c, dict)
        self.assertIn('name', c)
        self.assertIn('params', c)
        self.assertIsInstance(c['params'], dict)
        self.assertEqual(c['name'], 'test_thing')
        self.assertEqual(c['params'], {})

        conf = dict(name='test_thing')
        c = pipeline_manager.Pipeline.check_handler_config(conf, handler_map)
        self.assertIsInstance(c, dict)
        self.assertIn('name', c)
        self.assertIn('params', c)
        self.assertIsInstance(c['params'], dict)
        self.assertEqual(c['name'], 'test_thing')
        self.assertEqual(c['params'], {})

        conf = dict(name='test_thing', params={'book': 42})
        c = pipeline_manager.Pipeline.check_handler_config(conf, handler_map)
        self.assertIsInstance(c, dict)
        self.assertIn('name', c)
        self.assertIn('params', c)
        self.assertIsInstance(c['params'], dict)
        self.assertEqual(c['name'], 'test_thing')
        self.assertEqual(c['params'], {'book': 42})

        with self.assertRaises(pipeline_manager.PipelineConfigError):
            c = pipeline_manager.Pipeline.check_handler_config("other_thing", handler_map)

        with self.assertRaises(pipeline_manager.PipelineConfigError):
            conf = dict(params={'book': 42})
            c = pipeline_manager.Pipeline.check_handler_config(conf, handler_map)

    def test_init(self):
        conf = [dict(name='test_thing', params={'book': 42})]
        handler_class = mock.MagicMock()
        handler_map = {'test_thing':  handler_class}
        p = pipeline_manager.Pipeline("test_pipeline", conf, handler_map)
        self.assertEqual(p.name, "test_pipeline")
        self.assertEqual(len(p.handlers), 1)
        self.assertIs(handler_class.return_value, p.handlers[0])
        handler_class.assert_called_once_with(book=42)

    def test_handle_events(self):
        test_events = [dict(message_id="t000-0001"),
                       dict(message_id="t000-0002"),
                       dict(message_id="t000-0003")]
        new_events = [dict(message_id="t000-0004")]
        conf = [dict(name='test_thing', params={}),
                dict(name='other_thing', params={}),
                dict(name='some_thing', params={})]
        handler_class1 = mock.MagicMock(name='handler1')
        handler_class2 = mock.MagicMock(name='handler2')
        handler_class3 = mock.MagicMock(name='handler3')
        handler_class3.return_value.handle_events.return_value = test_events + new_events

        handler_map = {'test_thing':  handler_class1,
                       'other_thing': handler_class2,
                       'some_thing':  handler_class3}
        p = pipeline_manager.Pipeline("test_pipeline", conf, handler_map)
        p.commit = mock.MagicMock(name='commit')
        p.rollback = mock.MagicMock(name='rollback')

        ret = p.handle_events(test_events, self.debugger)
        handler_class1.return_value.handle_events.assert_called_once_with(test_events, p.env)
        events1 = handler_class1.return_value.handle_events.return_value
        handler_class2.return_value.handle_events.assert_called_once_with(events1, p.env)
        events2 = handler_class2.return_value.handle_events.return_value
        handler_class3.return_value.handle_events.assert_called_once_with(events2, p.env)
        p.commit.assert_called_once_with(self.debugger)
        self.assertFalse(p.rollback.called)
        self.assertEqual(ret, new_events)

    def test_handle_events_error(self):
        test_events = [dict(message_id="t000-0001"),
                       dict(message_id="t000-0002"),
                       dict(message_id="t000-0003")]
        conf = [dict(name='test_thing', params={}),
                dict(name='other_thing', params={}),
                dict(name='some_thing', params={})]
        handler_class1 = mock.MagicMock(name='handler1')
        handler_class2 = mock.MagicMock(name='handler2')
        handler_class3 = mock.MagicMock(name='handler3')

        class WhackyError(Exception):
            pass

        handler_class2.return_value.handle_events.side_effect = WhackyError("whoa!")

        handler_map = {'test_thing':  handler_class1,
                       'other_thing': handler_class2,
                       'some_thing':  handler_class3}
        p = pipeline_manager.Pipeline("test_pipeline", conf, handler_map)
        p.commit = mock.MagicMock(name='commit')
        p.rollback = mock.MagicMock(name='rollback')

        with self.assertRaises(pipeline_manager.PipelineExecutionError):
            p.handle_events(test_events, self.debugger)
        p.rollback.assert_called_once_with(self.debugger)
        self.assertFalse(p.commit.called)

    def test_commit(self):
        conf = [dict(name='test_thing', params={}),
                dict(name='other_thing', params={}),
                dict(name='some_thing', params={})]
        handler_class1 = mock.MagicMock(name='handler1')
        handler_class2 = mock.MagicMock(name='handler2')
        handler_class3 = mock.MagicMock(name='handler3')

        handler_map = {'test_thing':  handler_class1,
                       'other_thing': handler_class2,
                       'some_thing':  handler_class3}
        p = pipeline_manager.Pipeline("test_pipeline", conf, handler_map)
        p.commit(self.debugger)
        handler_class1.return_value.commit.assert_called_once_with()
        handler_class2.return_value.commit.assert_called_once_with()
        handler_class3.return_value.commit.assert_called_once_with()

    def test_commit_with_error(self):
        conf = [dict(name='test_thing', params={}),
                dict(name='other_thing', params={}),
                dict(name='some_thing', params={})]
        handler_class1 = mock.MagicMock(name='handler1')
        handler_class2 = mock.MagicMock(name='handler2')
        handler_class3 = mock.MagicMock(name='handler3')

        class WhackyError(Exception):
            pass

        handler_class2.return_value.commit.side_effect = WhackyError("whoa!")

        handler_map = {'test_thing':  handler_class1,
                       'other_thing': handler_class2,
                       'some_thing':  handler_class3}
        p = pipeline_manager.Pipeline("test_pipeline", conf, handler_map)
        p.commit(self.debugger)
        handler_class1.return_value.commit.assert_called_once_with()
        handler_class2.return_value.commit.assert_called_once_with()
        handler_class3.return_value.commit.assert_called_once_with()

    def test_rollback(self):
        conf = [dict(name='test_thing', params={}),
                dict(name='other_thing', params={}),
                dict(name='some_thing', params={})]
        handler_class1 = mock.MagicMock(name='handler1')
        handler_class2 = mock.MagicMock(name='handler2')
        handler_class3 = mock.MagicMock(name='handler3')

        handler_map = {'test_thing':  handler_class1,
                       'other_thing': handler_class2,
                       'some_thing':  handler_class3}
        p = pipeline_manager.Pipeline("test_pipeline", conf, handler_map)
        p.rollback(self.debugger)
        handler_class1.return_value.rollback.assert_called_once_with()
        handler_class2.return_value.rollback.assert_called_once_with()
        handler_class3.return_value.rollback.assert_called_once_with()

    def test_rollback_with_error(self):
        conf = [dict(name='test_thing', params={}),
                dict(name='other_thing', params={}),
                dict(name='some_thing', params={})]
        handler_class1 = mock.MagicMock(name='handler1')
        handler_class2 = mock.MagicMock(name='handler2')
        handler_class3 = mock.MagicMock(name='handler3')

        class WhackyError(Exception):
            pass

        handler_class2.return_value.rollback.side_effect = WhackyError("whoa!")

        handler_map = {'test_thing':  handler_class1,
                       'other_thing': handler_class2,
                       'some_thing':  handler_class3}
        p = pipeline_manager.Pipeline("test_pipeline", conf, handler_map)
        p.rollback(self.debugger)
        handler_class1.return_value.rollback.assert_called_once_with()
        handler_class2.return_value.rollback.assert_called_once_with()
        handler_class3.return_value.rollback.assert_called_once_with()


class TestPipelineManager(unittest.TestCase):

    def setUp(self):
        super(TestPipelineManager, self).setUp()
        self.debugger = debugging.NoOpDebugger()

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_complete_stream_nopurge(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        pm.db = mock.MagicMock(spec=pm.db)
        pm.purge_completed_streams = False
        stream = "test stream"
        pm._complete_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream, StreamState.completed)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_complete_stream_purge(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        pm.db = mock.MagicMock(spec=pm.db)
        pm.purge_completed_streams = True
        stream = "test stream"
        pm._complete_stream(stream)
        pm.db.purge_stream.assert_called_once_with(stream)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_error_stream(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        pm.db = mock.MagicMock(spec=pm.db)
        stream = "test stream"
        pm._error_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream, StreamState.error)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_expire_error_stream(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        pm.db = mock.MagicMock(spec=pm.db)
        stream = "test stream"
        pm._expire_error_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream, StreamState.expire_error)

    @mock.patch('winchester.pipeline_manager.Pipeline', autospec=True)
    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_run_pipeline(self, mock_config_wrap, mock_pipeline):
        pm = pipeline_manager.PipelineManager('test')
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        trigger_def = mock.MagicMock(name='trigger_def')
        trigger_def.debugger = self.debugger
        pipeline_name = "test"
        pipeline_config = mock.MagicMock(name='pipeline_config')
        stream = mock.MagicMock(name='stream')
        stream.name = "test"
        pm.add_new_events = mock.MagicMock(name='add_new_events')
        pm.pipeline_handlers = mock.MagicMock(name='pipeline_handlers')

        ret = pm._run_pipeline(stream, trigger_def, pipeline_name,
                               pipeline_config)
        pm.db.get_stream_events.assert_called_once_with(stream)
        mock_pipeline.assert_called_once_with(pipeline_name, pipeline_config,
                                              pm.pipeline_handlers)

        pipeline = mock_pipeline.return_value
        pipeline.handle_events.assert_called_once_with(
            pm.db.get_stream_events.return_value, self.debugger)
        pm.add_new_events.assert_called_once_with(
            mock_pipeline.return_value.handle_events.return_value)
        self.assertTrue(ret)

    @mock.patch('winchester.pipeline_manager.Pipeline', autospec=True)
    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_run_pipeline_with_error(self, mock_config_wrap, mock_pipeline):
        pm = pipeline_manager.PipelineManager('test')
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        trigger_def = mock.MagicMock(name='trigger_def')
        trigger_def.debugger = self.debugger
        pipeline_name = "test"
        pipeline_config = mock.MagicMock(name='pipeline_config')
        stream = mock.MagicMock(name='stream')
        stream.name = "test"
        pm.add_new_events = mock.MagicMock(name='add_nemw_events')
        pm.pipeline_handlers = mock.MagicMock(name='pipeline_handlers')
        pipeline = mock_pipeline.return_value
        pipeline.handle_events.side_effect = \
                    pipeline_manager.PipelineExecutionError('test', 'thing')

        ret = pm._run_pipeline(stream, trigger_def, pipeline_name,
                               pipeline_config)

        pm.db.get_stream_events.assert_called_once_with(stream)
        mock_pipeline.assert_called_once_with(pipeline_name, pipeline_config,
                                              pm.pipeline_handlers)

        pipeline.handle_events.assert_called_once_with(
            pm.db.get_stream_events.return_value, self.debugger)
        self.assertFalse(pm.add_new_events.called)
        self.assertFalse(ret)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_fire_stream(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        stream = mock.MagicMock(name='stream')
        stream.name = 'test'
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        pm.db.set_stream_state.return_value = stream
        trigger_def = mock.MagicMock(name='trigger_def')
        trigger_def.fire_pipeline = 'test_fire_pipeline'
        pm.trigger_map = dict(test=trigger_def)
        pipeline_config = mock.MagicMock(name='pipeline_config')
        pm.pipeline_config = dict(test_fire_pipeline=pipeline_config)
        pm._error_stream = mock.MagicMock(name='_error_stream')
        pm._complete_stream = mock.MagicMock(name='_complete_stream')
        pm._run_pipeline = mock.MagicMock(name='_run_pipeline')
        pm._run_pipeline.return_value = True

        ret = pm.fire_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream,
                                                       StreamState.firing)
        pm._run_pipeline.assert_called_once_with(stream, trigger_def,
                                    'test_fire_pipeline', pipeline_config)
        self.assertFalse(pm._error_stream.called)
        pm._complete_stream.assert_called_once_with(stream)
        self.assertTrue(ret)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_fire_stream_locked(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        stream = mock.MagicMock(name='stream')
        stream.name = 'test'
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        pm.db.set_stream_state.side_effect = winch_db.LockError('locked!')
        trigger_def = mock.MagicMock(name='trigger_def')
        trigger_def.fire_pipeline = 'test_fire_pipeline'
        pm.trigger_map = dict(test=trigger_def)
        pipeline_config = mock.MagicMock(name='pipeline_config')
        pm.pipeline_config = dict(test_fire_pipeline=pipeline_config)
        pm._error_stream = mock.MagicMock(name='_error_stream')
        pm._complete_stream = mock.MagicMock(name='_complete_stream')
        pm._run_pipeline = mock.MagicMock(name='_run_pipeline')
        pm._run_pipeline.return_value = True

        ret = pm.fire_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream, StreamState.firing)
        self.assertFalse(pm._run_pipeline.called)
        self.assertFalse(pm._error_stream.called)
        self.assertFalse(pm._complete_stream.called)
        self.assertFalse(ret)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_fire_stream_no_pipeline(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        stream = mock.MagicMock(name='stream')
        stream.name = 'test'
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        pm.db.set_stream_state.return_value = stream
        trigger_def = mock.MagicMock(name='trigger_def')
        trigger_def.fire_pipeline = None
        pm.trigger_map = dict(test=trigger_def)
        pm._error_stream = mock.MagicMock(name='_error_stream')
        pm._complete_stream = mock.MagicMock(name='_complete_stream')
        pm._run_pipeline = mock.MagicMock(name='_run_pipeline')
        pm._run_pipeline.return_value = True

        ret = pm.fire_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream,
                                                    StreamState.firing)
        self.assertFalse(pm._error_stream.called)
        self.assertFalse(pm._run_pipeline.called)
        pm._complete_stream.assert_called_once_with(stream)
        self.assertTrue(ret)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_fire_stream_error(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        stream = mock.MagicMock(name='stream')
        stream.name = 'test'
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        pm.db.set_stream_state.return_value = stream
        trigger_def = mock.MagicMock(name='trigger_def')
        trigger_def.fire_pipeline = 'test_fire_pipeline'
        pm.trigger_map = dict(test=trigger_def)
        pipeline_config = mock.MagicMock(name='pipeline_config')
        pm.pipeline_config = dict(test_fire_pipeline=pipeline_config)
        pm._error_stream = mock.MagicMock(name='_error_stream')
        pm._complete_stream = mock.MagicMock(name='_complete_stream')
        pm._run_pipeline = mock.MagicMock(name='_run_pipeline')
        pm._run_pipeline.return_value = False

        ret = pm.fire_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream,
                                                       StreamState.firing)
        pm._run_pipeline.assert_called_once_with(stream, trigger_def,
                                         'test_fire_pipeline',
                                         pipeline_config)
        self.assertFalse(pm._complete_stream.called)
        pm._error_stream.assert_called_once_with(stream)
        self.assertFalse(ret)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_expire_stream(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        stream = mock.MagicMock(name='stream')
        stream.name = 'test'
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        pm.db.set_stream_state.return_value = stream
        trigger_def = mock.MagicMock(name='trigger_def')
        trigger_def.expire_pipeline = 'test_fire_pipeline'
        pm.trigger_map = dict(test=trigger_def)
        pipeline_config = mock.MagicMock(name='pipeline_config')
        pm.pipeline_config = dict(test_fire_pipeline=pipeline_config)
        pm._error_stream = mock.MagicMock(name='_error_stream')
        pm._complete_stream = mock.MagicMock(name='_complete_stream')
        pm._run_pipeline = mock.MagicMock(name='_run_pipeline')
        pm._run_pipeline.return_value = True

        ret = pm.expire_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream,
                                                       StreamState.expiring)
        pm._run_pipeline.assert_called_once_with(stream, trigger_def,
                    'test_fire_pipeline', pipeline_config)
        self.assertFalse(pm._error_stream.called)
        pm._complete_stream.assert_called_once_with(stream)
        self.assertTrue(ret)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_expire_stream_locked(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        stream = mock.MagicMock(name='stream')
        stream.name = 'test'
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        pm.db.set_stream_state.side_effect = winch_db.LockError('locked!')
        trigger_def = mock.MagicMock(name='trigger_def')
        trigger_def.expire_pipeline = 'test_fire_pipeline'
        pm.trigger_map = dict(test=trigger_def)
        pipeline_config = mock.MagicMock(name='pipeline_config')
        pm.pipeline_config = dict(test_fire_pipeline=pipeline_config)
        pm._expire_error_stream = mock.MagicMock(name='_expire_error_stream')
        pm._complete_stream = mock.MagicMock(name='_complete_stream')
        pm._run_pipeline = mock.MagicMock(name='_run_pipeline')
        pm._run_pipeline.return_value = True

        ret = pm.expire_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream, StreamState.expiring)
        self.assertFalse(pm._run_pipeline.called)
        self.assertFalse(pm._expire_error_stream.called)
        self.assertFalse(pm._complete_stream.called)
        self.assertFalse(ret)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_expire_stream_no_pipeline(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        stream = mock.MagicMock(name='stream')
        stream.name = 'test'
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        pm.db.set_stream_state.return_value = stream
        trigger_def = mock.MagicMock(name='trigger_def')
        trigger_def.expire_pipeline = None
        pm.trigger_map = dict(test=trigger_def)
        pm._expire_error_stream = mock.MagicMock(name='_expire_error_stream')
        pm._complete_stream = mock.MagicMock(name='_complete_stream')
        pm._run_pipeline = mock.MagicMock(name='_run_pipeline')
        pm._run_pipeline.return_value = True

        ret = pm.expire_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream,
                                                    StreamState.expiring)
        self.assertFalse(pm._expire_error_stream.called)
        self.assertFalse(pm._run_pipeline.called)
        pm._complete_stream.assert_called_once_with(stream)
        self.assertTrue(ret)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_expire_stream_error(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        stream = mock.MagicMock(name='stream')
        stream.name = 'test'
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        pm.db.set_stream_state.return_value = stream
        trigger_def = mock.MagicMock(name='trigger_def')
        trigger_def.expire_pipeline = 'test_fire_pipeline'
        pm.trigger_map = dict(test=trigger_def)
        pipeline_config = mock.MagicMock(name='pipeline_config')
        pm.pipeline_config = dict(test_fire_pipeline=pipeline_config)
        pm._expire_error_stream = mock.MagicMock(name='_expire_error_stream')
        pm._complete_stream = mock.MagicMock(name='_complete_stream')
        pm._run_pipeline = mock.MagicMock(name='_run_pipeline')
        pm._run_pipeline.return_value = False

        ret = pm.expire_stream(stream)
        pm.db.set_stream_state.assert_called_once_with(stream,
                                                       StreamState.expiring)
        pm._run_pipeline.assert_called_once_with(stream, trigger_def,
                                    'test_fire_pipeline', pipeline_config)
        self.assertFalse(pm._complete_stream.called)
        pm._expire_error_stream.assert_called_once_with(stream)
        self.assertFalse(ret)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_process_ready_streams_fire(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        stream = mock.MagicMock(name='stream')
        stream.name = "my_stream"
        tdef = mock.MagicMock(name='tdef')
        pm.trigger_map['my_stream'] = tdef
        pm.expire_stream = mock.MagicMock(name='expire_stream')
        pm.fire_stream = mock.MagicMock(name='fire_stream')
        pm.current_time = mock.MagicMock(name='current_time')
        pm.db.get_ready_streams.return_value = [stream]

        ret = pm.process_ready_streams(42)
        pm.db.get_ready_streams.assert_called_once_with(42,
                                pm.current_time.return_value, expire=False)
        pm.fire_stream.assert_called_once_with(stream)
        self.assertFalse(pm.expire_stream.called)
        self.assertEqual(ret, 1)

    @mock.patch.object(pipeline_manager.ConfigManager, 'wrap')
    def test_process_ready_streams_expire(self, mock_config_wrap):
        pm = pipeline_manager.PipelineManager('test')
        pm.db = mock.MagicMock(spec=pm.db, name='db')
        stream = mock.MagicMock(name='stream')
        stream.name = "my_stream"
        pm.expire_stream = mock.MagicMock(name='expire_stream')
        pm.fire_stream = mock.MagicMock(name='fire_stream')
        pm.current_time = mock.MagicMock(name='current_time')
        pm.db.get_ready_streams.return_value = [stream]

        ret = pm.process_ready_streams(42, expire=True)
        pm.db.get_ready_streams.assert_called_once_with(42,
                                pm.current_time.return_value, expire=True)
        pm.expire_stream.assert_called_once_with(stream)
        self.assertFalse(pm.fire_stream.called)
        self.assertEqual(ret, 1)
