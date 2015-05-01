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

import mock

from winchester import debugging


class TestDebugManager(unittest.TestCase):

    def setUp(self):
        super(TestDebugManager, self).setUp()
        self.debug_manager = debugging.DebugManager()

    def test_get_debugger_none(self):
        debugger = self.debug_manager.get_debugger(None)
        self.assertEqual("n/a", debugger._name)
        self.assertEqual(2, debugger._debug_level)

    def test_get_debugger_off(self):
        tdef = mock.MagicMock(name="tdef")
        tdef.name = "my_trigger"
        tdef.debug_level = 0
        debugger = self.debug_manager.get_debugger(tdef)
        self.assertTrue(isinstance(debugger, debugging.NoOpDebugger))
        self.assertEqual(debugger,
                         self.debug_manager._debuggers['my_trigger'])

        debugger2 = self.debug_manager.get_debugger(tdef)
        self.assertEqual(debugger, debugger2)

    def test_get_debugger_on(self):
        tdef = mock.MagicMock(name="tdef")
        tdef.name = "my_trigger"
        tdef.debug_level = 1
        debugger = self.debug_manager.get_debugger(tdef)
        self.assertTrue(isinstance(debugger, debugging.DetailedDebugger))
        self.assertEqual(debugger,
                         self.debug_manager._debuggers['my_trigger'])

        debugger2 = self.debug_manager.get_debugger(tdef)
        self.assertEqual(debugger, debugger2)

    def test_dump_group_level1(self):
        debugger = mock.MagicMock(name="debugger")
        debugger.get_debug_level.return_value = 1
        group = mock.MagicMock(name="group")
        group._name = "my_group"
        group._match = 1
        group._mismatch = 2
        debugger.get_group.return_value = group
        with mock.patch.object(debugging, "logger") as log:
            self.debug_manager.dump_group(debugger, "my_group")

            log.info.assert_called_once_with(
                "my_group Criteria: 3 checks, 1 passed")

    def test_dump_group_level2(self):
        debugger = mock.MagicMock(name="debugger")
        debugger.get_debug_level.return_value = 2
        group = mock.MagicMock(name="group")
        group._name = "my_group"
        group._match = 1
        group._mismatch = 2
        group._reasons = {"foo": 12}
        debugger.get_group.return_value = group
        with mock.patch.object(debugging, "logger") as log:
            self.debug_manager.dump_group(debugger, "my_group")

            self.assertEqual(log.info.call_args_list,
                             [mock.call(
                                 "my_group Criteria: 3 checks, 1 passed"),
                              mock.call(" - foo = 12")])

    def test_dump_counters(self):
        debugger = mock.MagicMock(name="debugger")
        debugger._counters = {'foo': 12}
        with mock.patch.object(debugging, "logger") as log:
            self.debug_manager.dump_counters(debugger)
            log.info.assert_called_once_with('Counter "foo" = 12')

    def test_dump_debuggers_off(self):
        debugger = mock.MagicMock(name="debugger")
        debugger.get_debug_level.return_value = 0
        self.debug_manager._debuggers = {"foo": debugger}
        with mock.patch.object(debugging, "logger") as log:
            self.debug_manager.dump_debuggers()
            self.assertEqual(0, log.info.call_count)

    def test_dump_debuggers_on(self):
        debugger = mock.MagicMock(name="debugger")
        debugger.get_debug_level.return_value = 1
        debugger._name = "my_debugger"
        group = mock.MagicMock(name="group")
        debugger._groups = {"my_group": group}
        self.debug_manager._debuggers = {"foo": debugger}
        with mock.patch.object(self.debug_manager, "dump_counters") as ctr:
            with mock.patch.object(self.debug_manager, "dump_group") as grp:
                with mock.patch.object(debugging, "logger") as log:
                    self.debug_manager.dump_debuggers()
                    self.assertEqual(
                        log.info.call_args_list,
                        [mock.call(
                            "---- Trigger Definition: my_debugger ----"),
                         mock.call(
                             "----------------------------")])
                grp.assert_called_once_with(debugger, "my_group")
            ctr.assert_called_once_with(debugger)
        debugger.reset.assert_called_once_with()


class TestDetailedDebugger(unittest.TestCase):
    def setUp(self):
        super(TestDetailedDebugger, self).setUp()
        self.debugger = debugging.DetailedDebugger("my_debugger", 2)

    def test_constructor(self):
        with mock.patch("winchester.debugging.DetailedDebugger.reset") \
                as reset:
            debugging.DetailedDebugger("my_debugger", 2)
            reset.assert_called_once_with()

        self.assertEqual(self.debugger._name, "my_debugger")
        self.assertEqual(self.debugger._debug_level, 2)

    def test_reset(self):
        self.assertEqual(self.debugger._groups, {})
        self.assertEqual(self.debugger._counters, {})

    def test_get_group(self):
        self.assertEqual(self.debugger._groups, {})
        g = self.debugger.get_group("foo")
        self.assertEqual(g._name, "foo")
        self.assertTrue(self.debugger._groups['foo'])

    def test_bump_counter(self):
        self.assertEqual(self.debugger._counters, {})
        self.debugger.bump_counter("foo")
        self.assertEqual(self.debugger._counters['foo'], 1)

        self.debugger.bump_counter("foo", 2)
        self.assertEqual(self.debugger._counters['foo'], 3)

    def test_get_debug_level(self):
        self.assertEqual(self.debugger.get_debug_level(), 2)


class TestNoOpDebugger(unittest.TestCase):
    def setUp(self):
        super(TestNoOpDebugger, self).setUp()
        self.debugger = debugging.NoOpDebugger("my_debugger", 2)

    def test_reset(self):
        self.debugger.reset()

    def test_get_group(self):
        g = self.debugger.get_group("foo")
        self.assertEqual(g, self.debugger.noop_group)

    def test_bump_counter(self):
        self.debugger.bump_counter("foo")
        self.debugger.bump_counter("foo", 2)

    def test_get_debug_level(self):
        self.assertEqual(self.debugger.get_debug_level(), 0)


class TestGroup(unittest.TestCase):
    def setUp(self):
        super(TestGroup, self).setUp()
        self.group = debugging.Group("my_group")

    def test_constructor(self):
        self.assertEqual("my_group", self.group._name)
        self.assertEqual(0, self.group._match)
        self.assertEqual(0, self.group._mismatch)
        self.assertEqual({}, self.group._reasons)

    def test_match(self):
        self.assertTrue(self.group.match())
        self.assertEqual(1, self.group._match)

    def test_mismatch(self):
        self.assertFalse(self.group.mismatch("reason"))
        self.assertEqual(1, self.group._mismatch)
        self.assertEqual(1, self.group._reasons['reason'])

    def test_check(self):
        self.assertTrue(self.group.check(True, "reason"))
        self.assertEqual(1, self.group._match)
        self.assertEqual(0, self.group._mismatch)
        self.assertEqual({}, self.group._reasons)

        self.assertTrue(self.group.check(True, "reason"))
        self.assertEqual(2, self.group._match)
        self.assertEqual(0, self.group._mismatch)
        self.assertEqual({}, self.group._reasons)

        self.assertFalse(self.group.check(False, "reason"))
        self.assertEqual(2, self.group._match)
        self.assertEqual(1, self.group._mismatch)
        self.assertEqual(1, self.group._reasons['reason'])

        self.assertFalse(self.group.check(False, "reason"))
        self.assertEqual(2, self.group._match)
        self.assertEqual(2, self.group._mismatch)
        self.assertEqual(2, self.group._reasons['reason'])


class TestNoOpGroup(unittest.TestCase):
    def setUp(self):
        super(TestNoOpGroup, self).setUp()
        self.group = debugging.NoOpGroup()

    def test_match(self):
        self.assertTrue(self.group.match())

    def test_mismatch(self):
        self.assertFalse(self.group.mismatch("reason"))

    def test_check(self):
        self.assertTrue(self.group.check(True, "reason"))
        self.assertFalse(self.group.check(False, "reason"))
