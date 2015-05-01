# Copyright (c) 2014 Dark Secret Software Inc.
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

import abc
import logging
import six

logger = logging.getLogger(__name__)


class NoOpGroup(object):
    def match(self):
        return True

    def mismatch(self, reason):
        return False

    def check(self, value, reason):
        return value


class Group(object):
    def __init__(self, name):
        self._name = name  # Group name
        self._match = 0
        self._mismatch = 0
        self._reasons = {}

    def match(self):
        self._match += 1
        return True

    def mismatch(self, reason):
        count = self._reasons.get(reason, 0)
        self._reasons[reason] = count + 1
        self._mismatch += 1
        return False

    def check(self, value, reason):
        if value:
            return self.match()
        return self.mismatch(reason)


@six.add_metaclass(abc.ABCMeta)
class BaseDebugger(object):
    @abc.abstractmethod
    def reset(self):
        pass

    @abc.abstractmethod
    def get_group(self, name):
        pass

    @abc.abstractmethod
    def bump_counter(self, name, inc=1):
        pass

    @abc.abstractmethod
    def get_debug_level(self):
        pass


class NoOpDebugger(BaseDebugger):
    def __init__(self, *args, **kwargs):
        self.noop_group = NoOpGroup()

    def reset(self):
        pass

    def get_group(self, name):
        return self.noop_group

    def bump_counter(self, name, inc=1):
        pass

    def get_debug_level(self):
        return 0


class DetailedDebugger(BaseDebugger):
    def __init__(self, name, debug_level):
        super(DetailedDebugger, self).__init__()
        self._name = name
        self._debug_level = debug_level
        self.reset()

    def reset(self):
        # If it's not a match or a mismatch it was a fatal error.
        self._groups = {}
        self._counters = {}

    def get_group(self, name):
        group = self._groups.get(name, Group(name))
        self._groups[name] = group
        return group

    def bump_counter(self, name, inc=1):
        self._counters[name] = self._counters.get(name, 0) + inc

    def get_debug_level(self):
        return self._debug_level


class DebugManager(object):
    def __init__(self):
        self._debuggers = {}

    def get_debugger(self, trigger_def):
        name = "n/a"
        level = 2  # Default these unknowns to full debug.
        if trigger_def is not None:
            name = trigger_def.name
            level = trigger_def.debug_level
        debugger = self._debuggers.get(name)
        if not debugger:
            if level > 0:
                debugger = DetailedDebugger(name, level)
            else:
                debugger = NoOpDebugger()
            self._debuggers[name] = debugger
        return debugger

    def dump_group(self, debugger, group_name):
        group = debugger.get_group(group_name)
        logger.info("%s Criteria: %d checks, %d passed" %
                    (group._name,
                     group._match + group._mismatch, group._match))

        if debugger.get_debug_level() > 1:
            for kv in group._reasons.items():
                logger.info(" - %s = %d" % kv)

    def dump_counters(self, debugger):
        for kv in debugger._counters.items():
            logger.info("Counter \"%s\" = %d" % kv)

    def dump_debuggers(self):
        for debugger in self._debuggers.values():
            if debugger.get_debug_level() == 0:
                continue

            logger.info("---- Trigger Definition: %s ----" % debugger._name)
            for name in debugger._groups.keys():
                self.dump_group(debugger, name)

            self.dump_counters(debugger)
            debugger.reset()
            logger.info("----------------------------")
