import logging
import collections
import datetime
import six
import timex
import fnmatch


logger = logging.getLogger(__name__)


class DefinitionError(Exception):
    pass


def filter_event_timestamps(event):
    return dict((trait, value) for trait, value in event.items()
                if isinstance(value, datetime.datetime))


class Criterion(object):

    @classmethod
    def get_from_expression(cls, expression, trait_name):
        if isinstance(expression, collections.Mapping):
            if len(expression) != 1:
                raise DefinitionError("Only exactly one type of match is "
                                      "allowed per criterion expression")
            ctype = expression.keys()[0]
            expr = expression[ctype]
            if ctype == 'int':
                return NumericCriterion(expr, trait_name)
            elif ctype =='float':
                return FloatCriterion(expr, trait_name)
            elif ctype == 'datetime':
                return TimeCriterion(expr, trait_name)
            elif ctype == 'string' or ctype == 'text':
                return Criterion(expr, trait_name)
        else:
            # A constant. -mdragon
            return Criterion(expression, trait_name)

    def __init__(self, expr, trait_name):
        self.trait_name = trait_name
        #match a constant
        self.op = '='
        self.value = expr

    def match(self, event, debug_group):
        if self.trait_name not in event:
            return debug_group.mismatch("not %s" % self.trait_name)
        value = event[self.trait_name]
        if self.op == '=':
            return debug_group.check(value == self.value, "== failed")
        elif self.op == '>':
            return debug_group.check(value > self.value, "> failed")
        elif self.op == '<':
            return debug_group.check(value < self.value, "< failed")
        return debug_group.mismatch("Criterion match() fall-thru")


class NumericCriterion(Criterion):

    def __init__(self, expr, trait_name):
        self.trait_name = trait_name
        if not isinstance(expr, six.string_types):
            self.op = '='
            self.value = expr
        else:
            expr = expr.strip().split(None, 1)
            if len(expr) == 2:
                self.op = expr[0]
                value = expr[1].strip()
            elif len(expr) == 1:
                self.op = '='
                value = expr[0]
            else:
                raise DefinitionError('Invalid numeric criterion.')
            try:
                self.value = self._convert(value)
            except ValueError:
                raise DefinitionError('Invalid numeric criterion.')

    def _convert(self, value):
        return int(value)


class FloatCriterion(NumericCriterion):

    def _convert(self, value):
        return float(value)


class TimeCriterion(Criterion):

    def __init__(self, expression, trait_name):
        self.trait_name = trait_name
        self.time_expr = timex.parse(expression)

    def match(self, event, debug_group):
        if self.trait_name not in event:
            return debug_group.mismatch("Time: not '%s'" % self.trait_name)
        value = event[self.trait_name]
        try:
            timerange = self.time_expr(**filter_event_timestamps(event))
        except timex.TimexExpressionError:
            # the event doesn't contain a trait referenced in the expression.
            return debug_group.mismatch("Time: no referenced trait")
        return debug_group.check(value in timerange, "Not in timerange")


class Criteria(object):
    def __init__(self, config):
        self.included_types = []
        self.excluded_types = []
        if 'event_type' in config:
            event_types = config['event_type']
            if isinstance(event_types, six.string_types):
                event_types = [event_types]
            for t in event_types:
                if t.startswith('!'):
                    self.excluded_types.append(t[1:])
                else:
                    self.included_types.append(t)
        else:
            self.included_types.append('*')
        if self.excluded_types and not self.included_types:
            self.included_types.append('*')
        if 'number' in config:
            self.number = config['number']
        else:
            self.number = 1
        if 'timestamp' in config:
            self.timestamp = timex.parse(config['timestamp'])
        else:
            self.timestamp = None
        self.map_distinguished_by = dict()
        if 'map_distinguished_by' in config:
            self.map_distinguished_by = config['map_distinguished_by']
        self.traits = dict()
        if 'traits' in config:
            for trait, criterion in config['traits'].items():
                self.traits[trait] = Criterion.get_from_expression(criterion, trait)

    def included_type(self, event_type):
        return any(fnmatch.fnmatch(event_type, t) for t in self.included_types)

    def excluded_type(self, event_type):
        return any(fnmatch.fnmatch(event_type, t) for t in self.excluded_types)

    def match_type(self, event_type):
        return (self.included_type(event_type)
                and not self.excluded_type(event_type))

    def match(self, event, debug_group):
        if not self.match_type(event['event_type']):
            return debug_group.mismatch("Wrong event type")
        if self.timestamp:
            try:
                t = self.timestamp(**filter_event_timestamps(event))
            except timex.TimexExpressionError:
                # the event doesn't contain a trait referenced in the expression.
                return debug_group.mismatch("No timestamp trait")
            if event['timestamp'] not in t:
                return debug_group.mismatch("Not time yet.")
        if not self.traits:
            return debug_group.match()
        return all(criterion.match(event, debug_group) for
                       criterion in self.traits.values())


class TriggerDefinition(object):

    def __init__(self, config):
        if 'name' not in config:
            raise DefinitionError("Required field in trigger definition not "
                                  "specified 'name'")
        self.name = config['name']
        self.debug_level = int(config.get('xdebug_level', 2))
        self.distinguished_by = config.get('distinguished_by', [])
        for dt in self.distinguished_by:
            if isinstance(dt, collections.Mapping):
                if len(dt) > 1:
                    raise DefinitionError("Invalid distinguising expression "
                        "%s. Only one trait allowed in an expression" % str(dt))
        self.fire_delay = config.get('fire_delay', 0)
        if 'expiration' not in config:
            raise DefinitionError("Required field in trigger definition not "
                                  "specified 'expiration'")
        self.expiration = timex.parse(config['expiration'])
        self.fire_pipeline = config.get('fire_pipeline')
        self.expire_pipeline = config.get('expire_pipeline')
        if not self.fire_pipeline and not self.expire_pipeline:
            raise DefinitionError("At least one of: 'fire_pipeline' or "
                                  "'expire_pipeline' must be specified in a "
                                  "trigger definition.")
        if 'fire_criteria' not in config:
            raise DefinitionError("Required criteria in trigger definition not "
                                  "specified 'fire_criteria'")
        self.fire_criteria = [Criteria(c) for c in config['fire_criteria']]
        if 'match_criteria' not in config:
            raise DefinitionError("Required criteria in trigger definition not "
                                  "specified 'match_criteria'")
        self.match_criteria = [Criteria(c) for c in config['match_criteria']]
        self.load_criteria = []
        if 'load_criteria' in config:
            self.load_criteria = [Criteria(c) for c in config['load_criteria']]

    def match(self, event, debugger):
        # all distinguishing traits must exist to match.
        group = debugger.get_group("Match")
        for dt in self.distinguished_by:
            if isinstance(dt, collections.Mapping):
                trait_name = dt.keys()[0]
            else:
                trait_name = dt
            if trait_name not in event:
                group.mismatch("not '%s'" % trait_name)
                return None

        for criteria in self.match_criteria:
            if criteria.match(event, group):
                group.match()
                return criteria

        group.mismatch("No matching criteria")
        return None

    def get_distinguishing_traits(self, event, matching_criteria):
        dist_traits = dict()
        for dt in self.distinguished_by:
            d_expr = None
            if isinstance(dt, collections.Mapping):
                trait_name = dt.keys()[0]
                d_expr = timex.parse(dt[trait_name])
            else:
                trait_name = dt
            event_trait_name = matching_criteria.map_distinguished_by.get(trait_name, trait_name)
            if d_expr is not None:
                dist_traits[trait_name] = d_expr(timestamp=event[event_trait_name])
            else:
                dist_traits[trait_name] = event[event_trait_name]
        return dist_traits

    def get_fire_timestamp(self, timestamp):
        return timestamp + datetime.timedelta(seconds=self.fire_delay)

    def should_fire(self, events, debugger):
        group = debugger.get_group("Fire")
        for criteria in self.fire_criteria:
            matches = 0
            for event in events:
                if criteria.match(event, group):
                    matches += 1
                    if matches >= criteria.number:
                        break
            if matches < criteria.number:
                return group.mismatch("Not enough matching criteria")
        return group.match()
