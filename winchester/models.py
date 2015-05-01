# Copyright (c) 2014 Dark Secret Software Inc.
# Copyright (c) 2015 Rackspace
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import calendar
from datetime import datetime
from decimal import Decimal

from enum import IntEnum

import timex

from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import DECIMAL
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy.orm import backref
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm import composite
from sqlalchemy.orm.interfaces import PropComparator
from sqlalchemy.orm import relationship
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.types import TypeDecorator, DATETIME


class Datatype(IntEnum):
    none = 0
    string = 1
    int = 2
    float = 3
    datetime = 4
    timerange = 5


class StreamState(IntEnum):
    active = 1
    firing = 2
    expiring = 3
    error = 4
    expire_error = 5
    completed = 6
    retry_fire = 7
    retry_expire = 8


class DBException(Exception):
    pass


class InvalidTraitType(DBException):
    pass


def dt_to_decimal(dt):
    t_sec = calendar.timegm(dt.utctimetuple()) + (dt.microsecond/1e6)
    return Decimal("%.6f" % t_sec)


def decimal_to_dt(decimal_timestamp):
    return datetime.utcfromtimestamp(float(decimal_timestamp))


class PreciseTimestamp(TypeDecorator):
    """Represents a timestamp precise to the microsecond."""

    impl = DATETIME

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(DECIMAL(precision=20,
                                                   scale=6,
                                                   asdecimal=True))
        return dialect.type_descriptor(DATETIME())

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'mysql':
            return dt_to_decimal(value)
        return value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'mysql':
            return decimal_to_dt(value)
        return value


class DBTimeRange(object):
    def __init__(self, begin, end):
        self.begin = begin
        self.end = end

    def __composite_values__(self):
        return self.begin, self.end

    def __repr__(self):
        return "DBTimeRange(begin=%r, end=%r)" % (self.begin, self.end)

    def __eq__(self, other):
        return isinstance(other, DBTimeRange) and \
            other.begin == self.begin and \
            other.end == self.end

    def __ne__(self, other):
        return not self.__eq__(other)


class ProxiedDictMixin(object):
    """Adds obj[name] access to a mapped class.

    This class basically proxies dictionary access to an attribute
    called ``_proxied``.  The class which inherits this class
    should have an attribute called ``_proxied`` which points to a dictionary.

    """

    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, name):
        return self._proxied[name]

    def __contains__(self, name):
        return name in self._proxied

    def __setitem__(self, name, value):
        self._proxied[name] = value

    def __delitem__(self, name):
        del self._proxied[name]


class PolymorphicVerticalProperty(object):
    """A name/value pair with polymorphic value storage."""

    ATTRIBUTE_MAP = {Datatype.none: None}
    PY_TYPE_MAP = {unicode: Datatype.string,
                   int: Datatype.int,
                   float: Datatype.float,
                   datetime: Datatype.datetime,
                   DBTimeRange: Datatype.timerange}

    def __init__(self, name, value=None):
        self.name = name
        self.value = value

    @classmethod
    def get_type_value(cls, value):
        if value is None:
            return Datatype.none, None
        if isinstance(value, str):
            value = value.decode('utf8', 'ignore')
        if isinstance(value, timex.Timestamp):
            value = value.timestamp
        if isinstance(value, timex.TimeRange):
            value = DBTimeRange(value.begin, value.end)
        if type(value) in cls.PY_TYPE_MAP:
            return cls.PY_TYPE_MAP[type(value)], value
        return None, value

    @hybrid_property
    def value(self):
        if self.type not in self.ATTRIBUTE_MAP:
            raise InvalidTraitType(
                "Invalid trait type in db for %s: %s" % (self.name, self.type))
        attribute = self.ATTRIBUTE_MAP[self.type]
        if attribute is None:
            return None
        if self.type == Datatype.timerange:
            val = getattr(self, attribute)
            return timex.TimeRange(val.begin, val.end)
        else:
            return getattr(self, attribute)

    @value.setter
    def value(self, value):
        datatype, value = self.get_type_value(value)
        if datatype not in self.ATTRIBUTE_MAP:
            raise InvalidTraitType(
                "Invalid trait type for %s: %s" % (self.name, datatype))
        attribute = self.ATTRIBUTE_MAP[datatype]
        self.type = int(datatype)
        if attribute is not None:
            setattr(self, attribute, value)

    @value.deleter
    def value(self):
        self._set_value(None)

    @value.comparator
    class value(PropComparator):
        """A comparator for .value, builds a polymorphic comparison.

        """
        def __init__(self, cls):
            self.cls = cls

        def __eq__(self, other):
            dtype, value = self.cls.get_type_value(other)
            if dtype is None:
                dtype = Datatype.string
            if dtype == Datatype.none:
                return self.cls.type == int(Datatype.none)
            attr = getattr(self.cls, self.cls.ATTRIBUTE_MAP[dtype])
            return and_(attr == value, self.cls.type == int(dtype))

        def __ne__(self, other):
            dtype, value = self.cls.get_type_value(other)
            if dtype is None:
                dtype = Datatype.string
            if dtype == Datatype.none:
                return self.cls.type != int(Datatype.none)
            attr = getattr(self.cls, self.cls.ATTRIBUTE_MAP[dtype])
            return and_(attr != value, self.cls.type == int(dtype))

    def __repr__(self):
        return '<%s %r=%r>' % (self.__class__.__name__, self.name, self.value)


Base = declarative_base()


class Trait(PolymorphicVerticalProperty, Base):
    __tablename__ = 'trait'
    __table_args__ = (
        Index('ix_trait_t_int', 't_int'),
        Index('ix_trait_t_string', 't_string'),
        Index('ix_trait_t_datetime', 't_datetime'),
        Index('ix_trait_t_float', 't_float'),
    )
    event_id = Column(Integer, ForeignKey('event.id'), primary_key=True)
    name = Column(String(100), primary_key=True)
    type = Column(Integer)

    ATTRIBUTE_MAP = {Datatype.none: None,
                     Datatype.string: 't_string',
                     Datatype.int: 't_int',
                     Datatype.float: 't_float',
                     Datatype.datetime: 't_datetime', }

    t_string = Column(String(255), nullable=True, default=None)
    t_float = Column(Float, nullable=True, default=None)
    t_int = Column(Integer, nullable=True, default=None)
    t_datetime = Column(PreciseTimestamp(),
                        nullable=True, default=None)

    def __repr__(self):
        return "<Trait(%s) %s=%s/%s/%s/%s on %s>" % (self.name,
                                                     self.type,
                                                     self.t_string,
                                                     self.t_float,
                                                     self.t_int,
                                                     self.t_datetime,
                                                     self.event_id)


class EventType(Base):
    """Types of event records."""
    __tablename__ = 'event_type'

    id = Column(Integer, primary_key=True)
    desc = Column(String(255), unique=True)

    def __init__(self, event_type):
        self.desc = event_type

    def __repr__(self):
        return "<EventType: %s>" % self.desc


class Event(ProxiedDictMixin, Base):
    __tablename__ = 'event'
    __table_args__ = (
        Index('ix_event_message_id', 'message_id'),
        Index('ix_event_type_id', 'event_type_id'),
        Index('ix_event_generated', 'generated')
    )
    id = Column(Integer, primary_key=True)
    message_id = Column(String(50), unique=True)
    generated = Column(PreciseTimestamp())

    event_type_id = Column(Integer, ForeignKey('event_type.id'))
    event_type = relationship("EventType", backref=backref('event_type'))

    traits = relationship("Trait",
                          collection_class=attribute_mapped_collection('name'))
    _proxied = association_proxy("traits", "value",
                                 creator=lambda name, value: Trait(
                                     name=name,
                                     value=value))

    @property
    def event_type_string(self):
        return self.event_type.desc

    @property
    def as_dict(self):
        d = dict(self._proxied)
        d['message_id'] = self.message_id
        d['event_type'] = self.event_type_string
        d['timestamp'] = self.generated
        return d

    def __init__(self, message_id, event_type, generated):
        self.message_id = message_id
        self.event_type = event_type
        self.generated = generated

    def __repr__(self):
        return "<Event %s ('Event : %s %s, Generated: %s')>" % (
            self.id,
            self.message_id,
            self.event_type,
            self.generated)


stream_event_table = Table(
    'streamevent', Base.metadata,
    Column('stream_id', Integer, ForeignKey('stream.id'), primary_key=True),
    Column('event_id', Integer, ForeignKey('event.id'), primary_key=True)
)


class Stream(ProxiedDictMixin, Base):
    __tablename__ = 'stream'

    __table_args__ = (
        Index('ix_stream_name', 'name'),
        Index('ix_stream_state', 'state'),
        Index('ix_stream_expire_timestamp', 'expire_timestamp'),
        Index('ix_stream_fire_timestamp', 'fire_timestamp')
    )
    id = Column(Integer, primary_key=True)
    first_event = Column(PreciseTimestamp(), nullable=False)
    last_event = Column(PreciseTimestamp(), nullable=False)
    expire_timestamp = Column(PreciseTimestamp())
    fire_timestamp = Column(PreciseTimestamp())
    name = Column(String(255), nullable=False)
    state = Column(Integer, default=StreamState.active, nullable=False)
    state_serial_no = Column(Integer, default=0, nullable=False)

    distinguished_by = relationship(
        "DistinguishingTrait",
        cascade="save-update, merge, delete, delete-orphan",
        collection_class=attribute_mapped_collection(
            'name'))
    _proxied = association_proxy(
        "distinguished_by", "value",
        creator=lambda name, value: DistinguishingTrait(
            name=name, value=value))

    events = relationship(Event, secondary=stream_event_table,
                          order_by=Event.generated)

    @property
    def distinguished_by_dict(self):
        return dict(self._proxied)

    @property
    def as_dict(self):
        return {'name': self.name,
                'id': self.id,
                'state': StreamState(self.state).name,
                'first_event': self.first_event,
                'last_event': self.last_event,
                'fire_timestamp': self.fire_timestamp,
                'expire_timestamp': self.expire_timestamp,
                'distinguishing_traits': self.distinguished_by_dict}

    def __init__(self, name, first_event, last_event=None,
                 expire_timestamp=None,
                 fire_timestamp=None, state=None, state_serial_no=None):
        self.name = name
        self.first_event = first_event
        if last_event is None:
            last_event = first_event
        self.last_event = last_event
        self.expire_timestamp = expire_timestamp
        self.fire_timestamp = fire_timestamp
        if state is None:
            state = StreamState.active
        self.state = int(state)
        if state_serial_no is None:
            state_serial_no = 0
        self.state_serial_no = state_serial_no


class DistinguishingTrait(PolymorphicVerticalProperty, Base):
    __tablename__ = 'dist_trait'
    __table_args__ = (
        Index('ix_dist_trait_dt_int', 'dt_int'),
        Index('ix_dist_trait_dt_float', 'dt_float'),
        Index('ix_dist_trait_dt_string', 'dt_string'),
        Index('ix_dist_trait_dt_datetime', 'dt_datetime'),
        Index('ix_dist_trait_dt_timerange_begin', 'dt_timerange_begin'),
        Index('ix_dist_trait_dt_timerange_end', 'dt_timerange_end'),
    )
    stream_id = Column(Integer, ForeignKey('stream.id'), primary_key=True)
    name = Column(String(100), primary_key=True)
    type = Column(Integer)

    ATTRIBUTE_MAP = {
        Datatype.none: None,
        Datatype.string: 'dt_string',
        Datatype.int: 'dt_int',
        Datatype.float: 'dt_float',
        Datatype.datetime: 'dt_datetime',
        Datatype.timerange: 'dt_timerange',
    }

    dt_string = Column(String(255), nullable=True, default=None)
    dt_float = Column(Float, nullable=True, default=None)
    dt_int = Column(Integer, nullable=True, default=None)
    dt_datetime = Column(PreciseTimestamp(),
                         nullable=True, default=None)
    dt_timerange_begin = Column(
        PreciseTimestamp(), nullable=True, default=None)
    dt_timerange_end = Column(PreciseTimestamp(), nullable=True, default=None)

    dt_timerange = composite(DBTimeRange, dt_timerange_begin, dt_timerange_end)

    @property
    def as_dict(self):
        return {self.name: self.value}

    def __repr__(self):
        return ("<DistinguishingTrait(%s) %s=%s/%s/%s/%s/(%s to %s) on %s>"
                % (self.name,
                   self.type,
                   self.dt_string,
                   self.dt_float,
                   self.dt_int,
                   self.dt_datetime,
                   self.dt_timerange_begin,
                   self.dt_timerange_end,
                   self.stream_id))
