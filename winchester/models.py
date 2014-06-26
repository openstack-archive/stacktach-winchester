from datetime import datetime

from enum import IntEnum

from sqlalchemy import event
from sqlalchemy import literal_column
from sqlalchemy import Column, Table, ForeignKey, Index, UniqueConstraint
from sqlalchemy import Float, Boolean, Text, DateTime, Integer, String
from sqlalchemy import cast, null, case
from sqlalchemy.orm.interfaces import PropComparator
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.dialects.mysql import DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.types import TypeDecorator, DATETIME


class Datatype(IntEnum):
    none = 0
    string = 1
    int = 2
    float = 3
    datetime = 4


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
            return utils.dt_to_decimal(value)
        return value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'mysql':
            return utils.decimal_to_dt(value)
        return value


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

    def __init__(self, name, value=None):
        self.name = name
        self.value = value

    @hybrid_property
    def value(self):
        fieldname, discriminator = self.type_map[self.type]
        if fieldname is None:
            return None
        else:
            return getattr(self, fieldname)

    @value.setter
    def value(self, value):
        py_type = type(value)
        fieldname, discriminator = self.type_map[py_type]

        self.type = discriminator
        if fieldname is not None:
            setattr(self, fieldname, value)

    @value.deleter
    def value(self):
        self._set_value(None)

    @value.comparator
    class value(PropComparator):
        """A comparator for .value, builds a polymorphic comparison via CASE.

        """
        def __init__(self, cls):
            self.cls = cls

        def _case(self):
            pairs = set(self.cls.type_map.values())
            whens = [
                (
                    literal_column("'%s'" % discriminator),
                    cast(getattr(self.cls, attribute), String)
                ) for attribute, discriminator in pairs
                if attribute is not None
            ]
            return case(whens, self.cls.type, null())
        def __eq__(self, other):
            return self._case() == cast(other, String)
        def __ne__(self, other):
            return self._case() != cast(other, String)

    def __repr__(self):
        return '<%s %r=%r>' % (self.__class__.__name__, self.name, self.value)


@event.listens_for(PolymorphicVerticalProperty, "mapper_configured", propagate=True)
def on_new_class(mapper, cls_):
    """Add type lookup info for polymorphic value columns.
    """

    info_dict = {}
    info_dict[type(None)] = (None, Datatype.none)
    info_dict[Datatype.none] = (None, Datatype.none)

    for k in mapper.c.keys():
        col = mapper.c[k]
        if 'type' in col.info:
            python_type, discriminator = col.info['type']
            info_dict[python_type] = (k, discriminator)
            info_dict[discriminator] = (k, discriminator)
    cls_.type_map = info_dict


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


    t_string = Column(String(255), info=dict(type=(str, Datatype.string)),
                      nullable=True, default=None)
    t_float = Column(Float, info=dict(type=(float, Datatype.float)),
                      nullable=True, default=None)
    t_int = Column(Integer, info=dict(type=(int, Datatype.int)),
                      nullable=True, default=None)
    t_datetime = Column(PreciseTimestamp(), info=dict(type=(datetime, Datatype.datetime)),
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
                            creator=lambda name, value: Trait(name=name, value=value))

    def __init__(self, message_id, event_type, generated):

        self.message_id = message_id
        self.event_type = event_type
        self.generated = generated

    def __repr__(self):
        return "<Event %s ('Event: %s %s, Generated: %s')>" % (self.id,
                                                              self.message_id,
                                                              self.event_type,
                                                              self.generated)


