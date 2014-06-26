import sqlalchemy
from sqlalchemy.orm import sessionmaker

from winchester import models


ENGINES = dict()
SESSIONMAKERS = dict()


def sessioned(func):
    def with_session(self, *args, **kw):
        if 'session' in kw:
            return func(self, *args, **kw)
        else:
            try:
                session = self.get_session()
                kw['session'] = session
                retval = func(self, *args, **kw)
                session.commit()
                return retval
            except:
                session.rollback()
                raise
            finally:
                session.close()
    return with_session


class DBInterface(object):

    def __init__(self, config):
        self.config = config
        self.db_url = config['url']

    @property
    def engine(self):
        global ENGINES
        if self.db_url not in ENGINES:
            engine = sqlalchemy.create_engine(self.db_url)
            ENGINES[self.db_url] = engine
        return ENGINES[self.db_url]

    @property
    def sessionmaker(self):
        global SESSIONMAKERS
        if self.db_url not in SESSIONMAKERS:
            maker = sessionmaker(bind=self.engine)
            SESSIONMAKERS[self.db_url] = maker
        return SESSIONMAKERS[self.db_url]

    def get_session(self):
        return self.sessionmaker(expire_on_commit=False)

    @sessioned
    def get_event_type(self, description, session=None):
        t = session.query(models.EventType).filter(models.EventType.desc == description).first()
        if t is None:
            t = models.EventType(description)
            session.add(t)
        return t

    @sessioned
    def create_event(self, message_id, event_type, generated, traits, session=None):
        event_type = self.get_event_type(event_type, session=session)
        e = models.Event(message_id, event_type, generated)
        for name in traits:
            e[name] = traits[name]
        session.add(e)
        return e
