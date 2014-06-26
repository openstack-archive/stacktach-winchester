from winchester.db import DBInterface


class TriggerManager(object):

    def __init__(self, config):
        self.config = config
        self.db = DBInterface(config['database'])
