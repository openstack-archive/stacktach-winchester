import datetime
import dateutil.parser
import logging
import time

import requests

logger = logging.getLogger(__name__)


class TimeSync(object):
    def __init__(self, config={}, publishes=False):
        url = config.get('time_sync_endpoint')
        self.endpoint = None
        if url:
            self.endpoint = "%s/time" % url
        logger.debug("Time sync endpoint=%s" % self.endpoint)
        self.last_update = None
        self.last_tyme = self._get_now()
        self.publishes = publishes

    def _get_now(self):
        # Broken out for testing
        return datetime.datetime.utcnow()

    def _should_update(self, now):
        return (not self.last_update or (now - self.last_update).seconds > 20)

    def _fetch(self):
        while True:
            tyme = requests.get(self.endpoint).text
            if tyme and tyme != "None":
                return tyme
            logger.debug("No time published yet. Waiting ...")
            time.sleep(1)

    def current_time(self):
        now = self._get_now()
        if not self.endpoint:
            return now

        if not self.publishes and self._should_update(now):
            try:
                tyme = self._fetch()
                logger.debug("Requested time, got '%s'" % tyme)
                self.last_tyme = dateutil.parser.parse(tyme)
            except Exception as e:
                logger.exception("Could not get time: %s" % e)
            self.last_update = now

        return self.last_tyme

    def publish(self, tyme):
        if not self.endpoint:
            return

        daittyme = dateutil.parser.parse(tyme)
        self.last_tyme = daittyme
        if self._should_update(daittyme):
            self.last_update = daittyme
            try:
                requests.post(self.endpoint, data=tyme)
                logger.debug("Published time: %s" % tyme)
            except Exception as e:
                logger.exception("Could not publish time: %s" % e)
