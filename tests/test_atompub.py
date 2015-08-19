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

import datetime
import unittest2 as unittest

import mock

from winchester import pipeline_handler


class TestException(Exception):
    pass


class TestAtomPubHandler(unittest.TestCase):

    def test_constructor_event_types(self):
        fakeurl = 'fake://'
        h = pipeline_handler.AtomPubHandler(fakeurl)
        self.assertEqual(h.included_types, ['*'])
        self.assertEqual(h.excluded_types, [])

        h = pipeline_handler.AtomPubHandler(fakeurl,
                                            event_types='test.thing')
        self.assertEqual(h.included_types, ['test.thing'])
        self.assertEqual(h.excluded_types, [])

        h = pipeline_handler.AtomPubHandler(fakeurl,
                                            event_types=['test.thing'])
        self.assertEqual(h.included_types, ['test.thing'])
        self.assertEqual(h.excluded_types, [])

        h = pipeline_handler.AtomPubHandler(fakeurl,
                                            event_types=['!test.thing'])
        self.assertEqual(h.included_types, ['*'])
        self.assertEqual(h.excluded_types, ['test.thing'])

    def test_match_type(self):
        event_types = ["test.foo.bar", "!test.wakka.wakka"]
        h = pipeline_handler.AtomPubHandler('fakeurl',
                                            event_types=event_types)
        self.assertTrue(h.match_type('test.foo.bar'))
        self.assertFalse(h.match_type('test.wakka.wakka'))
        self.assertFalse(h.match_type('test.foo.baz'))

        event_types = ["test.foo.*", "!test.wakka.*"]
        h = pipeline_handler.AtomPubHandler('fakeurl',
                                            event_types=event_types)
        self.assertTrue(h.match_type('test.foo.bar'))
        self.assertTrue(h.match_type('test.foo.baz'))
        self.assertFalse(h.match_type('test.wakka.wakka'))

    def test_handle_events(self):
        event_types = ["test.foo.*", "!test.wakka.*"]
        h = pipeline_handler.AtomPubHandler('fakeurl',
                                            event_types=event_types)
        event1 = dict(event_type="test.foo.zazz")
        event2 = dict(event_type="test.wakka.zazz")
        event3 = dict(event_type="test.boingy")
        events = [event1, event2, event3]
        res = h.handle_events(events, dict())
        self.assertEqual(events, res)
        self.assertIn(event1, h.events)
        self.assertNotIn(event2, h.events)
        self.assertNotIn(event3, h.events)

    def test_format_cuf_xml(self):
        expected = ('<event xmlns="http://docs.rackspace.com/core/event" '
                    'xmlns:nova="http://docs.rackspace.com/event/nova" '
                    'version="1" id="1234-56789" resourceId="98765-4321" '
                    'resourceName="" dataCenter="TST1" region="TST" '
                    'tenantId="" startTime="2015-08-10T00:00:00Z" '
                    'endTime="2015-08-11T00:00:00Z" type="USAGE">'
                    '<nova:product version="1" '
                    'serviceCode="CloudServersOpenStack" '
                    'resourceType="SERVER" flavorId="" flavorName="" '
                    'status="ACTIVE" osLicenseType="WINDOWS" '
                    'bandwidthIn="" bandwidthOut=""/></event>')
        d1 = datetime.datetime(2015, 8, 10, 0, 0, 0)
        d2 = datetime.datetime(2015, 8, 11, 0, 0, 0)
        d3 = datetime.datetime(2015, 8, 9, 15, 21, 0)
        event = dict(message_id='1234-56789',
                     event_type='test.thing',
                     audit_period_beginning=d1,
                     audit_period_ending=d2,
                     launched_at=d3,
                     instance_id='98765-4321',
                     state='active',
                     state_description='',
                     rax_options='4')
        extra = dict(data_center='TST1', region='TST')
        h = pipeline_handler.AtomPubHandler('fakeurl',
                                            extra_info=extra)
        res, content_type = h.format_cuf_xml(event)
        self.assertEqual(res, expected)
        self.assertEqual(content_type, 'application/xml')

    def test_generate_atom(self):
        expected = ("""<atom:entry xmlns:atom="http://www.w3.org/2005/Atom">"""
                    """<atom:id>urn:uuid:12-34</atom:id>"""
                    """<atom:category term="test.thing.bar" />"""
                    """<atom:category """
                    """term="original_message_id:56-78" />"""
                    """<atom:title type="text">Server</atom:title>"""
                    """<atom:content type="test/thing">TEST_CONTENT"""
                    """</atom:content></atom:entry>""")
        event = dict(message_id='12-34',
                     original_message_id='56-78',
                     event_type='test.thing')
        event_type = 'test.thing.bar'
        ctype = 'test/thing'
        content = 'TEST_CONTENT'
        h = pipeline_handler.AtomPubHandler('fakeurl')
        atom = h.generate_atom(event, event_type, content, ctype)
        self.assertEqual(atom, expected)

    @mock.patch.object(pipeline_handler.requests, 'post')
    @mock.patch.object(pipeline_handler.AtomPubHandler, '_get_auth')
    def test_send_event(self, auth, rpost):
        test_headers = {'Content-Type': 'application/atom+xml',
                        'X-Auth-Token': 'testtoken'}
        auth.return_value = test_headers
        test_response = mock.MagicMock('http response')
        test_response.status_code = 200
        rpost.return_value = test_response
        h = pipeline_handler.AtomPubHandler('fakeurl', http_timeout=123,
                                            wait_interval=10, max_wait=100)
        test_atom = mock.MagicMock('atom content')

        status = h._send_event(test_atom)

        self.assertEqual(1, auth.call_count)
        self.assertEqual(1, rpost.call_count)
        rpost.assert_called_with('fakeurl',
                                 data=test_atom,
                                 headers=test_headers,
                                 timeout=123)
        self.assertEqual(status, 200)
