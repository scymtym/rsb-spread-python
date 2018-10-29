# ============================================================
#
# Copyright (C) 2010-2018 by Johannes Wienke
# Copyright (C) 2011-2018 Jan Moringen
#
# This file may be licensed under the terms of the
# GNU Lesser General Public License Version 3 (the ``LGPL''),
# or (at your option) any later version.
#
# Software distributed under the License is distributed
# on an ``AS IS'' basis, WITHOUT WARRANTY OF ANY KIND, either
# express or implied. See the LGPL for the specific language
# governing rights and limitations.
#
# You should have received a copy of the LGPL along with this
# program. If not, go to http://www.gnu.org/licenses/lgpl.html
# or write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
#
# The development of this software was supported by:
#   CoR-Lab, Research Institute for Cognition and Robotics
#     Bielefeld University
#
# ============================================================

from distutils.spawn import find_executable
import hashlib
import random
import string
import subprocess
import threading
import time
import uuid

import pytest

from rsb import Event, EventId, Scope
import rsb.converter
from rsb.transport import spread as spread_transport
from rsb.transport.transporttest import SettingReceiver, TransportCheck


@pytest.fixture(scope='session', autouse=True)
def spread_daemon():
    spread = find_executable('spread')
    assert spread, 'Spread executable not found. Is it on PATH?'

    spread_process = subprocess.Popen([spread,
                                       '-n', 'localhost',
                                       '-c', 'test/spread.conf'])

    time.sleep(5)
    assert spread_process.poll() is None, 'Spread could not be started'

    yield spread_daemon

    spread_process.terminate()
    try:
        spread_process.wait(10)
    except subprocess.TimeoutExpired:
        spread_process.kill()
        spread_process.wait()


@pytest.fixture(autouse=True)
def spread_config(tmpdir):
    conf_file = tmpdir.join('with-spread.conf')
    conf_file.write('''[introspection]
enabled = 0

[transport.inprocess]
enabled = 0

[transport.socket]
enabled = 0

[transport.spread]
enabled = 1
port    = 4569

[plugin.python]
load = rsb.transport.spread''')
    rsb.get_default_participant_config()
    rsb.set_default_participant_config(
        rsb.ParticipantConfig.from_file(str(conf_file)))

    rsb.transport.spread.rsb_initialize()


@pytest.fixture
def spread_config_with_introspection(tmpdir):
    conf_file = tmpdir.join('with-spread.conf')
    conf_file.write('''[introspection]
enabled = 1

[transport.inprocess]
enabled = 0

[transport.socket]
enabled = 0

[transport.spread]
enabled = 1
port    = 4569

[plugin.python]
load = rsb.transport.spread''')
    rsb.get_default_participant_config()
    rsb.set_default_participant_config(
        rsb.ParticipantConfig.from_file(str(conf_file)))

    rsb.transport.spread.rsb_initialize()


def get_connector(scope,
                  clazz=spread_transport.Connector,
                  module=None,
                  activate=True):
    kwargs = {}
    if module:
        kwargs['spread_module'] = module
    options = rsb.get_default_participant_config().get_transport(
        'spread').options
    daemon = '{port}@{host}'.format(port=options['port'],
                                    host=options.get('host', 'localhost'))
    connection = spread_transport.SpreadConnection(daemon, **kwargs)
    bus = spread_transport.Bus(connection)
    bus.activate()
    connector = clazz(
        bus=bus,
        converters=rsb.converter.get_global_converter_map(bytes))
    connector.scope = scope
    if activate:
        connector.activate()
    return connector


class TestSpreadConnector:

    class DummyMessage(object):
        def __init__(self):
            self.msg_type = 42

    class DummyConnection(object):

        def __init__(self):
            self.clear()
            self.__cond = threading.Condition()
            self.__last_message = None

        def clear(self):
            self.join_calls = []
            self.leaveCalls = []
            self.disconnect_calls = 0

        def join(self, group):
            self.join_calls.append(group)

        def leave(self, group):
            self.leaveCalls.append(group)

        def disconnect(self):
            self.disconnect_calls = self.disconnect_calls + 1

        def receive(self):
            self.__cond.acquire()
            while self.__last_message is None:
                self.__cond.wait()
            msg = self.__last_message
            self.__last_message = None
            self.__cond.release()
            return msg

        def multicast(self, type_, group, message):
            self.__cond.acquire()
            self.__last_message = TestSpreadConnector.DummyMessage()
            self.__last_message.groups = [group]
            self.__cond.notify()
            self.__cond.release()

    class DummySpread(object):

        def __init__(self):
            self.returned_connections = []

        def connect(self, daemon=None):
            c = TestSpreadConnector.DummyConnection()
            self.returned_connections.append(c)
            return c

    def test_activate(self):
        dummy_spread = self.DummySpread()
        connector = get_connector(Scope("/foo"), module=dummy_spread)
        assert len(dummy_spread.returned_connections) == 1
        connector.deactivate()

    def test_deactivate(self):
        dummy_spread = self.DummySpread()
        connector = get_connector(Scope("/foo"), module=dummy_spread)
        assert len(dummy_spread.returned_connections) == 1
        connection = dummy_spread.returned_connections[0]

        connector.deactivate()
        assert connection.disconnect_calls == 1

    def test_spread_subscription(self):
        s1 = Scope("/xxx")
        dummy_spread = self.DummySpread()
        connector = get_connector(s1,
                                  clazz=spread_transport.InConnector,
                                  module=dummy_spread)
        assert len(dummy_spread.returned_connections) == 1
        connection = dummy_spread.returned_connections[0]

        hasher = hashlib.md5()
        hasher.update(s1.to_string().encode('ascii'))
        hashed = hasher.hexdigest()[:-1].encode('ascii')
        assert hashed in connection.join_calls

        connector.deactivate()

    def test_sequencing(self):
        good_scope = Scope("/good")
        in_connector = get_connector(good_scope,
                                     clazz=spread_transport.InConnector)
        out_connector = get_connector(good_scope,
                                      clazz=spread_transport.OutConnector)

        try:
            receiver = SettingReceiver(good_scope)
            in_connector.set_observer_action(receiver)

            # first an event that we do not want
            event = Event(EventId(uuid.uuid4(), 0))
            event.scope = Scope("/notGood")
            event.data = ''.join(
                random.choice(
                    string.ascii_uppercase +
                    string.ascii_lowercase +
                    string.digits)
                for i in range(300502))
            event.data_type = str
            event.meta_data.sender_id = uuid.uuid4()
            out_connector.handle(event)

            # and then a desired event
            event.scope = good_scope
            out_connector.handle(event)

            with receiver.result_condition:
                receiver.result_condition.wait(10)
                assert receiver.result_event is not None
                # self.assertEqual(receiver.result_event, event)
        finally:
            in_connector.deactivate()
            out_connector.deactivate()


class TestSpreadTransport(TransportCheck):

    def _get_in_connector(self, scope, activate=True):
        return get_connector(scope, clazz=spread_transport.InConnector,
                             activate=activate)

    def _get_out_connector(self, scope, activate=True):
        return get_connector(scope, clazz=spread_transport.OutConnector,
                             activate=activate)


def test_user_level_roundtrip(spread_config_with_introspection):
    """Test high-level communication with full integration."""

    scope = '/a/test/scope'

    with rsb.create_reader(scope) as reader:
        with rsb.create_informer(scope) as informer:
            data = 'test_data'
            informer.publish_data(data)
            assert reader.read().data == data
