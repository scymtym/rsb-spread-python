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

"""
Realizes an RSB transport based on the Spread toolkit.

Spread is a multicast-based daemon network.

.. codeauthor:: jmoringe
.. codeauthor:: jwienke
"""

import hashlib
import threading
import uuid

import spread

import rsb
from rsb.protocol.FragmentedNotification_pb2 import FragmentedNotification
import rsb.transport
import rsb.transport.conversion as conversion
import rsb.util


class Notification:
    """
    Superclass for incoming and outgoing notifications.

    .. codeauthor:: jmoringe
    """

    def __init__(self, scope, wire_schema, serialized_payload, notification):
        self.scope = scope
        self.wire_schema = wire_schema
        self.serialized_payload = serialized_payload
        self.notification = notification


class IncomingNotification(Notification):
    """
    Specialized class for representing incoming notifications.

    .. codeauthor:: jmoringe
    """

    pass


class OutgoingNotification(Notification):
    """
    Specialized class for representing outgoing notifications.

    .. codeauthor:: jmoringe
    """

    def __init__(self, scope, wire_schema, serialized_payload, notification,
                 service_type, groups, fragments):
        super().__init__(scope, wire_schema, serialized_payload, notification)
        self.service_type = service_type
        self.groups = groups
        self.fragments = fragments


def make_key(notification):
    key = notification.event_id.sender_id + b'%08x' \
        % notification.event_id.sequence_number
    return key


class Assembly:
    """
    Collects fragments of a single fragmented notification.

    Fragments are assembled once all fragments have been received.

    .. codeauthor:: jwienke
    """

    def __init__(self, fragment):
        self._required_parts = fragment.num_data_parts
        assert(self._required_parts > 1)
        self._id = make_key(fragment.notification)
        self._parts = {fragment.data_part: fragment}

    def add(self, fragment):
        key = make_key(fragment.notification)
        assert(key == self._id)
        if fragment.data_part in self._parts:
            raise ValueError(
                'Received part {part} for notification '
                '{not_id} repeatedly.'.format(
                    part=fragment.data_part,
                    not_id=key))

        self._parts[fragment.data_part] = fragment

        if len(self._parts) == self._required_parts:
            return (self._parts[0].notification, self._join(),
                    self._parts[0].notification.wire_schema)
        else:
            return None

    def _join(self):
        keys = list(self._parts.keys())
        keys.sort()
        final_data = bytes()
        for key in keys:
            final_data += bytes(self._parts[key].notification.data)
        return final_data


class AssemblyPool:
    """
    Collects notification fragments until they can be assembled.

    Maintains the parallel joining of notification fragments that are
    received in an interleaved fashion.

    .. codeauthor:: jwienke
    """

    def __init__(self):
        self._assemblies = {}

    def add(self, fragment):
        notification = fragment.notification
        if fragment.num_data_parts == 1:
            return (notification, bytes(notification.data),
                    notification.wire_schema)
        key = make_key(notification)
        if key not in self._assemblies:
            self._assemblies[key] = Assembly(fragment)
            return None
        else:
            result = self._assemblies[key].add(fragment)
            if result is not None:
                del self._assemblies[key]
                return result


class SpreadConnection:
    """
    A wrapper around a Spread mailbox for some convenience.

    .. codeauthor:: jwienke
    .. codeauthor:: jmoringe
    """

    def __init__(self, daemon_name, spread_module=spread):
        self._logger = rsb.util.get_logger_by_class(self.__class__)

        self._daemon_name = daemon_name
        self._spread_module = spread_module
        self._mailbox = None

    def activate(self):
        if self._mailbox is not None:
            raise ValueError("Already activated")
        self._logger.info("Connecting to Spread daemon at '%s'",
                          self._daemon_name)
        self._mailbox = self._spread_module.connect(
            bytes(self._daemon_name, 'ascii'))

    def deactivate(self):
        if self._mailbox is None:
            raise ValueError("Not activated")
        self._logger.info("Disconnecting from Spread daemon at '%s'",
                          self._daemon_name)
        self._mailbox.disconnect()
        self._mailbox = None

    def join(self, group):
        self._logger.info("Joining Spread group '%s'", group)
        self._mailbox.join(group)

    def leave(self, group):
        self._logger.info("Leaving Spread group '%s'", group)
        self._mailbox.leave(group)

    def receive(self):
        return self._mailbox.receive()

    def send(self, service_type, groups, payload):
        self._mailbox.multigroup_multicast(
            service_type | spread.SELF_DISCARD, groups, payload)

    def interrupt(self, group):
        self._logger.info("Interrupting receive calls using group '%s'",
                          group)
        self._mailbox.multicast(spread.RELIABLE_MESS, group, b'')

    @property
    def host(self):
        name = self._daemon_name.split('@')
        return name[1] if '@' in name else 'localhost'

    @property
    def port(self):
        return int(self._daemon_name.split('@')[0])


class DeserializingHandler:
    """
    Assembles notification fragments into complete Notifications.

    .. codeauthor:: jmoringe
    """

    def __init__(self):
        self._logger = rsb.util.get_logger_by_class(self.__class__)

        self._assembly_pool = AssemblyPool()

    def handle_message(self, message):
        """
        Maybe returns notification extracted from `message`.

        If `message` is one part of a fragmented notification for
        which some parts are still pending, a complete notification
        cannot be constructed and ``None`` is returned.

        Args:
            message: The received Spread message.

        Returns:
            notification: The assembled notification or ``None``.
        """

        # Only handle regular messages.
        if not hasattr(message, 'msg_type'):
            return None

        fragment = FragmentedNotification()
        fragment.ParseFromString(message.message)

        self._logger.debug(
            "Received notification fragment "
            "from bus (%s/%s), data length: %s",
            fragment.data_part,
            fragment.num_data_parts,
            len(fragment.notification.data))

        result = self._assembly_pool.add(fragment)
        if result is not None:
            notification, wire_data, wire_schema = result
            return IncomingNotification(
                rsb.Scope(notification.scope.decode('ascii')),
                wire_schema.decode('ascii'), wire_data, notification)


class SpreadReceiverTask:
    """
    Thread used to receive messages from a spread connection.

    .. codeauthor:: jwienke
    .. codeauthor:: jmoringe
    """

    def __init__(self, connection, observer_action):
        """
        Constructor.

        Args:
            connection:
                Spread connection to receive messages from.
            observer_action:
                Callable to invoke when a new event is received.
        """

        self._logger = rsb.util.get_logger_by_class(self.__class__)

        self._connection = connection
        # Spread groups are 32 chars long and 0-terminated.
        self._wakeup_group = str(uuid.uuid1()).replace(
            '-', '')[:-1].encode('ascii')

        self._deserializing_handler = DeserializingHandler()

        self._observer_action = observer_action

    def __call__(self):

        # Join "wakeup group" to receive interrupt messages.
        # receive() does have a timeout, hence we need a way to stop
        # receiving messages on interruption even if no one else sends
        # messages.  Otherwise deactivate would block until another
        # message is received.
        self._logger.debug("Joining wakeup group %s", self._wakeup_group)
        self._connection.join(self._wakeup_group)

        while True:

            self._logger.debug("Waiting for messages")
            message = self._connection.receive()
            self._logger.debug("Received message %s", message)

            # Break out of receive loop if deactivating.
            if hasattr(message, 'msg_type') \
               and self._wakeup_group in message.groups:
                break

            try:
                notification \
                    = self._deserializing_handler.handle_message(message)
                if notification is not None:
                    self._observer_action(notification)
            except Exception as e:
                self._logger.exception("Error processing new event")
                raise e

        self._connection.leave(self._wakeup_group)

    def interrupt(self):
        # send the interruption message to make the
        # __connection.receive() call in __call__ return.
        self._connection.interrupt(self._wakeup_group)


class GroupNameCache:
    """
    Caches the conversion from :ref:`scopes <rsb.Scope>` to spread groups.

    .. codeauthor:: jmoringe
    """

    def scope_to_groups(self, scope):
        scopes = scope.super_scopes(True)
        return tuple(self.group_name(scope) for scope in scopes)

    @staticmethod
    def group_name(scope):
        hash_sum = hashlib.md5()
        hash_sum.update(scope.to_string().encode('ascii'))
        return hash_sum.hexdigest()[:-1].encode('ascii')


class Memberships:
    """
    Reference counting-based management of Spread group membership.

    Not thread-safe.

    .. codeauthor:: jmoringe
    """

    def __init__(self, connection):
        self._logger = rsb.util.get_logger_by_class(self.__class__)

        self._connection = connection
        self._groups = {}

    def join(self, group):
        if group not in self._groups:
            self._logger.debug("Incrementing group '%s', 0 -> 1", group)
            self._groups[group] = 1
            self._connection.join(group)
        else:
            count = self._groups[group]
            self._logger.debug("Incrementing group '%s', %d -> %d",
                               group, count, count + 1)
            self._groups[group] = count + 1

    def leave(self, group):
        count = self._groups[group]
        self._logger.debug("Decrementing group '%s', %d -> %d",
                           group, count, count - 1)
        if count == 1:
            del self._groups[group]
            self._connection.leave(group)
        else:
            self._groups[group] = count - 1


class Bus:
    """
    Manages a Spread connection and connectors, distributing notifications.

    Notifications received via the Spread connection are dispatched to
    :class:`InConnectors <InConnector>` with matching scopes.

    Notifications sent through :class:`OutConnectors <OutConnector>`
    are sent via the Spread connection and dispatched to in-direction
    connectors with matching scopes.

    ..  codeauthor:: jmoringe
    """

    def __init__(self, connection):
        self._logger = rsb.util.get_logger_by_class(self.__class__)

        self._active = False
        self._refs = 1

        self._connection = connection
        self._memberships = Memberships(connection)

        self._receiver_task = None
        self._receiver_thread = None

        self._dispatcher = rsb.eventprocessing.ScopeDispatcher()

        self._lock = threading.Lock()

    @property
    def active(self):
        return self._active

    def activate(self):
        self._logger.info("Activating")

        self._connection.activate()

        self._receive_task = SpreadReceiverTask(
            self._connection, self._handle_incoming_notification)
        self._receive_thread = threading.Thread(target=self._receive_task)
        self._receive_thread.start()

        self._active = True

    def deactivate(self):
        self._logger.info("Deactivating")

        self._active = False

        self._receive_task.interrupt()
        self._receive_thread.join(timeout=1)
        self._receive_thread = None
        self._receive_task = None

        self._connection.deactivate()

    def ref(self):
        """
        Increase the reference count by 1.

        Keeps the object alive at least until the corresponding
        :meth:`unref` call.
        """
        with self._lock:
            self._logger.info("Incrementing reference count %d -> %d",
                              self._refs, self._refs + 1)

            if self._refs == 0:
                return False
            self._refs += 1
            return True

    def unref(self):
        """
        Decrease the reference count by 1 after a previous :meth:`ref` call.

        If this causes the reference count to reach 0, the object deactivates
        itself.
        """
        with self._lock:
            self._logger.info("Decrementing reference count %d -> %d",
                              self._refs, self._refs - 1)

            self._refs -= 1
            if self._refs == 0:
                self.deactivate()

    def add_sink(self, scope, sink):
        """
        Register `sink` for events matching `scope`.

        Incoming and outgoing events matching `scope` will be
        dispatched to `sink`.
        """
        with self._lock:
            self._dispatcher.add_sink(scope, sink)

            self._memberships.join(GroupNameCache.group_name(scope))

    def remove_sink(self, scope, sink):
        """
        Unregister `sink` for events matching `scope`.

        Incoming and outgoing events matching `scope` will no longer
        be dispatched to `sink`.
        """
        with self._lock:
            self._memberships.leave(GroupNameCache.group_name(scope))

            self._dispatcher.add_sink(scope, sink)

    def _handle_incoming_notification(self, notification):
        with self._lock:
            scope = notification.scope
            for sink in self._dispatcher.matching_sinks(scope):
                sink.handle_notification(notification)

    def handle_outgoing_notification(self, notification):
        """
        Send `notification` via the bus object.

        Transmits `notification` through the Spread connection managed
        by the bus object and dispatches `notification` to connectors
        attaches to the bus object.
        """
        with self._lock:
            for fragment in notification.fragments:
                self._connection.send(
                    notification.service_type,
                    notification.groups,
                    fragment.SerializeToString())
            for sink in self._dispatcher.matching_sinks(notification.scope):
                sink.handle_notification(notification)

    def get_transport_url(self):
        return 'spread://' \
            + self._connection.host + ':' + str(self._connection.port)


class Connector(rsb.transport.Connector,
                rsb.transport.ConverterSelectingConnector):
    """
    Superclass for Spread-based connector classes.

    This class manages the direction-independent aspects like the Spread
    connection and (de)activation.

    .. codeauthor:: jwienke
    """

    MAX_MSG_LENGTH = 100000

    def __init__(self, bus, **kwargs):
        super().__init__(wire_type=bytes, **kwargs)

        self._logger = rsb.util.get_logger_by_class(self.__class__)
        self._bus = bus

        self._active = False

    def __del__(self):
        if self._active:
            self.deactivate()

    def set_quality_of_service_spec(self, qos):
        pass

    @property
    def bus(self):
        return self._bus

    @property
    def active(self):
        return self._active

    def activate(self):
        if self._active:
            raise RuntimeError('Trying to activate active Connector')

        self._logger.info('Activating')

        self._active = True

    def deactivate(self):
        if not self._active:
            raise RuntimeError('Trying to deactivate inactive Connector')

        self._logger.info('Deactivating')

        self.bus.unref()

        self._active = False

        self._logger.debug('Deactivated')

    def get_transport_url(self):
        return self._bus.get_transport_url()


class InConnector(Connector):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._logger = rsb.util.get_logger_by_class(self.__class__)

        self._observer_action = None

    def activate(self):
        super().activate()

        assert self.scope is not None
        self.bus.add_sink(self.scope, self)

    def deactivate(self):
        self.bus.remove_sink(self.scope, self)

        super().deactivate()

    def notification_to_event(self, notification):
        # Create event from (potentially assembled) notification(s)
        converter = self._converter_map.get_converter_for_wire_schema(
            notification.wire_schema)
        try:
            return conversion.notification_to_event(
                notification.notification,
                notification.serialized_payload,
                notification.wire_schema,
                converter)
        except Exception:
            self._logger.exception("Unable to decode event. "
                                   "Ignoring and continuing.")
            return None

    def set_observer_action(self, observer_action):
        self._observer_action = observer_action

    def handle_notification(self, notification):
        event = self.notification_to_event(notification)
        if event is not None and self._observer_action:
            self._observer_action(event)

    def filter_notify(self, the_filter, action):
        pass


class OutConnector(Connector, rsb.transport.OutConnector):

    def __init__(self, **kwargs):
        self._logger = rsb.util.get_logger_by_class(self.__class__)

        super().__init__(**kwargs)

        self._group_name_cache = GroupNameCache()

        self._service_type = spread.FIFO_MESS

    def set_quality_of_service_spec(self, qos):
        self._service_type = self.compute_service_type(qos)

    def handle(self, event):
        self._logger.debug("Sending event: %s", event)

        if not self.active:
            self._logger.warning("Connector not activated")
            return

        # Create one or more notification fragments for the event
        event.meta_data.set_send_time()

        #
        groups = self._group_name_cache.scope_to_groups(event.scope)

        #
        converter = self.get_converter_for_data_type(event.data_type)
        wire_data, wire_schema = converter.serialize(event.data)
        fragments = conversion.event_and_wire_data_to_notifications(
            event, wire_data, wire_schema, self.MAX_MSG_LENGTH)

        notification = OutgoingNotification(
            event.scope,
            wire_schema,
            wire_data,
            fragments[0].notification,
            self._service_type,
            groups,
            fragments)

        self.bus.handle_outgoing_notification(notification)

    def compute_service_type(self, qos):
        self._logger.debug("Adapting service type for QoS %s", qos)

        if qos.reliability == rsb.QualityOfServiceSpec.Reliability.UNRELIABLE:
            if qos.ordering == rsb.QualityOfServiceSpec.Ordering.UNORDERED:
                service_type = spread.UNRELIABLE_MESS
            elif qos.ordering == rsb.QualityOfServiceSpec.Ordering.ORDERED:
                service_type = spread.FIFO_MESS
        elif qos.reliability == rsb.QualityOfServiceSpec.Reliability.RELIABLE:
            if qos.ordering == rsb.QualityOfServiceSpec.Ordering.UNORDERED:
                service_type = spread.RELIABLE_MESS
            elif qos.ordering == rsb.QualityOfServiceSpec.Ordering.ORDERED:
                service_type = spread.FIFO_MESS

        self._logger.debug("Service type for %s is %s", qos, service_type)
        return service_type


class TransportFactory(rsb.transport.TransportFactory):
    """
    :obj:`TransportFactory` implementation for the spread transport.

    .. codeauthor:: jwienke
    .. codeauthor:: jmoringe
    """

    def __init__(self):
        self._logger = rsb.util.get_logger_by_class(self.__class__)

        self._buses = {}
        self._lock = threading.Lock()

    @property
    def name(self):
        return "spread"

    @property
    def remote(self):
        return True

    @staticmethod
    def _create_daemon_name(options):

        host = options.get('host', None)
        port = options.get('port', '4803')
        if host:
            return '{port}@{host}'.format(port=port, host=host)
        else:
            return port

    def obtain_bus(self, options):
        daemon_name = self._create_daemon_name(options)
        self._logger.debug("Obtaining bus for daemon name '%s'",
                           daemon_name)
        with self._lock:
            # Try to find an existing bus for the given Spread daemon
            # name.
            bus = self._buses.get(daemon_name)
            if bus is not None:
                # If there is a bus, try to atomically test and
                # increment the reference count. If this fails, the
                # bus became unreferenced and deactivate in the
                # meantime and cannot be used.
                if not bus.ref():
                    bus = None
                self._logger.debug("Found bus %s", bus)
            # If there was not existing bus or we lost the race to
            # reference it, create and store a new one. The bus is
            # created with a reference count of 1 so we don't have to
            # ref() it here.
            if bus is None:
                self._logger.info("Creating new bus for daemon name '%s'",
                                  daemon_name)
                bus = Bus(SpreadConnection(daemon_name))
                bus.activate()
                self._buses[daemon_name] = bus
            return bus

    def create_in_connector(self, converters, options):
        return InConnector(bus=self.obtain_bus(options),
                           converters=converters)

    def create_out_connector(self, converters, options):
        return OutConnector(bus=self.obtain_bus(options),
                            converters=converters)


def rsb_initialize():
    try:
        rsb.transport.register_transport(TransportFactory())
    except ValueError:
        pass
