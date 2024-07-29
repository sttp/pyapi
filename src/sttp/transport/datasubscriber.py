# ******************************************************************************************************
#  datasubscriber.py - Gbtc
#
#  Copyright © 2022, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
#  the NOTICE file distributed with this work for additional information regarding copyright ownership.
#  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
#  file except in compliance with the License. You may obtain a copy of the License at:
#
#      http://opensource.org/licenses/MIT
#
#  Unless agreed to in writing, the subject software distributed under the License is distributed on an
#  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
#  License for the specific language governing permissions and limitations.
#
#  Code Modification History:
#  ----------------------------------------------------------------------------------------------------
#  08/17/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from gsf import Empty, Limits, normalize_enumname
from gsf.endianorder import BigEndian
from gsf.binarystream import BinaryStream
from gsf.streamencoder import StreamEncoder
from .measurement import Measurement
from .compactmeasurement import CompactMeasurement
from ..metadata.record.measurement import MeasurementRecord
from ..metadata.cache import MetadataCache
from .bufferblock import BufferBlock
from .constants import OperationalModes, OperationalEncoding, CompressionModes, Defaults
from .constants import DataPacketFlags, ServerCommand, ServerResponse, StateFlags
from .subscriptioninfo import SubscriptionInfo
from .subscriberconnector import SubscriberConnector
from .signalindexcache import SignalIndexCache
from .tssc.decoder import Decoder
from ..ticks import Ticks
from ..version import Version
from typing import List, Callable, Optional, Tuple
from time import time
from uuid import UUID
from threading import Lock, Thread
from concurrent.futures import ThreadPoolExecutor
from Crypto.Cipher import AES
import gzip
import socket
import numpy as np

MAXPACKET_SIZE = 32768
PAYLOADHEADER_SIZE = 4
RESPONSEHEADER_SIZE = 6
EVEN_KEY = 0
ODD_KEY = 1
KEY_INDEX = 0
IV_INDEX = 1
MISSINGCACHEWARNING_INTERVAL = 20.0


class DataSubscriber:
    """
    Represents a subscription for an STTP connection.
    """

    DEFAULT_COMPRESS_PAYLOADDATA = Defaults.COMPRESS_PAYLOADDATA            # Defaults to TSSC
    DEFAULT_COMPRESS_METADATA = Defaults.COMPRESS_METADATA                  # Defaults to Gzip
    DEFAULT_COMPRESS_SIGNALINDEXCACHE = Defaults.COMPRESS_SIGNALINDEXCACHE  # Defaults to Gzip
    DEFAULT_VERSION = Defaults.VERSION
    DEFAULT_STTP_SOURCEINFO = Version.STTP_SOURCE
    DEFAULT_STTP_VERSIONINFO = Version.STTP_VERSION
    DEFAULT_STTP_UPDATEDONINFO = Version.STTP_UPDATEDON
    DEFAULT_SOCKET_TIMEOUT = Defaults.SOCKET_TIMEOUT

    def __init__(self,
                 compress_payloaddata: bool = ...,
                 compress_metadata: bool = ...,
                 compress_signalindexcache: bool = ...,
                 version: np.byte = ...,
                 sttp_sourceinfo: str = ...,
                 sttp_versioninfo: str = ...,
                 sttp_updatedoninfo: str = ...,
                 socket_timeout: float = ...
                 ):
        """
        Creates a new `DataSubscriber`.
        """

        self._subscription = SubscriptionInfo()
        self._subscriberid = Empty.GUID
        self._encoding = OperationalEncoding.UTF8
        self._connector = SubscriberConnector()
        self._connected = False
        self._subscribed = False

        self._commandchannel_socket: Optional[socket.socket] = None
        self._commandchannel_responsethread: Optional[Thread] = None
        self._datachannel_socket: Optional[socket.socket] = None
        self._datachannel_responsethread: Optional[Thread] = None

        self._connect_action_mutex = Lock()
        self._connection_terminationthread_mutex = Lock()
        self._connection_terminationthread: Optional[Thread] = None

        self._disconnectthread: Optional[Thread] = None
        self._disconnectthread_mutex = Lock()
        self._disconnecting = False
        self._disconnected = False
        self._disposing = False

        # Statistics counters
        self._total_commandchannel_bytesreceived = np.uint64(0)
        self._total_datachannel_bytesreceived = np.uint64(0)
        self._total_measurementsreceived = np.uint64(0)

        self.statusmessage_callback: Optional[Callable[[str], None]] = None
        """
        Called when a informational message should be logged.
        """

        self.errormessage_callback: Optional[Callable[[str], None]] = None
        """
        Called when an error message should be logged.
        """

        self.connectionterminated_callback: Optional[Callable[[], None]] = None
        """
        Called when `DataSubscriber` terminates its connection.
        """

        self.autoreconnect_callback: Optional[Callable[[], None]] = None
        """
        Called when `DataSubscriber` automatically reconnects.
        """

        self.metadatareceived_callback: Optional[Callable[[bytes], None]] = None
        """
        Called when `DataSubscriber` receives a metadata response.
        """

        self.subscriptionupdated_callback: Optional[Callable[[SignalIndexCache], None]] = None
        """
        Called when `DataSubscriber` receives a new signal index cache.
        """

        self.data_starttime_callback: Optional[Callable[[np.uint64], None]] = None
        """
        Called with timestamp of first received measurement in a subscription.
        """

        self.configurationchanged_callback: Optional[Callable[[], None]] = None
        """
        Called when the `DataPublisher` sends a notification that configuration has changed.
        """

        self.newmeasurements_callback: Optional[Callable[[List[Measurement]], None]] = None
        """
        Called when `DataSubscriber` receives a set of new measurements from the `DataPublisher`.
        """

        self.newbufferblocks_callback: Optional[Callable[[List[BufferBlock]], None]] = None
        """
        Called when `DataSubscriber` receives a set of new buffer block measurements from the `DataPublisher`.
        """

        self.processingcomplete_callback: Optional[Callable[[str], None]] = None
        """
        Called when the `DataPublisher` sends a notification that temporal processing has completed, i.e., the end of a historical playback data stream has been reached.
        """

        self.notificationreceived_callback: Optional[Callable[[str], None]] = None
        """
        Called when the `DataPublisher` sends a notification that requires receipt.
        """

        self.compress_payloaddata = DataSubscriber.DEFAULT_COMPRESS_PAYLOADDATA if compress_payloaddata is ... else compress_payloaddata
        """
        Determines whether payload data is compressed, defaults to TSSC.
        """

        self.compress_metadata = DataSubscriber.DEFAULT_COMPRESS_METADATA if compress_metadata is ... else compress_metadata
        """
        Determines whether the metadata transfer is compressed, defaults to GZip.
        """

        self.compress_signalindexcache = DataSubscriber.DEFAULT_COMPRESS_SIGNALINDEXCACHE if compress_signalindexcache is ... else compress_signalindexcache
        """
        Determines whether the signal index cache is compressed, defaults to GZip.
        """

        self.version = DataSubscriber.DEFAULT_VERSION if version is ... else version
        """
        Defines the STTP protocol version used by this library.
        """

        self.sttp_sourceinfo = DataSubscriber.DEFAULT_STTP_SOURCEINFO if sttp_sourceinfo is ... else sttp_sourceinfo
        """
        Defines the STTP library API title as identification information of `DataSubscriber` to a `DataPublisher`.
        """

        self.sttp_versioninfo = DataSubscriber.DEFAULT_STTP_VERSIONINFO if sttp_versioninfo is ... else sttp_versioninfo
        """
        Defines the STTP library API version as identification information of `DataSubscriber` to a `DataPublisher`.
        """

        self.sttp_updatedoninfo = DataSubscriber.DEFAULT_STTP_UPDATEDONINFO if sttp_updatedoninfo is ... else sttp_updatedoninfo
        """
        Defines when the STTP library API was last updated as identification information of `DataSubscriber` to a `DataPublisher`.
        """

        self.metadatacache = MetadataCache()
        """
        Defines the metadata cache associated with this `DataSubscriber`.
        """

        self.socket_timeout = DataSubscriber.DEFAULT_SOCKET_TIMEOUT if socket_timeout is ... else socket_timeout
        """
        Defines the socket timeout in seconds for the `DataSubscriber` connection.
        """

        # Measurement parsing
        self._metadatarequested = 0.0
        self._signalindexcache = [SignalIndexCache(), SignalIndexCache()]
        self._signalindexcache_mutex = Lock()
        self._cacheindex = 0
        self._timeindex = 0
        self._basetimeoffsets = np.zeros(2, dtype=np.int64)
        self._key_ivs: Optional[List[List[bytes]]] = None
        self._last_missingcachewarning = 0.0
        self._tssc_resetrequested = False
        self._tssc_lastoosreport = 0.0
        self._tssc_lastoosreport_mutex = Lock()

        self._bufferblock_expectedsequencenumber = np.uint32(0)
        self._bufferblock_cache: List[BufferBlock] = []

        self._threadpool = ThreadPoolExecutor(thread_name_prefix="DS-PoolThread")

    def dispose(self):
        """
        Cleanly shuts down a `DataSubscriber` that is no longer being used, e.g., during a normal application exit.
        """

        self._disposing = True
        self._connector.dispose()
        self._disconnect(True, False)

        # Wait for connection terminated event to complete
        self._threadpool.shutdown(wait=True)

    @property
    def connected(self) -> bool:
        """
        Determines if a `DataSubscriber` is currently connected to a `DataPublisher`.
        """

        return self._connected

    @property
    def subscribed(self) -> bool:
        """
        Determines if a `DataSubscriber` is currently subscribed to a data stream.
        """

        return self._subscribed

    @property
    def disposing(self) -> bool:
        """
        Determines if `DataSubscriber` is being disposed.
        """

        return self._disposing

    def encodestr(self, data: str) -> bytes:
        """
        Encodes an STTP string according to the defined operational modes.
        """

        # Latest version of STTP only encodes to UTF-8
        if self._encoding != OperationalEncoding.UTF8:
            raise RuntimeError("Python implementation of STTP only supports UTF-8 string encoding")

        return data.encode("utf-8")

    def decodestr(self, data: bytes) -> str:
        """
        Decodes an STTP string according to the defined operational modes.
        """

        # Latest version of STTP only encodes to UTF-8
        if self._encoding != OperationalEncoding.UTF8:
            raise RuntimeError("Python implementation of STTP only supports UTF-8 string encoding")

        return data.decode("utf-8")

    def lookup_metadata(self, signalid: UUID, source: str = ..., id: np.uint64 = ...) -> MeasurementRecord:
        """
        Gets the `MeasurementRecord` for the specified signal ID from the local registry.
        If the metadata does not exist, a new record is created and returned.
        """

        record = self.metadatacache.find_measurement_signalid(signalid)

        if record is not None:
            return record

        record = MeasurementRecord(signalid, source=source, id=id)
        self.metadatacache.add_measurement(record)
        return record

    def adjustedvalue(self, measurement: Measurement) -> np.float64:
        """
        Gets the `Value` of a `Measurement` with any linear adjustments applied from the measurement's `Adder` and `Multiplier` metadata, if found.
        """

        record = self.metadatacache.find_measurement_signalid(measurement.signalid)

        if record is not None:
            return measurement.value * record.multiplier + record.adder

        return measurement.value

    def connect(self, hostname: str, port: np.uint16) -> Optional[Exception]:
        """
        Requests the the `DataSubscriber` initiate a connection to the `DataPublisher`.
        """

        #  User requests to connection are not an auto-reconnect attempt
        return self._connect(hostname, port, False)

    def _connect(self, hostname: str, port: np.uint16, autoreconnecting: bool) -> Optional[Exception]:  # sourcery skip: extract-method
        if self._connected:
            return RuntimeError("subscriber is already connected; disconnect first")

        # Make sure any pending disconnect has completed to make sure socket is closed
        self._disconnectthread_mutex.acquire()
        disconnectthread = self._disconnectthread
        self._disconnectthread_mutex.release()

        if disconnectthread is not None and disconnectthread.is_alive():
            disconnectthread.join()

        err: Optional[Exception] = None

        # Let any pending connect or disconnect operation complete before new connect,
        # this prevents destruction disconnect before connection is completed
        try:
            self._connect_action_mutex.acquire()

            self._disconnected = False
            self._subscribed = False

            self._total_commandchannel_bytesreceived = np.int64(0)
            self._total_datachannel_bytesreceived = np.int64(0)
            self._total_measurementsreceived = np.int64(0)

            self._key_ivs = None
            self._bufferblock_expectedsequencenumber = np.uint32(0)
            self.metadatacache = MetadataCache()

            if not autoreconnecting:
                self._connector.reset_connection()

            self._connector._connectionrefused = False

            # TODO: Add TLS implementation options
            # TODO: Add reverse (server-based) connection options, see:
            # https://sttp.info/reverse-connections/

            self._commandchannel_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
            self._commandchannel_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._commandchannel_socket.settimeout(self.socket_timeout)

            try:
                hostendpoint = socket.getaddrinfo(hostname, int(port), family=socket.AF_INET, proto=socket.IPPROTO_TCP)[0][4]
            except Exception:
                hostendpoint = (hostname, int(port))

            self._commandchannel_socket.connect(hostendpoint)
        except Exception as ex:
            err = ex
        finally:
            self._connect_action_mutex.release()

        if err is None:
            self._commandchannel_responsethread = Thread(target=self._run_commandchannel_responsethread, name="CmdChannelThread")

            self._connected = True
            self._last_missingcachewarning = 0.0

            self._commandchannel_responsethread.start()
            self._send_operationalmodes()

        return err

    def subscribe(self) -> Optional[Exception]:
        """
        Notifies the `DataPublisher` that a `DataSubscriber` would like to start receiving streaming data.
        """

        if not self._connected:
            return RuntimeError("subscriber is not connected; cannot subscribe")

        self._total_measurementsreceived = np.uint64(0)

        subscription = self._subscription

        parmaterbuilder: List[str] = [
            f"throttled={subscription.throttled}",
            f";publishInterval={subscription.publishinterval:.6f}",
            f";includeTime={subscription.includetime}",
            f";enableTimeReasonabilityCheck={subscription.enabletimereasonabilitycheck}",
            f";lagTime={subscription.lagtime:.6f}",
            f";leadTime={subscription.leadtime:.6f}",
            f";useLocalClockAsRealTime={subscription.uselocalclockasrealtime}",
            f";processingInterval={subscription.processinginterval}",
            f";useMillisecondResolution={subscription.use_millisecondresolution}",
            f";requestNaNValueFilter={subscription.request_nanvaluefilter}",
            f";assemblyInfo={{source={self.sttp_sourceinfo}",
            f";version={self.sttp_versioninfo}",
            f";updatedOn={self.sttp_updatedoninfo}}}"
        ]

        if len(subscription.filterexpression) > 0:
            parmaterbuilder.append(f";filterExpression={{{subscription.filterexpression}}}")

        if subscription.udpdatachannel:
            udpport = subscription.datachannel_localport

            try:
                self._datachannel_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self._datachannel_socket.bind((subscription.datachannel_interface, udpport))
                self._datachannel_socket.settimeout(self.socket_timeout)
            except Exception as ex:
                return RuntimeError(f"failed to open UDP socket for port {udpport}: {ex}")

            self._datachannel_responsethread = Thread(target=self._run_datachannel_responsethread, name="DataChannelThread")
            self._datachannel_responsethread.start()

            parmaterbuilder.append(f";dataChannel={{localport={udpport}}}")

        if len(subscription.starttime) > 0:
            parmaterbuilder.append(f";startTimeConstraint={subscription.starttime}")

        if len(subscription.stoptime) > 0:
            parmaterbuilder.append(f";stopTimeConstraint={subscription.stoptime}")

        if len(subscription.constraintparameters) > 0:
            parmaterbuilder.append(f";timeConstraintParameters={subscription.constraintparameters}")

        if len(subscription.extra_connectionstring_parameters) > 0:
            parmaterbuilder.append(f";{subscription.extra_connectionstring_parameters}")

        parameterstring = "".join(parmaterbuilder)
        parameterexpression = self.encodestr(parameterstring)
        length = np.uint32(len(parameterexpression))
        buffer = bytearray(5 + length)

        buffer[0] = DataPacketFlags.COMPACT
        buffer[1:5] = BigEndian.from_uint32(length)
        buffer[5:] = parameterexpression

        self.send_servercommand(ServerCommand.SUBSCRIBE, buffer)

        # Reset TSSC decompressor on successful (re)subscription
        self._tssc_lastoosreport_mutex.acquire()
        self._tssc_lastoosreport = time()
        self._tssc_lastoosreport_mutex.release()
        self._tssc_resetrequested = True

        return None

    def unsubscribe(self):
        """
        Notifies the `DataPublisher` that a `DataSubscriber` would like to stop receiving streaming data.
        """

        if not self._connected:
            return

        self.send_servercommand(ServerCommand.UNSUBSCRIBE)

        self._disconnecting = True

        if self._datachannel_socket is not None:
            try:
                self._datachannel_socket.shutdown(socket.SHUT_RDWR)
                self._datachannel_socket.close()
            except Exception as ex:
                self._dispatch_errormessage(f"Exception while disconnecting data subscriber UDP data channel: {ex}")

        if self._datachannel_responsethread is not None and self._datachannel_responsethread.is_alive():
            self._datachannel_responsethread.join()

        self._disconnecting = False

    def disconnect(self):
        """
        Initiates a `DataSubscriber` disconnect sequence.
        """

        if self._disconnecting:
            return

        # Disconnect method executes shutdown on a separate thread without stopping to prevent
        # issues where user may call disconnect method from a dispatched event thread. Also,
        # user requests to disconnect are not an auto-reconnect attempt
        self._disconnect(False, False)

    def _disconnect(self, jointhread: bool, autoreconnecting: bool):
        # Check if disconnect thread is running or subscriber has already disconnected
        if self._disconnecting:
            if not autoreconnecting and not self._disconnected:
                self._connector.cancel()

            self._disconnectthread_mutex.acquire()
            disconnectthread = self._disconnectthread
            self._disconnectthread_mutex.release()

            if jointhread and not self._disconnected and disconnectthread is not None and disconnectthread.is_alive():
                disconnectthread.join()

            return

        # Notify running threads that the subscriber is disconnecting, i.e., disconnect thread is active
        self._disconnecting = True
        self._connected = False
        self._subscribed = False

        disconnectthread = Thread(target=lambda: self._run_disconnectthread(autoreconnecting), name="DisconnectThread")

        self._disconnectthread_mutex.acquire()
        disconnectthread.start()
        self._disconnectthread = disconnectthread
        self._disconnectthread_mutex.release()

        if jointhread and disconnectthread.is_alive():
            disconnectthread.join()

    def _run_disconnectthread(self, autoreconnecting: bool):  # sourcery skip: extract-method
        # Let any pending connect operation complete before disconnect - prevents destruction disconnect before connection is completed
        if not autoreconnecting:
            self._connector.cancel()

            self._connection_terminationthread_mutex.acquire()
            connection_terminationthread = self._connection_terminationthread
            self._connection_terminationthread_mutex.release()

            if connection_terminationthread is not None and connection_terminationthread.is_alive():
                connection_terminationthread.join()

            self._connect_action_mutex.acquire()

        # Release queues and close sockets so that threads can shut down gracefully
        if self._commandchannel_socket is not None:
            try:
                self._commandchannel_socket.shutdown(socket.SHUT_RDWR)
                self._commandchannel_socket.close()
            except Exception as ex:
                self._dispatch_errormessage(f"Exception while disconnecting data subscriber TCP command channel: {ex}")

        if self._datachannel_socket is not None:
            try:
                self._datachannel_socket.shutdown(socket.SHUT_RDWR)
                self._datachannel_socket.close()
            except Exception as ex:
                self._dispatch_errormessage(f"Exception while disconnecting data subscriber UDP data channel: {ex}")

        # Join with all threads to guarantee their completion before returning control to the caller
        if self._commandchannel_responsethread is not None and self._commandchannel_responsethread.is_alive():
            self._commandchannel_responsethread.join()

        if self._datachannel_responsethread is not None and self._datachannel_responsethread.is_alive():
            self._datachannel_responsethread.join()

        # Notify consumers of disconnect
        if self.connectionterminated_callback is not None:
            self.connectionterminated_callback()

        # Disconnect complete
        self._disconnected = True
        self._disconnecting = False

        if autoreconnecting:
            # Handling auto-connect callback separately from connection terminated callback
            # since they serve two different use cases and current implementation does not
            # support multiple callback registrations
            if self.autoreconnect_callback is not None and not self._disposing:
                self.autoreconnect_callback()
        else:
            self._connect_action_mutex.release()

    # Dispatcher for connection terminated. This is called from its own separate thread
    # in order to cleanly shut down the subscriber in case the connection was terminated
    # by the peer. Additionally, this allows the user to automatically reconnect in their
    # callback function without having to spawn their own separate thread.
    def _dispatch_connectionterminated(self):
        try:
            self._connection_terminationthread_mutex.acquire()

            if self._connection_terminationthread is not None:
                return

            self._connection_terminationthread = Thread(target=self._handle_connectionterminated, name="ConnectionTerminationThread")
            self._connection_terminationthread.start()
        finally:
            self._connection_terminationthread_mutex.release()

    def _handle_connectionterminated(self):
        self._disconnect(False, True)

        self._connection_terminationthread_mutex.acquire()
        self._connection_terminationthread = None
        self._connection_terminationthread_mutex.release()

    def _dispatch_statusmessage(self, message: str):
        if self.statusmessage_callback is not None:
            self._threadpool.submit(self.statusmessage_callback, message)

    def _dispatch_errormessage(self, message: str):
        if self.errormessage_callback is not None:
            self._threadpool.submit(self.errormessage_callback, message)

    def _run_commandchannel_responsethread(self):
        def recv_data(length: int) -> bytes:
            while self._connected:
                try:
                    return self._commandchannel_socket.recv(length)
                except (socket.timeout, OSError):
                    continue

        reader = BinaryStream(StreamEncoder(recv_data, lambda _: ...))
        buffer = bytearray(MAXPACKET_SIZE)

        while self._connected:
            try:
                reader.read_all(buffer, 0, PAYLOADHEADER_SIZE)

                # Gather statistics
                self._total_commandchannel_bytesreceived += PAYLOADHEADER_SIZE

                packetsize = BigEndian.to_uint32(buffer)

                if packetsize > len(buffer):
                    buffer = bytearray(packetsize)

                # Read packet (payload body)
                # This read method is guaranteed not to return until the
                # requested size has been read or an error has occurred.
                reader.read_all(buffer, 0, packetsize)

                # Gather statistics
                self._total_commandchannel_bytesreceived += packetsize
            except Exception:
                # Read error, connection may have been closed by peer; terminate connection
                self._dispatch_connectionterminated()
                return

            if self._disconnecting:
                return

            try:
                # Process response
                self._process_serverresponse(bytes(buffer[:packetsize]))
            except Exception as ex:
                self._dispatch_errormessage(f"Exception processing server response: {ex}")
                self._dispatch_connectionterminated()
                return

    # If the user defines a separate UDP channel for their
    # subscription, data packets get handled from this thread.
    def _run_datachannel_responsethread(self):
        def recv_data(length: int) -> bytes:
            while self._connected:
                try:
                    return self._datachannel_socket.recvfrom(length)[0]
                except (socket.timeout, OSError):
                    continue

        reader = StreamEncoder(recv_data, lambda _: ...)
        buffer = bytearray(MAXPACKET_SIZE)

        while self._connected:
            try:
                length = reader.read(buffer, 0, MAXPACKET_SIZE)

                # Gather statistics
                self._total_datachannel_bytesreceived += length
            except Exception:
                # Read error, connection may have been closed by peer; terminate connection
                self._dispatch_connectionterminated()
                return

            if self._disconnecting:
                return

            try:
                # Process response
                self._process_serverresponse(bytes(buffer[:length]))
            except Exception as ex:
                self._dispatch_errormessage(f"Exception processing server response: {ex}")
                self._dispatch_connectionterminated()
                return

    def _process_serverresponse(self, buffer: bytes):  # sourcery skip: remove-pass-elif
        if self._disconnecting:
            return

        # Note: internal payload size at buffer[2:6] ignored - future versions of STTP will likely exclude this
        data = buffer[RESPONSEHEADER_SIZE:]
        responsecode = ServerResponse(buffer[0])
        commandcode = ServerCommand(buffer[1])

        if responsecode == ServerResponse.SUCCEEDED:
            self._handle_succeeded(commandcode, data)
        elif responsecode == ServerResponse.FAILED:
            self._handle_failed(commandcode, data)
        elif responsecode == ServerResponse.DATAPACKET:
            self._handle_datapacket(data)
        elif responsecode == ServerResponse.DATASTARTTIME:
            self._handle_datastarttime(data)
        elif responsecode == ServerResponse.PROCESSINGCOMPLETE:
            self._handle_processingcomplete(data)
        elif responsecode == ServerResponse.UPDATESIGNALINDEXCACHE:
            self._handle_update_signalindexcache(data)
        elif responsecode == ServerResponse.UPDATEBASETIMES:
            self._handle_update_basetimes(data)
        elif responsecode == ServerResponse.UPDATECIPHERKEYS:
            self._handle_update_cipherkeys(data)
        elif responsecode == ServerResponse.CONFIGURATIONCHANGED:
            self._handle_configurationchanged()
        elif responsecode == ServerResponse.BUFFERBLOCK:
            self._handle_bufferblock(data)
        elif responsecode == ServerResponse.NOTIFY:
            self._handle_notification(data)
        elif responsecode == ServerResponse.NOOP:
            # NoOP Handled
            pass
        else:
            self._dispatch_errormessage(f"Encountered unexpected server response code: {str(responsecode)}")

    def _handle_succeeded(self, commandcode: ServerCommand, data: bytes):
        has_response_message = False

        if commandcode == ServerCommand.METADATAREFRESH:
            self._handle_metadatarefresh(data)
        elif commandcode in [ServerCommand.SUBSCRIBE, ServerCommand.UNSUBSCRIBE]:
            self._subscribed = commandcode == ServerCommand.SUBSCRIBE
            has_response_message = True
        elif commandcode in [ServerCommand.ROTATECIPHERKEYS, ServerCommand.UPDATEPROCESSINGINTERVAL]:
            has_response_message = True
        else:
            # If we don't know what the message is, we can't interpret
            # the data sent with the packet. Deliver an error message
            # to the user via the error message callback.
            self._dispatch_errormessage(f"Received success code in response to unknown server command: {str(commandcode)} ({hex(commandcode)})")

        if not has_response_message:
            return

        # Each of these responses come with a message that will
        # be delivered to the user via the status message callback.
        message: List[str] = [f"Received success code in response to server command: {normalize_enumname(commandcode)}"]

        if data:  # len > 0
            message.append("\n")
            message.append(self.decodestr(data))

        self._dispatch_statusmessage("".join(message))

    def _handle_failed(self, commandcode: ServerCommand, data: bytes):
        message: List[str] = []

        if commandcode == ServerCommand.CONNECT:
            self._connector._connectionrefused = True
        else:
            message.append(f"Received failure code in response to server command: {normalize_enumname(commandcode)}")

        if data:  # len > 0
            if message:  # len > 0
                message.append("\n")

            message.append(self.decodestr(data))

        if message:  # len > 0
            self._dispatch_errormessage("".join(message))

    def _handle_metadatarefresh(self, data: bytes):
        if self.metadatareceived_callback is not None:
            if self.compress_metadata:
                self._dispatch_statusmessage(f"Received {len(data):,} bytes of metadata in {(time() - self._metadatarequested):.3f} seconds. Decompressing...")

                decompress_started = time()

                try:
                    data = gzip.decompress(data)
                except Exception as ex:
                    self._dispatch_errormessage(f"Failed to decompress received metadata: {ex}")
                    return

                self._dispatch_statusmessage(f"Decompressed {len(data):,} bytes of metadata in {(time() - decompress_started):.3f} seconds. Parsing...")
            else:
                self._dispatch_statusmessage(f"Received {len(data):,} bytes of metadata in {(time() - self._metadatarequested):.3f} seconds. Parsing...")

            self._threadpool.submit(self.metadatareceived_callback, data)

    def _handle_datastarttime(self, data: bytes):
        if self.data_starttime_callback is not None:
            self.data_starttime_callback(BigEndian.to_uint64(data))

    def _handle_processingcomplete(self, data: bytes):
        if self.processingcomplete_callback is not None:
            self.processingcomplete_callback(self.decodestr(data))

    def _handle_update_signalindexcache(self, data: bytes):
        if not data:  # len == 0
            return

        version = self.version
        cacheindex = 0

        # Get active cache index
        if version > 1:
            if data[0] > 0:
                cacheindex = 1

            data = data[1:]

        if self.compress_signalindexcache:
            try:
                data = gzip.decompress(data)
            except Exception as ex:
                self._dispatch_errormessage(f"Failed to decompress received signal index cache: {ex}")
                return

        signalindexcache = SignalIndexCache()
        (self._subscriberid, err) = signalindexcache.decode(self, data)

        if err is not None:
            self._dispatch_errormessage(f"Failed to parse signal index cache: {err}")
            return

        self._signalindexcache_mutex.acquire()
        self._signalindexcache[cacheindex] = signalindexcache
        self._cacheindex = cacheindex
        self._signalindexcache_mutex.release()

        if version > 1:
            self.send_servercommand(ServerCommand.CONFIRMUPDATESIGNALINDEXCACHE)

        if self.subscriptionupdated_callback is not None:
            self.subscriptionupdated_callback(signalindexcache)

    def _handle_update_basetimes(self, data: bytes):
        if not data:  # len == 0
            return

        self._timeindex = 0 if BigEndian.to_uint32(data) == 0 else 1
        self._basetimeoffsets = [BigEndian.to_uint64(data[4:]), BigEndian.to_uint64(data[12:])]

        self._dispatch_statusmessage(f"Received new base time offset from publisher: {Ticks.to_string(self._basetimeoffsets[self._timeindex ^ 1])}")

    def _handle_update_cipherkeys(self, data: bytes):
        # Deserialize new cipher keys
        key_ivs = [[bytes(), bytes()], [bytes(), bytes()]]

        # Move past active cipher index (not currently used anywhere else)
        index = 1

        # Read even key size
        bufferlen = int(BigEndian.to_int32(data[index:]))
        index += 4

        # Read even key
        key_ivs[EVEN_KEY][KEY_INDEX] = data[index:bufferlen]
        index += bufferlen

        # Read even initialization vector size
        bufferlen = int(BigEndian.to_int32(data[index:]))
        index += 4

        # Read even initialization vector
        key_ivs[EVEN_KEY][IV_INDEX] = data[index:bufferlen]
        index += bufferlen

        # Read odd key size
        bufferlen = int(BigEndian.to_int32(data[index:]))
        index += 4

        # Read odd key
        key_ivs[ODD_KEY][KEY_INDEX] = data[index:bufferlen]
        index += bufferlen

        # Read odd initialization vector size
        bufferlen = int(BigEndian.to_int32(data[index:]))
        index += 4

        # Read odd initialization vector
        key_ivs[ODD_KEY][IV_INDEX] = data[index:bufferlen]
        #index += bufferLen

        # Exchange keys
        self._key_ivs = key_ivs

        self._dispatch_statusmessage("Successfully established new cipher keys for UDP data packet transmissions.")

    def _handle_configurationchanged(self):
        self._dispatch_statusmessage("Received notification from publisher that configuration has changed.")

        if self.configurationchanged_callback is not None:
            self.configurationchanged_callback()

    def _handle_datapacket(self, data: bytes):
        datapacketflags = DataPacketFlags(data[0])
        compressed = datapacketflags & DataPacketFlags.COMPRESSED > 0
        compact = datapacketflags & DataPacketFlags.COMPACT > 0

        if not compressed and not compact:
            self._dispatch_errormessage("Python implementation of STTP only supports compact or compressed data packet encoding - disconnecting.")
            self._dispatch_connectionterminated()
            return

        data = data[1:]

        if self._key_ivs is not None:
            # Get a local copy keyIVs - these can change at any time
            key_ivs = self._key_ivs
            cipherindex = 1 if datapacketflags & DataPacketFlags.CIPHERINDEX > 0 else 0

            try:
                cipher = AES.new(key_ivs[cipherindex][KEY_INDEX], AES.MODE_CBC, key_ivs[cipherindex][IV_INDEX])
                data = cipher.decrypt(data)
            except Exception as ex:
                self._dispatch_errormessage(f"Failed to decrypt data packet - disconnecting: {ex}")
                self._dispatch_connectionterminated()
                return

        count = BigEndian.to_uint32(data)
        cacheindex = 1 if datapacketflags & DataPacketFlags.CACHEINDEX > 0 else 0

        self._signalindexcache_mutex.acquire()
        signalindexcache = self._signalindexcache[cacheindex]
        self._signalindexcache_mutex.release()

        if compressed:
            measurements, err = self._parse_tssc_measurements(signalindexcache, data[4:], count)
        else:
            measurements, err = self._parse_compact_measurements(signalindexcache, data[4:], count)

        if err is not None:
            self._dispatch_errormessage(str(err))
            self._dispatch_connectionterminated()
        else:
            if self.newmeasurements_callback is not None and len(measurements) > 0:
                # Do not use thread pool here, processing sequence may be important.
                # Execute callback directly from socket processing thread:
                self.newmeasurements_callback(measurements)

            self._total_measurementsreceived += count

    def _parse_tssc_measurements(self, signalindexcache: SignalIndexCache, data: bytes, count: np.uint32) -> Tuple[List[Measurement], Optional[Exception]]:
        decoder = signalindexcache._tsscdecoder
        newdecoder = False

        if decoder is None:
            signalindexcache._tsscdecoder = Decoder()
            decoder = signalindexcache._tsscdecoder
            decoder.sequencenumber = 0
            newdecoder = True

        if data[0] != 85:
            return [], RuntimeError(f"TSSC version not recognized - disconnecting. Received version: {data[0]}")

        sequencenumber = BigEndian.to_uint16(data[1:])

        if sequencenumber == 0:
            if not newdecoder:
                if decoder.sequencenumber > 0:
                    self._dispatch_errormessage(f"TSSC algorithm reset before sequence number: {decoder.sequencenumber}")

                signalindexcache._tsscdecoder = Decoder()
                decoder = signalindexcache._tsscdecoder
                decoder.sequencenumber = 0

            self._tssc_resetrequested = False
            self._tssc_lastoosreport_mutex.acquire()
            self._tssc_lastoosreport = time()
            self._tssc_lastoosreport_mutex.release()

        if decoder.sequencenumber != sequencenumber:
            if not self._tssc_resetrequested:
                self._tssc_lastoosreport_mutex.acquire()

                if time() - self._tssc_lastoosreport > 2.0:
                    self._dispatch_errormessage(f"TSSC is out of sequence. Expecting: {decoder.sequencenumber}, received: {sequencenumber}")
                    self._tssc_lastoosreport = time()

                self._tssc_lastoosreport_mutex.release()

            return [], None

        decoder.set_buffer(data[3:])

        measurements = [Measurement] * count
        index = 0
        success = True

        while success:
            pointid, timestamp, stateflags, value, success, err = decoder.try_get_measurement()

            if success:
                measurements[index] = Measurement(
                    signalindexcache.signalid(pointid),
                    np.float64(value),
                    np.uint64(timestamp),
                    StateFlags(stateflags))

                index += 1

        if err is not None:
            return [], RuntimeError(f"Failed to parse TSSC measurements - disconnecting: {err}")

        decoder.sequencenumber += 1

        # Do not increment to 0 on roll-over
        if decoder.sequencenumber > Limits.MAXUINT16:
            decoder.sequencenumber = 1

        return measurements, None

    def _parse_compact_measurements(self, signalindexcache: SignalIndexCache, data: bytes, count: np.uint32) -> Tuple[List[Measurement], Optional[Exception]]:
        if signalindexcache.count == 0:
            if self._last_missingcachewarning + MISSINGCACHEWARNING_INTERVAL < time():
                # Warning message for missing signal index cache
                if self._last_missingcachewarning > 0.0:
                    self._dispatch_statusmessage("WARNING: Signal index cache has not arrived. No compact measurements can be parsed.")

                self._last_missingcachewarning = time()

            return [], None

        measurements: List[Measurement] = []
        use_millisecondresolution = self.subscription.use_millisecondresolution
        includetime = self.subscription.includetime
        index = 0

        for _ in range(count):
            # Deserialize compact measurement format
            measurement = CompactMeasurement(signalindexcache, includetime, use_millisecondresolution, self._basetimeoffsets)
            (bytesdecoded, err) = measurement.decode(data[index:])

            if err is not None:
                return [], RuntimeError(f"Failed to parse compact measurements - disconnecting: {err}")

            index += bytesdecoded
            measurements.append(measurement)

        return measurements, None

    def _handle_bufferblock(self, data: bytes):  # sourcery skip: low-code-quality, extract-method
        # Buffer block received - wrap as a BufferBlockMeasurement and expose back to consumer
        sequencenumber = BigEndian.to_uint32(data)
        buffercacheindex = int(sequencenumber - self._bufferblock_expectedsequencenumber)

        # Check if this buffer block has already been processed (e.g., mistaken retransmission due to timeout)
        if buffercacheindex >= 0 and (buffercacheindex >= len(self._bufferblock_cache) and self._bufferblock_cache[buffercacheindex].buffer is None):
            # Send confirmation that buffer block is received
            self.send_servercommand(ServerCommand.CONFIRMBUFFERBLOCK, data[:4])

            signalindexcacheindex = 1 if self.version > 1 and data[4:][0] > 0 else 0
            data = data[5:] if self.version > 1 else data[4:]

            # Get measurement key from signal index cache
            signalindex = BigEndian.to_uint32(data)

            self._signalindexcache_mutex.acquire()
            signalindexCache = self._signalindexcache[signalindexcacheindex]
            self._signalindexcache_mutex.release()

            signalid = signalindexCache.signalid(signalindex)
            bufferblockmeasurement = BufferBlock(signalid)

            # Determine if this is the next buffer block in the sequence
            if sequencenumber == self._bufferblock_expectedsequencenumber:
                bufferblockmeasurements = np.empty(1 + len(self._bufferblock_cache), BufferBlock)

                # Add the buffer block measurement to the list of measurements to be published
                bufferblockmeasurements[0] = bufferblockmeasurement
                self._bufferblock_expectedsequencenumber += 1

                # Add cached buffer block measurements to the list of measurements to be published
                for i in range(len(self._bufferblock_cache)):
                    if self._bufferblock_cache[i].buffer is None:
                        break

                    bufferblockmeasurements[i] = self._bufferblock_cache[i]
                    self._bufferblock_expectedsequencenumber += 1

                # Remove published buffer block measurements from the buffer block queue
                if len(self._bufferblock_cache) > 0:
                    self._bufferblock_cache = self._bufferblock_cache[i:]

                # Publish buffer block measurements
                if self.newbufferblocks_callback is not None:
                    # Do not use thread pool here, processing sequence may be important.
                    # Execute callback directly from socket processing thread:
                    self.newbufferblocks_callback(bufferblockmeasurements)
            else:
                # Ensure that the list has at least as many elements as it needs to cache this measurement.
                # This edge case handles possible dropouts and/or out of order packet deliver when data
                # transport is UDP - this use case is not expected when using a TCP only connection.
                for _ in range(len(self._bufferblock_cache), buffercacheindex + 1):
                    self._bufferblock_cache.append(BufferBlock())

                # Insert this buffer block into the proper location in the list
                self._bufferblock_cache[buffercacheindex] = bufferblockmeasurement

    def _handle_notification(self, data: bytes):
        message = self.decodestr(data)

        self._dispatch_statusmessage(f"NOTIFICATION: {message}")

        if self.notificationreceived_callback is not None:
            self.notificationreceived_callback()

    def send_servercommand_withmessage(self, commandcode: ServerCommand, message: str):
        """
        Sends a server command code to the `DataPublisher` along with the specified string message as payload.
        """

        self.send_servercommand(self.encodestr(message))

    def send_servercommand(self, commandcode: ServerCommand, data: bytes = None):
        """
        Sends a server command code to the `DataPublisher` with specified payload.
        """

        if not self._connected:
            return

        packetsize = np.uint32(1) if data is None else np.uint32(len(data)) + 1
        commandbuffersize = np.uint32(packetsize + PAYLOADHEADER_SIZE)
        buffer = bytearray(commandbuffersize)

        # Insert packet size
        buffer[:4] = BigEndian.from_uint32(packetsize)

        # Insert command code
        buffer[4] = commandcode

        if data is not None and data:  # len > 0
            buffer[5:commandbuffersize] = data

        if commandcode == ServerCommand.METADATAREFRESH:
            # Track start time of metadata request to calculate round-trip receive time
            self._metadatarequested = time()

        try:
            self._commandchannel_socket.send(buffer)
        except Exception as ex:
            # Write error, connection may have been closed by peer; terminate connection
            self._dispatch_errormessage(f"Failed to send server command - disconnecting: {ex}")
            self._dispatch_connectionterminated()

    def _send_operationalmodes(self):
        operationalModes = np.uint32(CompressionModes.GZIP)
        operationalModes |= OperationalModes.VERSIONMASK & np.uint32(self.version)
        operationalModes |= self._encoding

        # TSSC compression only works with stateful connections
        if self.compress_payloaddata and not self.subscription.udpdatachannel:
            operationalModes |= OperationalModes.COMPRESSPAYLOADDATA | CompressionModes.TSSC

        if self.compress_metadata:
            operationalModes |= OperationalModes.COMPRESSMETADATA

        if self.compress_signalindexcache:
            operationalModes |= OperationalModes.COMPRESSSIGNALINDEXCACHE

        self.send_servercommand(ServerCommand.DEFINEOPERATIONALMODES, BigEndian.from_uint32(np.uint32(operationalModes)))

    @property
    def subscription(self) -> SubscriptionInfo:
        """
        Gets the `SubscriptionInfo` associated with this `DataSubscriber`.
        """

        return self._subscription

    @property
    def connector(self) -> SubscriberConnector:
        """
        Gets the `SubscriberConnector` associated with this `DataSubscriber`.
        """

        return self._connector

    @property
    def activesignalindexcache(self) -> SignalIndexCache:
        """
        Gets the active signal index cache.
        """

        self._signalindexcache_mutex.acquire()
        signalindexcache = self._signalindexcache[self._cacheindex]
        self._signalindexcache_mutex.release()

        return signalindexcache

    @property
    def subscriberid(self) -> UUID:
        """
        Gets the subscriber ID as assigned by the `DataPublisher` upon receipt of the `SignalIndexCache`.
        """

        return self._subscriberid

    @property
    def total_commandchannel_bytesreceived(self) -> np.uint64:
        """
        Gets the total number of bytes received via the command channel since last connection.
        """

        return self._total_commandchannel_bytesreceived

    @property
    def total_datachannel_bytesreceived(self) -> np.uint64:
        """
        Gets the total number of bytes received via the data channel since last connection.
        """

        return self._total_datachannel_bytesreceived

    @property
    def total_measurementsreceived(self) -> np.uint64:
        """
        Gets the total number of measurements received since last subscription.
        """

        return self._total_measurementsreceived
