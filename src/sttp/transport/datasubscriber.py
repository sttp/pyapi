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

from gsf import Empty
from gsf.endianorder import BigEndian
from gsf.binarystream import BinaryStream
from gsf.streamencoder import StreamEncoder
from .measurement import Measurement
from .compactmeasurement import CompactMeasurement
from ..metadata.record.measurement import MeasurementRecord
from ..metadata.cache import MetadataCache
from .bufferblock import BufferBlock
from .constants import *
from .subscriptioninfo import SubscriptionInfo
from .subscriberconnector import SubscriberConnector
from .signalindexcache import SignalIndexCache
from ..ticks import Ticks
from ..version import Version
from typing import List, Callable, Optional
from datetime import datetime
from time import time
from uuid import UUID
from threading import Lock, Thread, Event
from concurrent.futures import ThreadPoolExecutor
from Crypto.Cipher import AES
import gzip
import socket
import numpy as np


class DataSubscriber:
    """
    Represents a client subscription for an STTP connection.
    """

    DEFAULT_COMPRESS_PAYLOADDATA = True       # Defaults to TSSC
    DEFAULT_COMPRESS_METADATA = True          # Defaults to Gzip
    DEFAULT_COMPRESS_SIGNALINDEXCACHE = True  # Defaults to Gzip
    DEFAULT_VERSION = np.byte(2)
    DEFAULT_STTP_SOURCEINFO = Version.STTP_SOURCE
    DEFAULT_STTP_VERSIONINFO = Version.STTP_VERSION
    DEFAULT_STTP_UPDATEDONINFO = Version.STTP_UPDATEDON

    def __init__(self,
                 compress_payloaddata: bool = ...,
                 compress_metadata: bool = ...,
                 compress_signalindexcache: bool = ...,
                 version: np.byte = ...,
                 sttp_sourceinfo: str = ...,
                 sttp_versioninfo: str = ...,
                 sttp_updatedoninfo: str = ...
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
        self._readbuffer = bytearray(MAXPACKET_SIZE)
        self._reader: Optional[BinaryStream] = None
        self._writebuffer = bytearray(MAXPACKET_SIZE)
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

        self.connectionterminated_callback: Optional[Callable] = None
        """
        Called when `DataSubscriber` terminates its connection.
        """

        self.autoreconnect_callback: Optional[Callable] = None
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

        self.configurationchanged_callback: Optional[Callable] = None
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
        Called when the `DataPublished` sends a notification that temporal processing has completed, i.e., the end of a historical playback data stream has been reached.
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
        self._bufferblock_cache: List[BufferBlock] = list()

        self._threadpool = ThreadPoolExecutor()

    def dispose(self):
        """
        Cleanly shuts down a `DataSubscriber` that is no longer being used, e.g., during a normal application exit.
        """

        self._disposing = True
        self._connector.cancel()
        self._disconnect(True, False)

        # Allow a moment for connection terminated event to complete
        Event().wait(0.01)  # 10ms

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
        Determines if DataSubscriber is being disposed.
        """

        return self._disposing

    def encodestr(self, data: str) -> bytes:
        """
        Encodes an STTP string according to the defined operational modes.
        """

        # Latest version of STTP only encodes to UTF8
        if self._encoding != OperationalEncoding.UTF8:
            raise RuntimeError("Python implementation of STTP only supports UTF8 string encoding")

        return data.encode("utf-8")

    def decodestr(self, data: bytes) -> str:
        """
        Decodes an STTP string according to the defined operational modes.
        """

        # Latest version of STTP only encodes to UTF8
        if self._encoding != OperationalEncoding.UTF8:
            raise RuntimeError("Python implementation of STTP only supports UTF8 string encoding")

        return data.decode("utf-8")

    def lookupmetadata(self, signalid: UUID) -> MeasurementRecord:
        """
        Gets the `MeasurementRecord` for the specified signal ID from the local registry.
        If the metadata does not exist, a new record is created and returned.
        """

        record = self.metadatacache.find_measurement_signalid(signalid)

        if record is not None:
            return record

        record = MeasurementRecord(signalid)
        self.metadatacache.add_measurement(record)
        return record

    def adjustedvalue(self, measurement: Measurement) -> np.float64:
        """
        Gets the Value of a `Measurement` with any linear adjustments applied from the measurement's Adder and Multiplier metadata, if found.
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

    def _connect(self, hostname: str, port: np.uint16, autoreconnecting: bool) -> Optional[Exception]:
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

            self._totalcommandchannel_bytesreceived = np.int64(0)
            self._totaldatachannel_bytesreceived = np.int64(0)
            self._totalmeasurements_received = np.int64(0)

            self._key_ivs = None
            self._bufferblock_expectedsequencenumber = np.uint32(0)
            self.metadatacache = MetadataCache()

            if not autoreconnecting:
                self._connector.resetconnection()

            self._connector._connectionrefused = False

            # TODO: Add TLS implementation options
            # TODO: Add reverse (server-based) connection options, see:
            # https://sttp.github.io/documentation/reverse-connections/

            self._commandchannel_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
            self._commandchannel_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            try:
                hostendpoint = socket.getaddrinfo(hostname, int(port), family=socket.AF_INET, proto=socket.IPPROTO_TCP)[0][4]
            except:
                hostendpoint = (hostname, int(port))

            self._commandchannel_socket.connect(hostendpoint)
        except Exception as ex:
            err = ex
        finally:
            self._connect_action_mutex.release()

        if err is None:
            self._commandchannel_responsethread = Thread(target=self._run_commandchannel_responsethread)

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
        connectionbuilder: List[str] = []

        connectionbuilder.append(f"throttled={subscription.throttled}")
        connectionbuilder.append(f";publishInterval={subscription.publishinterval:.6f}")
        connectionbuilder.append(f";includeTime={subscription.includetime}")
        connectionbuilder.append(f";processingInterval={subscription.processinginterval}")
        connectionbuilder.append(f";useMillisecondResolution={subscription.usemillisecondresolution}")
        connectionbuilder.append(f";requestNaNValueFilter={subscription.requestnanvaluefilter}")
        connectionbuilder.append(f";assemblyInfo={{source={self.sttp_sourceinfo}")
        connectionbuilder.append(f";version={self.sttp_versioninfo}")
        connectionbuilder.append(f";updatedOn={self.sttp_updatedoninfo}}}")

        if len(subscription.filterexpression) > 0:
            connectionbuilder.append(f";filterExpression={{{subscription.filterexpression}}}")

        if subscription.udpdatachannel:
            udpport = subscription.datachannel_localport

            try:
                self._datachannel_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self._datachannel_socket.bind(("127.0.0.1", udpport))
            except Exception as ex:
                return RuntimeError(f"failed to open UDP socket for port {udpport}:{ex}")

            self._datachannel_responsethread = Thread(target=self._run_datachannel_responsethread)
            self._datachannel_responsethread.start()

            connectionbuilder.append(f";dataChannel={{localport={udpport}}}")

        if len(subscription.starttime) > 0:
            connectionbuilder.append(f";startTimeConstraint={subscription.starttime}")

        if len(subscription.stoptime) > 0:
            connectionbuilder.append(f";stopTimeConstraint={subscription.stoptime}")

        if len(subscription.constraintparameters) > 0:
            connectionbuilder.append(f";timeConstraintParameters={subscription.constraintparameters}")

        if len(subscription.extra_connectionstring_parameters) > 0:
            connectionbuilder.append(f";{subscription.extra_connectionstring_parameters}")

        connectionstring = "".join(connectionbuilder)
        length = np.uint32(len(connectionstring))
        buffer = bytearray(5 + length)

        buffer[0] = DataPacketFlags.COMPACT
        buffer[1:5] = BigEndian.from_uint32(length)
        buffer[5:] = self.encodestr(connectionstring)

        self.send_servercommand(ServerCommand.SUBSCRIBE, buffer)

        # Reset TSSC decompressor on successful (re)subscription
        self._tssc_lastoosreport_mutex.acquire()
        self._tssc_lastoosreport = datetime.utcnow()
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
            if not autoreconnecting and self._disconnecting and not self._disconnected:
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

        disconnectthread = Thread(target=lambda: self._run_disconnectthread(autoreconnecting))

        self._disconnectthread_mutex.acquire()
        self._disconnectThread = disconnectthread
        self._disconnectthread_mutex.release()

        disconnectthread.start()

        if jointhread and disconnectthread.is_alive():
            disconnectthread.join()

    def _run_disconnectthread(self, autoreconnecting: bool):
        # Let any pending connect operation complete before disconnect - prevents destruction disconnect before connection is completed
        if not autoreconnecting:
            self._connector.cancel()

            self._connection_terminationthread_mutex.acquire()

            if self._connection_terminationthread is not None and self._connection_terminationthread.is_alive():
                self._connection_terminationthread.join()

            self._connection_terminationthread_mutex.release()

            self._connect_action_mutex.acquire()

        # Release queues and close sockets so that threads can shut down gracefully
        if self._commandchannel_socket is not None:
            try:
                self._commandchannel_socket.close()
            except Exception as ex:
                self._dispatch_errormessage(f"Exception while disconnecting data subscriber TCP command channel: {ex}")

        if self._datachannel_socket is not None:
            try:
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
        self._connection_terminationthread_mutex.acquire()

        if self._connection_terminationthread is not None:
            return

        self._connection_terminationthread = Thread(target=self._handle_connectionterminated)
        self._connection_terminationthread.start()

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
        self._reader = BinaryStream(StreamEncoder(
            (lambda length: self._commandchannel_socket.recv(length)),
            (lambda buffer: self._commandchannel_socket.send(buffer))))

        while self._connected:
            try:
                self._reader.read_all(self._readbuffer, 0, PAYLOADHEADER_SIZE)

                # Gather statistics
                self._total_commandchannel_bytesreceived += PAYLOADHEADER_SIZE

                self._read_payloadheader()
            except Exception as ex:
                # DEBUG:
                #self._dispatch_errormessage(f"Exception processing server response: {ex}")

                # Read error, connection may have been closed by peer; terminate connection
                self._dispatch_connectionterminated()
                return

    def _read_payloadheader(self):
        if self._disconnecting:
            return

        packetsize = BigEndian.to_uint32(self._readbuffer)

        if packetsize > len(self._readbuffer):
            self._readbuffer = bytearray(packetsize)

        # Read packet (payload body)
        # This read method is guaranteed not to return until the
        # requested size has been read or an error has occurred.
        self._reader.read_all(self._readbuffer, 0, packetsize)

        # Gather statistics
        self._total_commandchannel_bytesreceived += packetsize

        # Process response
        self._process_serverresponse(bytes(self._readbuffer[:packetsize]))

    # If the user defines a separate UDP channel for their
    # subscription, data packets get handled from this thread.
    def _run_datachannel_responsethread(self):
        reader = StreamEncoder(
            (lambda length: self._datachannel_socket.recv(length)),
            (lambda buffer: self._datachannel_socket.send(buffer)))

        buffer = bytearray(MAXPACKET_SIZE)

        while self._connected:
            try:
                length = reader.read(buffer, 0, MAXPACKET_SIZE)

                # Gather statistics
                self._total_datachannel_bytesreceived += length

                # Process response
                self._process_serverresponse(bytes(self._readbuffer[:length]))
            except:
                # Read error, connection may have been closed by peer; terminate connection
                self._dispatch_connectionterminated()
                return

    def _process_serverresponse(self, buffer: bytes):
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
        elif responsecode == ServerResponse.NOTIFICATION:
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
        elif commandcode == ServerCommand.SUBSCRIBE or commandcode == ServerCommand.UNSUBSCRIBE:
            self._subscribed = commandcode == ServerCommand.SUBSCRIBE
            has_response_message = True
        elif commandcode == ServerCommand.ROTATECIPHERKEYS or commandcode == ServerCommand.UPDATEPROCESSINGINTERVAL:
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
        message: List[str] = []
        message.append(f"Received success code in response to server command: {str(commandcode)}")

        if len(data) > 0:
            message.append("\n")
            message.append(self.decodestr(data))

        self._dispatch_statusmessage("".join(message))

    def _handle_failed(self, commandcode: ServerCommand, data: bytes):
        message: List[str] = []

        if commandcode == ServerCommand.CONNECT:
            self._connector._connectionrefused = True
        else:
            message.append(f"Received failure code in response to server command: {str(commandcode)}")

        if len(data) > 0:
            if len(message) > 0:
                message.append("\n")

            message.append(self.decodestr(data))

        if len(message) > 0:
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
        if len(data) == 0:
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
            self.send_servercommand(ServerCommand.CONFIRMSIGNALINDEXCACHE)

        if self.subscriptionupdated_callback is not None:
            self.subscriptionupdated_callback(signalindexcache)

    def _handle_update_basetimes(self, data: bytes):
        if len(data) == 0:
            return

        self._timeindex = 0 if BigEndian.to_uint32(data) == 0 else 1
        self._basetimeoffsets = [BigEndian.to_uint64(data[4:]), BigEndian.to_uint64(data[12:])]

        self._dispatch_statusmessage(f"Received new base time offset from publisher: {Ticks.tostring(self._basetimeoffsets[self._timeindex ^ 1])}")

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
            cipherindex = 0

            if datapacketflags & DataPacketFlags.CIPHERINDEX > 0:
                cipherindex = 1

            try:
                cipher = AES.new(key_ivs[cipherindex][KEY_INDEX], AES.MODE_CBC, key_ivs[cipherindex][IV_INDEX])
                data = cipher.decrypt(data)
            except Exception as ex:
                self._dispatch_errormessage(f"Failed to decrypt data packet - disconnecting: {ex}")
                self._dispatch_connectionterminated()
                return

        count = BigEndian.to_uint32(data)
        cacheindex = 0

        if datapacketflags & DataPacketFlags.CACHEINDEX > 0:
            cacheindex = 1

        self._signalindexcache_mutex.acquire()
        signalindexcache = self._signalindexcache[cacheindex]
        self._signalindexcache_mutex.release()

        if compressed:
            measurements = self._parse_tssc_measurements(signalindexcache, data[4:], count)
        else:
            measurements = self._parse_compact_measurements(signalindexcache, data[4:], count)

        if self.newmeasurements_callback is not None and len(measurements) > 0:
            # Do not use thread pool here, processing sequence may be important.
            # Execute callback directly from socket processing thread:
            self.newmeasurements_callback(measurements)

        self._total_measurementsreceived += count

    def _parse_tssc_measurements(self, signalindexcache: SignalIndexCache, data: bytes, count: np.uint32) -> List[Measurement]:
        self._dispatch_errormessage("Python TSSC not implemented yet - disconnecting.")
        self._dispatch_connectionterminated()
        return list()

    def _parse_compact_measurements(self, signalindexcache: SignalIndexCache, data: bytes, count: np.uint32) -> List[Measurement]:
        if signalindexcache.count == 0:
            if self._last_missingcachewarning + MISSINGCACHEWARNING_INTERVAL < time():
                # Warning message for missing signal index cache
                if self._last_missingcachewarning > 0.0:
                    self._dispatch_statusmessage("Signal index cache has not arrived. No compact measurements can be parsed.")

                self._last_missingcachewarning = time()

            return list()

        measurements: List[Measurement] = []
        usemillisecondresolution = self.subscription.usemillisecondresolution
        includetime = self.subscription.includetime
        index = 0

        for i in range(count):
            # Deserialize compact measurement format
            measurement = CompactMeasurement(signalindexcache, includetime, usemillisecondresolution, self._basetimeoffsets)
            (bytesdecoded, err) = measurement.decode(data[index:])

            if err is not None:
                self._dispatch_errormessage(f"Failed to parse compact measurements - disconnecting: {err}")
                self._dispatch_connectionterminated()
                return

            index += bytesdecoded
            measurements.append(measurement)

        return measurements

    def _handle_bufferblock(self, data: bytes):
        # Buffer block received - wrap as a BufferBlockMeasurement and expose back to consumer
        sequencenumber = BigEndian.to_uint32(data)
        buffercacheindex = int(sequencenumber - self._bufferblock_expectedsequencenumber)
        signalindexcacheindex = 0

        if self.version > 1 and data[4:][0] > 0:
            signalindexcacheindex = 1

        # Check if this buffer block has already been processed (e.g., mistaken retransmission due to timeout)
        if buffercacheindex >= 0 and (buffercacheindex >= len(self._bufferblock_cache) and self._bufferblock_cache[buffercacheindex].buffer is None):
            # Send confirmation that buffer block is received
            self.send_servercommand(ServerCommand.CONFIRMBUFFERBLOCK, data[:4])

            if self.version > 1:
                data = data[5:]
            else:
                data = data[4:]

            # Get measurement key from signal index cache
            signalindex = BigEndian.to_uint32(data)

            self._signalindexcache_mutex.acquire()
            signalIndexCache = self._signalindexcache[signalindexcacheindex]
            self._signalindexcache_mutex.release()

            signalid = signalIndexCache.signalid(signalindex)
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
                for i in range(len(self._bufferblock_cache), buffercacheindex + 1):
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

        if commandbuffersize > len(self._writebuffer):
            self._writebuffer = bytearray(commandbuffersize)

        # Insert packet size
        self._writebuffer[0:4] = BigEndian.from_uint32(packetsize)

        # Insert command code
        self._writebuffer[4] = commandcode

        if data is not None and len(data) > 0:
            self._writebuffer[5:commandbuffersize] = data

        if commandcode == ServerCommand.METADATAREFRESH:
            # Track start time of metadata request to calculate round-trip receive time
            self._metadata_requested = time()

        try:
            self._commandchannel_socket.send(self._writebuffer[:commandbuffersize])
        except Exception as ex:
            # Write error, connection may have been closed by peer; terminate connection
            self._dispatch_errormessage(f"Failed to send server command - disconnecting: {ex}")
            self._dispatch_connectionterminated()

    def _send_operationalmodes(self):
        operationalModes = CompressionModes.GZIP
        operationalModes |= OperationalModes.VERSIONMASK & self.version
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
        Gets the SubscriberConnector associated with this DataSubscriber.
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
