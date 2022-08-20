#******************************************************************************************************
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
#******************************************************************************************************

from gsf import Empty
from gsf.endianorder import BigEndian
from gsf.binarystream import BinaryStream
from gsf.streamencoder import StreamEncoder
from measurement import Measurement
from measurementmetadata import MeasurementMetadata
from bufferblock import BufferBlock
from constants import *
from subscriptioninfo import SubscriptionInfo
from subscriberconnector import SubscriberConnector
from signalindexcache import SignalIndexCache
from version import Version
from typing import List, Dict, Callable, Optional
from datetime import datetime
from uuid import UUID
from threading import Lock, Thread, Event
from readerwriterlock.rwlock import RWLockRead
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

        self._assigninghandler_mutex = RWLockRead()
        self._assigninghandler_readmutex = self._assigninghandler_mutex.gen_rlock()
        self._assigninghandler_writemutex = self._assigninghandler_mutex.gen_wlock()

        self._connect_action_mutex = Lock()
        self._connection_terminationthread: Thread(target=lambda self: self._disconnect(False, True))

        self._disconnectthread: Optional[Thread] = None
        self._disconnectthread_mutex = Lock()
        self._disconnecting = False
        self._disconnected = False
        self._disposing = False

        # Statistics counters
        self._total_commandchannel_bytesreceived = np.uint64(0)
        self._total_datachannel_bytesreceived = np.uint64(0)
        self._total_measurements_received = np.uint64(0)

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

        # Measurement parsing
        self._metadatarequested = Empty.DATETIME
        self._measurementregistry: Dict[UUID, MeasurementMetadata] = dict()
        self._signalindexcache: List[SignalIndexCache] = list()
        self._signalindexcache_mutex = Lock()
        self._cacheindex = np.int32(0)
        self._timeindex = np.int32(0)
        self._basetimeoffsets = np.empty(2, dtype=np.int64)
        self._key_ivs = np.empty((2, 2), dtype=bytes)
        self._last_missingcachewarning = Empty.DATETIME
        self._tssc_resetrequested = bool
        self._tssc_lastoosreport = Empty.DATETIME
        self._tssc_lastoosreport_mutex = Lock()

        self._bufferblock_expectedsequencenumber = np.uint32(0)
        self._bufferblock_cache: List[BufferBlock] = list()

    def dispose(self):
        """
        Cleanly shuts down a `DataSubscriber` that is no longer being used, e.g., during a normal application exit.
        """

        self._disposing = True
        self._connector.cancel()
        self._disconnect(True, False)

        # Allow a moment for connection terminated event to complete
        Event().wait(0.01)  # 10ms

    def begincallbackassignment(self):
        """
        Informs `DataSubscriber` that a callback change has been initiated.
        """

        self._assigninghandler_writemutex.acquire()

    def begincallbacksync(self):
        """
        Begins a callback synchronization operation.
        """

        self._assigninghandler_readmutex.acquire()

    def endcallbacksync(self):
        """
        Ends a callback synchronization operation.
        """

        self._assigninghandler_readmutex.release()

    def endcallbackassignment(self):
        """
        Informs `DataSubscriber` that a callback change has been completed.
        """

        self._assigninghandler_writemutex.release()

    def isconnected(self) -> bool:
        """
        Determines if a `DataSubscriber` is currently connected to a `DataPublisher`.
        """

        return self._connected

    def issubscribed(self) -> bool:
        """
        Determines if a `DataSubscriber` is currently subscribed to a data stream.
        """

        return self._subscribed

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

    def lookupmetadata(self, signalid: UUID) -> MeasurementMetadata:
        """
        Gets the `MeasurementMetadata` for the specified signal ID from the local registry. If the metadata does not exist, a new record is created and returned.
        """
        if signalid in self._measurementregistry:
            return self._measurementregistry[signalid]

        metadata = MeasurementMetadata(signalid)
        self._measurementregistry[signalid] = metadata
        return metadata

    def metadata(self, measurement: Measurement) -> MeasurementMetadata:
        """
        Gets the `MeasurementMetadata` associated with a measurement from the local registry. If the metadata does not exist, a new record is created and returned.
        """

        return self.lookupmetadata(measurement.signalid)

    def adjustedvalue(self, measurement: Measurement) -> np.float64:
        """
        Gets the Value of a `Measurement` with any linear adjustments applied from the measurement's Adder and Multiplier metadata, if found.
        """

        if measurement.signalid in self._measurementregistry:
            metadata = self._measurementregistry[measurement.signalid]
            return measurement.value * metadata.multiplier + metadata.adder

        return measurement.value

    def connect(self, hostname: str, port: np.uint16) -> Optional[Exception]:
        """
        Requests the the `DataSubscriber` initiate a connection to the `DataPublisher`.
        """

        #  User requests to connection are not an auto-reconnect attempt
        return self._connect(hostname, port, False)

    def _connect(self, hostname: str, port: np.uint16, autoreconnecting: bool) -> Optional[Exception]:
        if self._connected:
            raise RuntimeError("subscriber is already connected; disconnect first")

        # Make sure any pending disconnect has completed to make sure socket is closed
        self._disconnectthread_mutex.acquire()
        disconnectthread = self._disconnectthread
        self._disconnectthread_mutex.release()

        if disconnectthread is not None:
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

            self._key_ivs = np.empty((2, 2), dtype=bytes)
            self._bufferblock_expectedsequencenumber = np.uint32(0)
            self._measurementregistry = dict()

            if not autoreconnecting:
                self._connector.resetconnection()

            self._connector._connectionrefused = False

            # TODO: Add TLS implementation options
            # TODO: Add reverse (server-based) connection options, see:
            # https://sttp.github.io/documentation/reverse-connections/

            self._commandchannel_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
            self._commandchannel_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            try:
                hostendpoint = socket.getaddrinfo(hostname, port, family=socket.AF_INET, proto=socket.IPPROTO_TCP)[0][4]
            except:
                hostendpoint = (self.hostAddress, self.port)

            self._commandchannel_socket.connect(hostendpoint)
        except BaseException as ex:
            err = ex
        finally:
            self._connect_action_mutex.release()

        if err is None:
            self._commandchannel_responsethread = Thread(target=self._run_commandchannel_responsethread)
            self._commandchannel_responsethread.run()

            self._connected = True
            self._last_missingcachewarning = Empty.DATETIME
            self._send_operationalmodes()

        return err

    def subscribe(self) -> Optional[Exception]:
        """
        Notifies the `DataPublisher` that a `DataSubscriber` would like to start receiving streaming data.
        """

        if not self._connected:
            return RuntimeError("subscriber is not connected; cannot subscribe")

        self._total_measurements_received = np.uint64(0)

        subscription = self._subscription
        connectionbuilder = []

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
            except BaseException as ex:
                return RuntimeError(f"failed to open UDP socket for port {udpport}:{ex}")

            self._datachannel_responsethread = Thread(target=self._run_datachannel_responsethread)
            self._datachannel_responsethread.run()

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
        BigEndian.putuint32(buffer[1:], length)
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

    def disconnect(self):
        """
        Initiates a DataSubscriber disconnect sequence.
        """

        if self._disconnecting:
            return

        # Disconnect method executes shutdown on a separate thread without stopping to prevent
        # issues where user may call disconnect method from a dispatched event thread. Also,
        # user requests to disconnect are not an auto-reconnect attempt
        self._disconnect(False, False)

    def _disconnect(self, jointhread: bool, autoreconnecting: bool):
        pass

    def _run_disconnectthread(self):
        pass

    def _dispatch_connectionterminated(self):
        pass

    def _dispatch_statusmessage(message: str):
        pass

    def _dispatch_errormessage(message: str):
        pass

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
            except:
                # Read error, connection may have been closed by peer; terminate connection
                self._dispatch_connectionterminated()
                return

    def _read_payloadheader(self):
        if self._disconnecting:
            return

        packetsize = BigEndian.uint32(self._readbuffer)

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
        elif responsecode == ServerResponse.DataPacket:
            self._handle_datapacket(data)
        elif responsecode == ServerResponse.DataStartTime:
            self._handle_datastarttime(data)
        elif responsecode == ServerResponse.ProcessingComplete:
            self._handle_processingcomplete(data)
        elif responsecode == ServerResponse.UpdateSignalIndexCache:
            self._handle_update_signalindexcache(data)
        elif responsecode == ServerResponse.UpdateBaseTimes:
            self._handle_update_basetimes(data)
        elif responsecode == ServerResponse.UpdateCipherKeys:
            self._handle_update_cipherkeys(data)
        elif responsecode == ServerResponse.ConfigurationChanged:
            self._handle_configurationchanged()
        elif responsecode == ServerResponse.BufferBlock:
            self._handle_bufferblock(data)
        elif responsecode == ServerResponse.Notification:
            self._handle_notification(data)
        elif responsecode == ServerResponse.NoOP:
            # NoOP Handled
            pass
        else:
            self._dispatch_errormessage(f"Encountered unexpected server response code: {str(responsecode)}")

    def _handle_succeeded(self, commandcode: ServerCommand, data: bytes):
        pass

    def _handle_failed(self, commandcode: ServerCommand, data: bytes):
        pass

    def _handle_metadatarefresh(self, data: bytes):
        pass

    def _handle_datastarttime(self, data: bytes):
        pass

    def _handle_processingcomplete(self, data: bytes):
        pass

    def _handle_update_signalindexcache(self, data: bytes):
        pass

    def _handle_update_basetimes(self, data: bytes):
        pass

    def _handle_update_cipherkeys(self, data: bytes):
        pass

    def _handle_configurationchanged(self, data: bytes):
        pass

    def _handle_datapacket(self, data: bytes):
        pass

    def _parse_tssc_measurements(self, signalindexcache: SignalIndexCache, data: bytes, measurements: List[Measurement]):
        pass

    def _parse_compact_measurements(self, signalindexcache: SignalIndexCache, datapacketflags: DataPacketFlags, data: bytes, measurements: List[Measurement]):
        pass

    def _handle_bufferblock(self, data: bytes):
        pass

    def _handle_notification(self, data: bytes):
        pass

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

    def _send_operationalmodes(self):
        pass

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

        return self._total_measurements_received
