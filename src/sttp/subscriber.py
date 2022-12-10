# ******************************************************************************************************
#  subscriber.py - Gbtc
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
#  08/23/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from gsf import Limits
from gsf.endianorder import BigEndian
from .data.dataset import DataSet
from .transport.bufferblock import BufferBlock
from .transport.constants import ConnectStatus, ServerCommand, Defaults
from .transport.datasubscriber import DataSubscriber
from .transport.measurement import Measurement
from .transport.signalindexcache import SignalIndexCache
from .config import Config
from .settings import Settings
from .reader import MeasurementReader
from .metadata.cache import MetadataCache
from .metadata.record.measurement import MeasurementRecord
from typing import List, Optional, Callable
from time import time
from uuid import UUID
from threading import Lock
import sys
import numpy as np


class Subscriber:
    """
    Represents an STTP data subscriber.

    Notes
    -----
    The `Subscriber` class exists as a simplified implementation of the `DataSubscriber`
    class found in the `transport` namespace. This class maintains an internal instance
    of the `DataSubscriber` class for subscription based functionality and is intended
    to simplify common uses of STTP data reception.
    """

    def __init__(self):
        """
        Creates a new `Subscriber`.
        """

        # Configuration reference
        self._config = Config()

        # DataSubscriber reference
        self._datasubscriber = DataSubscriber()

        # Callback references
        self._statusmessage_logger: Optional[Callable[[str], None]] = self.default_statusmessage_logger
        self._errormessage_logger: Optional[Callable[[str], None]] = self.default_errormessage_logger
        self._metadatanotification_receiver: Optional[Callable[[DataSet], None]] = None
        self._data_starttime_receiver: Optional[Callable[[np.int64], None]] = None
        self._configurationchanged_receiver: Optional[Callable[[], None]] = None
        self._historicalreadcomplete_receiver: Optional[Callable[[], None]] = None
        self._connectionestablished_receiver: Optional[Callable[[], None]] = self.default_connectionestablished_receiver
        self._connectionterminated_receiver: Optional[Callable[[], None]] = self.default_connectionterminated_receiver

        # MeasurementReader reference
        self._reader: Optional[MeasurementReader] = None

        # Lock used to synchronize console writes
        self._consolelock = Lock()

    def dispose(self):
        """
        Cleanly shuts down a `Subscriber` that is no longer being used, e.g.,
        during a normal application exit.
        """

        if self._datasubscriber is not None:
            self._datasubscriber.dispose()

    @property
    def connected(self) -> bool:
        """
        Gets flag that determines if `Subscriber` is currently connected to a data publisher.
        """

        return self._datasubscriber.connected

    @property
    def subscribed(self) -> bool:
        """
        Gets flag that determines if `Subscriber` is currently subscribed to a data stream.
        """

        return self._datasubscriber.subscribed

    @property
    def metadatacache(self) -> MetadataCache:
        """
        Gets the current metadata cache.
        """

        return self._datasubscriber.metadatacache

    @property
    def activesignalindexcache(self) -> SignalIndexCache:
        """
        Gets the active signal index cache.
        """

        return self._datasubscriber.activesignalindexcache

    @property
    def subscriberid(self) -> UUID:
        """
        Gets the subscriber ID as assigned by the data publisher upon receipt of the `SignalIndexCache`.        
        """

        return self._datasubscriber.subscriberid

    @property
    def total_commandchannel_bytesreceived(self) -> np.uint64:
        """
        Gets the total number of bytes received via the command channel since last connection.
        """

        return self._datasubscriber.total_commandchannel_bytesreceived

    @property
    def total_datachannel_bytesreceived(self) -> np.uint64:
        """
        Gets the total number of bytes received via the data channel since last connection.
        """

        return self._datasubscriber.total_datachannel_bytesreceived

    @property
    def total_measurementsreceived(self) -> np.uint64:
        """
        Gets the total number of measurements received since last subscription.
        """

        return self._datasubscriber.total_measurementsreceived

    def measurement_metadata(self, measurement: Measurement) -> MeasurementRecord:
        """
        Gets the `MeasurementRecord` for the specified measurement, based on its signal ID,
        from the local metadata cache; or, None if the measurement cannot be found.
        """

        return self.metadatacache.find_measurement_signalid(measurement.signalid)

    def adjustedvalue(self, measurement: Measurement) -> np.float64:
        """
        Gets the Value of a `Measurement` with any linear adjustments applied from the
        measurement's Adder and Multiplier metadata, if found.
        """

        return self._datasubscriber.adjustedvalue(measurement)

    def connect(self, address: str, config: Config = ...) -> Optional[Exception]:
        """
        Starts the client-based connection cycle to an STTP publisher. Config parameter controls
        connection related settings. When the config defines `AutoReconnect` as True, the connection
        will automatically be retried when the connection drops. If the config parameter defines
        `AutoRequestMetadata` as True, then upon successful connection, meta-data will be requested.
        When the config defines both `AutoRequestMetadata` and `AutoSubscribe` as True, subscription
        will occur after reception of metadata. When the config defines `AutoRequestMetadata` as
        False and `AutoSubscribe` as True, subscription will occur at successful connection.
        """

        try:
            lastcolonindex = address.rindex(":")
        except ValueError:
            return ValueError("address does not contain a port number, format: hostname:port")

        hostname = address[:lastcolonindex]
        portname = address[lastcolonindex + 1:]

        try:
            port = int(portname)
        except ValueError as ex:
            return ValueError(f"invalid port number \"{portname}\": {ex}")

        if port < 1 or port > Limits.MAXUINT16:
            return ValueError(f"port number \"{portname}\" is out of range: must be 1 to {Limits.MAXUINT16}")

        self._config = Config() if config is ... else config

        return self._connect(hostname, np.uint16(port))

    def _connect(self, hostname: str, port: np.uint16) -> Optional[Exception]:
        ds = self._datasubscriber
        con = ds.connector

        # Set connection properties
        con.hostname = hostname
        con.port = port

        con.maxretries = self._config.maxretries
        con.retryinterval = self._config.retryinterval
        con.maxretryinterval = self._config.maxretryinterval
        con.autoreconnect = self._config.autoreconnect

        ds.compress_payloaddata = self._config.compress_payloaddata
        ds.compress_metadata = self._config.compress_metadata
        ds.compress_signalindexcache = self._config.compress_signalindexcache
        ds.socket_timeout = self._config.socket_timeout
        ds.version = self._config.version

        # Register direct Subscriber callbacks
        con.errormessage_callback = self._errormessage_logger
        ds.statusmessage_callback = self._statusmessage_logger
        ds.errormessage_callback = self._errormessage_logger

        # Register callbacks with intermediate handlers
        con.reconnect_callback = self._handle_reconnect
        ds.metadatareceived_callback = self._handle_metadatareceived
        ds.connectionterminated_callback = self._handle_connectionterminated
        ds.data_starttime_callback = self._handle_data_starttime
        ds.configurationchanged_callback = self._handle_configurationchanged
        ds.processingcomplete_callback = self._handle_processingcomplete

        err: Optional[Exception] = None

        # Connect and subscribe to publisher
        status = con.connect(ds)

        if status == ConnectStatus.SUCCESS:
            if self._connectionestablished_receiver is not None:
                self._connectionestablished_receiver()

            # If automatically parsing metadata, request metadata upon successful connection,
            # after metadata is received the SubscriberInstance will then initiate subscribe;
            # otherwise, subscribe is initiated immediately (when auto subscribe requested)
            if self._config.autorequestmetadata:
                self.request_metadata()
            elif self._config.autosubscribe:
                ds.subscribe()
        elif status == ConnectStatus.FAILED:
            err = RuntimeError("all connection attempts failed")
        elif status == ConnectStatus.CANCELED:
            err = RuntimeError("connection canceled")

        return err

    def disconnect(self):
        """
        Disconnects from an STTP publisher.
        """

        self._datasubscriber.disconnect()

    def request_metadata(self):
        """
        Sends a request to the data publisher indicating that the `Subscriber` would
        like new metadata. Any defined MetadataFilters will be included in request.
        """

        ds = self._datasubscriber

        if len(self._config.metadatafilters) == 0:
            ds.send_servercommand(ServerCommand.METADATAREFRESH)
            return

        filters = ds.encodestr(self._config.metadatafilters)
        buffer = bytearray(4 + len(filters))

        buffer[:4] = BigEndian.from_uint32(np.uint32(len(filters)))
        buffer[4:] = filters

        ds.send_servercommand(ServerCommand.METADATAREFRESH, buffer)

    def subscribe(self, filterexpression: str, settings: Settings = ...):
        """
        Subscribe sets up a request indicating that the `Subscriber` would like to start receiving
        streaming data from a data publisher. If the subscriber is already connected, the updated
        filter expression and subscription settings will be requested immediately; otherwise, the
        settings will be used when the connection to the data publisher is established.

        The filterExpression defines the desired measurements for a subscription. Examples include:

        * Directly specified signal IDs (UUID values in string format):
            38A47B0-F10B-4143-9A0A-0DBC4FFEF1E8; E4BBFE6A-35BD-4E5B-92C9-11FF913E7877

        * Directly specified tag names:
            DOM_GPLAINS-BUS1:VH; TVA_SHELBY-BUS1:VH

        * Directly specified identifiers in "measurement key" format:
            PPA:15; STAT:20

        * A filter expression against a selection view:
            FILTER ActiveMeasurements WHERE Company='GPA' AND SignalType='FREQ'

        Settings parameter controls subscription related settings.        
        """

        ds = self._datasubscriber
        sub = ds.subscription

        settings = Settings() if settings is ... else settings

        sub.filterexpression = filterexpression
        sub.throttled = settings.throttled
        sub.publishinterval = settings.publishinterval

        if settings.udpport > 0:
            sub.udpdatachannel = True
            sub.datachannel_localport = settings.udpport
            sub.datachannel_interface = settings.udpinterface
        else:
            sub.udpdatachannel = False
            sub.datachannel_localport = Defaults.DATACHANNEL_LOCALPORT
            sub.datachannel_interface = Defaults.DATACHANNEL_INTERFACE

        sub.includetime = settings.includetime
        sub.enabletimereasonabilitycheck = settings.enabletimereasonabilitycheck
        sub.lagtime = settings.lagtime
        sub.leadtime = settings.leadtime
        sub.uselocalclockasrealtime = settings.uselocalclockasrealtime
        sub.use_millisecondresolution = settings.use_millisecondresolution
        sub.request_nanvaluefilter = settings.request_nanvaluefilter
        sub.starttime = settings.starttime
        sub.stoptime = settings.stoptime
        sub.constraintparameters = settings.constraintparameters
        sub.processinginterval = settings.processinginterval
        sub.extra_connectionstring_parameters = settings.extra_connectionstring_parameters

        if ds.connected:
            ds.subscribe()

    def unsubscribe(self):
        """
        Sends a request to the data publisher indicating that the Subscriber would
        like to stop receiving streaming data.
        """

        self._datasubscriber.unsubscribe()

    def read_measurements(self) -> MeasurementReader:
        """
        Sets up a new `MeasurementReader` to start reading measurements.
        """

        if self._reader is None:
            self._reader = MeasurementReader(self)

        return self._reader

    # Local callback handlers:

    def statusmessage(self, message: str):
        """
        Executes the defined status message logger callback.
        """

        if self._statusmessage_logger is not None:
            self._statusmessage_logger(message)

    def errormessage(self, message: str):
        """
        Executes the defined error message logger callback.
        """

        if self._errormessage_logger is not None:
            self._errormessage_logger(message)

    # Intermediate callback handlers:

    def _handle_reconnect(self, ds: DataSubscriber):
        if ds.connected:
            if self._connectionestablished_receiver is not None:
                self._connectionestablished_receiver()

            # If automatically parsing metadata, request metadata upon successful connection,
            # after metadata is received the SubscriberInstance will then initiate subscribe;
            # otherwise, subscribe is initiated immediately (when auto subscribe requested)
            if self._config.autorequestmetadata:
                self.request_metadata()
            elif self._config.autosubscribe:
                ds.subscribe()
        else:
            ds.disconnect()
            self.statusmessage("Connection retry attempts exceeded.")

    def _handle_metadatareceived(self, metadata: bytes):
        parsestarted = time()
        dataset, err = DataSet.from_xml(metadata)

        if err is not None:
            self.errormessage(f"Failed to parse metadata: {err}")
            return

        # Generate a record model focused implementation of parsed XML
        # metadata with lookup maps to simplify typical metadata usages
        self._datasubscriber.metadatacache = MetadataCache(dataset)

        self._show_metadatasummary(dataset, parsestarted)

        if self._metadatanotification_receiver is not None:
            self._metadatanotification_receiver(dataset)

        if self._config.autorequestmetadata and self._config.autosubscribe:
            self._datasubscriber.subscribe()

    def _show_metadatasummary(self, dataset: DataSet, parsestarted: float):
        tabledetails = ["    Discovered:\n"]
        totalrows = 0

        for table in dataset:
            tablename = table.name
            tablerows = table.rowcount
            totalrows += tablerows
            tabledetails.append(f"        {tablerows:,} {tablename} records\n")

        message = [
            f"Parsed {totalrows:,} metadata records in {(time() - parsestarted):.3f} seconds\n",
            "".join(tabledetails)
        ]

        schemaversion = dataset["SchemaVersion"]

        if schemaversion is not None:
            message.append(f"Metadata schema version: {schemaversion.rowvalue_as_string_byname(0, 'VersionNumber')}")
        else:
            message.append("No SchemaVersion table found in metadata")

        self.statusmessage("".join(message))

    def _handle_connectionterminated(self):
        # Release any blocking reader
        if self._reader is not None:
            self.set_newmeasurements_receiver(None)
            self._reader.dispose()
            self._reader = None

        if self._connectionterminated_receiver is not None:
            self._connectionterminated_receiver()

    def _handle_data_starttime(self, starttime: np.int64):
        if self._data_starttime_receiver is not None:
            self._data_starttime_receiver(starttime)

    def _handle_configurationchanged(self):
        if self._configurationchanged_receiver is not None:
            self._configurationchanged_receiver()

    def _handle_processingcomplete(self, message: str):
        self.statusmessage(message)

        if self._historicalreadcomplete_receiver is not None:
            self._historicalreadcomplete_receiver()

    def default_statusmessage_logger(self, message: str):
        """
        Implements the default handler for the status message callback.
        Default implementation synchronously writes output to stdio.
        Logging is recommended.
        """

        self._consolelock.acquire()

        try:
            print(message)
        finally:
            self._consolelock.release()

    def default_errormessage_logger(self, message: str):
        """
        Implements the default handler for the error message callback.
        Default implementation synchronously writes output to stderr.
        Logging is recommended.
        """

        self._consolelock.acquire()

        try:
            print(message, file=sys.stderr)
        finally:
            self._consolelock.release()

    def default_connectionestablished_receiver(self):
        """
        Implements the default handler for the connection established callback.
        Default implementation simply writes connection feedback to status message callback.
        """

        con = self._datasubscriber.connector
        self.statusmessage(f"Connection to {con.hostname}:{con.port} established.")

    def default_connectionterminated_receiver(self):
        """
        Implements the default handler for the connection terminated callback.
        Default implementation simply writes connection terminated feedback to error message callback.
        """

        con = self._datasubscriber.connector
        self.errormessage(f"Connection to {con.hostname}:{con.port} terminated.")

    def set_statusmessage_logger(self, callback: Optional[Callable[[str], None]]):
        """
        Defines the callback that handles informational message logging.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._statusmessage_logger = callback

    def set_errormessage_logger(self, callback: Optional[Callable[[str], None]]):
        """
        Defines the callback that handles error message logging.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._errormessage_logger = callback

    def set_metadatanotification_receiver(self, callback: Optional[Callable[[DataSet], None]]):
        """
        Defines the callback that handles reception of the metadata received notification response.
        Receiver parameter defines full XML response received from publisher.
        Parsed metadata available via `Subscriber.metadatacache` property.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._metadatanotification_receiver = callback

    def set_subscriptionupdated_receiver(self, callback: Optional[Callable[[SignalIndexCache], None]]):
        """
        Defines the callback that handles notifications that a new `SignalIndexCache` has been received.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._datasubscriber.subscriptionupdated_callback = callback

    def set_data_starttime_receiver(self, callback: Optional[Callable[[np.int64], None]]):
        """
        Defines the callback that handles notification of first received measurement.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._data_starttime_receiver = callback

    def set_configurationchanged_receiver(self, callback: Optional[Callable[[], None]]):
        """
        Defines the callback that handles notifications that the data publisher configuration has changed.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._configurationchanged_receiver = callback

    def set_newmeasurements_receiver(self, callback: Optional[Callable[[List[Measurement]], None]]):
        """
        Defines the callback that handles reception of new measurements.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._datasubscriber.newmeasurements_callback = callback

    def set_newbufferblock_receiver(self, callback: Optional[Callable[[List[BufferBlock]], None]]):
        """
        Defines the callback that handles reception of new buffer blocks.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._datasubscriber.newbufferblocks_callback = callback

    def set_notification_receiver(self, callback: Optional[Callable[[str], None]]):
        """
        Defines the callback that handles reception of a notification.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._datasubscriber.notificationreceived_callback = callback

    def set_historicalreadcomplete_receiver(self, callback: Optional[Callable[[], None]]):
        """
        Defines the callback that handles notification that temporal processing has completed, i.e.,
        the end of a historical playback data stream has been reached.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._historicalreadcomplete_receiver = callback

    def set_connectionestablished_receiver(self, callback: Optional[Callable[[], None]]):
        """
        Defines the callback that handles notification that a connection has been established.
        Default implementation simply writes connection feedback to status message handler.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._connectionestablished_receiver = callback

    def set_connectionterminated_receiver(self, callback: Optional[Callable[[], None]]):
        """
        Defines the callback that handles notification that a connection has been terminated.
        Default implementation simply writes connection terminated feedback to error message handler.
        Assignment will take effect immediately, even while subscription is active.
        """

        self._connectionterminated_receiver = callback
