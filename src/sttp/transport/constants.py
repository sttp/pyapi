# ******************************************************************************************************
#  constants.py - Gbtc
#
#  Copyright © 2022, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
#  the NOTICE file distributed with this work for additional information regarding copyright ownership.
#  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
#  file except in compliance with the License. You may obtain a copy of the License at =
#
#      http =//opensource.org/licenses/MIT
#
#  Unless agreed to in writing, the subject software distributed under the License is distributed on an
#  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
#  License for the specific language governing permissions and limitations.
#
#  Code Modification History =
#  ----------------------------------------------------------------------------------------------------
#  08/14/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from gsf import Empty
from enum import IntEnum, IntFlag
import numpy as np


class Defaults:
    MAXRETRIES = -1
    """
    Default for maximum number of retries for a connection attempt.
    """

    RETRYINTERVAL = 1.0
    """
    Default for retry interval in seconds.
    """

    MAXRETRYINTERVAL = 30.0
    """
    Default for maximum retry interval in seconds.
    """

    AUTORECONNECT = True
    """
    Default for auto-reconnect flag.
    """

    AUTOREQUESTMETADATA = True
    """
    Default for auto-request metadata flag.
    """

    AUTOSUBSCRIBE = True
    """
    Default for auto-subscribe flag.
    """

    COMPRESS_PAYLOADDATA = True
    """
    Default for compress payload data flag.
    """

    COMPRESS_METADATA = True
    """
    Default for compress metadata flag.
    """

    COMPRESS_SIGNALINDEXCACHE = True
    """
    Default for compress signal index cache flag.
    """

    METADATAFILTERS = Empty.STRING
    """
    Default for metadata filters.
    """

    SOCKET_TIMEOUT = 2.0
    """
    Default for socket timeout in seconds.
    """

    VERSION = np.byte(2)
    """
    Default for STTP version.
    """

    FILTEREXPRESSION = Empty.STRING
    """
    Default for filter expression.
    """

    THROTTLED = False
    """
    Default for throttled flag.
    """

    PUBLISHINTERVAL = np.float64(1.0)
    """
    Default for publish interval in seconds.
    """

    UDPDATACHANNEL = False
    """
    Default for UDP data channel flag.
    """

    DATACHANNEL_LOCALPORT = np.uint16(0)
    """
    Default for local port for data channel.
    """

    DATACHANNEL_INTERFACE = ""
    """
    Default for interface for data channel.
    """

    INCLUDETIME = True
    """
    Default for include time flag.
    """

    ENABLE_TIME_REASONABILITY_CHECK = False
    """
    Default for enable time reasonability check flag.
    """

    LAGTIME = np.float64(10.0)
    """
    Default for lag time in seconds.
    """

    LEADTIME = np.float64(5.0)
    """
    Default for lead time in seconds.
    """

    USE_LOCALCLOCK_AS_REALTIME = False
    """
    Default for use local clock as real time flag.
    """

    USE_MILLISECONDRESOLUTION = False
    """
    Default for use millisecond resolution flag.
    """

    REQUEST_NANVALUEFILTER = False
    """
    Default for request nan-value filter flag.
    """

    STARTTIME = Empty.STRING
    """
    Default for start time.
    """

    STOPTIME = Empty.STRING
    """
    Default for stop time.
    """

    CONSTRAINTPARAMETERS = Empty.STRING
    """
    Default for constraint parameters.
    """

    PROCESSINGINTERVAL = np.int32(-1)
    """
    Default for processing interval in seconds.
    """

    EXTRA_CONNECTIONSTRING_PARAMETERS = Empty.STRING
    """
    Default for extra connection string parameters.
    """


class StateFlags(IntFlag):
    """
    Enumeration of the possible quality states of a `Measurement` value.
    """

    NORMAL = 0x0
    """
    Measurement flag for a normal state.
    """

    BADDATA = 0x1
    """
    Measurement flag for a bad data state.
    """

    SUSPECTDATA = 0x2
    """
    Measurement flag for a suspect data state.
    """

    OVERRANGEERROR = 0x4
    """
    Measurement flag for an over range error, i.e., unreasonable high value.
    """

    UNDERRANGEERROR = 0x8
    """
    Measurement flag for an under range error, i.e., unreasonable low value.
    """

    ALARMHIGH = 0x10
    """
    Measurement flag for an alarm for high value.
    """

    ALARMLOW = 0x20
    """
    Measurement flag for an alarm for low value.
    """

    WARNINGHIGH = 0x40
    """
    Measurement flag for a warning for high value.
    """

    WARNINGLOW = 0x80
    """
    Measurement flag for a warning for low value.
    """

    FLATLINEALARM = 0x100
    """
    Measurement flag for an alarm for flat-lined value, i.e., latched value test alarm.
    """

    COMPARISONALARM = 0x200
    """
    Measurement flag for a comparison alarm, i.e., outside threshold of comparison with a real-time value.
    """

    ROCALARM = 0x400
    """
    Measurement flag for a rate-of-change alarm.
    """

    RECEIVEDASBAD = 0x800
    """
    Measurement flag for a bad value received.
    """

    CALCULATEDVALUE = 0x1000
    """
    Measurement flag for a calculated value state.
    """

    CALCULATIONERROR = 0x2000
    """
    Measurement flag for a calculation error with the value.
    """

    CALCULATIONWARNING = 0x4000
    """
    Measurement flag for a calculation warning with the value.
    """

    RESERVEDQUALITYFLAG = 0x8000
    """
    Measurement flag for a reserved quality.
    """

    BADTIME = 0x10000
    """
    Measurement flag for a bad time state.
    """

    SUSPECTTIME = 0x20000
    """
    Measurement flag for a suspect time state.
    """

    LATETIMEALARM = 0x40000
    """
    Measurement flag for a late time alarm.
    """

    FUTURETIMEALARM = 0x80000
    """
    Measurement flag for a future time alarm.
    """

    UPSAMPLED = 0x100000
    """
    Measurement flag for an up-sampled state.
    """

    DOWNSAMPLED = 0x200000
    """
    Measurement flag for a down-sampled state.
    """

    DISCARDEDVALUE = 0x400000
    """
    Measurement flag for a discarded value state.
    """

    RESERVEDTIMEFLAG = 0x800000
    """
    Measurement flag for a reserved time state.
    """

    USERDEFINEDFLAG1 = 0x1000000
    """
    Measurement flag for user defined state 1.
    """

    USERDEFINEDFLAG2 = 0x2000000
    """
    Measurement flag for user defined state 2.
    """

    USERDEFINEDFLAG3 = 0x4000000
    """
    Measurement flag for user defined state 3.
    """

    USERDEFINEDFLAG4 = 0x8000000
    """
    Measurement flag for user defined state 4.
    """

    USERDEFINEDFLAG5 = 0x10000000
    """
    Measurement flag for user defined state 5.
    """

    SYSTEMERROR = 0x20000000
    """
    Measurement flag for a system error state.
    """

    SYSTEMWARNING = 0x40000000
    """
    Measurement flag for a system warning state.
    """

    MEASUREMENTERROR = 0x80000000
    """
    Measurement flag for a measurement error state.
    """


class DataPacketFlags(IntFlag):
    """
    Enumeration of the possible flags for a data packet.
    """

    COMPACT = 0x02
    """
    Determines if serialized measurement is compact. 
    
    Obsolete: Bit will be removed in future version. Currently this bit is always set.
    """

    CIPHERINDEX = 0x04
    """
    Determines which cipher index to use when encrypting data packet.
    
    Bit set = use odd cipher index (i.e., 1), bit clear = use even cipher index (i.e., 0).    
    """

    COMPRESSED = 0x08
    """
    Determines if data packet payload is compressed.
    
    Bit set = payload compressed, bit clear = payload normal.
    """

    CACHEINDEX = 0x10
    """
    Determines which signal index cache to use when decoding a data packet. Used by STTP version 2 or greater.
    
    Bit set = use odd cache index (i.e., 1), bit clear = use even cache index (i.e., 0).
    """

    NOFLAGS = 0x0
    """
    Defines state where there are no flags set.
    """


class ServerCommand(IntEnum):
    """
    Enumeration of the possible server commands received by a `DataPublisher` and sent by a `DataSubscriber` during an STTP session.
    """

    # Although the server commands and responses will be on two different paths, the response enumeration values
    # are defined as distinct from the command values to make it easier to identify codes from a wire analysis.

    CONNECT = 0x00
    """
    Command code handling connect operations.
    
    Only used as part of connection refused response -- value not sent on the wire.
    """

    METADATAREFRESH = 0x01
    """
    Command code for requesting an updated set of metadata.
    """

    SUBSCRIBE = 0x02
    """
    Command code for requesting a subscription of streaming data from server based on connection string that follows.
    """

    UNSUBSCRIBE = 0x03
    """
    Command code for requesting that server stop sending streaming data to the client and cancel the current subscription.
    """

    ROTATECIPHERKEYS = 0x04
    """
    Command code for manually requesting that server send a new set of cipher keys for data packet encryption (UDP only).
    """

    UPDATEPROCESSINGINTERVAL = 0x05
    """
    Command code for manually requesting that server to update the processing interval with the following specified value.
    """

    DEFINEOPERATIONALMODES = 0x06
    """
    Command code for establishing operational modes.
    
    As soon as connection is established, requests that server set operational modes that affect how the subscriber and publisher will communicate.
    """

    CONFIRMNOTIFICATION = 0x07
    """
    Command code for receipt of a notification.
    
    This message is sent in response to ServerResponse.Notify.
    """

    CONFIRMBUFFERBLOCK = 0x08
    """
    Command code for receipt of a buffer block measurement.
    
    This message is sent in response to ServerResponse.BufferBlock.
    """

    CONFIRMUPDATEBASETIMES = 0x09
    """
    Command code for receipt of a base time update.
    
    This message is sent in response to ServerResponse.UpdateBaseTimes.
    """

    CONFIRMUPDATESIGNALINDEXCACHE = 0x0A
    """
    Command code for confirming the receipt of a signal index cache.
    
    This allows publisher to safely transition to next signal index cache.
    """

    CONFIRMUPDATECIPHERKEYS = 0x0B
    """
    Command code for confirming the receipt of a cipher key update.

    This verifies delivery of the cipher keys indicating that it is safe to transition to the new keys.
    """

    GETPRIMARYMETADATASCHEMA = 0x0C
    """
    Command code for requesting the primary metadata schema.
    """

    GETSIGNALSELECTIONSCHEMA = 0x0D
    """
    Command code for requesting the signal selection schema.
    """

    USERCOMMAND00 = 0xD0
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND01 = 0xD1
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND02 = 0xD2
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND03 = 0xD3
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND04 = 0xD4
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND05 = 0xD5
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND06 = 0xD6
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND07 = 0xD7
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND08 = 0xD8
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND09 = 0xD9
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND10 = 0xDA
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND11 = 0xDB
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND12 = 0xDC
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND13 = 0xDD
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND14 = 0xDE
    """
    Command code handling user-defined commands.
    """

    USERCOMMAND15 = 0xDF
    """
    Command code handling user-defined commands.
    """


class ServerResponse(IntEnum):
    """
    Enumeration of the possible server responses received sent by a `DataPublisher` and received by a `DataSubscriber` during an STTP session.
    """

    # Although the server commands and responses will be on two different paths, the response enumeration values
    # are defined as distinct from the command values to make it easier to identify codes from a wire analysis.

    SUCCEEDED = 0x80
    """
    Response code indicating a succeeded response.
    
    Informs client that its solicited server command succeeded, original command and success message follow.
    """

    FAILED = 0x81
    """
    Response code indicating a failed response.
    
    Informs client that its solicited server command failed, original command and failure message follow.
    """

    DATAPACKET = 0x82
    """
    Response code indicating a data packet.
    
    Unsolicited response informs client that a data packet follows.
    """

    UPDATESIGNALINDEXCACHE = 0x83
    """
    Response code indicating a signal index cache update.
    
    Unsolicited response requests that client update its runtime signal index cache with the one that follows.
    """

    UPDATEBASETIMES = 0x84
    """
    Response code indicating a runtime base-timestamp offsets have been updated.
    
    Unsolicited response requests that client update its runtime base-timestamp offsets with those that follow.
    """

    UPDATECIPHERKEYS = 0x85
    """
    Response code indicating a runtime cipher keys have been updated.
    
    Response, solicited or unsolicited, requests that client update its runtime data cipher keys with those that follow.
    """

    DATASTARTTIME = 0x86
    """
    Response code indicating the start time of data being published.
    
    Unsolicited response provides the start time of data being processed from the first measurement.
    """

    PROCESSINGCOMPLETE = 0x87
    """
    Response code indicating that processing has completed.
    
    Unsolicited response provides notification that input processing has completed, typically via temporal constraint.
    """

    BUFFERBLOCK = 0x88
    """
    Response code indicating a buffer block.
    
    Unsolicited response informs client that a raw buffer block follows.
    """

    NOTIFY = 0x89
    """
    Response code indicating a notification.
    
    Unsolicited response provides a notification message to the client.
    """

    CONFIGURATIONCHANGED = 0x8A
    """
    Response code indicating a that the publisher configuration metadata has changed.
    
    Unsolicited response provides a notification that the publisher's source configuration has changed and that client may want to request a meta-data refresh.
    """

    USERRESPONSE00 = 0xE0
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE01 = 0xE1
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE02 = 0xE2
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE03 = 0xE3
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE04 = 0xE4
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE05 = 0xE5
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE06 = 0xE6
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE07 = 0xE7
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE08 = 0xE8
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE09 = 0xE9
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE10 = 0xEA
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE11 = 0xEB
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE12 = 0xEC
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE13 = 0xED
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE14 = 0xEE
    """
    Response code handling user-defined responses.
    """

    USERRESPONSE15 = 0xEF
    """
    Response code handling user-defined responses.
    """

    NOOP = 0xFF
    """
    Response code indicating an empty-operation keep-alive ping.
    
    The command channel can remain quiet for some time, this command allows a period test of client connectivity.
    """


class OperationalModes(IntFlag):
    """
    Enumeration of the possible modes that affect how `DataPublisher` and `DataSubscriber` communicate during an STTP session.
    """

    # Operational modes are sent from a subscriber to a publisher to request operational behaviors for the
    # connection, as a result the operation modes must be sent before any other command. The publisher may
    # silently refuse some requests (e.g., compression) based on its configuration. Operational modes only
    # apply to fundamental protocol control.

    VERSIONMASK = 0x000000FF
    """
    Bit mask used to get version number of protocol.
    
    Version number is currently set to 2.
    """

    ENCODINGMASK = 0x00000300
    """
    Bit mask used to get character encoding used when exchanging messages between publisher and subscriber.

    STTP currently only supports UTF-8 string encoding.
    """

    IMPLEMENTATIONSPECIFICEXTENSIONMASK = 0x00FF0000
    """
    Bit mask used to apply an implementation-specific extension to STTP.

    If the value is zero, no implementation specific extensions are applied.
    If the value is non-zero, an implementation specific extension is applied, and all parties need to coordinate and agree to the extension.
    If extended flags are unsupported, returned failure message text should be prefixed with UNSUPPORTED EXTENSION: as the context reference.
    """

    RECEIVEEXTERNALMETADATA = 0x02000000
    """
    Bit flag used to determine whether external measurements are exchanged during metadata synchronization.
    
    Bit set = external measurements are exchanged, bit clear = no external measurements are exchanged.
    """

    RECEIVEINTERNALMETADATA = 0x04000000
    """
    Bit flag used to determine whether internal measurements are exchanged during metadata synchronization.
    
    Bit set = internal measurements are exchanged, bit clear = no internal measurements are exchanged.
    """

    COMPRESSPAYLOADDATA = 0x20000000
    """
    Bit flag used to determine whether payload data is compressed when exchanging between publisher and subscriber.
    
    Bit set = compress, bit clear = no compression.
    """

    COMPRESSSIGNALINDEXCACHE = 0x40000000
    """
    Bit flag used to determine whether the signal index cache is compressed when exchanging between publisher and subscriber.
    
    Bit set = compress, bit clear = no compression.
    """

    COMPRESSMETADATA = 0x80000000
    """
    Bit flag used to determine whether metadata is compressed when exchanging between publisher and subscriber.
    
    Bit set = compress, bit clear = no compression.
    """

    NOFLAGS = 0x00000000
    """
    State where there are no flags set.
    """


class OperationalEncoding(IntEnum):
    """
    Enumeration of the possible string encoding options of an STTP session.
    """

    UTF16LE = 0x00000000
    """
    Targets little-endian 16-bit Unicode character encoding for strings.

    Obsolete: STTP currently only supports UTF-8 string encoding.
    """

    UTF16BE = 0x00000100
    """
    Targets big-endian 16-bit Unicode character encoding for strings.

    Obsolete: STTP currently only supports UTF-8 string encoding.
    """

    UTF8 = 0x00000200
    """
    Targets 8-bit variable-width Unicode character encoding for strings.
    """


class CompressionModes(IntFlag):
    """
    Enumeration of the possible compression modes supported by STTP.

    Obsolete: Only used for backwards compatibility with pre-standard STTP implementations.
    OperationalModes now supports custom compression types
    """

    GZIP = 0x00000020
    """
    Bit flag used determine if GZip compression will be used to metadata exchange.
    """

    TSSC = 0x00000040
    """
    Bit flag used determine if the time-series special compression algorithm will be used for data exchange.
    """

    NOFLAGS = 0x00000000
    """
    Defines state where no compression will be used.
    """


class SecurityMode(IntEnum):
    """
    Enumeration of the possible security modes used by the DataPublisher to secure data sent over the command channel in STTP.
    """

    OFF = 0
    """
    Defines security mode where data will be sent over the wire unencrypted.
    """

    TLS = 1
    """
    Defines security mode where data will be sent over wire using Transport Layer Security (TLS).
    """


class ConnectStatus(IntEnum):
    """
    Enumeration of the possible connection status results used by the SubscriberConnector.
    """

    SUCCESS = 1
    """
    Connection succeeded status.
    """

    FAILED = 0
    """
    Connection failed status.
    """

    CANCELED = -1
    """
    Connection cancelled status.
    """
