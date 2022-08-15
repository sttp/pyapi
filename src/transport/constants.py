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

from enum import IntEnum, IntFlag

MAXPACKETSIZE = 32768
PAYLOADHEADERSIZE = 4
RESPONSEHEADERSIZE = 6
EVENKEY = 0
ODDKEY = 1
KEYINDEX = 0
IVINDEX = 1
MISSINGCACHEWARNINGINTERVAL = 20000000
DEFAULTLAGTIME = 5.0
DEFAULTLEADTIME = 5.0
DEFAULTPUBLISHINTERVAL = 1.0


class StateFlags(IntFlag):
    """
    Enumeration of the possible quality states of a Measurement value.
    """

    Normal = 0x0
    """
    Measurement flag for a normal state.
    """

    BadData = 0x1
    """
    Measurement flag for a bad data state.
    """

    SuspectData = 0x2
    """
    Measurement flag for a suspect data state.
    """

    OverRangeError = 0x4
    """
    Measurement flag for a over range error, i.e., unreasonable high value.
    """

    UnderRangeError = 0x8
    """
    Measurement flag for a under range error, i.e., unreasonable low value.
    """

    AlarmHigh = 0x10
    """
    Measurement flag for a alarm for high value.
    """

    AlarmLow = 0x20
    """
    Measurement flag for a alarm for low value.
    """

    WarningHigh = 0x40
    """
    Measurement flag for a warning for high value.
    """

    WarningLow = 0x80
    """
    Measurement flag for a warning for low value.
    """

    FlatlineAlarm = 0x100
    """
    Measurement flag for a alarm for flat-lined value, i.e., latched value test alarm.
    """

    ComparisonAlarm = 0x200
    """
    Measurement flag for a comparison alarm, i.e., outside threshold of comparison with a real-time value.
    """

    ROCAlarm = 0x400
    """
    Measurement flag for a rate-of-change alarm.
    """

    ReceivedAsBad = 0x800
    """
    Measurement flag for a bad value received.
    """

    CalculatedValue = 0x1000
    """
    Measurement flag for a calculated value state.
    """

    CalculationError = 0x2000
    """
    Measurement flag for a calculation error with the value.
    """

    CalculationWarning = 0x4000
    """
    Measurement flag for a calculation warning with the value.
    """

    ReservedQualityFlag = 0x8000
    """
    Measurement flag for a reserved quality.
    """

    BadTime = 0x10000
    """
    Measurement flag for a bad time state.
    """

    SuspectTime = 0x20000
    """
    Measurement flag for a suspect time state.
    """

    LateTimeAlarm = 0x40000
    """
    Measurement flag for a late time alarm.
    """

    FutureTimeAlarm = 0x80000
    """
    Measurement flag for a future time alarm.
    """

    UpSampled = 0x100000
    """
    Measurement flag for a up-sampled state.
    """

    DownSampled = 0x200000
    """
    Measurement flag for a down-sampled state.
    """

    DiscardedValue = 0x400000
    """
    Measurement flag for a discarded value state.
    """

    ReservedTimeFlag = 0x800000
    """
    Measurement flag for a reserved time
    """

    UserDefinedFlag1 = 0x1000000
    """
    Measurement flag for user defined state 1.
    """

    UserDefinedFlag2 = 0x2000000
    """
    Measurement flag for user defined state 2.
    """

    UserDefinedFlag3 = 0x4000000
    """
    Measurement flag for user defined state 3.
    """

    UserDefinedFlag4 = 0x8000000
    """
    Measurement flag for user defined state 4.
    """

    UserDefinedFlag5 = 0x10000000
    """
    Measurement flag for user defined state 5.
    """

    SystemError = 0x20000000
    """
    Measurement flag for a system error state.
    """

    SystemWarning = 0x40000000
    """
    Measurement flag for a system warning state.
    """

    MeasurementError = 0x80000000
    """
    Measurement flag for an error state.
    """


class DataPacketFlags(IntFlag):
    """
    Enumeration of the possible flags for a data packet.
    """

    Compact = 0x02
    """
    Determines if serialized measurement is compact. Bit set = compact, bit clear = full fidelity.
    """

    CipherIndex = 0x04
    """
    Determines which cipher index to use when encrypting data packet. Bit set = use odd cipher index (i.e., 1), bit clear = use even cipher index (i.e., 0).    
    """

    Compressed = 0x08
    """
    Determines if data packet payload is compressed. Bit set = payload compressed, bit clear = payload normal.
    """

    CacheIndex = 0x10
    """
    Determines with signal index cache to use when decoding a data packet. Used by STTP version 2 or greater.
    """

    NoFlags = 0x0
    """
    Defines state where there are no flags set. This would represent unsynchronized, full fidelity measurement data packets.
    """


class ServerCommand(IntEnum):
    """
    Enumeration of the possible server commands received by a DataPublisher and sent by a DataSubscriber during an STTP session.
    """

    # Although the server commands and responses will be on two different paths, the response enumeration values
    # are defined as distinct from the command values to make it easier to identify codes from a wire analysis.

    Succeeded = 0x80
    """
    Command code for indicating a succeeded response. Informs client that its solicited server command succeeded, original command and success message follow.
    """

    Failed = 0x81
    """
    Command code for indicating a failed response. Informs client that its solicited server command failed, original command and failure message follow.
    """

    DataPacket = 0x82
    """
    Command code for indicating a data packet. Unsolicited response informs client that a data packet follows.
    """

    UpdateSignalIndexCache = 0x83
    """
    Command code for indicating a signal index cache update. Unsolicited response requests that client update its runtime signal index cache with the one that follows.
    """

    UpdateBaseTimes = 0x84
    """
    Command code for indicating a runtime base-timestamp offsets have been updated. Unsolicited response requests that client update its runtime base-timestamp offsets with those that follow.
    """

    UpdateCipherKeys = 0x85
    """
    Command code for indicating a runtime cipher keys have been updated. Response, solicited or unsolicited, requests that client update its runtime data cipher keys with those that follow.
    """

    DataStartTime = 0x86
    """
    Command code for indicating the start time of data being published. Unsolicited response provides the start time of data being processed from the first measurement.
    """

    ProcessingComplete = 0x87
    """
    Command code for indicating that processing has completed. Unsolicited response provides notification that input processing has completed, typically via temporal constraint.
    """

    BufferBlock = 0x88
    """
    Command code for indicating a buffer block. Unsolicited response informs client that a raw buffer block follows.
    """

    Notification = 0x89
    """
    Command code for indicating a notification. Unsolicited response provides a notification message to the client.
    """

    ConfigurationChanged = 0x8A
    """
    Command code for indicating a that the publisher configuration metadata has changed. Unsolicited response provides a notification that the publisher's source configuration has changed and that client may want to request a meta-data refresh.
    """

    UserResponse00 = 0xE0
    """
    Command code for handling user-defined responses.
    """

    UserResponse01 = 0xE1
    """
    Command code for handling user-defined responses.
    """

    UserResponse02 = 0xE2
    """
    Command code for handling user-defined responses.
    """

    UserResponse03 = 0xE3
    """
    Command code for handling user-defined responses.
    """

    UserResponse04 = 0xE4
    """
    Command code for handling user-defined responses.
    """

    UserResponse05 = 0xE5
    """
    Command code for handling user-defined responses.
    """

    UserResponse06 = 0xE6
    """
    Command code for handling user-defined responses.
    """

    UserResponse07 = 0xE7
    """
    Command code for handling user-defined responses.
    """

    UserResponse08 = 0xE8
    """
    Command code for handling user-defined responses.
    """

    UserResponse09 = 0xE9
    """
    Command code for handling user-defined responses.
    """

    UserResponse10 = 0xEA
    """
    Command code for handling user-defined responses.
    """

    UserResponse11 = 0xEB
    """
    Command code for handling user-defined responses.
    """

    UserResponse12 = 0xEC
    """
    Command code for handling user-defined responses.
    """

    UserResponse13 = 0xED
    """
    Command code for handling user-defined responses.
    """

    UserResponse14 = 0xEE
    """
    Command code for handling user-defined responses.
    """

    UserResponse15 = 0xEF
    """
    Command code for handling user-defined responses.
    """

    NoOP = 0xFF
    """
    Command code for indicating a nil-operation keep-alive ping. The command channel can remain quiet for some time, this command allows a period test of client connectivity.
    """


class ServerResponse(IntEnum):
    """
    Enumeration of the possible server responses received sent by a DataPublisher and received by a DataSubscriber during an STTP session.
    """

    # Although the server commands and responses will be on two different paths, the response enumeration values
    # are defined as distinct from the command values to make it easier to identify codes from a wire analysis.

    Succeeded = 0x80
    """
    Response code indicating a succeeded response. Informs client that its solicited server command succeeded, original command and success message follow.
    """

    Failed = 0x81
    """
    Response code indicating a failed response. Informs client that its solicited server command failed, original command and failure message follow.
    """

    DataPacket = 0x82
    """
    Response code indicating a data packet. Unsolicited response informs client that a data packet follows.
    """

    UpdateSignalIndexCache = 0x83
    """
    Response code indicating a signal index cache update. Unsolicited response requests that client update its runtime signal index cache with the one that follows.
    """

    UpdateBaseTimes = 0x84
    """
    Response code indicating a runtime base-timestamp offsets have been updated. Unsolicited response requests that client update its runtime base-timestamp offsets with those that follow.
    """

    UpdateCipherKeys = 0x85
    """
    Response code indicating a runtime cipher keys have been updated. Response, solicited or unsolicited, requests that client update its runtime data cipher keys with those that follow.
    """

    DataStartTime = 0x86
    """
    Response code indicating the start time of data being published. Unsolicited response provides the start time of data being processed from the first measurement.
    """

    ProcessingComplete = 0x87
    """
    Response code indicating that processing has completed. Unsolicited response provides notification that input processing has completed, typically via temporal constraint.
    """

    BufferBlock = 0x88
    """
    Response code indicating a buffer block. Unsolicited response informs client that a raw buffer block follows.
    """

    Notification = 0x89
    """
    Response code indicating a notification. Unsolicited response provides a notification message to the client.
    """

    ConfigurationChanged = 0x8A
    """
    Response code indicating a that the publisher configuration metadata has changed. Unsolicited response provides a notification that the publisher's source configuration has changed and that client may want to request a meta-data refresh.
    """

    UserResponse00 = 0xE0
    """
    Response code handling user-defined responses.
    """

    UserResponse01 = 0xE1
    """
    Response code handling user-defined responses.
    """

    UserResponse02 = 0xE2
    """
    Response code handling user-defined responses.
    """

    UserResponse03 = 0xE3
    """
    Response code handling user-defined responses.
    """

    UserResponse04 = 0xE4
    """
    Response code handling user-defined responses.
    """

    UserResponse05 = 0xE5
    """
    Response code handling user-defined responses.
    """

    UserResponse06 = 0xE6
    """
    Response code handling user-defined responses.
    """

    UserResponse07 = 0xE7
    """
    Response code handling user-defined responses.
    """

    UserResponse08 = 0xE8
    """
    Response code handling user-defined responses.
    """

    UserResponse09 = 0xE9
    """
    Response code handling user-defined responses.
    """

    UserResponse10 = 0xEA
    """
    Response code handling user-defined responses.
    """

    UserResponse11 = 0xEB
    """
    Response code handling user-defined responses.
    """

    UserResponse12 = 0xEC
    """
    Response code handling user-defined responses.
    """

    UserResponse13 = 0xED
    """
    Response code handling user-defined responses.
    """

    UserResponse14 = 0xEE
    """
    Response code handling user-defined responses.
    """

    UserResponse15 = 0xEF
    """
    Response code handling user-defined responses.
    """

    NoOP = 0xFF
    """
    Response code indicating a nil-operation keep-alive ping. The command channel can remain quiet for some time, this command allows a period test of client connectivity.
    """


class OperationalModes(IntFlag):
    """
    Enumeration of the possible modes that affect how DataPublisher and DataSubscriber communicate during an STTP session.
    """

    # Operational modes are sent from a subscriber to a publisher to request operational behaviors for the
    # connection, as a result the operation modes must be sent before any other command. The publisher may
    # silently refuse some requests (e.g., compression) based on its configuration. Operational modes only
    # apply to fundamental protocol control.

    VersionMask = 0x0000001F
    """
    Bit mask used to get version number of protocol. Version number is currently set to 2.
    """

    CompressionModeMask = 0x000000E0
    """
    Bit mask used to get mode of compression. GZip and TSSC compression are the only modes currently supported. Remaining bits are reserved for future compression modes.
    """

    EncodingMask = 0x00000300
    """
    Bit mask used to get character encoding used when exchanging messages between publisher and subscriber.
    """

    ReceiveExternalMetadata = 0x02000000
    """
    Bit flag used to determine whether external measurements are exchanged during metadata synchronization. Bit set = external measurements are exchanged, bit clear = no external measurements are exchanged.
    """

    ReceiveInternalMetadata = 0x04000000
    """
    Bit flag used to determine whether internal measurements are exchanged during metadata synchronization. Bit set = internal measurements are exchanged, bit clear = no internal measurements are exchanged.
    """

    CompressPayloadData = 0x20000000
    """
    Bit flag used to determine whether payload data is compressed when exchanging between publisher and subscriber. Bit set = compress, bit clear = no compression.
    """

    CompressSignalIndexCache = 0x40000000
    """
    Bit flag used to determine whether the signal index cache is compressed when exchanging between publisher and subscriber. Bit set = compress, bit clear = no compression.
    """

    CompressMetadata = 0x80000000
    """
    Bit flag used to determine whether metadata is compressed when exchanging between publisher and subscriber. Bit set = compress, bit clear = no compression.
    """

    NoFlags = 0x00000000
    """
    State where there are no flags set.
    """


class OperationalEncoding(IntEnum):
    """
    Enumeration of the possible string encoding options of an STTP session.
    """

    UTF16LE = 0x00000000
    """
    Targets little-endian 16-bit Unicode character encoding for strings (deprecated).
    """

    UTF16BE = 0x00000100
    """
    Targets big-endian 16-bit Unicode character encoding for strings (deprecated).
    """

    UTF8 = 0x00000200
    """
    Targets 8-bit variable-width Unicode character encoding for strings.
    """


class CompressionModes(IntFlag):
    """
    Enumeration of the possible compression modes supported by STTP.
    """

    GZip = 0x00000020
    """
    Bit flag used determine if GZip compression will be used to metadata exchange.
    """

    TSSC = 0x00000040
    """
    Bit flag used determine if the time-series special compression algorithm will be used for data exchange.
    """

    NoFlags = 0x00000000
    """
    Defines state where no compression will be used.
    """


class SecurityMode(IntEnum):
    """
    Enumeration of the possible security modes used by the DataPublisher to secure data sent over the command channel in STTP.
    """

    Off = 0
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

    Success = 1
    """
    Connection succeeded status.
    """

    Failed = 0
    """
    Connection failed status.
    """

    Canceled = -1
    """
    Connection cancelled status.
    """
