# ******************************************************************************************************
#  decoder.py - Gbtc
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
#  08/30/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from gsf import Limits
from gsf.endianorder import NativeEndian
from .pointmetadata import PointMetadata, CodeWords
from typing import Dict, Optional, Tuple
import numpy as np

INT32_0 = np.int32(0)
INT32_1 = np.int32(1)
INT32_2 = np.int32(2)
INT32_3 = np.int32(3)
INT32_4 = np.int32(4)
INT32_8 = np.int32(8)
INT32_12 = np.int32(12)
INT32_16 = np.int32(16)
INT32_24 = np.int32(24)

UINT32_0 = np.uint32(0)
UINT32_4 = np.uint32(4)
UINT32_7 = np.uint32(7)
UINT32_8 = np.uint32(8)
UINT32_12 = np.uint32(12)
UINT32_14 = np.uint32(14)
UINT32_16 = np.uint32(16)
UINT32_20 = np.uint32(20)
UINT32_21 = np.uint32(21)
UINT32_24 = np.uint32(24)
UINT32_28 = np.uint32(28)
UINT32_128 = np.uint32(128)

UINT32_16K = np.uint32(16384)
UINT32_2M = np.uint32(2097152)
UINT32_256M = np.uint32(268435456)

UINT32_1BYTEXOR = np.uint32(0x00000080)
UINT32_2BYTEXOR = np.uint32(0x00004080)
UINT32_3BYTEXOR = np.uint32(0x00204080)
UINT32_4BYTEXOR = np.uint32(0x10204080)

INT64_0 = np.int64(0)
INT64_MAX = np.int64(Limits.MAXINT64)

UINT64_7 = np.uint64(7)
UINT64_14 = np.uint64(14)
UINT64_21 = np.uint64(21)
UINT64_28 = np.uint64(28)
UINT64_35 = np.uint64(35)
UINT64_42 = np.uint64(42)
UINT64_49 = np.uint64(49)
UINT64_56 = np.uint64(56)
UINT64_128 = np.uint64(128)

UINT64_16K = np.uint64(16384)
UINT64_2M = np.uint64(2097152)
UINT64_256M = np.uint64(268435456)
UINT64_32G = np.uint64(34359738368)
UINT64_4T = np.uint64(4398046511104)
UINT64_512T = np.uint64(562949953421312)
UINT64_64P = np.uint64(72057594037927936)

UINT64_1BYTEXOR = np.uint64(0x000000000000080)
UINT64_2BYTEXOR = np.uint64(0x000000000004080)
UINT64_3BYTEXOR = np.uint64(0x000000000204080)
UINT64_4BYTEXOR = np.uint64(0x000000010204080)
UINT64_5BYTEXOR = np.uint64(0x000000810204080)
UINT64_6BYTEXOR = np.uint64(0x000040810204080)
UINT64_7BYTEXOR = np.uint64(0x002040810204080)
UINT64_8BYTEXOR = np.uint64(0x102040810204080)


class Decoder:
    """
    The decoder for the Time-Series Special Compression (TSSC) algorithm of STTP.
    """

    def __init__(self):
        """
        Creates a new TSSC decoder.
        """

        self._data = bytes()
        self._position = 0
        self._lastposition = 0

        self._prevtimestamp1 = INT64_0
        self._prevtimestamp2 = INT64_0

        self._prevtimedelta1 = INT64_MAX
        self._prevtimedelta2 = INT64_MAX
        self._prevtimedelta3 = INT64_MAX
        self._prevtimedelta4 = INT64_MAX

        self._lastpoint = self._new_pointmetadata()
        self._points: Dict[np.int32, Optional[PointMetadata]] = {}

        # The number of bits in m_bitStreamCache that are valid. 0 Means the bitstream is empty
        self._bitstreamcount = INT32_0

        # A cache of bits that need to be flushed to m_buffer when full. Bits filled starting from the right moving left
        self._bitstreamcache = INT32_0

        self.sequencenumber = 0
        """
        SequenceNumber is the sequence used to synchronize encoding and decoding.
        """

    def _new_pointmetadata(self) -> PointMetadata:
        return PointMetadata(None, self._read_bit, self._read_bits5)

    def _bitstream_isempty(self) -> bool:
        return self._bitstreamcount == INT32_0

    def _clear_bitstream(self) -> None:
        self._bitstreamcount = INT32_0
        self._bitstreamcache = INT32_0

    def set_buffer(self, data: bytes) -> None:
        """
        Assigns the working buffer to use for decoding measurements.
        """

        self._clear_bitstream()
        self._data = data
        self._position = 0
        self._lastposition = len(data)

    def try_get_measurement(self) -> Tuple[np.int32, np.int64, np.uint32, np.float32, bool, Optional[Exception]]:  # sourcery skip: low-code-quality
        if self._position == self._lastposition and self._bitstream_isempty():
            self._clear_bitstream()
            return 0, 0, 0, 0.0, False, None

        # Given that the incoming pointID is not known in advance, the current
        # measurement will contain the encoding details for the next.

        # General compression strategy is to use delta-encoding for each
        # measurement component value that is received with the same identity.
        # See https://en.wikipedia.org/wiki/Delta_encoding

        # Delta-encoding sizes are embedded in the stream as type-specific
        # codes using as few bits as possible

        # Read next code for measurement ID decoding
        code, err = self._lastpoint.read_code()

        if err is not None:
            return 0, 0, 0, 0.0, False, err

        if code == np.int32(CodeWords.ENDOFSTREAM):
            self._clear_bitstream()
            return 0, 0, 0, 0.0, False, None

        # Decode measurement ID and read next code for timestamp decoding
        if code <= np.int32(CodeWords.POINTIDXOR32):
            err = self._decode_pointid(np.byte(code), self._lastpoint)

            if err is not None:
                return 0, 0, 0, 0.0, False, err

            code, err = self._lastpoint.read_code()

            if err is not None:
                return 0, 0, 0, 0.0, False, err

            if err := self._validate_nextcode(code, CodeWords.TIMEDELTA1FORWARD):
                return 0, 0, 0, 0.0, False, err

        pointid = self._lastpoint.prevnextpointid1

        # Setup tracking for metadata associated with measurement ID and next point to decode
        if (nextpoint := self._points.get(pointid)) is None:
            nextpoint = self._new_pointmetadata()
            self._points[pointid] = nextpoint
            nextpoint.prevnextpointid1 = pointid + 1

        # Decode measurement timestamp and read next code for quality flags decoding
        if code <= np.int32(CodeWords.TIMEXOR7BIT):
            timestamp = self._decode_timestamp(np.byte(code))
            code, err = self._lastpoint.read_code()

            if err is not None:
                return 0, 0, 0, 0.0, False, err

            if err := self._validate_nextcode(code, CodeWords.STATEFLAGS2):
                return 0, 0, 0, 0.0, False, err
        else:
            timestamp = self._prevtimestamp1

        # Decode measurement state flags and read next code for measurement value decoding
        if code <= np.int32(CodeWords.STATEFLAGS7BIT32):
            stateflags = self._decode_stateflags(np.byte(code), nextpoint)
            code, err = self._lastpoint.read_code()

            if err is not None:
                return 0, 0, 0, 0.0, False, err

            if err := self._validate_nextcode(code, CodeWords.VALUE1):
                return 0, 0, 0, 0.0, False, err
        else:
            stateflags = nextpoint.prevstateflags1

        # Decode measurement value
        value, err = self._decode_value(np.byte(code), nextpoint)

        if err is not None:
            return 0, 0, 0, 0.0, False, err

        return pointid, timestamp, stateflags, value, True, None

    def _validate_nextcode(self, code: np.int32, nextcode: np.byte) -> Optional[Exception]:
        if code < np.int32(nextcode):
            message = [
                f"expecting code >= {nextcode}"
                f" at position {self._position}"
                f" with last position {self._lastposition}"
            ]

            return RuntimeError("".join(message))

        return None

    def _decode_pointid(self, code: np.byte, lastpoint: PointMetadata) -> Optional[Exception]:
        if code == CodeWords.POINTIDXOR4:
            lastpoint.prevnextpointid1 = self._read_bits4() ^ lastpoint.prevnextpointid1
        elif code == CodeWords.POINTIDXOR8:
            lastpoint.prevnextpointid1 = np.int32(self._data[self._position]) ^ lastpoint.prevnextpointid1
            self._position += 1
        elif code == CodeWords.POINTIDXOR12:
            lastpoint.prevnextpointid1 = self._read_bits4() ^ (np.int32(self._data[self._position]) << INT32_4) ^ lastpoint.prevnextpointid1
            self._position += 1
        elif code == CodeWords.POINTIDXOR16:
            lastpoint.prevnextpointid1 = np.int32(self._data[self._position]) ^ (np.int32(self._data[self._position + 1]) << INT32_8) ^ \
                lastpoint.prevnextpointid1
            self._position += 2
        elif code == CodeWords.POINTIDXOR20:
            lastpoint.prevnextpointid1 = self._read_bits4() ^ (np.int32(self._data[self._position]) << INT32_4) ^ \
                (np.int32(self._data[self._position + 1]) << INT32_12) ^ lastpoint.prevnextpointid1
            self._position += 2
        elif code == CodeWords.POINTIDXOR24:
            lastpoint.prevnextpointid1 = np.int32(self._data[self._position]) ^ (np.int32(self._data[self._position + 1]) << INT32_8) ^ \
                (np.int32(self._data[self._position + 2]) << INT32_16) ^ lastpoint.prevnextpointid1
            self._position += 3
        elif code == CodeWords.POINTIDXOR32:
            lastpoint.prevnextpointid1 = np.int32(self._data[self._position]) ^ (np.int32(self._data[self._position + 1]) << INT32_8) ^ \
                (np.int32(self._data[self._position + 2]) << INT32_16) ^ (np.int32(self._data[self._position + 3]) << INT32_24) ^ lastpoint.prevnextpointid1
            self._position += 4
        else:
            message = [
                f"invalid code received {code}"
                f" at position {self._position}"
                f" with last position {self._lastposition}"
            ]

            return RuntimeError("".join(message))

        return None

    def _decode_timestamp(self, code: np.byte) -> np.int64:
        if code == CodeWords.TIMEDELTA1FORWARD:
            timestamp = self._prevtimestamp1 + self._prevtimedelta1
        elif code == CodeWords.TIMEDELTA2FORWARD:
            timestamp = self._prevtimestamp1 + self._prevtimedelta2
        elif code == CodeWords.TIMEDELTA3FORWARD:
            timestamp = self._prevtimestamp1 + self._prevtimedelta3
        elif code == CodeWords.TIMEDELTA4FORWARD:
            timestamp = self._prevtimestamp1 + self._prevtimedelta4
        elif code == CodeWords.TIMEDELTA1REVERSE:
            timestamp = self._prevtimestamp1 - self._prevtimedelta1
        elif code == CodeWords.TIMEDELTA2REVERSE:
            timestamp = self._prevtimestamp1 - self._prevtimedelta2
        elif code == CodeWords.TIMEDELTA3REVERSE:
            timestamp = self._prevtimestamp1 - self._prevtimedelta3
        elif code == CodeWords.TIMEDELTA4REVERSE:
            timestamp = self._prevtimestamp1 - self._prevtimedelta4
        elif code == CodeWords.TIMESTAMP2:
            timestamp = self._prevtimestamp2
        else:
            value, self._position = Decoder._decode_7bituint64(self._data, self._position)
            timestamp = self._prevtimestamp1 ^ np.int64(value)

        # Save the smallest delta time
        minDelta = abs(self._prevtimestamp1 - timestamp)

        if minDelta < self._prevtimedelta4 and minDelta != self._prevtimedelta1 and minDelta != self._prevtimedelta2 and minDelta != self._prevtimedelta3:
            if minDelta < self._prevtimedelta1:
                self._prevtimedelta4 = self._prevtimedelta3
                self._prevtimedelta3 = self._prevtimedelta2
                self._prevtimedelta2 = self._prevtimedelta1
                self._prevtimedelta1 = minDelta
            elif minDelta < self._prevtimedelta2:
                self._prevtimedelta4 = self._prevtimedelta3
                self._prevtimedelta3 = self._prevtimedelta2
                self._prevtimedelta2 = minDelta
            elif minDelta < self._prevtimedelta3:
                self._prevtimedelta4 = self._prevtimedelta3
                self._prevtimedelta3 = minDelta
            else:
                self._prevtimedelta4 = minDelta

        self._prevtimestamp2 = self._prevtimestamp1
        self._prevtimestamp1 = timestamp

        return timestamp

    def _decode_stateflags(self, code: np.byte, nextPoint: PointMetadata) -> np.uint32:
        if code == CodeWords.STATEFLAGS2:
            stateFlags = nextPoint.prevstateflags2
        else:
            stateFlags, self._position = self._decode_7bituint32(self._data, self._position)

        nextPoint.prevstateflags2 = nextPoint.prevstateflags1
        nextPoint.prevstateflags1 = stateFlags

        return stateFlags

    def _decode_value(self, code: np.byte, nextpoint: PointMetadata) -> Tuple[np.float32, Optional[Exception]]:
        valueraw = UINT32_0

        def update_prevvalues(value: np.uint32):
            nextpoint.prevvalue3 = nextpoint.prevvalue2
            nextpoint.prevvalue2 = nextpoint.prevvalue1
            nextpoint.prevvalue1 = value

        if code == CodeWords.VALUE1:
            valueraw = nextpoint.prevvalue1
        elif code == CodeWords.VALUE2:
            valueraw = nextpoint.prevvalue2
            nextpoint.prevvalue2 = nextpoint.prevvalue1
            nextpoint.prevvalue1 = valueraw
        elif code == CodeWords.VALUE3:
            valueraw = nextpoint.prevvalue3
            update_prevvalues(valueraw)
        elif code == CodeWords.VALUEZERO:
            update_prevvalues(valueraw)
        else:
            if code == CodeWords.VALUEXOR4:
                valueraw = np.uint32(self._read_bits4()) ^ nextpoint.prevvalue1
            elif code == CodeWords.VALUEXOR8:
                valueraw = np.uint32(self._data[self._position]) ^ nextpoint.prevvalue1
                self._position += 1
            elif code == CodeWords.VALUEXOR12:
                valueraw = np.uint32(self._read_bits4()) ^ (np.uint32(self._data[self._position]) << UINT32_4) ^ nextpoint.prevvalue1
                self._position += 1
            elif code == CodeWords.VALUEXOR16:
                valueraw = np.uint32(self._data[self._position]) ^ (np.uint32(self._data[self._position + 1]) << UINT32_8) ^ nextpoint.prevvalue1
                self._position += 2
            elif code == CodeWords.VALUEXOR20:
                valueraw = np.uint32(self._read_bits4()) ^ (np.uint32(self._data[self._position]) << UINT32_4) ^ \
                    (np.uint32(self._data[self._position + 1]) << UINT32_12) ^ nextpoint.prevvalue1
                self._position += 2
            elif code == CodeWords.VALUEXOR24:
                valueraw = np.uint32(self._data[self._position]) ^ (np.uint32(self._data[self._position + 1]) << UINT32_8) ^ \
                    (np.uint32(self._data[self._position + 2]) << UINT32_16) ^ nextpoint.prevvalue1
                self._position += 3
            elif code == CodeWords.VALUEXOR28:
                valueraw = np.uint32(self._read_bits4()) ^ (np.uint32(self._data[self._position]) << UINT32_4) ^ \
                    (np.uint32(self._data[self._position + 1]) << UINT32_12) ^ (np.uint32(self._data[self._position + 2]) << UINT32_20) ^ nextpoint.prevvalue1
                self._position += 3
            elif code == CodeWords.VALUEXOR32:
                valueraw = np.uint32(self._data[self._position]) ^ (np.uint32(self._data[self._position + 1]) << UINT32_8) ^ \
                    (np.uint32(self._data[self._position + 2]) << UINT32_16) ^ (np.uint32(self._data[self._position + 3]) << UINT32_24) ^ nextpoint.prevvalue1
                self._position += 4
            else:
                message = [
                    f"invalid code received {code}"
                    f" at position {self._position}"
                    f" with last position {self._lastposition}"
                ]

                return np.float32(np.nan), RuntimeError("".join(message))

            update_prevvalues(valueraw)

        self._lastpoint = nextpoint

        return NativeEndian.to_float32(NativeEndian.from_uint32(valueraw)), None

    def _read_bit(self) -> np.int32:
        if self._bitstreamcount == INT32_0:
            self._bitstreamcount = INT32_8
            self._bitstreamcache = np.int32(self._data[self._position])
            self._position += 1

        self._bitstreamcount -= INT32_1

        return (self._bitstreamcache >> self._bitstreamcount) & INT32_1

    def _read_bits4(self) -> np.int32:
        return self._read_bit() << INT32_3 | self._read_bit() << INT32_2 | self._read_bit() << INT32_1 | self._read_bit()

    def _read_bits5(self) -> np.int32:
        return self._read_bit() << INT32_4 | self._read_bit() << INT32_3 | self._read_bit() << INT32_2 | self._read_bit() << INT32_1 | self._read_bit()

    @staticmethod
    def _decode_7bituint32(stream: bytes, position: int) -> Tuple[np.uint32, int]:
        stream = stream[position:]
        value = np.uint32(stream[0])

        if value < UINT32_128:
            position += 1
            return (value, position)

        value ^= np.uint32(stream[1]) << UINT32_7

        if value < UINT32_16K:
            position += 2
            return (value ^ UINT32_1BYTEXOR, position)

        value ^= np.uint32(stream[2]) << UINT32_14

        if value < UINT32_2M:
            position += 3
            return (value ^ UINT32_2BYTEXOR, position)

        value ^= np.uint32(stream[3]) << UINT32_21

        if value < UINT32_256M:
            position += 4
            return (value ^ UINT32_3BYTEXOR, position)

        value ^= np.uint32(stream[4]) << UINT32_28
        position += 5

        return (value ^ UINT32_4BYTEXOR, position)

    @staticmethod
    def _decode_7bituint64(stream: bytes, position: int) -> Tuple[np.uint64, int]:
        stream = stream[position:]
        value = np.uint64(stream[0])

        if value < UINT64_128:
            position += 1
            return (value, position)

        value ^= np.uint64(stream[1]) << UINT64_7

        if value < UINT64_16K:
            position += 2
            return (value ^ UINT64_1BYTEXOR, position)

        value ^= np.uint64(stream[2]) << UINT64_14

        if value < np.uint64(UINT64_2M):
            position += 3
            return (value ^ UINT64_2BYTEXOR, position)

        value ^= np.uint64(stream[3]) << UINT64_21

        if value < UINT64_256M:
            position += 4
            return (value ^ UINT64_3BYTEXOR, position)

        value ^= np.uint64(stream[4]) << UINT64_28

        if value < UINT64_32G:
            position += 5
            return (value ^ UINT64_4BYTEXOR, position)

        value ^= np.uint64(stream[5]) << UINT64_35

        if value < UINT64_4T:
            position += 6
            return (value ^ UINT64_5BYTEXOR, position)

        value ^= np.uint64(stream[6]) << UINT64_42

        if value < UINT64_512T:
            position += 7
            return (value ^ UINT64_6BYTEXOR, position)

        value ^= np.uint64(stream[7]) << UINT64_49

        if value < UINT64_64P:
            position += 8
            return (value ^ UINT64_7BYTEXOR, position)

        value ^= np.uint64(stream[8]) << UINT64_56
        position += 9

        return (value ^ UINT64_8BYTEXOR, position)
