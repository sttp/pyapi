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
from typing import List, Optional, Tuple
import numpy as np


class Decoder:
    """
    The decoder for the Time-Series Special Compression (TSSC) algorithm of STTP.
    """

    def __init__(self,
                 maxsignalindex: np.uint32
                 ):
        """
        Creates a new TSSC decoder.
        """

        self._data = bytes()
        self._position = 0
        self._lastposition = 0

        self._prevtimestamp1 = np.int64(0)
        self._prevtimestamp2 = np.int64(0)

        self._prevtimedelta1 = np.int64(Limits.MAXINT64)
        self._prevtimedelta2 = np.int64(Limits.MAXINT64)
        self._prevtimedelta3 = np.int64(Limits.MAXINT64)
        self._prevtimedelta4 = np.int64(Limits.MAXINT64)

        self._lastpoint = self._new_pointmetadata()
        self._points: List[Optional[PointMetadata]] = [None] * maxsignalindex

        # The number of bits in m_bitStreamCache that are valid. 0 Means the bitstream is empty
        self._bitstreamcount = np.int32(0)

        # A cache of bits that need to be flushed to m_buffer when full. Bits filled starting from the right moving left
        self._bitstreamcache = np.int32(0)

        self.sequencenumber = 0
        """
        SequenceNumber is the sequence used to synchronize encoding and decoding.
        """

    def _new_pointmetadata(self) -> PointMetadata:
        return PointMetadata(None, self._read_bit, self._read_bits5)

    def _bitstream_isempty(self) -> bool:
        return self._bitstreamcount == 0

    def _clear_bitstream(self) -> None:
        self._bitstreamcount = np.int32(0)
        self._bitstreamcache = np.int32(0)

    def set_buffer(self, data: bytes) -> None:
        """
        Assigns the working buffer to use for decoding measurements.
        """

        self._clear_bitstream()
        self._data = data
        self._position = 0
        self._lastposition = len(data)

    def try_get_measurement(self) -> Tuple[np.int32, np.int64, np.uint32, np.float32, bool, Optional[Exception]]:
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
            err = self._decode_pointid(code)

            if err is not None:
                return 0, 0, 0, 0.0, False, err

            code, err = self._lastpoint.read_code()

            if err is not None:
                return 0, 0, 0, 0.0, False, err

            if code < np.int32(CodeWords.TIMEDELTA1FORWARD):
                message = [
                    f"expecting code >= {CodeWords.TIMEDELTA1FORWARD}"
                    f" at position {self._position}"
                    f" with last position {self._lastposition}"
                ]

                return 0, 0, 0, 0.0, False, RuntimeError("".join(message))

        # Assign decoded measurement ID to out parameter
        pointid = self._lastpoint.prevnextpointid1

        # Setup tracking for metadata associated with measurement ID and next point to decode
        pointcount = np.int32(len(self._points))

        nextpoint = None if pointid >= pointcount else self._points[pointid]

        if nextpoint is None:
            nextpoint = self._new_pointmetadata()

            if pointid >= pointcount:
                while pointid + 1 > len(self._points):
                    self._points.append(None)

        self._points[pointid] = nextpoint
        nextpoint.prevnextpointid1 = pointid + 1

        # Decode measurement timestamp and read next code for quality flags decoding
        if code <= np.int32(CodeWords.TIMEXOR7BIT):
            timestamp = self._decode_timestamp(np.byte(code))
            code, err = self._lastpoint.read_code()

            if err is not None:
                return 0, 0, 0, 0.0, False, err

            if code < np.int32(CodeWords.STATEFLAGS2):
                message = [
                    f"expecting code >= {CodeWords.STATEFLAGS2}"
                    f" at position {self._position}"
                    f" with last position {self._lastposition}"
                ]

                return 0, 0, 0, 0.0, False, RuntimeError("".join(message))
        else:
            timestamp = self._prevtimestamp1

        # Decode measurement state flags and read next code for measurement value decoding
        if code <= np.int32(CodeWords.STATEFLAGS7BIT32):
            stateflags = self._decode_stateflags(np.byte(code), nextpoint)
            code, err = self._lastpoint.read_code()

            if err is not None:
                return 0, 0, 0, 0.0, False, err

            if code < np.int32(CodeWords.VALUE1):
                message = [
                    f"expecting code >= {CodeWords.VALUE1}"
                    f" at position {self._position}"
                    f" with last position {self._lastposition}"
                ]

                return 0, 0, 0, 0.0, False, RuntimeError("".join(message))
        else:
            stateflags = self._lastpoint.prevstateflags1

        # Since measurement value will almost always change, this is not put inside a function call             
        valueraw = np.uint32(0)

        # Decode measurement value
        if code == np.int32(CodeWords.VALUE1):
            valueraw = nextpoint.prevvalue1
        elif code == np.int32(CodeWords.VALUE2):
            valueraw = nextpoint.prevvalue2
            nextpoint.prevvalue2 = nextpoint.prevvalue1
            nextpoint.prevvalue1 = valueraw
        elif code == np.int32(CodeWords.VALUE3):
            valueraw = nextpoint.prevvalue3
            nextpoint.prevvalue3 = nextpoint.prevvalue2
            nextpoint.prevvalue2 = nextpoint.prevvalue1
            nextpoint.prevvalue1 = valueraw
        elif code == np.int32(CodeWords.VALUEZERO):
            valueraw = 0
            nextpoint.prevvalue3 = nextpoint.prevvalue2
            nextpoint.prevvalue2 = nextpoint.prevvalue1
            nextpoint.prevvalue1 = valueraw
        else:
            if code == np.int32(CodeWords.VALUEXOR4):
                valueraw = np.uint32(self._read_bits4()) ^ nextpoint.prevvalue1
            elif code == np.int32(CodeWords.VALUEXOR8):
                valueraw = np.uint32(self._data[self._position]) ^ nextpoint.prevvalue1
                self._position += 1
            elif code == np.int32(CodeWords.VALUEXOR12):
                valueraw = np.uint32(self._read_bits4()) ^ np.uint32(self._data[self._position]) << 4 ^ nextpoint.prevvalue1
                self._position += 1
            elif code == np.int32(CodeWords.VALUEXOR16):
                valueraw = np.uint32(self._data[self._position]) ^ np.uint32(self._data[self._position + 1]) << 8 ^ nextpoint.prevvalue1
                self._position += 2
            elif code == np.int32(CodeWords.VALUEXOR20):
                valueraw = np.uint32(self._read_bits4()) ^ np.uint32(self._data[self._position]) << 4 ^ np.uint32(self._data[self._position + 1]) << 12 ^ nextpoint.prevvalue1
                self._position += 2
            elif code == np.int32(CodeWords.VALUEXOR24):
                valueraw = np.uint32(self._data[self._position]) ^ np.uint32(self._data[self._position + 1]) << 8 ^ np.uint32(self._data[self._position + 2]) << 16 ^ nextpoint.prevvalue1
                self._position += 3
            elif code == np.int32(CodeWords.VALUEXOR28):
                valueraw = np.uint32(self._read_bits4()) ^ np.uint32(self._data[self._position]) << 4 ^ np.uint32(self._data[self._position + 1]) << 12 ^ np.uint32(self._data[self._position + 2]) << 20 ^ nextpoint.prevvalue1
                self._position += 3
            elif code == np.int32(CodeWords.VALUEXOR32):
                valueraw = np.uint32(self._data[self._position]) ^ np.uint32(self._data[self._position + 1]) << 8 ^ np.uint32(self._data[self._position + 2]) << 16 ^ np.uint32(self._data[self._position + 3]) << 24 ^ nextpoint.prevvalue1
                self._position += 4
            else:
                message = [
                    f"invalid code received {code}"
                    f" at position {self._position}"
                    f" with last position {self._lastposition}"
                ]

                return 0, 0, 0, 0.0, False, RuntimeError("".join(message))

            nextpoint.prevvalue3 = nextpoint.prevvalue2
            nextpoint.prevvalue2 = nextpoint.prevvalue1
            nextpoint.prevvalue1 = valueraw

        # Assign decoded measurement value to out parameter
        value = NativeEndian.to_float32(NativeEndian.from_uint32(valueraw))
        self._lastpoint = nextpoint       

        return pointid, timestamp, stateflags, value, True, None     

    def _decode_pointid(self, code: np.byte) -> Optional[Exception]:
        if code == CodeWords.POINTIDXOR4:
            self._lastpoint.prevnextpointid1 = self._read_bits4() ^ self._lastpoint.prevnextpointid1
        elif code == CodeWords.POINTIDXOR8:
            self._lastpoint.prevnextpointid1 = np.int32(self._data[self._position]) ^ self._lastpoint.prevnextpointid1
            self._position += 1
        elif code == CodeWords.POINTIDXOR12:
            self._lastpoint.prevnextpointid1 = self._read_bits4() ^ np.int32(self._data[self._position]) << 4 ^ self._lastpoint.prevnextpointid1
            self._position += 1
        elif code == CodeWords.POINTIDXOR16:
            self._lastpoint.prevnextpointid1 = np.int32(self._data[self._position]) ^ np.int32(self._data[self._position + 1]) << 8 ^ self._lastpoint.prevnextpointid1
            self._position += 2
        elif code == CodeWords.POINTIDXOR20:
            self._lastpoint.prevnextpointid1 = self._read_bits4() ^ np.int32(self._data[self._position]) << 4 ^ np.int32(self._data[self._position + 1]) << 12 ^ self._lastpoint.prevnextpointid1
            self._position += 2
        elif code == CodeWords.POINTIDXOR24:
            self._lastpoint.prevnextpointid1 = np.int32(self._data[self._position]) ^ np.int32(self._data[self._position + 1]) << 8 ^ np.int32(self._data[self._position + 2]) << 16 ^ self._lastpoint.prevnextpointid1
            self._position += 3
        elif code == CodeWords.POINTIDXOR32:
            self._lastpoint.prevnextpointid1 = np.int32(self._data[self._position]) ^ np.int32(self._data[self._position + 1]) << 8 ^ np.int32(self._data[self._position + 2]) << 16 ^ np.int32(self._data[self._position + 3]) << 24 ^ self._lastpoint.prevnextpointid1
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
        timestamp = np.int64(0)

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

    def _read_bit(self) -> np.int32:
        if self._bitstreamcount == np.int32(0):
            self._bitstreamcount = np.int32(8)
            self._bitstreamcache = np.int32(self._data[self._position])
            self._position += 1

        self._bitstreamcount -= np.int32(1)

        return self._bitstreamcache >> self._bitstreamcount & np.int32(1)

    def _read_bits4(self) -> np.int32:
        return self._read_bit() << np.int32(3) | self._read_bit() << np.int32(2) | self._read_bit() << np.int32(1) | self._read_bit()

    def _read_bits5(self) -> np.int32:
        return self._read_bit() << np.int32(4) | self._read_bit() << np.int32(3) | self._read_bit() << np.int32(2) | self._read_bit() << np.int32(1) | self._read_bit()

    @staticmethod
    def _decode_7bituint32(stream: bytes, position: int) -> Tuple[np.uint32, int]:
        stream = stream[position:]
        value = np.uint32(stream[0])

        if value < np.uint32(128):
            position += 1
            return (value, position)

        value ^= np.uint32(stream[1]) << np.uint32(7)

        if value < np.uint32(16384):
            position += 2
            return (value ^ np.uint32(0x80), position)

        value ^= np.uint32(stream[2]) << np.uint32(14)

        if value < np.uint32(2097152):
            position += 3
            return (value ^ np.uint32(0x4080), position)

        value ^= np.uint32(stream[3]) << np.uint32(21)

        if value < np.uint32(268435456):
            position += 4
            return (value ^ np.uint32(0x204080), position)

        value ^= np.uint32(stream[4]) << np.uint32(28)
        position += 5

        return (value ^ np.uint32(0x10204080), position)

    @staticmethod
    def _decode_7bituint64(stream: bytes, position: int) -> Tuple[np.uint64, int]:
        stream = stream[position:]
        value = np.uint64(stream[0])

        if value < np.uint64(128):
            position += 1
            return (value, position)

        value ^= np.uint64(stream[1]) << np.uint64(7)

        if value < np.uint64(16384):
            position += 2
            return (value ^ np.uint64(0x80), position)

        value ^= np.uint64(stream[2]) << np.uint64(14)

        if value < np.uint64(2097152):
            position += 3
            return (value ^ np.uint64(0x4080), position)

        value ^= np.uint64(stream[3]) << np.uint64(21)

        if value < np.uint64(268435456):
            position += 4
            return (value ^ np.uint64(0x204080), position)

        value ^= np.uint64(stream[4]) << np.uint64(28)

        if value < np.uint64(34359738368):
            position += 5
            return (value ^ np.uint64(0x10204080), position)

        value ^= np.uint64(stream[5]) << np.uint64(35)

        if value < np.uint64(4398046511104):
            position += 6
            return (value ^ np.uint64(0x810204080), position)

        value ^= np.uint64(stream[6]) << np.uint64(42)

        if value < np.uint64(562949953421312):
            position += 7
            return (value ^ np.uint64(0x40810204080), position)

        value ^= np.uint64(stream[7]) << np.uint64(49)

        if value < np.uint64(72057594037927936):
            position += 8
            return (value ^ np.uint64(0x2040810204080), position)

        value ^= np.uint64(stream[8]) << np.uint64(56)
        position += 9

        return (value ^ np.uint64(0x102040810204080), position)