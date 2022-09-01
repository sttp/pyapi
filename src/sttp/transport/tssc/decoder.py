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
from .pointmetadata import PointMetadata, codeWords
from typing import List, Optional, Tuple
import ctypes
import numpy as np


class Decoder:
    """
    The decoder for the Time-Series Special Compression (TSSC) algorithm of STTP.
    """

    def __init__(self,
                 maxSignalIndex: np.uint32
                 ):
        """
        Creates a new TSSC decoder.
        """

        self.data = bytes()
        self.position = 0
        self.lastPosition = 0

        self.prevTimestamp1 = np.int64(0)
        self.prevTimestamp2 = np.int64(0)

        self.prevTimeDelta1 = np.int64(Limits.MAXINT64)
        self.prevTimeDelta2 = np.int64(Limits.MAXINT64)
        self.prevTimeDelta3 = np.int64(Limits.MAXINT64)
        self.prevTimeDelta4 = np.int64(Limits.MAXINT64)

        self.lastPoint = self._new_pointmetadata()
        self.points: List[Optional[PointMetadata]] = [None] * maxSignalIndex

        # The number of bits in m_bitStreamCache that are valid. 0 Means the bitstream is empty
        self.bitStreamCount = np.int32(0)

        # A cache of bits that need to be flushed to m_buffer when full. Bits filled starting from the right moving left
        self.bitStreamCache = np.int32(0)

        self.SequenceNumber = np.uint16(0)
        """
        SequenceNumber is the sequence used to synchronize encoding and decoding.
        """

    def _new_pointmetadata(self) -> PointMetadata:
        return PointMetadata(None, self._readbit, self.readbits5)

    def _bitstream_isempty(self) -> bool:
        return self.bitStreamCount == 0

    def _clear_bitstream(self) -> None:
        self.bitStreamCount = np.int32(0)
        self.bitStreamCache = np.int32(0)

    def set_buffer(self, data: bytes) -> None:
        """
        Assigns the working buffer to use for decoding measurements.
        """

        self.data = data
        self.position = 0
        self.lastPosition = len(data)

    def try_get_measurement(self) -> Tuple[np.int32, np.int64, np.uint32, np.float32, bool, Optional[Exception]]:
        if self.position == self.lastPosition or self._bitstream_isempty():
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
        code, err = self.lastPoint.ReadCode()

        if err is not None:
            return 0, 0, 0, 0.0, False, err

        if code == np.int32(codeWords.EndOfStream):
            self._clear_bitstream()
            return 0, 0, 0, 0.0, False, None

        # Decode measurement ID and read next code for timestamp decoding
        if code < np.int32(codeWords.PointIDXor32):
            err = self.decode_pointid(code)

            if err is not None:
                return 0, 0, 0, 0.0, False, err

            code, err = self.lastPoint.ReadCode()

            if err is not None:
                return 0, 0, 0, 0.0, False, err

            if code < np.int32(codeWords.TimeDelta1Forward):
                message = [
                    f"expecting code >= {codeWords.TimeDelta1Forward}"
                    f" at position {self.position}"
                    f" with last position {self.lastPosition}"
                ]

                return 0, 0, 0, 0.0, False, RuntimeError("".join(message))

            # Assign decoded measurement ID to out parameter
            pointid = self.lastPoint.PrevNextPointID1

            # Setup tracking for metadata associated with measurement ID and next point to decode
            pointCount = np.int32(len(self.points))

            nextPoint = None if pointid > pointCount else self.points[pointid]

            if nextPoint is None:
                nextPoint = self._new_pointmetadata()

                if pointid > pointCount:
                    while pointid + 1 > len(self.points):
                        self.points.append(None)

            self.points[pointid] = nextPoint
            nextPoint.PrevNextPointID1 = pointid + 1

            # Decode measurement timestamp and read next code for quality flags decoding
            if code < np.int32(codeWords.TimeXor7Bit):
                timestamp = self.decode_timestamp(np.byte(code))
                code, err = self.lastPoint.ReadCode()

                if err is not None:
                    return 0, 0, 0, 0.0, False, err

                if code < np.int32(codeWords.StateFlags2):
                    message = [
                        f"expecting code >= {codeWords.StateFlags2}"
                        f" at position {self.position}"
                        f" with last position {self.lastPosition}"
                    ]

                    return 0, 0, 0, 0.0, False, RuntimeError("".join(message))
            else:
                timestamp = self.prevTimestamp1

            # Decode measurement state flags and read next code for measurement value decoding
            if code <= np.int32(codeWords.StateFlags7Bit32):
                stateFlags = self.decode_stateflags(np.byte(code), nextPoint)
                code, err = self.lastPoint.ReadCode()

                if err is not None:
                    return 0, 0, 0, 0.0, False, err

                if code < np.int32(codeWords.Value1):
                    message = [
                        f"expecting code >= {codeWords.Value1}"
                        f" at position {self.position}"
                        f" with last position {self.lastPosition}"
                    ]

                    return 0, 0, 0, 0.0, False, RuntimeError("".join(message))
            else:
                stateFlags = self.lastPoint.PrevStateFlags1

            # Since measurement value will almost always change, this is not put inside a function call             
            valueRaw = np.uint32(0)

            # Decode measurement value
            if code == np.int32(codeWords.Value1):
                valueRaw = nextPoint.PrevValue1
            elif code == np.int32(codeWords.Value2):
                valueRaw = nextPoint.PrevValue2
                nextPoint.PrevValue2 = nextPoint.PrevValue1
                nextPoint.PrevValue1 = valueRaw
            elif code == np.int32(codeWords.Value3):
                valueRaw = nextPoint.PrevValue3
                nextPoint.PrevValue3 = nextPoint.PrevValue2
                nextPoint.PrevValue2 = nextPoint.PrevValue1
                nextPoint.PrevValue1 = valueRaw
            elif code == np.int32(codeWords.ValueZero):
                valueRaw = 0
                nextPoint.PrevValue3 = nextPoint.PrevValue2
                nextPoint.PrevValue2 = nextPoint.PrevValue1
                nextPoint.PrevValue1 = valueRaw
            else:
                if code == codeWords.ValueXor4:
                    valueRaw = np.uint32(self.readBits4()) ^ nextPoint.PrevValue1
                elif code == codeWords.ValueXor8:
                    valueRaw = np.uint32(self.data[self.position]) ^ nextPoint.PrevValue1
                    self.position += 1
                elif code == codeWords.ValueXor12:
                    valueRaw = np.uint32(self.readBits4()) ^ np.uint32(self.data[self.position]) << 4 ^ nextPoint.PrevValue1
                    self.position += 1
                elif code == codeWords.ValueXor16:
                    valueRaw = np.uint32(self.data[self.position]) ^ np.uint32(self.data[self.position+1]) << 8 ^ nextPoint.PrevValue1
                    self.position += 2
                elif code == codeWords.ValueXor20:
                    valueRaw = np.uint32(self.readBits4()) ^ np.uint32(self.data[self.position]) << 4 ^ np.uint32(self.data[self.position+1]) << 12 ^ nextPoint.PrevValue1
                    self.position += 2
                elif code == codeWords.ValueXor24:
                    valueRaw = np.uint32(self.data[self.position]) ^ np.uint32(self.data[self.position+1]) << 8 ^ np.uint32(self.data[self.position+2]) << 16 ^ nextPoint.PrevValue1
                    self.position += 3
                elif code == codeWords.ValueXor28:
                    valueRaw = np.uint32(self.readBits4()) ^ np.uint32(self.data[self.position]) << 4 ^ np.uint32(self.data[self.position+1]) << 12 ^ np.uint32(self.data[self.position+2]) << 20 ^ nextPoint.PrevValue1
                    self.position += 3
                elif code == codeWords.ValueXor32:
                    valueRaw = np.uint32(self.data[self.position]) ^ np.uint32(self.data[self.position+1]) << 8 ^ np.uint32(self.data[self.position+2]) << 16 ^ np.uint32(self.data[self.position+3]) << 24 ^ nextPoint.PrevValue1
                    self.position += 4
                else:
                    message = [
                        f"invalid code received {code}"
                        f" at position {self.position}"
                        f" with last position {self.lastPosition}"
                    ]

                    return 0, 0, 0, 0.0, False, RuntimeError("".join(message))

                nextPoint.PrevValue3 = nextPoint.PrevValue2
                nextPoint.PrevValue2 = nextPoint.PrevValue1
                nextPoint.PrevValue1 = valueRaw

            # Assign decoded measurement value to out parameter
            value = NativeEndian.to_float32(NativeEndian.from_int32(valueRaw))
            self.lastPoint = nextPoint       

        return pointid, timestamp, stateFlags, value, True, None     

    def decode_pointid(self, code: np.byte) -> Optional[Exception]:
        if code == codeWords.PointIDXor4:
            self.lastPoint.PrevNextPointID1 = self.readBits4() ^ self.lastPoint.PrevNextPointID1
        elif code == codeWords.PointIDXor8:
            self.lastPoint.PrevNextPointID1 = np.int32(self.data[self.position]) ^ self.lastPoint.PrevNextPointID1
            self.position += 1
        elif code == codeWords.PointIDXor12:
            self.lastPoint.PrevNextPointID1 = self.readBits4() ^ np.int32(self.data[self.position]) << 4 ^ self.lastPoint.PrevNextPointID1
            self.position += 1
        elif code == codeWords.PointIDXor16:
            self.lastPoint.PrevNextPointID1 = np.int32(self.data[self.position]) ^ np.int32(self.data[self.position + 1]) << 8 ^ self.lastPoint.PrevNextPointID1
            self.position += 2
        elif code == codeWords.PointIDXor20:
            self.lastPoint.PrevNextPointID1 = self.readBits4() ^ np.int32(self.data[self.position]) << 4 ^ np.int32(self.data[self.position + 1]) << 12 ^ self.lastPoint.PrevNextPointID1
            self.position += 2
        elif code == codeWords.PointIDXor24:
            self.lastPoint.PrevNextPointID1 = np.int32(self.data[self.position]) ^ np.int32(self.data[self.position + 1]) << 8 ^ np.int32(self.data[self.position + 2]) << 16 ^ self.lastPoint.PrevNextPointID1
            self.position += 3
        elif code == codeWords.PointIDXor32:
            self.lastPoint.PrevNextPointID1 = np.int32(self.data[self.position]) ^ np.int32(self.data[self.position + 1]) << 8 ^ np.int32(self.data[self.position + 2]) << 16 ^ np.int32(self.data[self.position + 3]) << 24 ^ self.lastPoint.PrevNextPointID1
            self.position += 4
        else:
            message = [
                f"invalid code received {code}"
                f" at position {self.position}"
                f" with last position {self.lastPosition}"
            ]

            return RuntimeError("".join(message))
        
        return None

    def decode_timestamp(self, code: np.byte) -> np.int64:
        timestamp = np.int64(0)

        if code == codeWords.TimeDelta1Forward:
            timestamp = self.prevTimestamp1 + self.prevTimeDelta1
        elif code == codeWords.TimeDelta2Forward:
            timestamp = self.prevTimestamp1 + self.prevTimeDelta2
        elif code == codeWords.TimeDelta3Forward:
            timestamp = self.prevTimestamp1 + self.prevTimeDelta3
        elif code == codeWords.TimeDelta4Forward:
            timestamp = self.prevTimestamp1 + self.prevTimeDelta4
        elif code == codeWords.TimeDelta1Reverse:
            timestamp = self.prevTimestamp1 - self.prevTimeDelta1
        elif code == codeWords.TimeDelta2Reverse:
            timestamp = self.prevTimestamp1 - self.prevTimeDelta2
        elif code == codeWords.TimeDelta3Reverse:
            timestamp = self.prevTimestamp1 - self.prevTimeDelta3
        elif code == codeWords.TimeDelta4Reverse:
            timestamp = self.prevTimestamp1 - self.prevTimeDelta4
        elif code == codeWords.Timestamp2:
            timestamp = self.prevTimestamp2
        else:
            value, self.position = Decoder.decode7BitUInt64(self.data, self.position)
            timestamp = self.prevTimestamp1 ^ np.int64(value)

        # Save the smallest delta time
        minDelta = abs(self.prevTimestamp1 - timestamp)

        if minDelta < self.prevTimeDelta4 and minDelta != self.prevTimeDelta1 and minDelta != self.prevTimeDelta2 and minDelta != self.prevTimeDelta3:
            if minDelta < self.prevTimeDelta1:
                self.prevTimeDelta4 = self.prevTimeDelta3
                self.prevTimeDelta3 = self.prevTimeDelta2
                self.prevTimeDelta2 = self.prevTimeDelta1
                self.prevTimeDelta1 = minDelta
            elif minDelta < self.prevTimeDelta2:
                self.prevTimeDelta4 = self.prevTimeDelta3
                self.prevTimeDelta3 = self.prevTimeDelta2
                self.prevTimeDelta2 = minDelta
            elif minDelta < self.prevTimeDelta3:
                self.prevTimeDelta4 = self.prevTimeDelta3
                self.prevTimeDelta3 = minDelta
            else:
                self.prevTimeDelta4 = minDelta

        self.prevTimestamp2 = self.prevTimestamp1
        self.prevTimestamp1 = timestamp

        return timestamp

    def decodeStateFlags(self, code: np.byte, nextPoint: PointMetadata) -> np.uint32:
        if code == codeWords.StateFlags2:
            stateFlags = nextPoint.PrevStateFlags2
        else:
            stateFlags, self.position = self.decode7BitUInt32(self.data, self.position)

        nextPoint.PrevStateFlags2 = nextPoint.PrevStateFlags1
        nextPoint.PrevStateFlags1 = stateFlags

        return stateFlags

    def readBit(self) -> np.int32:
        if self.bitStreamCount == np.int32(0):
            self.bitStreamCount = np.int32(8)
            self.bitStreamCache = np.int32(self.position)
            self.position += 1

        self.bitStreamCount -= 1

        return self.bitStreamCache >> self.bitStreamCount & np.int32(1)

    def readBits4(self) -> np.int32:
        return self.readBit() << np.int32(3) | self.readBit() << np.int32(2) | self.readBit() << np.int32(1) | self.readBit()

    def readBits5(self) -> np.int32:
        return self.readBit() << np.int32(4) | self.readBit() << np.int32(3) | self.readBit() << np.int32(2) | self.readBit() << np.int32(1) | self.readBit()

    @staticmethod
    def decode7BitUInt32(stream: bytes, position: int) -> Tuple[np.uint32, int]:
        stream = stream[position:]
        value = np.uint32(stream[0])

        if value < np.uint32(128):
            position += 1
            return value

        value ^= np.uint32(stream[1]) << np.uint32(7)

        if value < np.uint32(16384):
            position += 2
            return value ^ np.uint32(0x80)

        value ^= np.uint32(stream[2]) << np.uint32(14)

        if value < np.uint32(2097152):
            position += 3
            return value ^ np.uint32(0x4080)

        value ^= np.uint32(stream[3]) << np.uint32(21)

        if value < np.uint32(268435456):
            position += 4
            return value ^ np.uint32(0x204080)

        value ^= np.uint32(stream[4]) << np.uint32(28)
        position += 5

        return (value ^ np.uint32(0x10204080), position)

    @staticmethod
    def decode7BitUInt64(stream: bytes, position: int) -> Tuple[np.uint64, int]:
        stream = stream[position:]
        value = np.uint64(stream[0])

        if value < np.uint64(128):
            position += 1
            return value

        value ^= np.uint64(stream[1]) << np.uint64(7)

        if value < np.uint64(16384):
            position += 2
            return value ^ np.uint64(0x80)

        value ^= np.uint64(stream[2]) << np.uint64(14)

        if value < np.uint64(2097152):
            position += 3
            return value ^ np.uint64(0x4080)

        value ^= np.uint64(stream[3]) << np.uint64(21)

        if value < np.uint64(268435456):
            position += 4
            return value ^ np.uint64(0x204080)

        value ^= np.uint64(stream[4]) << np.uint64(28)

        if value < np.uint64(34359738368):
            position += 5
            return value ^ np.uint64(0x10204080)

        value ^= np.uint64(stream[5]) << np.uint64(35)

        if value < np.uint64(4398046511104):
            position += 6
            return value ^ np.uint64(0x810204080)

        value ^= np.uint64(stream[6]) << np.uint64(42)

        if value < np.uint64(562949953421312):
            position += 7
            return value ^ np.uint64(0x40810204080)

        value ^= np.uint64(stream[7]) << np.uint64(49)

        if value < np.uint64(72057594037927936):
            position += 8
            return value ^ np.uint64(0x2040810204080)

        value ^= np.uint64(stream[8]) << np.uint64(56)
        position += 9

        return (value ^ np.uint64(0x102040810204080), position)