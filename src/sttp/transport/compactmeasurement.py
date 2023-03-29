#******************************************************************************************************
#  compact_measurement.py - Gbtc
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
#  08/!4/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from enum import IntFlag
from gsf import Limits
from gsf.endianorder import BigEndian
from ..ticks import Ticks
from .measurement import Measurement
from .constants import StateFlags
from .signalindexcache import SignalIndexCache
from typing import List, Tuple, Optional
from uuid import UUID
import numpy as np


class CompactStateFlags(IntFlag):
    """
    Enumeration constants represent each flag in the 8-bit compact measurement state flags.
    """

    DATARANGE = 0x01
    DATAQUALITY = 0x02
    TIMEQUALITY = 0x04
    SYSTEMISSUE = 0x08
    CALCULATEDVALUE = 0x10
    DISCARDEDVALUE = 0x20
    BASETIMEOFFSET = 0x40
    TIMEINDEX = 0x80


DATARANGEMASK: StateFlags = 0x000000FC
DATAQUALITYMASK: StateFlags = 0x0000EF03
TIMEQUALITYMASK: StateFlags = 0x00BF0000
SYSTEMISSUEMASK: StateFlags = 0xE0000000
CALCULATEDVALUEMASK: StateFlags = 0x00001000
DISCARDEDVALUEMASK: StateFlags = 0x00400000

FIXEDLENGTH: np.uint32 = 9


def _map_to_fullflags(compactflags: CompactStateFlags) -> StateFlags:
    fullflags: StateFlags = StateFlags.NORMAL

    if (compactflags & CompactStateFlags.DATARANGE) > 0:
        fullflags |= DATARANGEMASK

    if (compactflags & CompactStateFlags.DATAQUALITY) > 0:
        fullflags |= DATAQUALITYMASK

    if (compactflags & CompactStateFlags.TIMEQUALITY) > 0:
        fullflags |= TIMEQUALITYMASK

    if (compactflags & CompactStateFlags.SYSTEMISSUE) > 0:
        fullflags |= SYSTEMISSUEMASK

    if (compactflags & CompactStateFlags.CALCULATEDVALUE) > 0:
        fullflags |= CALCULATEDVALUEMASK

    if (compactflags & CompactStateFlags.DISCARDEDVALUE) > 0:
        fullflags |= DISCARDEDVALUEMASK

    return fullflags


def _map_to_compactflags(fullflags: StateFlags) -> CompactStateFlags:
    compactflags: CompactStateFlags = 0

    if (fullflags & DATARANGEMASK) > 0:
        compactflags |= CompactStateFlags.DATARANGE

    if (fullflags & DATAQUALITYMASK) > 0:
        compactflags |= CompactStateFlags.DATAQUALITY

    if (fullflags & TIMEQUALITYMASK) > 0:
        compactflags |= CompactStateFlags.TIMEQUALITY

    if (fullflags & SYSTEMISSUEMASK) > 0:
        compactflags |= CompactStateFlags.SYSTEMISSUE

    if (fullflags & CALCULATEDVALUEMASK) > 0:
        compactflags |= CompactStateFlags.CALCULATEDVALUE

    if (fullflags & DISCARDEDVALUEMASK) > 0:
        compactflags |= CompactStateFlags.DISCARDEDVALUE

    return compactflags


class CompactMeasurement(Measurement):
    """
    Represents a measured value, in simple compact format, for transmission or reception in STTP.
    """

    def __init__(self,
                 signalindexcache: SignalIndexCache,
                 includetime: bool,
                 usemillisecondresolution: bool,
                 basetimeoffsets: List[np.int64],
                 signalid: UUID = ...,
                 value: np.float64 = ...,
                 timestamp: np.uint64 = ...,
                 flags: StateFlags = ...
                 ):

        super().__init__(signalid, value, timestamp, flags)

        self._signalindexcache = signalindexcache
        self._includetime = includetime
        self._usemillisecondresolution = usemillisecondresolution
        self._basetimeoffsets = basetimeoffsets
        self._timeindex = 0
        self._usingbasetimeoffset = False

    def get_binarylength(self) -> np.uint32:  # sourcery skip: assign-if-exp
        """
        Gets the binary byte length of a `CompactMeasurement`
        """

        length = FIXEDLENGTH

        if not self._includetime:
            return length

        basetimeoffset = self._basetimeoffsets[self._timeindex]

        if basetimeoffset > 0:
            # See if timestamp will fit within space allowed for active base offset. We cache result so that post call
            # to GetBinaryLength, result will speed other subsequent parsing operations by not having to reevaluate.
            difference = self.timestampvalue - basetimeoffset

            if difference > 0:
                if self._usemillisecondresolution:
                    self._usingbasetimeoffset = np.int64(
                        difference / Ticks.PERMILLISECOND) < Limits.MAXUINT16
                else:
                    self._usingbasetimeoffset = difference < Limits.MAXUINT32
            else:
                self._usingbasetimeoffset = False

            if self._usingbasetimeoffset:
                if self._usemillisecondresolution:
                    length += 2  # Use two bytes for millisecond resolution timestamp with valid offset
                else:
                    length += 4  # Use four bytes for tick resolution timestamp with valid offset
            else:
                length += 8  # Use eight bytes for full fidelity time
        else:
            # Use eight bytes for full fidelity time
            length += 8

        return length

    def get_timestamp_c2(self) -> np.uint16:
        """
        Gets offset compressed millisecond-resolution 2-byte timestamp.
        """

        return np.uint16((self.timestampvalue - self._basetimeoffsets[self._timeindex]) / Ticks.PERMILLISECOND)

    def get_timestamp_c4(self) -> np.uint32:
        """
        Gets offset compressed tick-resolution 4-byte timestamp.
        """

        return np.uint32(self.timestampvalue - self._basetimeoffsets[self._timeindex])

    def get_compact_stateflags(self) -> np.byte:
        """
        Gets byte level compact state flags with encoded time index and base time offset bits.
        """

        # Encode compact state flags
        flags: CompactStateFlags = _map_to_compactflags(self.flags)

        if self._timeindex != 0:
            flags |= CompactStateFlags.TIMEINDEX

        if self._usingbasetimeoffset:
            flags |= CompactStateFlags.BASETIMEOFFSET

        return np.byte(flags)

    def set_compact_stateflags(self, value: np.byte):
        """
        Sets byte level compact state flags with encoded time index and base time offset bits.
        """

        # Decode compact state flags
        flags = CompactStateFlags(value)

        self.flags = _map_to_fullflags(flags)

        self._timeindex = 1 if flags & CompactStateFlags.TIMEINDEX > 0 else 0

        self._usingbasetimeoffset = (
            flags & CompactStateFlags.BASETIMEOFFSET) > 0

    @property
    def runtimeid(self) -> np.int32:
        """
        Gets the 4-byte run-time signal index for this measurement.
        """

        return self._signalindexcache.signalindex(self.signalid)

    @runtimeid.setter
    def runtimeid(self, value: np.int32):
        """
        Sets the 4-byte run-time signal index for this measurement.

        Notes
        -----
        This assigns `CompactMeasurement` signal ID from the specified signal index
        based on lookup in the active `SignalIndexCache`.
        """

        self.signalid = self._signalindexcache.signalid(value)

    def decode(self, buffer: bytes) -> Tuple[int, Optional[Exception]]:
        """
        Parses a `CompactMeasurement` from the specified byte buffer.
        """

        if len(buffer) < FIXEDLENGTH:
            return 0, ValueError("not enough buffer available to deserialize compact measurement")

        # Basic Compact Measurement Format:
        # 		Field:     Bytes:
        # 		--------   -------
        #		 Flags        1
        #		  ID          4
        #		 Value        4
        #		 [Time]    0/2/4/8

        # Decode state flags
        self.set_compact_stateflags(buffer[0])
        index = 1

        # Decode runtime ID
        self.runtimeid = np.int32(BigEndian.to_uint32(buffer[index:]))
        index += 4

        # Decode value
        self.value = np.float64(BigEndian.to_float32(buffer[index:]))
        index += 4

        if not self._includetime:
            return index, None

        if self._usingbasetimeoffset:
            basetimeoffset = np.uint64(self._basetimeoffsets[self._timeindex])

            if self._usemillisecondresolution:
                # Decode 2-byte millisecond offset timestamp
                if basetimeoffset > 0:
                    self.timestamp = basetimeoffset + np.uint64(BigEndian.to_uint16(buffer[index:])) * Ticks.PERMILLISECOND

                index += 2
            else:
                # Decode 4-byte tick offset timestamp
                if basetimeoffset > 0:
                    self.timestamp = basetimeoffset + np.uint64(BigEndian.to_uint32(buffer[index:]))

                index += 4
        else:
            # Decode 8-byte full fidelity timestamp
            # Note that only a full fidelity timestamp can carry leap second flags
            self.timestamp = BigEndian.to_uint64(buffer[index:])
            index += 8

        return index, None
