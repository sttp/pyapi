#******************************************************************************************************
#  compactMeasurement.py - Gbtc
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

from enum import IntFlags
from ..gsf import Limits
from ..ticks.ticks import Ticks
from .measurement import Measurement
from .constants import StateFlags
from .signalIndexCache import SignalIndexCache
from uuid import UUID
import numpy as np


class compactStateFlags(IntFlags):
    """
    Enumeration constants represent each flag in the 8-bit compact measurement state flags.
    """

    DataRange = 0x01
    DataQuality = 0x02
    TimeQuality = 0x04
    SystemIssue = 0x08
    CalculatedValue = 0x10
    DiscardedValue = 0x20
    BaseTimeOffset = 0x40
    TimeIndex = 0x80


DATARANGEMASK: StateFlags = 0x000000FC
DATAQUALITYMASK: StateFlags = 0x0000EF03
TIMEQUALITYMASK: StateFlags = 0x00BF0000
SYSTEMISSUEMASK: StateFlags = 0xE0000000
CALCULATEDVALUEMASK: StateFlags = 0x00001000
DISCARDEDVALUEMASK: StateFlags = 0x00400000

FIXEDLENGTH: np.uint32 = 9


def mapToFullFlags(compactFlags: compactStateFlags) -> StateFlags:
    fullFlags: StateFlags = StateFlags.Normal

    if (compactFlags & compactStateFlags.DataRange) > 0:
        fullFlags |= DATARANGEMASK

    if (compactFlags & compactStateFlags.DataQuality) > 0:
        fullFlags |= DATAQUALITYMASK

    if (compactFlags & compactStateFlags.TimeQuality) > 0:
        fullFlags |= TIMEQUALITYMASK

    if (compactFlags & compactStateFlags.SystemIssue) > 0:
        fullFlags |= SYSTEMISSUEMASK

    if (compactFlags & compactStateFlags.CalculatedValue) > 0:
        fullFlags |= CALCULATEDVALUEMASK

    if (compactFlags & compactStateFlags.DiscardedValue) > 0:
        fullFlags |= DISCARDEDVALUEMASK

    return fullFlags


def mapToCompactFlags(fullFlags: StateFlags) -> compactStateFlags:
    compactFlags: compactStateFlags = 0

    if (fullFlags & DATARANGEMASK) > 0:
        compactFlags |= compactStateFlags.DataRange

    if (fullFlags & DATAQUALITYMASK) > 0:
        compactFlags |= compactStateFlags.DataQuality

    if (fullFlags & TIMEQUALITYMASK) > 0:
        compactFlags |= compactStateFlags.TimeQuality

    if (fullFlags & SYSTEMISSUEMASK) > 0:
        compactFlags |= compactStateFlags.SystemIssue

    if (fullFlags & CALCULATEDVALUEMASK) > 0:
        compactFlags |= compactStateFlags.CalculatedValue

    if (fullFlags & DISCARDEDVALUEMASK) > 0:
        compactFlags |= compactStateFlags.DiscardedValue

    return compactFlags


class CompactMeasurement(Measurement):
    """
    Represents a measured value, in simple compact format, for transmission or reception in STTP.
    """

    def __init__(self,
                 signalIndexCache: SignalIndexCache,
                 includeTime: bool,
                 useMillisecondResolution: bool,
                 baseTimeOffsets: np.int64[2],
                 signalID: UUID = ...,
                 value: np.float64 = ...,
                 timestamp: np.uint64 = ...,
                 flags: StateFlags = ...
                 ):
        super().__init__(signalID, value, timestamp, flags)
        self.signalIndexCache = signalIndexCache
        self.includeTime = includeTime
        self.useMillisecondResolution = useMillisecondResolution
        self.baseTimeOffsets = baseTimeOffsets
        self.timeIndex = 0
        self.usingBaseTimeOffset = False

    def GetBinaryLength(self) -> np.uint32:
        """
        Gets the binary byte length of a CompactMeasurement
        """

        length: np.uint32 = FIXEDLENGTH

        if not self.includeTime:
            return length

        baseTimeOffset: np.int64 = self.baseTimeOffsets[self.timeIndex]

        if baseTimeOffset > 0:
            # See if timestamp will fit within space allowed for active base offset. We cache result so that post call
            # to GetBinaryLength, result will speed other subsequent parsing operations by not having to reevaluate.
            difference = self.TicksValue() - baseTimeOffset

            if difference > 0:
                if self.useMillisecondResolution:
                    self.usingBaseTimeOffset = np.int64(
                        difference / Ticks.PERMILLISECOND) < Limits.MAXUINT16
                else:
                    self.usingBaseTimeOffset = difference < Limits.MAXUINT32
            else:
                self.usingBaseTimeOffset = False

            if self.usingBaseTimeOffset:
                if self.useMillisecondResolution:
                    length += 2  # Use two bytes for millisecond resolution timestamp with valid offset
                else:
                    length += 4  # Use four bytes for tick resolution timestamp with valid offset
            else:
                length += 8  # Use eight bytes for full fidelity time
        else:
            # Use eight bytes for full fidelity time
            length += 8

        return length

    def GetTimestampC2(self) -> np.uint16:
        """
        Gets offset compressed millisecond-resolution 2-byte timestamp.
        """
        return np.uint16((self.TicksValue() - self.baseTimeOffsets[self.timeIndex]) / Ticks.PERMILLISECOND)

    def GetTimestampC4(self) -> np.uint32:
        """
        Gets offset compressed tick-resolution 4-byte timestamp.
        """
        return np.uint32(self.TicksValue() - self.baseTimeOffsets[self.timeIndex])

    def GetCompactStateFlags(self) -> np.byte:
        """
        Gets byte level compact state flags with encoded time index and base time offset bits.
        """

        # Encode compact state flags
        flags: compactStateFlags = mapToCompactFlags(self.Flags)

        if self.timeIndex != 0:
            flags |= compactStateFlags.TimeIndex

        if self.usingBaseTimeOffset:
            flags |= compactStateFlags.BaseTimeOffset

        return np.byte(flags)

    def SetCompactStateFlags(self, value: np.byte):
        """
        Sets byte level compact state flags with encoded time index and base time offset bits.
        """

        # Decode compact state flags
        flags = compactStateFlags(value)

        self.Flags = mapToFullFlags(flags)

        if (flags & compactStateFlags.TimeIndex) > 0:
            self.timeIndex = 1
        else:
            self.timeIndex = 0

        self.usingBaseTimeOffset = (
            flags & compactStateFlags.BaseTimeOffset) > 0
