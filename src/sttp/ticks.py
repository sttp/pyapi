#******************************************************************************************************
#  ticks.py - Gbtc
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
#  08/15/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from datetime import datetime, timedelta
from gsf import Empty
import numpy as np


class Ticks:
    """
    Defines constants and functions for tick values, 64-bit integers used to designate time in STTP. A tick value represents
    the number of 100-nanosecond intervals that have elapsed since 12:00:00 midnight, January 1, 0001 UTC, Gregorian calendar.
    A single tick represents one hundred nanoseconds, or one ten-millionth of a second. There are 10,000 ticks in a millisecond
    and 10 million ticks in a second. Only bits 01 to 62 (0x3FFFFFFFFFFFFFFF) are used to represent the timestamp value.
    Bit 64 (0x8000000000000000) is used to denote leap second, i.e., second 60, where actual second value would remain at 59.
    Bit 63 is reserved and unset.
    """

    PERSECOND = np.uint64(10000000)
    """
    Number of Ticks that occur in a second.
    """

    PERMILLISECOND = np.uint64(PERSECOND / 1000)
    """
    Number of Ticks that occur in a millisecond.
    """

    PERMICROSECOND = np.uint64(PERSECOND / 1000000)
    """
    Number of Ticks that occur in a microsecond.
    """

    PERMINUTE = np.uint64(60 * PERSECOND)
    """
    Number of Ticks that occur in a minute.
    """

    PERHOUR = np.uint64(60 * PERMINUTE)
    """
    Number of Ticks that occur in an hour.
    """

    PERDAY = np.uint64(24 * PERHOUR)
    """
    Number of Ticks that occur in a day.
    """

    LEAPSECONDFLAG = np.uint64(1 << 63)
    """
    Flag (64th bit) that marks a Ticks value as a leap second, i.e., second 60 (one beyond normal second 59).
    """

    RESERVEDUTCFLAG = np.uint64(1 << 62)
    """
    Reserved flag (63rd bit) that should be unset when serializing and deserializing Ticks.
    """

    VALUEMASK = np.uint64(~LEAPSECONDFLAG & ~RESERVEDUTCFLAG)
    """
    All bits (bits 1 to 62) that make up the value portion of a Ticks that represent time.
    """

    UNIXBASEOFFSET = np.uint64(621355968000000000)
    """
    Ticks representation of the Unix epoch timestamp starting at January 1, 1970.
    """

    @staticmethod
    def from_datetime(dt: datetime) -> np.uint64:
        """
        Converts a standard Python dattime value to a Ticks value.
        """
        return np.uint64((dt - Empty.DATETIME).total_seconds() * 10000000)

    @staticmethod
    def from_timedelta(td: timedelta) -> np.uint64:
        """
        Converts a standard Python timedelta value to a Ticks value.
        """
        return np.uint64(td.total_seconds() * 10000000)

    @staticmethod
    def to_datetime(ticks: np.uint64) -> datetime:
        """
        Converts a Ticks value to standard Python datetime value.
        """
        return Empty.DATETIME + timedelta(microseconds=ticks // 10)

    @staticmethod
    def is_leapsecond(ticks: np.uint64) -> bool:
        """
        Determines if the deserialized Ticks value represents a leap second, i.e., second 60.
        """
        return (ticks & Ticks.LEAPSECONDFLAG) > 0

    @staticmethod
    def set_leapsecond(ticks: np.uint64) -> np.uint64:
        """
        Flags a Ticks value to represent a leap second, i.e., second 60, before wire serialization.
        """
        return np.uint64(ticks | Ticks.LEAPSECONDFLAG)

    @staticmethod
    def now() -> np.uint64:
        """
        Gets the current local time as a Ticks value.
        """
        return Ticks.from_datetime(datetime.now())

    @staticmethod
    def utcnow() -> np.uint64:  # sourcery skip: aware-datetime-for-utc
        """
        Gets the current time in UTC as a Ticks value.
        """
        return Ticks.from_datetime(datetime.utcnow())

    @staticmethod
    def to_string(ticks: np.uint64, timespec: str = 'microseconds') -> str:
        """
        Standard timestamp representation for a Ticks value, e.g., 2006-01-02 15:04:05.999999999.
        """
        return Ticks.to_datetime(ticks).isoformat(sep=' ', timespec=timespec)

    @staticmethod
    def to_shortstring(ticks: np.uint64) -> str:
        """
        Shows just the timestamp portion of a Ticks value with milliseconds, e.g., 15:04:05.999.
        """
        return Ticks.to_string(ticks, "milliseconds").split(" ")[1]
