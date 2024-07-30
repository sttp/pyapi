# ******************************************************************************************************
#  __init__.py - Gbtc
#
#  Copyright © 2021, Grid Protection Alliance.  All Rights Reserved.
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
#  02/01/2021 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from enum import Enum
from decimal import Decimal
from datetime import datetime, timezone
from dateutil import parser
from dateutil.tz import tzoffset
from uuid import UUID
from typing import Sequence
import numpy as np


def static_init(cls):
    """
    Marks a class as having a static initialization function and
    executes the function when class is statically constructed.
    """
    if getattr(cls, "static_init", None):
        cls.static_init()

    return cls


def virtual(self):
    """
    Marks a method as overridable (for documentation purposes).
    """
    return self


def override(self):
    """
    Marks a method as an override (for documentation purposes).
    """
    return self


class Empty:
    GUID = UUID(int=0)
    DATETIME = datetime.min.replace(tzinfo=timezone.utc)
    DECIMAL = Decimal(0.0)
    TICKS = np.uint64(0)
    STRING = ""
    SINGLE = np.float32(0.0)
    DOUBLE = np.float64(0.0)
    INT8 = np.int8(0)
    INT16 = np.int16(0)
    INT32 = np.int32(0)
    INT64 = np.int64(0)
    UINT8 = np.uint8(0)
    UINT16 = np.uint16(0)
    UINT32 = np.uint32(0)
    UINT64 = np.uint64(0)


class Limits:
    MAXTICKS = 3155378975999999999
    MAXBYTE = np.iinfo(np.uint8).max
    MININT16 = np.iinfo(np.int16).min
    MAXINT16 = np.iinfo(np.int16).max
    MAXUINT16 = np.iinfo(np.uint16).max
    MININT32 = np.iinfo(np.int32).min
    MAXINT32 = np.iinfo(np.int32).max
    MAXUINT32 = np.iinfo(np.uint32).max
    MININT64 = np.iinfo(np.int64).min
    MAXINT64 = np.iinfo(np.int64).max
    MAXUINT64 = np.iinfo(np.uint64).max


class ByteSize:
    INT8 = 1
    UINT8 = 1
    INT16 = 2
    UINT16 = 2
    INT32 = 4
    UINT32 = 4
    INT64 = 8
    UINT64 = 8
    FLOAT16 = 2
    FLOAT32 = 4
    FLOAT64 = 8


def normalize_enumname(value: Enum) -> str:
    parts = str(value).split(".")
    return parts[1].capitalize() if len(parts) == 2 else str(value).capitalize()

class Validate:
    @staticmethod
    def parameters(array: Sequence, startIndex: int, length: int):
        """
        Validates array or buffer parameters.
        """

        if array is None:
            raise TypeError("array is None")

        if startIndex < 0:
            raise ValueError("startIndex cannot be negative")

        if length < 0:
            raise ValueError("value cannot be negative")

        if startIndex + length > len(array):
            raise ValueError(
                f"startIndex of {startIndex:,} and length of {length:,} will exceed array size of {len(array):,}")


class Convert:
    @staticmethod
    def from_str(value: str, dtype: np.dtype) -> object:
        """
        Converts a string value to the specified type.
        """

        if dtype == datetime:
            dt = parser.parse(value)

            if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt).seconds != 0:
                dt = dt.astimezone(tzoffset(None, 0))

            return dt.replace(tzinfo=None)

        if dtype in [float, np.float32, np.float64]:
            return np.float64(value).astype(dtype)

        if dtype in [int, np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64]:
            if "X" in value.upper():
                return np.uint64(int(value, base=16)).astype(dtype)

            if dtype == int:
                return int(value)

            if "-" in value:
                return np.int64(value).astype(dtype)

            return np.uint64(value).astype(dtype)

        if dtype == str:
            return value

        raise TypeError(f"Unsupported target conversion type: {dtype}")
