#******************************************************************************************************
#  endianorder.py - Gbtc
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
#  08/18/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from gsf import static_init, ByteSize
from typing import Union
import struct
import sys
import numpy as np


@static_init
class NativeEndian:
    """
    Manages conversion of bytes to basic types in native endian order.
    """

    @classmethod
    def static_init(cls):
        cls.target_byteorder = sys.byteorder
        cls.swaporder = False

    @classmethod
    def _from_buffer(cls, buffer: bytes, bytesize: int, dtype: np.dtype) -> Union[int, float]:
        if len(buffer) < bytesize:
            raise ValueError(f"Buffer size too small, {bytesize} bytes required to convert bytes to {bytesize * 8}-bit type")

        if cls.swaporder:
            dtype = dtype.newbyteorder()

        return np.frombuffer(buffer[:bytesize], dtype)[0]

    @classmethod
    def _int_to_bytes(cls, bytesize: int, value: int, signed: bool) -> bytes:
        # sourcery skip: remove-unnecessary-cast
        return int(value).to_bytes(bytesize, cls.target_byteorder, signed=signed)

    @classmethod
    def _float_to_bytes(cls, bytesize: int, value: float) -> bytes:
        return struct.pack(f"{'>' if cls.target_byteorder == 'big' else '<'}{'e' if bytesize == 2 else 'f' if bytesize == 4 else 'd'}", value)

    @classmethod
    def to_int16(cls, buffer: bytes) -> np.int16:
        return cls._from_buffer(buffer, ByteSize.INT16, np.dtype(np.int16))

    @classmethod
    def from_int16(cls, value: np.int16) -> bytes:
        return cls._int_to_bytes(ByteSize.INT16, value, True)

    @classmethod
    def to_uint16(cls, buffer: bytes) -> np.uint16:
        return cls._from_buffer(buffer, ByteSize.UINT16, np.dtype(np.uint16))

    @classmethod
    def from_uint16(cls, value: np.uint16) -> bytes:
        return cls._int_to_bytes(ByteSize.UINT16, value, False)

    @classmethod
    def to_int32(cls, buffer: bytes) -> np.int32:
        return cls._from_buffer(buffer, ByteSize.INT32, np.dtype(np.int32))

    @classmethod
    def from_int32(cls, value: np.int32) -> bytes:
        return cls._int_to_bytes(ByteSize.INT32, value, True)

    @classmethod
    def to_uint32(cls, buffer: bytes) -> np.uint32:
        return cls._from_buffer(buffer, ByteSize.UINT32, np.dtype(np.uint32))

    @classmethod
    def from_uint32(cls, value: np.uint32) -> bytes:
        return cls._int_to_bytes(ByteSize.UINT32, value, False)

    @classmethod
    def to_int64(cls, buffer: bytes) -> np.int64:
        return cls._from_buffer(buffer, ByteSize.INT64, np.dtype(np.int64))

    @classmethod
    def from_int64(cls, value: np.int64) -> bytes:
        return cls._int_to_bytes(ByteSize.INT64, value, True)

    @classmethod
    def to_uint64(cls, buffer: bytes) -> np.uint64:
        return cls._from_buffer(buffer, ByteSize.UINT64, np.dtype(np.uint64))

    @classmethod
    def from_uint64(cls, value: np.uint64) -> bytes:
        return cls._int_to_bytes(ByteSize.UINT64, value, False)

    @classmethod
    def to_float16(cls, buffer: bytes) -> np.float16:
        return cls._from_buffer(buffer, ByteSize.FLOAT16, np.dtype(np.float16))

    @classmethod
    def from_float16(cls, value: np.float16) -> bytes:
        return cls._float_to_bytes(ByteSize.FLOAT16, value)

    @classmethod
    def to_float32(cls, buffer: bytes) -> np.float32:
        return cls._from_buffer(buffer, ByteSize.FLOAT32, np.dtype(np.float32))

    @classmethod
    def from_float32(cls, value: np.float32) -> bytes:
        return cls._float_to_bytes(ByteSize.FLOAT32, value)

    @classmethod
    def to_float64(cls, buffer: bytes) -> np.float64:
        return cls._from_buffer(buffer, ByteSize.FLOAT64, np.dtype(np.float64))

    @classmethod
    def from_float64(cls, value: np.float64) -> bytes:
        return cls._float_to_bytes(ByteSize.FLOAT64, value)


@static_init
class BigEndian(NativeEndian):
    """
    Manages conversion of bytes to basic types in big endian order.
    """

    @classmethod
    def static_init(cls):
        cls.target_byteorder = "big"
        cls.swaporder = sys.byteorder != cls.target_byteorder


@static_init
class LittleEndian(NativeEndian):
    """
    Manages conversion of bytes to basic types in little endian order.
    """

    @classmethod
    def static_init(cls):
        cls.target_byteorder = "little"
        cls.swaporder = sys.byteorder != cls.target_byteorder
