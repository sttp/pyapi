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
from typing import Any
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
    def _read(cls, buffer: bytes, bytesize: int, dtype: np.dtype) -> Any:
        if len(buffer) < bytesize:
            raise ValueError(f"Buffer size too small, {bytesize} bytes required to read {bytesize * 8}-bit type")

        if cls.swaporder:
            dtype = dtype.newbyteorder()

        return np.frombuffer(buffer[0:bytesize], dtype)[0]

    @classmethod
    def _write_int(cls, buffer: bytearray, bytesize: int, value: int, signed: bool):
        if len(buffer) < bytesize:
            raise ValueError(f"Buffer size too small, {bytesize} bytes required to write {bytesize * 8}-bit type")

        buffer[:bytesize] = int(value).to_bytes(bytesize, cls.target_byteorder, signed=signed)

    @classmethod
    def _write_float(cls, buffer: bytearray, bytesize: int, value: float):
        if len(buffer) < bytesize:
            raise ValueError(f"Buffer size too small, {bytesize} bytes required to write {bytesize * 8}-bit type")

        buffer[:bytesize] = struct.pack(
            f"{'>' if cls.target_byteorder == 'big' else '<'}{'e' if bytesize == 2 else 'f' if bytesize == 4 else 'd'}", value)

    @classmethod
    def int16(cls, buffer: bytes) -> np.int16:
        return cls._read(buffer, ByteSize.INT16, np.dtype(np.int16))

    @classmethod
    def putint16(cls, buffer: bytearray, value: np.int16):
        cls._write_int(buffer, ByteSize.INT16, value, True)

    @classmethod
    def uint16(cls, buffer: bytes) -> np.uint16:
        return cls._read(buffer, ByteSize.UINT16, np.dtype(np.uint16))

    @classmethod
    def putuint16(cls, buffer: bytearray, value: np.uint16):
        cls._write_int(buffer, ByteSize.UINT16, value, False)

    @classmethod
    def int32(cls, buffer: bytes) -> np.int32:
        return cls._read(buffer, ByteSize.INT32, np.dtype(np.int32))

    @classmethod
    def putint32(cls, buffer: bytearray, value: np.int32):
        cls._write_int(buffer, ByteSize.INT32, value, True)

    @classmethod
    def uint32(cls, buffer: bytes) -> np.uint32:
        return cls._read(buffer, ByteSize.UINT32, np.dtype(np.uint32))

    @classmethod
    def putuint32(cls, buffer: bytearray, value: np.uint32):
        cls._write_int(buffer, ByteSize.UINT32, value, False)

    @classmethod
    def int64(cls, buffer: bytes) -> np.int64:
        return cls._read(buffer, ByteSize.INT64, np.dtype(np.int64))

    @classmethod
    def putint64(cls, buffer: bytearray, value: np.int64):
        cls._write_int(buffer, ByteSize.INT64, value, True)

    @classmethod
    def uint64(cls, buffer: bytes) -> np.uint64:
        return cls._read(buffer, ByteSize.UINT64, np.dtype(np.uint64))

    @classmethod
    def putuint64(cls, buffer: bytearray, value: np.uint64):
        cls._write_int(buffer, ByteSize.UINT64, value, False)

    @classmethod
    def float16(cls, buffer: bytes) -> np.float16:
        return cls._read(buffer, ByteSize.FLOAT16, np.dtype(np.float16))

    @classmethod
    def putfloat16(cls, buffer: bytearray, value: np.float16):
        cls._write_float(buffer, ByteSize.FLOAT16, value)

    @classmethod
    def float32(cls, buffer: bytes) -> np.float32:
        return cls._read(buffer, ByteSize.FLOAT32, np.dtype(np.float32))

    @classmethod
    def putfloat32(cls, buffer: bytearray, value: np.float32):
        cls._write_float(buffer, ByteSize.FLOAT32, value)

    @classmethod
    def float64(cls, buffer: bytes) -> np.float64:
        return cls._read(buffer, ByteSize.FLOAT64, np.dtype(np.float64))

    @classmethod
    def putfloat64(cls, buffer: bytearray, value: np.float64):
        cls._write_float(buffer, ByteSize.FLOAT64, value)


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
