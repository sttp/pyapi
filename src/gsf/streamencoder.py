#******************************************************************************************************
#  streamencoder.py - Gbtc
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
#  01/31/2021 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from .encoding7bit import Encoding7Bit
from . import ByteSize
from typing import Callable, Optional
import sys
import numpy as np


class StreamEncoder:
    """
    Defines functions for encoding and decoding native types to and from a stream.
    For this class, a stream is simply an abstract notion based on provided functions
    that read and write byte buffers as Python `bytes` objects advancing a position
    in the base stream. The read/write functions simply wrap a base object that can
    handle input and output as bytes, e.g., a `bytearray` or a `socket`.
    """

    # Source C# reference: GSF.IO.StreamExtensions

    def __init__(self, read: Callable[[int], bytes], write: Callable[[bytes], int], default_byteorder: str = sys.byteorder):
        """
        Parameters
        ----------
        read : func(length: int) -> bytes
            Read function that accepts desired number of bytes to read and returns `bytes` object of read bytes.
            Actual length of returned `bytes` object may be less than desired number of bytes.
        write: func(buffer: bytes) -> int
            Write function that accepts a `bytes` object and returns count of bytes written. It is expected that
            call to write will successfully write all bytes, i.e., returned length should match `buffer` length.
        """
        self._read = read
        self._write = write
        self._default_byteorder = default_byteorder
        self._default_is_native = self._default_byteorder == sys.byteorder

    @property
    def default_byteorder(self) -> str:
        return self._default_byteorder

    def write(self, source_buffer: bytes, offset: int, count: int) -> int:
        if self._write(source_buffer[offset:offset + count]) != count:
            raise RuntimeError(f"Failed to write {count:,} bytes to stream")

        return count

    def read(self, target_buffer: bytearray, offset: int, count: int) -> int:
        # `count` is requested size, value is treated as max return size
        buffer = self._read(count)
        read_length = len(buffer)

        for i in range(read_length):
            target_buffer[offset + i] = buffer[i]

        return read_length

    def write_byte(self, value: np.uint8) -> int:
        size = ByteSize.UINT8

        if self._write(int(value).to_bytes(size, self._default_byteorder)) != size:
            raise RuntimeError("Failed to write 1-byte to stream")

        return size

    def read_byte(self) -> np.uint8:
        size = ByteSize.UINT8

        # call expects one byte to be available in base stream
        buffer = self._read(size)

        if len(buffer) != size:
            raise RuntimeError("Failed to read 1-byte from stream")

        return np.uint8(buffer[0])

    def write_bool(self, value: bool) -> int:
        if value:
            self.write_byte(1)
        else:
            self.write_byte(0)

        return 1

    def read_bool(self) -> bool:
        # call expects one byte to be available in base stream
        return self.read_byte() != 0

    def _write_int(self, size: int, value: int, signed: bool, byteorder: Optional[str]) -> int:
        # sourcery skip: remove-unnecessary-cast
        if self._write(int(value).to_bytes(size, self._default_byteorder if byteorder is None else byteorder, signed=signed)) != size:
            raise RuntimeError(f"Failed to write {size}-bytes to stream")

        return size

    def _read_int(self, size: int, dtype: np.dtype, byteorder: Optional[str]) -> int:
        # call expects needed bytes to be available in base stream
        buffer = self._read(size)

        if len(buffer) != size:
            raise RuntimeError(f"Failed to read {size}-bytes from stream")

        if not (byteorder is None and self._default_is_native) and byteorder != sys.byteorder:
            dtype = dtype.newbyteorder()

        return np.frombuffer(buffer, dtype)[0]

    def write_int16(self, value: np.int16, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.INT16, value, True, byteorder)

    def read_int16(self, byteorder: Optional[str] = None) -> np.int16:
        return self._read_int(ByteSize.INT16, np.dtype(np.int16), byteorder)

    def write_uint16(self, value: np.uint16, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.UINT16, value, False, byteorder)

    def read_uint16(self, byteorder: Optional[str] = None) -> np.uint16:
        return self._read_int(ByteSize.UINT16, np.dtype(np.uint16), byteorder)

    def write_int32(self, value: np.int32, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.INT32, value, True, byteorder)

    def read_int32(self, byteorder: Optional[str] = None) -> np.int32:
        return self._read_int(ByteSize.INT32, np.dtype(np.int32), byteorder)

    def write_uint32(self, value: np.uint32, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.UINT32, value, False, byteorder)

    def read_uint32(self, byteorder: Optional[str] = None) -> np.uint32:
        return self._read_int(ByteSize.UINT32, np.dtype(np.uint32), byteorder)

    def write_int64(self, value: np.int64, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.INT64, value, True, byteorder)

    def read_int64(self, byteorder: Optional[str] = None) -> np.int64:
        return self._read_int(ByteSize.INT64, np.dtype(np.int64), byteorder)

    def write_uint64(self, value: np.uint64, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.UINT64, value, False, byteorder)

    def read_uint64(self, byteorder: Optional[str] = None) -> np.uint64:
        return self._read_int(ByteSize.UINT64, np.dtype(np.uint64), byteorder)

    def write7bit_uint32(self, value: np.uint32) -> int:
        return Encoding7Bit.WriteUInt32(self.write_byte, value)

    def read7bit_uint32(self) -> np.uint32:
        # call expects one to five bytes to be available in base stream
        return Encoding7Bit.ReadUInt32(self.read_byte)

    def write7bit_uint64(self, value: np.uint64) -> int:
        return Encoding7Bit.WriteUInt64(self.write_byte, value)

    def read7bit_uint64(self) -> np.uint64:
        # call expects one to nine bytes to be available in base stream
        return Encoding7Bit.ReadUInt64(self.read_byte)
