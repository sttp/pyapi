#******************************************************************************************************
#  binarystream.py - Gbtc
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

from .streamencoder import StreamEncoder
from .encoding7bit import Encoding7Bit
from . import ByteSize, Validate
from typing import Optional
from uuid import UUID
import sys
import numpy as np


class BinaryStream:
    """
    Establishes buffered I/O around a base stream, e.g., a socket.
    """

    # Source C# references:
    #     RemoteBinaryStream
    #     BinaryStreamBase

    IO_BUFFERSIZE = 1420
    VALUE_BUFFERSIZE = 16

    def __init__(self, stream: StreamEncoder):
        self._stream = stream
        self._default_byteorder = stream.default_byteorder
        self._default_is_native = self._default_byteorder == sys.byteorder
        self._buffer = bytearray(BinaryStream.VALUE_BUFFERSIZE)
        self._receive_buffer = bytearray(BinaryStream.IO_BUFFERSIZE)
        self._send_buffer = bytearray(BinaryStream.IO_BUFFERSIZE)
        self._send_length = 0
        self._receive_length = 0
        self._receive_position = 0

    def _send_buffer_freespace(self) -> int:
        return BinaryStream.IO_BUFFERSIZE - self._send_length

    def _receive_buffer_available(self) -> int:
        return self._receive_length - self._receive_position

    def flush(self):
        if self._send_length <= 0:
            return

        self._stream.write(bytes(self._send_buffer), 0, self._send_length)
        self._send_length = 0

    def read(self, buffer: bytearray, offset: int, count: int) -> int:  # sourcery skip: low-code-quality
        if count <= 0:
            return 0

        receive_buffer_length = self._receive_buffer_available()

        # Check if there is enough in the receive buffer to handle request
        if count <= receive_buffer_length:
            for i in range(count):
                buffer[offset + i] = self._receive_buffer[self._receive_position + i]

            self._receive_position += count
            return count

        original_count = count

        # Empty existing receive buffer
        if receive_buffer_length > 0:
            for i in range(receive_buffer_length):
                buffer[offset + i] = self._receive_buffer[self._receive_position + i]

            self._receive_position = 0
            self._receive_length = 0
            offset += receive_buffer_length
            count -= receive_buffer_length

        # If more than 100 bytes remain, skip receive buffer
        # and copy directly to the destination
        if count > 100:
            # Loop since socket reads can return partial results
            while count > 0:
                receive_buffer_length = self._stream.read(buffer, offset, count)

                if receive_buffer_length == 0:
                    raise RuntimeError("End of stream")

                offset += receive_buffer_length
                count -= receive_buffer_length
        else:
            # With fewer than 100 bytes requested, fill receive buffer
            # then copy to destination
            pre_buffer_length = BinaryStream.IO_BUFFERSIZE
            self._receive_length = 0

            while self._receive_length < count:
                receive_buffer_length = self._stream.read(self._receive_buffer, self._receive_length, pre_buffer_length)

                if receive_buffer_length == 0:
                    raise RuntimeError("End of stream")

                self._receive_length += receive_buffer_length
                pre_buffer_length -= receive_buffer_length

            for i in range(count):
                buffer[offset + i] = self._receive_buffer[i]

            self._receive_position = count

        return original_count

    def write(self, buffer: bytes, offset: int, count: int) -> int:
        if self._send_buffer_freespace() < count:
            self.flush()

        if count > 100:
            self.flush()
            self._stream.write(buffer, offset, count)
        else:
            for i in range(count):
                self._send_buffer[self._send_length + i] = buffer[offset + i]

            self._send_length += count

        return count

    def read_all(self, buffer: bytearray, position: int, length: int):
        """
        Reads all of the provided bytes. Will not return prematurely, continues
        to execute `Read` operation until the entire `length` has been read.
        """
        Validate.parameters(buffer, position, length)

        while length > 0:
            bytes_read = self.read(buffer, position, length)

            if bytes_read == 0:
                raise RuntimeError("End of stream")

            length -= bytes_read
            position += bytes_read

    def read_bytes(self, count: int) -> bytes:
        buffer = self._buffer if count <= BinaryStream.VALUE_BUFFERSIZE else bytearray(count)
        self.read_all(buffer, 0, count)
        return bytes(buffer[:count])

    def read_buffer(self) -> bytes:
        return self.read_bytes(self.read7bit_uint32())

    def read_string(self) -> str:
        return self.read_buffer().decode("utf-8")

    def read_guid(self) -> UUID:
        return UUID(bytes_le=self.read_bytes(16))

    def read7bit_int32(self) -> np.int32:
        return Encoding7Bit.read_int32(self.read_byte)

    def read7bit_uint32(self) -> np.uint32:
        return Encoding7Bit.read_uint32(self.read_byte)

    def read7bit_int64(self) -> np.int64:
        return Encoding7Bit.read_int64(self.read_byte)

    def read7bit_uint64(self) -> np.uint64:
        if self._receive_position <= self._receive_length - 9:
            stream = StreamEncoder(self._send_buffer_read, self._send_buffer_write)
            return stream.read7bit_uint64()

        return Encoding7Bit.read_uint64(self.read_byte)

    def read_byte(self) -> np.uint8:
        size = ByteSize.UINT8

        if self._receive_position < self._receive_length:
            value = self._receive_buffer[self._receive_position]
            self._receive_position += size
            return np.uint8(value)

        self.read_all(self._buffer, 0, size)
        return self._buffer[0]

    def write_buffer(self, value: bytes) -> int:
        count = len(value)
        return self.write7bit_uint32(count) + self.write(value, 0, count)

    def write_string(self, value: str) -> int:
        return self.write_buffer(value.encode("utf-8"))

    def write_guid(self, value: UUID) -> int:
        return self.write(value.bytes_le, 0, 16)

    def write7bit_int32(self, value: np.int32) -> int:
        return Encoding7Bit.write_int32(self.write_byte, value)

    def write7bit_uint32(self, value: np.uint32) -> int:
        return Encoding7Bit.write_uint32(self.write_byte, value)

    def write7bit_int64(self, value: np.int64) -> int:
        return Encoding7Bit.write_int64(self.write_byte, value)

    def write7bit_uint64(self, value: np.uint64) -> int:
        if self._send_length <= BinaryStream.IO_BUFFERSIZE - 9:
            stream = StreamEncoder(self._send_buffer_read, self._send_buffer_write)
            return stream.write7bit_uint64(value)

        return Encoding7Bit.write_uint64(self.write_byte, value)

    def write_byte(self, value: np.uint8) -> int:
        size = ByteSize.UINT8

        if self._send_length < BinaryStream.IO_BUFFERSIZE:
            self._send_buffer[self._send_length] = value
            self._send_length += size
            return size

        self._buffer[0] = value
        return self.write(self._buffer, 0, size)

    def read_boolean(self) -> bool:
        return self.read_byte() != 0

    def write_boolean(self, value: bool) -> int:
        if value:
            self.write_byte(1)
        else:
            self.write_byte(0)

        return 1

    def _read_int(self, size: int, dtype: np.dtype, byteorder: Optional[str]) -> int:
        if not (byteorder is None and self._default_is_native) and byteorder != sys.byteorder:
            dtype = dtype.newbyteorder()

        if self._receive_position <= self._receive_length - size:
            value = np.frombuffer(self._receive_buffer[self._receive_position:self._receive_position + size], dtype)[0]
            self._receive_position += size
            return value

        self.ReadAll(self._buffer, 0, size)
        return np.frombuffer(self._buffer[:size], dtype)[0]

    def _write_int(self, size: int, value: int, signed: bool, byteorder: Optional[str]) -> int:
        # sourcery skip: class-extract-method, remove-unnecessary-cast
        buffer = int(value).to_bytes(size, self._default_byteorder if byteorder is None else byteorder, signed=signed)

        if self._send_length <= BinaryStream.IO_BUFFERSIZE - size:
            for i in range(size):
                self._send_buffer[self._send_length + i] = buffer[i]

            self._send_length += size
            return size

        for i in range(size):
            self._buffer[i] = buffer[i]

        return self.write(self._buffer, 0, size)

    def read_int16(self, byteorder: Optional[str] = None) -> np.int16:
        return self._read_int(ByteSize.INT16, np.dtype(np.int16), byteorder)

    def write_int16(self, value: np.int16, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.INT16, value, True, byteorder)

    def read_uint16(self, byteorder: Optional[str] = None) -> np.uint16:
        return self._read_int(ByteSize.UINT16, np.dtype(np.uint16), byteorder)

    def write_uint16(self, value: np.uint16, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.UINT16, value, False, byteorder)

    def read_int32(self, byteorder: Optional[str] = None) -> np.int32:
        return self._read_int(ByteSize.INT32, np.dtype(np.int32), byteorder)

    def write_int32(self, value: np.int32, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.INT32, value, True, byteorder)

    def read_uint32(self, byteorder: Optional[str] = None) -> np.uint32:
        return self._read_int(ByteSize.UINT32, np.dtype(np.uint32), byteorder)

    def write_uint32(self, value: np.uint32, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.UINT32, value, False, byteorder)

    def read_int64(self, byteorder: Optional[str] = None) -> np.int64:
        return self._read_int(ByteSize.INT64, np.dtype(np.int64), byteorder)

    def write_int64(self, value: np.int64, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.INT64, value, True, byteorder)

    def read_uint64(self, byteorder: Optional[str] = None) -> np.uint64:
        return self._read_int(ByteSize.UINT64, np.dtype(np.uint64), byteorder)

    def write_uint64(self, value: np.uint64, byteorder: Optional[str] = None) -> int:
        return self._write_int(ByteSize.UINT64, value, False, byteorder)

    def _send_buffer_read(self, length: int) -> bytes:
        buffer = self._buffer if length <= BinaryStream.VALUE_BUFFERSIZE else bytearray(length)

        for i in range(length):
            buffer[i] = self._receive_buffer[self._receive_position + i]

        self._receive_position += length
        return bytes(buffer[:length])

    def _send_buffer_write(self, buffer: bytes) -> int:
        length = len(buffer)

        for i in range(length):
            self._send_buffer[self._send_length + i] = buffer[i]

        self._send_length += length
        return length
