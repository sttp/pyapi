# ******************************************************************************************************
#  encoding7bit.py - Gbtc
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

from . import ByteSize
from typing import Callable
import numpy as np


class Encoding7Bit:
    """
    Defines 7-bit encoding/decoding functions.
    """

    @staticmethod
    def write_int16(stream_writer: Callable[[np.uint8], int], value: np.int16) -> int:
        """
        Writes 16-bit signed integer value using 7-bit encoding to the provided stream writer.

        Parameters
        ----------
        stream_writer: function to write a byte value to a stream
        value: 16-bit value to write
        """

        return Encoding7Bit.write_uint16(stream_writer, np.frombuffer(int(value).to_bytes(ByteSize.INT16, "little", signed=True), np.uint16)[0])

    @staticmethod
    def write_uint16(stream_writer: Callable[[np.uint8], int], value: np.uint16) -> int:
        """
        Writes 16-bit unsigned integer value using 7-bit encoding to the provided stream writer.

        Parameters
        ----------
        stream_writer: function to write a byte value to a stream
        value: 16-bit value to write
        """

        np128 = np.uint16(128)
        np7 = np.uint16(7)
        def np_stream_writer(uint16): return stream_writer(np.uint8(uint16))

        if value < np128:
            np_stream_writer(value)  # 1
            return 1

        np_stream_writer(value | np128)  # 1
        np_stream_writer(value >> np7)  # 2
        return 2

    @staticmethod
    def write_int32(stream_writer: Callable[[np.uint8], int], value: np.int32) -> int:
        """
        Writes 32-bit signed integer value using 7-bit encoding to the provided stream writer.

        Parameters
        ----------
        stream_writer: function to write a byte value to a stream
        value: 32-bit value to write
        """

        return Encoding7Bit.write_uint32(stream_writer, np.frombuffer(int(value).to_bytes(ByteSize.INT32, "little", signed=True), np.uint32)[0])

    @staticmethod
    def write_uint32(stream_writer: Callable[[np.uint8], int], value: np.uint32) -> int:
        """
        Writes 32-bit unsigned integer value using 7-bit encoding to the provided stream writer.

        Parameters
        ----------
        stream_writer: function to write a byte value to a stream
        value: 32-bit value to write
        """

        np128 = np.uint32(128)
        np7 = np.uint32(7)
        def np_stream_writer(uint32): return stream_writer(np.uint8(uint32))

        if value < np128:
            np_stream_writer(value)  # 1
            return 1

        np_stream_writer(value | np128)  # 1
        if value < np128 * np128:
            np_stream_writer(value >> np7)  # 2
            return 2

        np_stream_writer((value >> np7) | np128)  # 2
        if value < np128 * np128 * np128:
            np_stream_writer(value >> (np7 + np7))  # 3
            return 3

        np_stream_writer((value >> (np7 + np7)) | np128)  # 3
        if value < np128 * np128 * np128 * np128:
            np_stream_writer(value >> (np7 + np7 + np7))  # 4
            return 4

        np_stream_writer((value >> (np7 + np7 + np7)) | np128)  # 4
        np_stream_writer(value >> (np7 + np7 + np7 + np7))  # 5
        return 5

    @staticmethod
    def write_int64(stream_writer: Callable[[np.uint8], int], value: np.int64) -> int:
        """
        Writes 64-bit signed integer value using 7-bit encoding to the provided stream writer.

        Parameters
        ----------
        stream_writer: function to write a byte value to a stream
        value: 64-bit value to write
        """

        return Encoding7Bit.write_uint64(stream_writer, np.frombuffer(int(value).to_bytes(ByteSize.INT64, "little", signed=True), np.uint64)[0])

    @staticmethod
    def write_uint64(stream_writer: Callable[[np.uint8], int], value: np.uint64) -> int:
        """
        Writes 64-bit unsigned integer value using 7-bit encoding to the provided stream writer.

        Parameters
        ----------
        stream_writer: function to write a byte value to a stream
        value: 64-bit value to write
        """

        np128 = np.uint64(128)
        np7 = np.uint64(7)
        def np_stream_writer(uint64): return stream_writer(np.uint8(uint64))

        if value < np128:
            np_stream_writer(value)  # 1
            return 1

        np_stream_writer(value | np128)  # 1
        if value < np128 * np128:
            np_stream_writer(value >> np7)  # 2
            return 2

        np_stream_writer((value >> np7) | np128)  # 2
        if value < np128 * np128 * np128:
            np_stream_writer(value >> (np7 + np7))  # 3
            return 3

        np_stream_writer((value >> (np7 + np7)) | np128)  # 3
        if value < np128 * np128 * np128 * np128:
            np_stream_writer(value >> (np7 + np7 + np7))  # 4
            return 4

        np_stream_writer((value >> (np7 + np7 + np7)) | np128)  # 4
        if value < np128 * np128 * np128 * np128 * np128:
            np_stream_writer(value >> (np7 + np7 + np7 + np7))  # 5
            return 5

        np_stream_writer((value >> (np7 + np7 + np7 + np7)) | np128)  # 5
        if value < np128 * np128 * np128 * np128 * np128 * np128:
            np_stream_writer(value >> (np7 + np7 + np7 + np7 + np7))  # 6
            return 6

        np_stream_writer((value >> (np7 + np7 + np7 + np7 + np7)) | np128)  # 6
        if value < np128 * np128 * np128 * np128 * np128 * np128 * np128:
            np_stream_writer(value >> (np7 + np7 + np7 + np7 + np7 + np7))  # 7
            return 7

        np_stream_writer(
            (value >> (np7 + np7 + np7 + np7 + np7 + np7)) | np128)  # 7
        if value < np128 * np128 * np128 * np128 * np128 * np128 * np128 * np128:
            np_stream_writer(value >> (np7 + np7 + np7 + np7 + np7 + np7 + np7))  # 8
            return 8

        np_stream_writer(value >> (np7 + np7 + np7 + np7 + np7 + np7 + np7) | np128)  # 8
        np_stream_writer(value >> (np7 + np7 + np7 + np7 + np7 + np7 + np7 + np7))  # 9
        return 9

    @staticmethod
    def read_int16(stream_reader: Callable[[], np.uint8]) -> np.int16:
        """
        Reads 16-bit signed integer value using 7-bit encoding from the provided stream reader.

        Parameters
        ----------
        stream_reader: function to read a byte value from a stream

        Notes
        -----
        Call expects one to two bytes to be available in base stream.
        """

        return np.frombuffer(int(Encoding7Bit.read_uint16(stream_reader)).to_bytes(ByteSize.UINT16, "little"), np.int16)[0]

    @staticmethod
    def read_uint16(stream_reader: Callable[[], np.uint8]) -> np.uint16:
        """
        Reads 16-bit unsigned integer value using 7-bit encoding from the provided stream reader.

        Parameters
        ----------
        stream_reader: function to read a byte value from a stream

        Notes
        -----
        Call expects one to two bytes to be available in base stream.
        """

        np128 = np.uint16(128)
        np7 = np.uint16(7)
        def np_stream_reader(): return np.uint16(stream_reader())

        value = np_stream_reader()

        if value < np128:
            return value

        value ^= (np_stream_reader() << (np7))
        return value ^ np.uint16(0x80)

    @staticmethod
    def read_int32(stream_reader: Callable[[], np.uint8]) -> np.int32:
        """
        Reads 32-bit signed integer value using 7-bit encoding from the provided stream reader.

        Parameters
        ----------
        stream_reader: function to read a byte value from a stream

        Notes
        -----
        Call expects one to five bytes to be available in base stream.
        """

        return np.frombuffer(int(Encoding7Bit.read_uint32(stream_reader)).to_bytes(ByteSize.UINT32, "little"), np.int32)[0]

    @staticmethod
    def read_uint32(stream_reader: Callable[[], np.uint8]) -> np.uint32:
        """
        Reads 32-bit unsigned integer value using 7-bit encoding from the provided stream reader.

        Parameters
        ----------
        stream_reader: function to read a byte value from a stream

        Notes
        -----
        Call expects one to five bytes to be available in base stream.
        """

        np128 = np.uint32(128)
        np7 = np.uint32(7)
        def np_stream_reader(): return np.uint32(stream_reader())

        value = np_stream_reader()

        if value < np128:
            return value

        value ^= (np_stream_reader() << (np7))
        if value < np128 * np128:
            return value ^ np.uint32(0x80)

        value ^= (np_stream_reader() << (np7 + np7))
        if value < np128 * np128 * np128:
            return value ^ np.uint32(0x4080)

        value ^= (np_stream_reader() << (np7 + np7 + np7))
        if value < np128 * np128 * np128 * np128:
            return value ^ np.uint32(0x204080)

        value ^= (np_stream_reader() << (np7 + np7 + np7 + np7))
        return value ^ np.uint32(0x10204080)

    @staticmethod
    def read_int64(stream_reader: Callable[[], np.uint8]) -> np.int64:
        """
        Reads 64-bit signed integer value using 7-bit encoding from the provided stream reader.

        Parameters
        ----------
        stream_writer: function to write a byte value to a stream
        value: 64-bit value to write

        Notes
        -----
        Call expects one to nine bytes to be available in base stream.
        """

        return np.frombuffer(int(Encoding7Bit.read_uint64(stream_reader)).to_bytes(ByteSize.UINT64, "little"), np.int64)[0]

    @staticmethod
    def read_uint64(stream_reader: Callable[[], np.uint8]) -> np.uint64:
        """
        Reads 64-bit unsigned integer value using 7-bit encoding from the provided stream reader.

        Parameters
        ----------
        stream_writer: function to write a byte value to a stream
        value: 64-bit value to write

        Notes
        -----
        Call expects one to nine bytes to be available in base stream.
        """

        np128 = np.uint64(128)
        np7 = np.uint64(7)
        def np_stream_reader(): return np.uint64(stream_reader())

        value = np_stream_reader()

        if value < np128:
            return value

        value ^= (np_stream_reader() << (np7))
        if value < np128 * np128:
            return value ^ np.uint64(0x80)

        value ^= (np_stream_reader() << (np7 + np7))
        if value < np128 * np128 * np128:
            return value ^ np.uint64(0x4080)

        value ^= (np_stream_reader() << (np7 + np7 + np7))
        if value < np128 * np128 * np128 * np128:
            return value ^ np.uint64(0x204080)

        value ^= (np_stream_reader() << (np7 + np7 + np7 + np7))
        if value < np128 * np128 * np128 * np128 * np128:
            return value ^ np.uint64(0x10204080)

        value ^= (np_stream_reader() << (np7 + np7 + np7 + np7 + np7))
        if value < np128 * np128 * np128 * np128 * np128 * np128:
            return value ^ np.uint64(0x810204080)

        value ^= (np_stream_reader() << (np7 + np7 + np7 + np7 + np7 + np7))
        if value < np128 * np128 * np128 * np128 * np128 * np128 * np128:
            return value ^ np.uint64(0x40810204080)

        value ^= (np_stream_reader() << (np7 + np7 + np7 + np7 + np7 + np7 + np7))
        if value < np128 * np128 * np128 * np128 * np128 * np128 * np128 * np128:
            return value ^ np.uint64(0x2040810204080)

        value ^= (np_stream_reader() << (np7 + np7 + np7 + np7 + np7 + np7 + np7 + np7))
        return value ^ np.uint64(0x102040810204080)
