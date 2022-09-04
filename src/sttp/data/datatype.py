# ******************************************************************************************************
#  datatype.py - Gbtc
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
#  08/26/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from gsf import Empty
from enum import IntEnum
from typing import Optional, Tuple


class DataType(IntEnum):
    """
    Enumeration of the possible data types for a `DataColumn`.
    """

    STRING = 0,
    """
    Represents a Python `str` data type.
    """

    BOOLEAN = 1,
    """
    Represents a Python `bool` data type.
    """

    DATETIME = 2,
    """
    Represents a Python `datetime` data type.
    """

    SINGLE = 3,
    """
    Represents a Python `numpy.float32` data type.
    """

    DOUBLE = 4,
    """
    Represents a Python `numpy.float64` data type.
    """

    DECIMAL = 5,
    """
    Represents a Python `decimal.Decimal` data type.
    """

    GUID = 6,
    """
    Represents a Python `uuid.UUID` data type.
    """

    INT8 = 7,
    """
    Represents a Python `numpy.int8` data type.
    """

    INT16 = 8,
    """
    Represents a Python `numpy.int16` data type.
    """

    INT32 = 9,
    """
    Represents a Python `numpy.int32` data type.
    """

    INT64 = 10,
    """
    Represents a Python `numpy.int64` data type.
    """

    UINT8 = 11,
    """
    Represents a Python `numpy.uint8` data type.
    """

    UINT16 = 12,
    """
    Represents a Python `numpy.uint16` data type.
    """

    UINT32 = 13,
    """
    Represents a Python `numpy.uint32` data type.
    """

    UINT64 = 14,
    """
    Represents a Python `numpy.uint64` data type.
    """


def default_datatype(datatype: DataType) -> object:  # sourcery skip: assign-if-exp, reintroduce-else
    if datatype == DataType.STRING:
        return Empty.STRING
    if datatype == DataType.BOOLEAN:
        return False
    if datatype == DataType.DATETIME:
        return Empty.DATETIME
    if datatype == DataType.SINGLE:
        return Empty.SINGLE
    if datatype == DataType.DOUBLE:
        return Empty.DOUBLE
    if datatype == DataType.DECIMAL:
        return Empty.DECIMAL
    if datatype == DataType.GUID:
        return Empty.GUID
    if datatype == DataType.INT8:
        return Empty.INT8
    if datatype == DataType.INT16:
        return Empty.INT16
    if datatype == DataType.INT32:
        return Empty.INT32
    if datatype == DataType.INT64:
        return Empty.INT64
    if datatype == DataType.UINT8:
        return Empty.UINT8
    if datatype == DataType.UINT16:
        return Empty.UINT16
    if datatype == DataType.UINT32:
        return Empty.UINT32
    if datatype == DataType.UINT64:
        return Empty.UINT64

    return None

def parse_xsddatatype(xsdtypename: str, extdatatype: Optional[str]) -> Tuple[DataType, bool]:
    """
    Gets the `DataType` from the provided XSD data type. Return tuple includes boolean
    value that determines if parse was successful. See XML Schema Language Datatypes
    for possible xsd type name values: https://www.w3.org/TR/xmlschema-2/
    """

    if xsdtypename == "string":
        if extdatatype is not None and extdatatype.startswith("System.Guid"):
            return DataType.GUID, True

        return DataType.STRING, True

    if xsdtypename == "boolean":
        return DataType.BOOLEAN, True

    if xsdtypename == "dateTime":
        return DataType.DATETIME, True

    if xsdtypename == "float":
        return DataType.SINGLE, True

    if xsdtypename == "double":
        return DataType.DOUBLE, True

    if xsdtypename == "decimal":
        return DataType.DECIMAL, True

    if xsdtypename == "byte":  # XSD defines byte as signed 8-bit int
        return DataType.INT8, True

    if xsdtypename == "short":
        return DataType.INT16, True

    if xsdtypename == "int":
        return DataType.INT32, True

    if xsdtypename == "long":
        return DataType.INT64, True

    if xsdtypename == "unsignedByte":
        return DataType.UINT8, True

    if xsdtypename == "unsignedShort":
        return DataType.UINT16, True

    if xsdtypename == "unsignedInt":
        return DataType.UINT32, True

    if xsdtypename == "unsignedLong":
        return DataType.UINT64, True

    return DataType.STRING, False
