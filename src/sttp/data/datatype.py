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

from enum import IntEnum
from typing import Tuple


class DataType(IntEnum):
    """
    Enumeration of the possible data types for a `DataColumn`.
    """

    String = 0,
    """
    Represents a Python `str` data type.
    """

    Boolean = 1,
    """
    Represents a Python `bool` data type.
    """

    DateTime = 2,
    """
    Represents a Python `datetime` data type.
    """

    Single = 3,
    """
    Represents a Python `numpy.float32` data type.
    """

    Double = 4,
    """
    Represents a Python `numpy.float64` data type.
    """

    Decimal = 5,
    """
    Represents a Python `decimal.Decimal` data type.
    """

    Guid = 6,
    """
    Represents a Python `uuid.UUID` data type.
    """

    Int8 = 7,
    """
    Represents a Python `numpy.int8` data type.
    """

    Int16 = 8,
    """
    Represents a Python `numpy.int16` data type.
    """

    Int32 = 9,
    """
    Represents a Python `numpy.int32` data type.
    """

    Int64 = 10,
    """
    Represents a Python `numpy.int64` data type.
    """

    UInt8 = 11,
    """
    Represents a Python `numpy.uint8` data type.
    """

    UInt16 = 12,
    """
    Represents a Python `numpy.uint16` data type.
    """

    UInt32 = 13,
    """
    Represents a Python `numpy.uint32` data type.
    """

    UInt64 = 14,
    """
    Represents a Python `numpy.uint64` data type.
    """


def parse_xsddatatype(xsdtypename: str, extdatatype: str) -> Tuple[DataType, bool]:
    """
    Gets the `DataType` from the provided XSD data type. Return tuple includes boolean
    value that determines if parse was successful. See XML Schema Language Datatypes
    for possible xsd type name values: https://www.w3.org/TR/xmlschema-2/
    """

    if xsdtypename == "string":
        if extdatatype.startswith("System.Guid"):
            return DataType.Guid, True

        return DataType.String, True

    if xsdtypename == "boolean":
        return DataType.Boolean, True

    if xsdtypename == "dateTime":
        return DataType.DateTime, True

    if xsdtypename == "float":
        return DataType.Single, True

    if xsdtypename == "double":
        return DataType.Double, True

    if xsdtypename == "decimal":
        return DataType.Decimal, True

    if xsdtypename == "byte":  # XSD defines byte as signed 8-bit int
        return DataType.Int8, True

    if xsdtypename == "short":
        return DataType.Int16, True

    if xsdtypename == "int":
        return DataType.Int32, True

    if xsdtypename == "long":
        return DataType.Int64, True

    if xsdtypename == "unsignedByte":
        return DataType.UInt8, True

    if xsdtypename == "unsignedShort":
        return DataType.UInt16, True

    if xsdtypename == "unsignedInt":
        return DataType.UInt32, True

    if xsdtypename == "unsignedLong":
        return DataType.UInt64, True

    return DataType.String, False
