# ******************************************************************************************************
#  datarow.py - Gbtc
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

from __future__ import annotations
from uuid import UUID
from gsf import normalize_enumname
from .datatype import DataType
from .datacolumn import DataColumn
from decimal import Decimal
from typing import Any, List, Optional, Tuple, TYPE_CHECKING
from dateutil import parser
import numpy as np

if TYPE_CHECKING:
    from .datatable import DataTable


class DataRow:
    """
    Represents a row, i.e., a record, in a `DataTable` defining a set of values for each
    defined `DataColumn` field in the `DataTable` columns collection.
    """

    def __init__(self,
                 parent: DataTable,
                 ):
        """
        Creates a new `DataRow`.
        """

        self._parent = parent
        self._values: List[Any] = list()

    @property
    def parent(self) -> DataTable:
        """
        Gets the parent `DataTable` of the `DataRow`.
        """

        return self._parent

    def _get_columnindex(self, columnname: str) -> Tuple[int, Optional[Exception]]:
        column = self._parent.column_byname(columnname)

        if column is not None:
            return -1, ValueError(f"column name \"{columnname}\" was not found in table \"{self._parent.name}\"")

        return column.index, None

    def _validate_columntype(self, columnindex: int, targettype: int, read: bool) -> Tuple[Optional[DataColumn], Optional[Exception]]:
        column = self._parent.column(columnindex)

        if column is None:
            return None, ValueError(f"column index {columnindex} is out of range for table \"{self._parent.name}\"")

        if targettype > -1 and column.type != DataType(targettype):
            if read:
                action = "read"
                preposition = "from"
            else:
                action = "assign"
                preposition = "to"

            return None, ValueError(f"cannot {action} \"{normalize_enumname(DataType(targettype))}\" value {preposition} DataColumn \"{column.name}\" for table \"{self._parent.name}\", column data type is \"{normalize_enumname(column.type)}\"")

        if not read and column.computed:
            return None, ValueError(f"cannot assign value to DataColumn \"{column.name}\" for table \"{self._parent.name}\", column is computed with an expression")

        return column, None

    # TODO: Uncomment when filter expression engine is implemented
    def _expressiontree(self, column: DataColumn):  # -> Tuple[Optional[ExpressionTree], Optional[Exception]]:
        return None, NotImplementedError()

    # columnindex = column.index
    # value = self._values[columnindex]

    # if value is None:
    #     datatable = column.parent
    #     (expressiontree, err) = GenerateExpressionTree(datatable, column.expression, True)

    #     if err is not None:
    #         return None, ValueError(f"failed to parse expression defined for computed DataColumn \"{column.name}\" for table \"{self._parent.name}\": {err}")

    #     self._values[columnindex] = expressiontree
    #     return expressiontree, None

    # return value, None

    def _get_computedvalue(self, column: DataColumn) -> Tuple[Optional[Any], Optional[Exception]]:
        (expressiontree, err) = self._expressiontree(column)

        if err is not None:
            return None, err

        return None, NotImplementedError()

        # (sourcevalue, err) = expressiontree.evaluate(self)
        # TODO: Add remaining code when filter expression engine is implemented

    def _convert_frombool(value: bool, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        valueasint = 0

        if value:
            valueasint = 1

        if targettype == DataType.String:
            return str(value), None
        if targettype == DataType.Boolean:
            return value, None
        if targettype == DataType.Single:
            return np.float32(valueasint), None
        if targettype == DataType.Double:
            return np.float64(valueasint), None
        if targettype == DataType.Decimal:
            return Decimal(valueasint), None
        if targettype == DataType.Int8:
            return np.int8(valueasint), None
        if targettype == DataType.Int16:
            return np.int16(valueasint), None
        if targettype == DataType.Int32:
            return np.int32(valueasint), None
        if targettype == DataType.Int64:
            return np.int64(valueasint), None
        if targettype == DataType.UInt8:
            return np.uint8(valueasint), None
        if targettype == DataType.UInt16:
            return np.uint16(valueasint), None
        if targettype == DataType.UInt32:
            return np.uint32(valueasint), None
        if targettype == DataType.UInt64:
            return np.uint64(valueasint), None
        if targettype == DataType.DateTime or targettype == DataType.Guid:
            return None, ValueError(f"cannot convert \"Boolean\" expression value to \"{normalize_enumname(targettype)}\" column")

        return None, ValueError("unexpected column data type encountered")

    def _convert_fromint32(value: np.int32, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        if targettype == DataType.String:
            return str(value), None
        if targettype == DataType.Boolean:
            return value != 0, None
        if targettype == DataType.Single:
            return np.float32(value), None
        if targettype == DataType.Double:
            return np.float64(value), None
        if targettype == DataType.Decimal:
            return Decimal(value), None
        if targettype == DataType.Int8:
            return np.int8(value), None
        if targettype == DataType.Int16:
            return np.int16(value), None
        if targettype == DataType.Int32:
            return value, None
        if targettype == DataType.Int64:
            return np.int64(value), None
        if targettype == DataType.UInt8:
            return np.uint8(value), None
        if targettype == DataType.UInt16:
            return np.uint16(value), None
        if targettype == DataType.UInt32:
            return np.uint32(value), None
        if targettype == DataType.UInt64:
            return np.uint64(value), None
        if targettype == DataType.DateTime or targettype == DataType.Guid:
            return None, ValueError(f"cannot convert \"Int32\" expression value to \"{normalize_enumname(targettype)}\" column")

        return None, ValueError("unexpected column data type encountered")

    def _convert_fromint64(value: np.int64, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        if targettype == DataType.String:
            return str(value), None
        if targettype == DataType.Boolean:
            return value != 0, None
        if targettype == DataType.Single:
            return np.float32(value), None
        if targettype == DataType.Double:
            return np.float64(value), None
        if targettype == DataType.Decimal:
            return Decimal(value), None
        if targettype == DataType.Int8:
            return np.int8(value), None
        if targettype == DataType.Int16:
            return np.int16(value), None
        if targettype == DataType.Int32:
            return np.int32(value), None
        if targettype == DataType.Int64:
            return value, None
        if targettype == DataType.UInt8:
            return np.uint8(value), None
        if targettype == DataType.UInt16:
            return np.uint16(value), None
        if targettype == DataType.UInt32:
            return np.uint32(value), None
        if targettype == DataType.UInt64:
            return np.uint64(value), None
        if targettype == DataType.DateTime or targettype == DataType.Guid:
            return None, ValueError(f"cannot convert \"Int64\" expression value to \"{normalize_enumname(targettype)}\" column")

        return None, ValueError("unexpected column data type encountered")

    def _convert_fromdecimal(value: Decimal, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        if targettype == DataType.String:
            return str(value), None
        if targettype == DataType.Boolean:
            return value != 0.0, None
        if targettype == DataType.Single:
            return np.float32(value), None
        if targettype == DataType.Double:
            return np.float64(value), None
        if targettype == DataType.Decimal:
            return value, None
        if targettype == DataType.Int8:
            return np.int8(value), None
        if targettype == DataType.Int16:
            return np.int16(value), None
        if targettype == DataType.Int32:
            return np.int32(value), None
        if targettype == DataType.Int64:
            return np.int64(value), None
        if targettype == DataType.UInt8:
            return np.uint8(value), None
        if targettype == DataType.UInt16:
            return np.uint16(value), None
        if targettype == DataType.UInt32:
            return np.uint32(value), None
        if targettype == DataType.UInt64:
            return np.uint64(value), None
        if targettype == DataType.DateTime or targettype == DataType.Guid:
            return None, ValueError(f"cannot convert \"Decimal\" expression value to \"{normalize_enumname(targettype)}\" column")

        return None, ValueError("unexpected column data type encountered")

    def _convert_fromdouble(value: np.float64, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        if targettype == DataType.String:
            return str(value), None
        if targettype == DataType.Boolean:
            return value != 0.0, None
        if targettype == DataType.Single:
            return np.float32(value), None
        if targettype == DataType.Double:
            return value, None
        if targettype == DataType.Decimal:
            return Decimal(value), None
        if targettype == DataType.Int8:
            return np.int8(value), None
        if targettype == DataType.Int16:
            return np.int16(value), None
        if targettype == DataType.Int32:
            return np.int32(value), None
        if targettype == DataType.Int64:
            return np.int64(value), None
        if targettype == DataType.UInt8:
            return np.uint8(value), None
        if targettype == DataType.UInt16:
            return np.uint16(value), None
        if targettype == DataType.UInt32:
            return np.uint32(value), None
        if targettype == DataType.UInt64:
            return np.uint64(value), None
        if targettype == DataType.DateTime or targettype == DataType.Guid:
            return None, ValueError(f"cannot convert \"Double\" expression value to \"{normalize_enumname(targettype)}\" column")

        return None, ValueError("unexpected column data type encountered")

    def _convert_fromstring(value: str, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        try:
            if targettype == DataType.String:
                return value, None
            if targettype == DataType.DateTime:
                return parser.parse(value)
            if targettype == DataType.Guid:
                return UUID(value), None

            def from_string(dtype): return np.array([value]).astype(dtype)[0]

            if targettype == DataType.Boolean:
                return bool(value), None
            if targettype == DataType.Single:
                return from_string(np.float32), None
            if targettype == DataType.Double:
                return from_string(np.float64), None
            if targettype == DataType.Decimal:
                return from_string(Decimal), None
            if targettype == DataType.Int8:
                return from_string(np.int8), None
            if targettype == DataType.Int16:
                return from_string(np.int16), None
            if targettype == DataType.Int32:
                return from_string(np.int32), None
            if targettype == DataType.Int64:
                return from_string(np.int64), None
            if targettype == DataType.UInt8:
                return from_string(np.uint8), None
            if targettype == DataType.UInt16:
                return from_string(np.uint16), None
            if targettype == DataType.UInt32:
                return from_string(np.uint32), None
            if targettype == DataType.UInt64:
                return from_string(np.uint64), None

            return None, ValueError("unexpected column data type encountered")

        except Exception as ex:
            return None, ValueError(f"failed to convert \"String\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")
