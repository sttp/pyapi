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
from gsf import Convert, Empty, normalize_enumname
from .datatype import DataType, default_datatype
from .datacolumn import DataColumn
from .constants import ExpressionValueType
from .errors import EvaluateError
from decimal import Decimal
from datetime import datetime
from uuid import UUID
from typing import Callable, Iterator, Optional, Tuple, Union, TYPE_CHECKING
import numpy as np

if TYPE_CHECKING:
    from .dataset import xsdformat
    from .datatable import DataTable
    from .expressiontree import ExpressionTree


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
        self._values = np.empty(parent.columncount, object)

    def __getitem__(self, key: Union[int, str]) -> object:
        if isinstance(key, str):
            value, err = self.value_byname(key)
        else:
            value, err = self.value(key)

        if err is not None:
            raise err

        return value

    def __setitem__(self, key: Union[int, str], value: object):
        if isinstance(key, str):
            err = self.set_value_byname(key, value)
        else:
            err = self.set_value(key, value)

        if err is not None:
            raise err

    def __len__(self) -> int:
        return len(self._values)

    def __contains__(self, item: object) -> bool:
        return item in self._values

    def __iter__(self) -> Iterator[object]:
        return iter(self._values)

    @property
    def parent(self) -> DataTable:
        """
        Gets the parent `DataTable` of the `DataRow`.
        """

        return self._parent

    def _get_columnindex(self, columnname: str) -> Tuple[int, Optional[Exception]]:
        if (column := self._parent.column_byname(columnname)) is None:
            return -1, ValueError(f"column name \"{columnname}\" was not found in table \"{self._parent.name}\"")

        return column.index, None

    def _validate_columntype(self, columnindex: int, targettype: Union[int, DataType], read: bool) -> Tuple[Optional[DataColumn], Optional[Exception]]:
        if (column := self._parent.column(columnindex)) is None:
            return None, IndexError(f"column index {columnindex} is out of range for table \"{self._parent.name}\"")

        if targettype > -1 and column.datatype != targettype:
            if read:
                action = "read"
                preposition = "from"
            else:
                action = "assign"
                preposition = "to"

            return None, ValueError(f"cannot {action} \"{normalize_enumname(DataType(targettype))}\" value {preposition} DataColumn \"{column.name}\" for table \"{self._parent.name}\", column data type is \"{normalize_enumname(column.datatype)}\"")

        if not read and column.computed:
            return None, ValueError(f"cannot assign value to DataColumn \"{column.name}\" for table \"{self._parent.name}\", column is computed with an expression")

        return column, None

    def _expressiontree(self, column: DataColumn) -> Tuple[Optional[ExpressionTree], Optional[Exception]]:
        columnindex = column.index
        value = self._values[columnindex]

        if value is None:
            from .filterexpressionparser import FilterExpressionParser

            expressiontree, err = FilterExpressionParser.generate_expressiontree(column.parent, column.expression, True)

            if err is not None:
                return None, EvaluateError(f"failed to parse expression \"{column.expression}\" defined for computed DataColumn \"{column.name}\" for table \"{self._parent.name}\": {err}")

            self._values[columnindex] = expressiontree
            return expressiontree, None

        return value, None

    def _get_computedvalue(self, column: DataColumn) -> Tuple[Optional[object], Optional[Exception]]:
        expressiontree, err = self._expressiontree(column)

        if err is not None:
            return None, err

        try:
            sourcevalue, err = expressiontree.evaluate(self)
        except Exception as ex:
            err = ex

        if err is not None:
            return None, EvaluateError(f"failed to evaluate expression \"{column.expression}\" defined for computed DataColumn \"{column.name}\" for table \"{self._parent.name}\": {err}")

        sourcetype = sourcevalue.valuetype
        targettype = column.datatype

        if sourcetype == ExpressionValueType.BOOLEAN:
            return self._convert_frombool(sourcevalue._booleanvalue(), targettype)
        if sourcetype == ExpressionValueType.INT32:
            return self._convert_fromint32(sourcevalue._int32value(), targettype)
        if sourcetype == ExpressionValueType.INT64:
            return self._convert_fromint64(sourcevalue._int64value(), targettype)
        if sourcetype == ExpressionValueType.DECIMAL:
            return self._convert_fromdecimal(sourcevalue._decimalvalue(), targettype)
        if sourcetype == ExpressionValueType.DOUBLE:
            return self._convert_fromdouble(sourcevalue._doublevalue(), targettype)
        if sourcetype == ExpressionValueType.STRING:
            return self._convert_fromstring(sourcevalue._stringvalue(), targettype)
        if sourcetype == ExpressionValueType.GUID:
            return self._convert_fromguid(sourcevalue._guidvalue(), targettype)
        if sourcetype == ExpressionValueType.DATETIME:
            return self._convert_fromdatetime(sourcevalue._datetimevalue(), targettype)

        return None, TypeError("unexpected expression value type encountered")

    def _convert_fromstring(self, value: str, targettype: DataType) -> Tuple[Optional[object], Optional[Exception]]:
        try:
            if targettype == DataType.STRING:
                return value, None
            if targettype == DataType.BOOLEAN:
                return bool(value), None
            if targettype == DataType.DATETIME:
                return Convert.from_str(value, datetime)
            if targettype == DataType.GUID:
                return UUID(value), None
            if targettype == DataType.SINGLE:
                return Convert.from_str(value, np.float32), None
            if targettype == DataType.DOUBLE:
                return Convert.from_str(value, np.float64), None
            if targettype == DataType.DECIMAL:
                return Decimal(value), None
            if targettype == DataType.INT8:
                return Convert.from_str(value, np.int8), None
            if targettype == DataType.INT16:
                return Convert.from_str(value, np.int16), None
            if targettype == DataType.INT32:
                return Convert.from_str(value, np.int32), None
            if targettype == DataType.INT64:
                return Convert.from_str(value, np.int64), None
            if targettype == DataType.UINT8:
                return Convert.from_str(value, np.uint8), None
            if targettype == DataType.UINT16:
                return Convert.from_str(value, np.uint16), None
            if targettype == DataType.UINT32:
                return Convert.from_str(value, np.uint32), None
            if targettype == DataType.UINT64:
                return Convert.from_str(value, np.uint64), None

            return None, TypeError("unexpected column data type encountered")
        except Exception as ex:
            return None, ValueError(f"failed to convert \"String\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")

    def _convert_fromguid(self, value: UUID, targettype: DataType) -> Tuple[Optional[object], Optional[Exception]]:
        try:
            if targettype == DataType.STRING:
                return str(value), None
            if targettype == DataType.GUID:
                return value, None
            if targettype in [DataType.BOOLEAN, DataType.DATETIME, DataType.SINGLE,
                              DataType.DOUBLE, DataType.DECIMAL, DataType.INT8, DataType.INT16,
                              DataType.INT32, DataType.INT64, DataType.UINT8, DataType.UINT16,
                              DataType.UINT32, DataType.UINT64]:
                return None, ValueError(f'cannot convert \"Guid\" expression value to \"{normalize_enumname(targettype)}\" column')

            return None, TypeError("unexpected column data type encountered")
        except Exception as ex:
            return None, ValueError(f'failed to convert \"Guid\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}')

    def _convert_fromvalue(self, value: object, sourcetype: DataType, targettype: DataType) -> Tuple[Optional[object], Optional[Exception]]:
        try:
            if targettype == DataType.STRING:
                return str(value), None
            if targettype == DataType.BOOLEAN:
                return value != 0, None
            if targettype == DataType.SINGLE:
                return np.float32(value), None
            if targettype == DataType.DOUBLE:
                return np.float64(value), None
            if targettype == DataType.DECIMAL:
                return Decimal(value), None
            if targettype == DataType.INT8:
                return np.int8(value), None
            if targettype == DataType.INT16:
                return np.int16(value), None
            if targettype == DataType.INT32:
                return np.int32(value), None
            if targettype == DataType.INT64:
                return np.int64(value), None
            if targettype == DataType.UINT8:
                return np.uint8(value), None
            if targettype == DataType.UINT16:
                return np.uint16(value), None
            if targettype == DataType.UINT32:
                return np.uint32(value), None
            if targettype == DataType.UINT64:
                return np.uint64(value), None
            if targettype in [DataType.DATETIME, DataType.GUID]:
                return None, ValueError(f"cannot convert \"{normalize_enumname(sourcetype)}\" expression value to \"{normalize_enumname(targettype)}\" column")

            return None, TypeError("unexpected column data type encountered")
        except Exception as ex:
            return None, ValueError(f"failed to convert \"{normalize_enumname(sourcetype)}\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")

    def _convert_frombool(self, value: bool, targettype: DataType) -> Tuple[Optional[object], Optional[Exception]]:
        return self._convert_fromvalue(1 if value else 0, DataType.BOOLEAN, targettype)

    def _convert_fromint32(self, value: np.int32, targettype: DataType) -> Tuple[Optional[object], Optional[Exception]]:
        return self._convert_fromvalue(value.item(), DataType.INT32, targettype)

    def _convert_fromint64(self, value: np.int64, targettype: DataType) -> Tuple[Optional[object], Optional[Exception]]:
        return self._convert_fromvalue(value.item(), DataType.INT64, targettype)

    def _convert_fromdecimal(self, value: Decimal, targettype: DataType) -> Tuple[Optional[object], Optional[Exception]]:
        return self._convert_fromvalue(value, DataType.DECIMAL, targettype)

    def _convert_fromdouble(self, value: np.float64, targettype: DataType) -> Tuple[Optional[object], Optional[Exception]]:
        return self._convert_fromvalue(value.item(), DataType.DOUBLE, targettype)

    def _convert_fromdatetime(self, value: datetime, targettype: DataType) -> Tuple[Optional[object], Optional[Exception]]:
        try:
            if targettype == DataType.STRING:
                return xsdformat(value), None
            if targettype == DataType.DATETIME:
                return value, None
        except Exception as ex:
            return None, ValueError(f"failed to convert \"DateTime\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")

        return self._convert_fromvalue(int(value.timestamp()), DataType.DATETIME, targettype)

    def value(self, columnindex: int) -> Tuple[Optional[object], Optional[Exception]]:
        """
        Reads the record value at the specified column index.
        """

        column, err = self._validate_columntype(columnindex, -1, True)

        if err is not None:
            return None, err

        if column.computed:
            return self._get_computedvalue(column)

        return self._values[columnindex], None

    def value_byname(self, columnname: str) -> Tuple[Optional[object], Optional[Exception]]:
        """
        Reads the record value for the specified column name.
        """

        index, err = self._get_columnindex(columnname)
        return (None, err) if err is not None else (self._values[index], None)

    def set_value(self, columnindex: int, value: object) -> Optional[Exception]:
        # sourcery skip: assign-if-exp
        """
        Assigns the record value at the specified column index.
        """

        _, err = self._validate_columntype(columnindex, -1, False)

        if err is not None:
            return err

        self._values[columnindex] = value
        return None

    def set_value_byname(self, columnname: str, value: object) -> Optional[Exception]:
        """
        Assigns the record value for the specified column name.
        """

        index, err = self._get_columnindex(columnname)
        return err if err is not None else self.set_value(index, value)

    def value_as_string(self, columnindex: int) -> str:
        """
        Reads the record value at the specified columnIndex converted to a string.
        For column index out of range or any other errors, an empty string will be returned.
        """

        return self.columnvalue_as_string(self._parent.column(columnindex))

    def value_as_string_byname(self, columnname: str) -> str:
        """
        Reads the record value for the specified columnName converted to a string.
        For column name not found or any other errors, an empty string will be returned.
        """

        return self.columnvalue_as_string(self._parent.column_byname(columnname))

    def columnvalue_as_string(self, column: Optional[DataColumn]) -> str:
        """
        Reads the record value for the specified data column converted
        to a string. For any errors, an empty string will be returned.
        """

        if column is None:
            return Empty.STRING

        index = column.index
        datatype = column.datatype

        if datatype == DataType.STRING:
            return self._string_from_typevalue(index, self.stringvalue)
        if datatype == DataType.BOOLEAN:
            return self._string_from_typevalue(index, self.booleanvalue)
        if datatype == DataType.DATETIME:
            return self._string_from_typevalue(index, self.datetimevalue, xsdformat)
        if datatype == DataType.SINGLE:
            return self._string_from_typevalue(index, self.singlevalue)
        if datatype == DataType.DOUBLE:
            return self._string_from_typevalue(index, self.doublevalue)
        if datatype == DataType.DECIMAL:
            return self._string_from_typevalue(index, self.decimalvalue)
        if datatype == DataType.GUID:
            return self._string_from_typevalue(index, self.guidvalue)
        if datatype == DataType.INT8:
            return self._string_from_typevalue(index, self.int8value)
        if datatype == DataType.INT16:
            return self._string_from_typevalue(index, self.int16value)
        if datatype == DataType.INT32:
            return self._string_from_typevalue(index, self.int32value)
        if datatype == DataType.INT64:
            return self._string_from_typevalue(index, self.int64value)
        if datatype == DataType.UINT8:
            return self._string_from_typevalue(index, self.uint8value)
        if datatype == DataType.UINT16:
            return self._string_from_typevalue(index, self.uint16value)
        if datatype == DataType.UINT32:
            return self._string_from_typevalue(index, self.uint32value)
        if datatype == DataType.UINT64:
            return self._string_from_typevalue(index, self.uint64value)

        return Empty.STRING

    def _checkstate(self, null: bool, err: Optional[Exception]) -> Tuple[bool, str]:
        if err is not None:
            return True, Empty.STRING

        return (True, "<NULL>") if null else (False, Empty.STRING)

    def _string_from_typevalue(self, index: int, getvalue: Callable[[int], Tuple[object, bool, Optional[Exception]]], strconv: Callable[[object], str] = str) -> str:
        value, null, err = getvalue(index)
        invalid, result = self._checkstate(null, err)
        
        return result if invalid else strconv(value)

    def _typevalue(self, columnindex: int, targettype: DataType) -> Tuple[object, bool, Optional[Exception]]:
        column, err = self._validate_columntype(columnindex, targettype, True)
        default = default_datatype(targettype)

        if err is not None:
            return default, False, err

        if column.computed:
            value, err = self._get_computedvalue(column)

            if err is not None:
                return default, False, err

            return (default, True, None) if value is None else (value, False, None)

        value = self._values[columnindex]

        return (default, True, None) if value is None else (value, False, None)

    def _typevalue_byname(self, columnname: str, targettype: DataType) -> Tuple[object, bool, Optional[Exception]]:
        index, err = self._get_columnindex(columnname)

        if err is not None:
            return default_datatype(targettype), False, err

        return self._typevalue(index, targettype)

    def stringvalue(self, columnindex: int) -> Tuple[str, bool, Optional[Exception]]:
        """
        Gets the string-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.STRING`.
        """

        return self._typevalue(columnindex, DataType.STRING)

    def stringvalue_byname(self, columnname: str) -> Tuple[str, bool, Optional[Exception]]:
        """
        Gets the string-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.STRING`.
        """

        return self._typevalue_byname(columnname, DataType.STRING)

    def booleanvalue(self, columnindex: int) -> Tuple[bool, bool, Optional[Exception]]:
        """
        Gets the boolean-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.BOOLEAN`.
        """

        return self._typevalue(columnindex, DataType.BOOLEAN)

    def booleanvalue_byname(self, columnname: str) -> Tuple[bool, bool, Optional[Exception]]:
        """
        Gets the boolean-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.BOOLEAN`.
        """

        return self._typevalue_byname(columnname, DataType.BOOLEAN)

    def datetimevalue(self, columnindex: int) -> Tuple[datetime, bool, Optional[Exception]]:
        """
        Gets the datetime-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.DATETIME`.
        """

        return self._typevalue(columnindex, DataType.DATETIME)

    def datetimevalue_byname(self, columnname: str) -> Tuple[datetime, bool, Optional[Exception]]:
        """
        Gets the datetime-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.DATETIME`.
        """

        return self._typevalue_byname(columnname, DataType.DATETIME)

    def singlevalue(self, columnindex: int) -> Tuple[np.float32, bool, Optional[Exception]]:
        """
        Gets the single-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.SINGLE`.
        """

        return self._typevalue(columnindex, DataType.SINGLE)

    def singlevalue_byname(self, columnname: str) -> Tuple[np.float32, bool, Optional[Exception]]:
        """
        Gets the single-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.SINGLE`.
        """

        return self._typevalue_byname(columnname, DataType.SINGLE)

    def doublevalue(self, columnindex: int) -> Tuple[np.float64, bool, Optional[Exception]]:
        """
        Gets the double-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.DOUBLE`.
        """

        return self._typevalue(columnindex, DataType.DOUBLE)

    def doublevalue_byname(self, columnname: str) -> Tuple[np.float64, bool, Optional[Exception]]:
        """
        Gets the double-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.DOUBLE`.
        """

        return self._typevalue_byname(columnname, DataType.DOUBLE)

    def decimalvalue(self, columnindex: int) -> Tuple[Decimal, bool, Optional[Exception]]:
        """
        Gets the decimal-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.DECIMAL`.
        """

        return self._typevalue(columnindex, DataType.DECIMAL)

    def decimalvalue_byname(self, columnname: str) -> Tuple[Decimal, bool, Optional[Exception]]:
        """
        Gets the decimal-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.DECIMAL`.
        """

        return self._typevalue_byname(columnname, DataType.DECIMAL)

    def guidvalue(self, columnindex: int) -> Tuple[UUID, bool, Optional[Exception]]:
        """
        Gets the guid-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.GUID`.
        """

        return self._typevalue(columnindex, DataType.GUID)

    def guidvalue_byname(self, columnname: str) -> Tuple[UUID, bool, Optional[Exception]]:
        """
        Gets the guid-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.GUID`.
        """

        return self._typevalue_byname(columnname, DataType.GUID)

    def int8value(self, columnindex: int) -> Tuple[np.int8, bool, Optional[Exception]]:
        """
        Gets the int8-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.INT8`.
        """

        return self._typevalue(columnindex, DataType.INT8)

    def int8value_byname(self, columnname: str) -> Tuple[np.int8, bool, Optional[Exception]]:
        """
        Gets the int8-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.INT8`.
        """

        return self._typevalue_byname(columnname, DataType.INT8)

    def int16value(self, columnindex: int) -> Tuple[np.int16, bool, Optional[Exception]]:
        """
        Gets the int16-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.INT16`.
        """

        return self._typevalue(columnindex, DataType.INT16)

    def int16value_byname(self, columnname: str) -> Tuple[np.int16, bool, Optional[Exception]]:
        """
        Gets the int16-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.INT16`.
        """

        return self._typevalue_byname(columnname, DataType.INT16)

    def int32value(self, columnindex: int) -> Tuple[np.int32, bool, Optional[Exception]]:
        """
        Gets the int32-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.INT32`.
        """

        return self._typevalue(columnindex, DataType.INT32)

    def int32value_byname(self, columnname: str) -> Tuple[np.int32, bool, Optional[Exception]]:
        """
        Gets the int32-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.INT32`.
        """

        return self._typevalue_byname(columnname, DataType.INT32)

    def int64value(self, columnindex: int) -> Tuple[np.int64, bool, Optional[Exception]]:
        """
        Gets the int64-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.INT64`.
        """

        return self._typevalue(columnindex, DataType.INT64)

    def int64value_byname(self, columnname: str) -> Tuple[np.int64, bool, Optional[Exception]]:
        """
        Gets the int64-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.INT64`.
        """

        return self._typevalue_byname(columnname, DataType.INT64)

    def uint8value(self, columnindex: int) -> Tuple[np.uint8, bool, Optional[Exception]]:
        """
        Gets the uint8-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.UINT8`.
        """

        return self._typevalue(columnindex, DataType.UINT8)

    def uint8value_byname(self, columnname: str) -> Tuple[np.uint8, bool, Optional[Exception]]:
        """
        Gets the uint8-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.UINT8`.
        """

        return self._typevalue_byname(columnname, DataType.UINT8)

    def uint16value(self, columnindex: int) -> Tuple[np.uint16, bool, Optional[Exception]]:
        """
        Gets the uint16-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.UINT16`.
        """

        return self._typevalue(columnindex, DataType.UINT16)

    def uint16value_byname(self, columnname: str) -> Tuple[np.uint16, bool, Optional[Exception]]:
        """
        Gets the uint16-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.UINT16`.
        """

        return self._typevalue_byname(columnname, DataType.UINT16)

    def uint32value(self, columnindex: int) -> Tuple[np.uint32, bool, Optional[Exception]]:
        """
        Gets the uint32-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.UINT32`.
        """

        return self._typevalue(columnindex, DataType.UINT32)

    def uint32value_byname(self, columnname: str) -> Tuple[np.uint32, bool, Optional[Exception]]:
        """
        Gets the uint32-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.UINT32`.
        """

        return self._typevalue_byname(columnname, DataType.UINT32)

    def uint64value(self, columnindex: int) -> Tuple[np.uint64, bool, Optional[Exception]]:
        """
        Gets the uint64-based record value at the specified column index.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.UINT64`.
        """

        return self._typevalue(columnindex, DataType.UINT64)

    def uint64value_byname(self, columnname: str) -> Tuple[np.uint64, bool, Optional[Exception]]:
        """
        Gets the uint64-based record value for the specified column name.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.UINT64`.
        """

        return self._typevalue_byname(columnname, DataType.UINT64)

    def __repr__(self):
        image = ["["]

        for i in range(self._parent.columncount):
            if i > 0:
                image.append(", ")

            strcol = self._parent.column(i).datatype == DataType.STRING

            if strcol:
                image.append("\"")

            image.append(self.value_as_string(i))

            if strcol:
                image.append("\"")

        image.append("]")

        return "".join(image)

    @staticmethod
    def compare_datarowcolumns(leftrow: DataRow, rightrow: DataRow, columnindex: int, exactmatch: bool) -> Tuple[int, Optional[Exception]]:  # sourcery skip: low-code-quality
        """
        Returns an integer comparing two `DataRow` column values for the specified column index.
        The result will be 0 if `leftrow`==`rightrow`, -1 if `leftrow` < `rightrow`, and +1 if `leftrow` > `rightrow`.
        An error will br returned if column index is out of range of either row, or row types do not match.
        """

        leftcolumn = leftrow.parent.column(columnindex)
        rightcolumn = rightrow.parent.column(columnindex)

        if leftcolumn is None or rightcolumn is None:
            return 0, IndexError("cannot compare, column index out of range")

        lefttype = leftcolumn.datatype
        righttype = rightcolumn.datatype

        if lefttype != righttype:
            return 0, ValueError("cannot compare, types do not match")

        def nullcompare(lefthasvalue: bool, righthasvalue: bool) -> int:
            if not lefthasvalue and not righthasvalue:
                return 0, None

            return 1 if lefthasvalue else -1, None

        def typecompare(
                leftrow_getvalue: Callable[[int], Tuple[object, bool, Optional[Exception]]],
                rightrow_getvalue: Callable[[int], Tuple[object, bool, Optional[Exception]]]) -> \
                            Tuple[int, Optional[Exception]]:

            leftvalue, leftnull, lefterr = leftrow_getvalue(columnindex)
            rightvalue, rightnull, righterr = rightrow_getvalue(columnindex)

            lefthasvalue = not leftnull and lefterr is None
            righthasvalue = not rightnull and righterr is None

            if lefthasvalue and righthasvalue:
                if leftvalue < rightvalue:
                    return -1, None

                return (1, None) if leftvalue > rightvalue else (0, None)

            return nullcompare(lefthasvalue, righthasvalue)

        if lefttype == DataType.STRING:
            if exactmatch:
                def upperstringvalue(index: int, getvalue: Callable[[int], Tuple[object, bool, Optional[Exception]]]) -> Tuple[str, bool, Optional[Exception]]:
                    value, null, err = getvalue(index)

                    if not null and err is None:
                        return value.upper(), False, None

                    return value, null, err

                def leftrowvalue(index):
                    return upperstringvalue(index, leftrow.stringvalue)

                def rightrowvalue(index):
                    return upperstringvalue(index, rightrow.stringvalue)
            else:
                leftrowvalue = leftrow.stringvalue
                rightrowvalue = rightrow.stringvalue

            return typecompare(leftrowvalue, rightrowvalue)
        if lefttype == DataType.BOOLEAN:
            leftvalue, leftnull, lefterr = leftrow.booleanvalue(columnindex)
            rightvalue, rightnull, righterr = rightrow.booleanvalue(columnindex)

            lefthasvalue = not leftnull and lefterr is None
            righthasvalue = not rightnull and righterr is None

            if lefthasvalue and righthasvalue:
                if leftvalue and not rightvalue:
                    return -1, None

                return (1, None) if not leftvalue and rightvalue else (0, None)

            return nullcompare(lefthasvalue, righthasvalue)
        if lefttype == DataType.DATETIME:
            return typecompare(leftrow.datetimevalue, rightrow.datetimevalue)
        if lefttype == DataType.SINGLE:
            return typecompare(leftrow.singlevalue, rightrow.singlevalue)
        if lefttype == DataType.DOUBLE:
            return typecompare(leftrow.doublevalue, rightrow.doublevalue)
        if lefttype == DataType.DECIMAL:
            return typecompare(leftrow.decimalvalue, rightrow.decimalvalue)
        if lefttype == DataType.GUID:
            return typecompare(leftrow.guidvalue, rightrow.guidvalue)
        if lefttype == DataType.INT8:
            return typecompare(leftrow.int8value, rightrow.int8value)
        if lefttype == DataType.INT16:
            return typecompare(leftrow.int16value, rightrow.int16value)
        if lefttype == DataType.INT32:
            return typecompare(leftrow.int32value, rightrow.int32value)
        if lefttype == DataType.INT64:
            return typecompare(leftrow.int64value, rightrow.int64value)
        if lefttype == DataType.UINT8:
            return typecompare(leftrow.uint8value, rightrow.uint8value)
        if lefttype == DataType.UINT16:
            return typecompare(leftrow.uint16value, rightrow.uint16value)
        if lefttype == DataType.UINT32:
            return typecompare(leftrow.uint32value, rightrow.uint32value)
        if lefttype == DataType.UINT64:
            return typecompare(leftrow.uint64value, rightrow.uint64value)

        return 0, TypeError("unexpected column data type encountered")
