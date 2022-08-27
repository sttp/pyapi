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
from gsf import Empty, normalize_enumname
from .dataset import xsdformat
from .datatype import DataType
from .datacolumn import DataColumn
from decimal import Decimal
from datetime import datetime
from dateutil import parser
from uuid import UUID
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union, TYPE_CHECKING
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

    def __getitem__(self, key: Union[int, str]) -> Any:
        if isinstance(key, int):
            return self.value(key)

        return self.value_byname(str(key))

    def __setitem__(self, key: Union[int, str], value: Any):
        if isinstance(key, int):
            self.set_value(key, value)

        self.set_value_byname(str(key), value)

    def __len__(self) -> int:
        return len(self._values)

    def __contains__(self, item: Any) -> bool:
        return item in self._values

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

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

    def _validate_columntype(self, columnindex: int, targettype: Union[int, DataType], read: bool) -> Tuple[Optional[DataColumn], Optional[Exception]]:
        column = self._parent.column(columnindex)

        if column is None:
            return None, ValueError(f"column index {columnindex} is out of range for table \"{self._parent.name}\"")

        if targettype > -1 and column.type != targettype:
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
        try:
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
        except Exception as ex:
            return None, ValueError(f"failed to convert \"Boolean\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")
        
    def _convert_fromint32(value: np.int32, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        try:
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
        except Exception as ex:
            return None, ValueError(f"failed to convert \"Int32\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")
            

    def _convert_fromint64(value: np.int64, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        try:
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
        except Exception as ex:
            return None, ValueError(f"failed to convert \"Int64\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")

    def _convert_fromdecimal(value: Decimal, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        try:
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
        except Exception as ex:
            return None, ValueError(f"failed to convert \"Decimal\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")

    def _convert_fromdouble(value: np.float64, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        try:
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
        except Exception as ex:
            return None, ValueError(f"failed to convert \"Double\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")

    def _convert_fromstring(value: str, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        try:
            if targettype == DataType.String:
                return value, None
            if targettype == DataType.Boolean:
                return bool(value), None
            if targettype == DataType.DateTime:
                return parser.parse(value)
            if targettype == DataType.Guid:
                return UUID(value), None

            def from_string(dtype): return np.array([value]).astype(dtype)[0]

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

    def _convert_fromguid(value: UUID, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        try:
            if targettype == DataType.String:
                return str(value), None
            if targettype == DataType.Guid:
                return value, None                
            if targettype == DataType.Boolean or targettype == DataType.DateTime or \
                    targettype == DataType.Single or targettype == DataType.Double or \
                    targettype == DataType.Decimal or targettype == DataType.Int8 or \
                    targettype == DataType.Int16 or targettype == DataType.Int32 or \
                    targettype == DataType.Int64 or targettype == DataType.UInt8 or \
                    targettype == DataType.UInt16 or targettype == DataType.UInt32 or \
                    targettype == DataType.UInt64:
                return None, ValueError(f"cannot convert \"Guid\" expression value to \"{normalize_enumname(targettype)}\" column")
            
            return None, ValueError("unexpected column data type encountered")
        except Exception as ex:
            return None, ValueError(f"failed to convert \"Guid\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")

    def _convert_fromdatetime(value: datetime, targettype: DataType) -> Tuple[Optional[Any], Optional[Exception]]:
        try:
            if targettype == DataType.String:
                return xsdformat(value), None
            if targettype == DataType.DateTime:
                return value, None

            seconds = int(value.timestamp())

            if targettype == DataType.Boolean:
                return seconds == 0, None
            if targettype == DataType.Single:
                return np.float32(seconds), None
            if targettype == DataType.Double:
                return np.float64(seconds), None
            if targettype == DataType.Decimal:
                return Decimal(seconds), None
            if targettype == DataType.Int8:
                return np.int8(seconds), None
            if targettype == DataType.Int16:
                return np.int16(seconds), None
            if targettype == DataType.Int32:
                return np.int32(seconds), None
            if targettype == DataType.Int64:
                return np.int64(seconds), None
            if targettype == DataType.UInt8:
                return np.uint8(seconds), None
            if targettype == DataType.UInt16:
                return np.uint16(seconds), None
            if targettype == DataType.UInt32:
                return np.uint32(seconds), None
            if targettype == DataType.UInt64:
                return np.uint64(seconds), None
            if targettype == DataType.Guid:
                return None, ValueError(f"cannot convert \"DateTime\" expression value to \"{normalize_enumname(targettype)}\" column")
            
            return None, ValueError("unexpected column data type encountered")
        except Exception as ex:
            return None, ValueError(f"failed to convert \"DateTime\" expression value to \"{normalize_enumname(targettype)}\" column: {ex}")

    def value(self, columnindex: int) -> Tuple[Optional[Any], Optional[Exception]]:
        """
        Reads the record value at the specified column index.
        """

        column, err = self._validate_columntype(columnindex, -1, True)

        if err is not None:
            return None, err
        
        if column.computed:
            return self._get_computedvalue(column)

        return self._values[columnindex]

    def value_byname(self, columnname: str) -> Tuple[Optional[Any], Optional[Exception]]:
        """
        Reads the record value for the specified column name.
        """

        index, err = self._get_columnindex(columnname)

        if err is not None:
            return None, err

        return self._values[index], None
    
    def set_value(self, columnindex: int, value: Any) -> Optional[Exception]:
        """
        Assigns the record value at the specified column index.
        """

        _, err = self._validate_columntype(columnindex, -1, False)

        if err is not None:
            return err

        self._values[columnindex] = value
        return None
        
    def set_value_byname(self, columnname: str, value: Any) -> Optional[Exception]:
        """
        Assigns the record value for the specified column name.
        """

        index, err = self._get_columnindex(columnname)

        if err is not None:
            return err
        
        return self.set_value(index, value)

    def columnvalue_as_string(self, column: DataColumn) -> str:
        """
        Reads the record value for the specified data column converted
        to a string. For any errors, an empty string will be returned.
        """

        if column is None:
            return Empty.STRING

        index = column.index
        type = column.type

        if type == DataType.String:
            return self._stringvalue_from_string(index)
        if type == DataType.Boolean:
            return self._stringvalue_from_typevalue(index, self.booleanvalue)
        if type == DataType.DateTime:
            return self._stringvalue_from_datetime(index)
        if type == DataType.Single:
            return self._stringvalue_from_typevalue(index, self.singlevalue)
        if type == DataType.Double:
            return self._stringvalue_from_typevalue(index, self.doublevalue)
        if type == DataType.Decimal:
            return self._stringvalue_from_typevalue(index, self.decimalvalue)
        if type == DataType.Guid:
            return self._stringvalue_from_typevalue(index, self.guidvalue)
        if type == DataType.Int8:
            return self._stringvalue_from_typevalue(index, self.int8value)
        if type == DataType.Int16:
            return self._stringvalue_from_typevalue(index, self.int16value)
        if type == DataType.Int32:
            return self._stringvalue_from_typevalue(index, self.int32value)
        if type == DataType.Int64:
            return self._stringvalue_from_typevalue(index, self.int64value)
        if type == DataType.UInt8:
            return self._stringvalue_from_typevalue(index, self.uint8value)
        if type == DataType.UInt16:
            return self._stringvalue_from_typevalue(index, self.uint16value)
        if type == DataType.UInt32:
            return self._stringvalue_from_typevalue(index, self.uint32value)
        if type == DataType.UInt64:
            return self._stringvalue_from_typevalue(index, self.uint64value)
        
        return Empty.STRING

    def _checkstate(self, null: bool, err: Optional[Exception]) -> Tuple[bool, str]:
        if err is not None:
            return True, Empty.STRING

        if null:
            return True, "<NULL>"

        return False, Empty.STRING

    def _stringvalue_from_string(self, index: int) -> str:
        value, null, err = self.stringvalue(index)

        invalid, result = self._checkstate(null, err)

        if invalid:
            return result

        return value

    def _stringvalue_from_datetime(self, index: int) -> str:
        value, null, err = self.datetimevalue(index)

        invalid, result = self._checkstate(null, err)

        if invalid:
            return result

        return xsdformat(value)

    def _stringvalue_from_typevalue(self, index: int, getvalue: Callable[[int], Tuple[str, bool, Optional[Exception]]]) -> str:
        value, null, err = getvalue(index)

        invalid, result = self._checkstate(null, err)

        if invalid:
            return result

        return str(value)

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

    def stringvalue(self, columnindex: int) -> Tuple[str, bool, Optional[Exception]]:
        """
        Gets the record value at the specified column index cast as a string.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.String`.
        """
        
        column, err = self._validate_columntype(columnindex, DataType.String, True)

        if err is not None:
            return Empty.STRING, False, err

        if column.computed:
            value, err = self._get_computedvalue(column)

            if err is not None:
                return Empty.STRING, False, err
            
            if value is None:
                return Empty.STRING, True, None

            return str(value), False, None

        value = self._values[columnindex]

        if value is None:
            return Empty.STRING, True, None

        return str(value), False, None

    def stringvalue_byname(self, columnname: str) -> Tuple[str, bool, Optional[Exception]]:
        """
        Gets the record value for the specified column name cast as a string.
        Second parameter in tuple return value indicates if original value was None.
        An error will be returned if column type is not `DataType.String`.
        """

        index, err = self._get_columnindex(columnname)

        if err is not None:
            return Empty.STRING, False, err

        return self.stringvalue(index)