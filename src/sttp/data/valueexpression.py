# ******************************************************************************************************
#  valueexpression.py - Gbtc
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
#  09/03/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from typing import Optional, Tuple, Union
from gsf import Convert, Empty, override, normalize_enumname
from .expression import Expression
from .dataset import xsdformat
from .constants import ExpressionType, ExpressionValueType
from decimal import Decimal
from datetime import datetime
from uuid import UUID
import numpy as np


class ValueExpression(Expression):
    """
    Represents a value expression.
    """

    def __init__(self, valuetype: ExpressionValueType, value: object):
        self._valuetype = valuetype

        if value is None:
            self._value = None
        elif valuetype == ExpressionValueType.BOOLEAN:
            self._value = bool(value)
        elif valuetype == ExpressionValueType.INT32:
            self._value = np.int32(value)
        elif valuetype == ExpressionValueType.INT64:
            self._value = np.int64(value)
        elif valuetype == ExpressionValueType.DECIMAL:
            self._value = Decimal(value)
        elif valuetype == ExpressionValueType.DOUBLE:
            self._value = np.float64(value)
        elif valuetype in [ExpressionValueType.STRING, ExpressionValueType.GUID, ExpressionValueType.DATETIME]:
            self._value = value
        else:
            raise TypeError(f"cannot create new value expression; unexpected expression value type: {normalize_enumname(valuetype)}")

    @override
    @property
    def expressiontype(self) -> ExpressionType:
        """
        Gets the type of this `ValueExpression`.
        """

        return ExpressionType.VALUE

    @property
    def valuetype(self) -> ExpressionValueType:
        """
        Gets the value type of this `ValueExpression`.
        """

        return self._valuetype

    @property
    def value(self) -> object:
        """
        Gets the value of this `ValueExpression`.
        """

        return self._value

    def __repr__(self) -> str:
        if self._valuetype == ExpressionValueType.DATETIME:
            return xsdformat(datetime(self._value))

        return str(self._valuetype)

    def is_null(self) -> bool:
        """
        Gets a flag that determines if this `ValueExpression` is null, i.e., `None`.
        """

        return self._value is None

    def integervalue(self, defaultvalue: int = 0) -> int:
        """
        Gets the `ValueExpression` as an integer value or specified default value if not possible.
        """

        if self._valuetype == ExpressionValueType.BOOLEAN:
            return self._booleanvalue_asint()

        if self._valuetype == ExpressionValueType.INT32:
            return int(self._int32value())

        if self._valuetype == ExpressionValueType.INT64:
            return int(self._int64value())

        return defaultvalue

    def _validate_valuetype(self, valuetype: ExpressionValueType) -> Optional[Exception]:
        if valuetype != self._valuetype:
            return TypeError(f"cannot read expression value expression as \"{normalize_enumname(valuetype)}\", type is \"{normalize_enumname(self._valuetype)}\"")

        return None

    def booleanvalue(self) -> Tuple[bool, Optional[Exception]]:
        """
        Gets the `ValueExpression` as a boolean value.
        An error will be returned if value type is not `ExpressionValueType.BOOLEAN`.
        """

        err = self._validate_valuetype(ExpressionValueType.BOOLEAN)
        return (self._booleanvalue(), None) if err is None else (False, err)

    def _booleanvalue(self) -> bool:
        return False if self._value is None else bool(self._value)

    def _booleanvalue_asint(self) -> int:
        return 1 if self._booleanvalue() else 0

    def int32value(self) -> Tuple[np.int32, Optional[Exception]]:
        """
        Gets the `ValueExpression` as a 32-bit integer value.
        An error will be returned if value type is not `ExpressionValueType.INT32`.
        """

        err = self._validate_valuetype(ExpressionValueType.INT32)
        return (self._int32value(), None) if err is None else (Empty.INT32, err)

    def _int32value(self) -> np.int32:
        return Empty.INT32 if self._value is None else np.int32(self._value)

    def int64value(self) -> Tuple[np.int64, Optional[Exception]]:
        """
        Gets the `ValueExpression` as a 64-bit integer value.
        An error will be returned if value type is not `ExpressionValueType.INT64`.
        """

        err = self._validate_valuetype(ExpressionValueType.INT64)
        return (self._int64value(), None) if err is None else (Empty.INT64, err)

    def _int64value(self) -> np.int64:
        return Empty.INT64 if self._value is None else np.int64(self._value)

    def decimalvalue(self) -> Tuple[Decimal, Optional[Exception]]:
        """
        Gets the `ValueExpression` as a Decimal value.
        An error will be returned if value type is not `ExpressionValueType.DECIMAL`.
        """

        err = self._validate_valuetype(ExpressionValueType.DECIMAL)
        return (self._decimalvalue(), None) if err is None else (Empty.DECIMAL, err)

    def _decimalvalue(self) -> Decimal:
        return Empty.DECIMAL if self._value is None else Decimal(self._value)

    def doublevalue(self) -> Tuple[np.float64, Optional[Exception]]:
        """
        Gets the `ValueExpression` as a double value.
        An error will be returned if value type is not `ExpressionValueType.DOUBLE`.
        """

        err = self._validate_valuetype(ExpressionValueType.DOUBLE)
        return (self._doublevalue(), None) if err is None else (Empty.DOUBLE, err)

    def _doublevalue(self) -> np.float64:
        return Empty.DOUBLE if self._value is None else np.float64(self._value)

    def stringvalue(self) -> Tuple[str, Optional[Exception]]:
        """
        Gets the `ValueExpression` as a string value.
        An error will be returned if value type is not `ExpressionValueType.STRING`.
        """

        err = self._validate_valuetype(ExpressionValueType.STRING)
        return (self._stringvalue(), None) if err is None else (Empty.STRING, err)

    def _stringvalue(self) -> str:
        return Empty.STRING if self._value is None else self._value

    def guidvalue(self) -> Tuple[UUID, Optional[Exception]]:
        """
        Gets the `ValueExpression` as a GUID value.
        An error will be returned if value type is not `ExpressionValueType.GUID`.
        """

        err = self._validate_valuetype(ExpressionValueType.GUID)
        return (self._guidvalue(), None) if err is None else (Empty.GUID, err)

    def _guidvalue(self) -> UUID:
        return Empty.GUID if self._value is None else self._value

    def datetimevalue(self) -> Tuple[datetime, Optional[Exception]]:
        """
        Gets the `ValueExpression` as a datetime value.
        An error will be returned if value type is not `ExpressionValueType.DATETIME`.
        """

        err = self._validate_valuetype(ExpressionValueType.DATETIME)
        return (self._datetimevalue(), None) if err is None else (Empty.DATETIME, err)

    def _datetimevalue(self) -> datetime:
        return Empty.DATETIME if self._value is None else self._value

    def convert(self, target_typevalue: ExpressionValueType) -> Tuple[Optional["ValueExpression"], Optional[Exception]]:
        """
        Attempts to convert the `ValueExpression` to the specified type.
        """

        # If source value is Null, result is Null, regardless of target type
        if self.is_null():
            return ValueExpression.nullvalue(target_typevalue), None

        valuetype = self._valuetype

        if valuetype == ExpressionValueType.BOOLEAN:
            return self._convert_fromboolean(target_typevalue)

        if valuetype == ExpressionValueType.INT32:
            return self._convert_fromint32(target_typevalue)

        if valuetype == ExpressionValueType.INT64:
            return self._convert_fromint64(target_typevalue)

        if valuetype == ExpressionValueType.DECIMAL:
            return self._convert_fromdecimal(target_typevalue)

        if valuetype == ExpressionValueType.DOUBLE:
            return self._convert_fromdouble(target_typevalue)

        if valuetype == ExpressionValueType.STRING:
            return self._convert_fromstring(target_typevalue)

        if valuetype == ExpressionValueType.GUID:
            return self._convert_fromguid(target_typevalue)

        if valuetype == ExpressionValueType.DATETIME:
            return self._convert_fromdatetime(target_typevalue)

        return None, TypeError("unexpected expression value type encountered")

    def _convert_fromnumeric(self, value: Union[int, float], from_typename: str, target_typevalue: ExpressionValueType) -> Tuple[Optional["ValueExpression"], Optional[Exception]]:
        try:
            if target_typevalue == ExpressionValueType.BOOLEAN:
                return ValueExpression(target_typevalue, value != 0), None

            if target_typevalue == ExpressionValueType.INT32:
                return ValueExpression(target_typevalue, np.int32(value)), None

            if target_typevalue == ExpressionValueType.INT64:
                return ValueExpression(target_typevalue, np.int64(value)), None

            if target_typevalue == ExpressionValueType.DECIMAL:
                return ValueExpression(target_typevalue, Decimal(value)), None

            if target_typevalue == ExpressionValueType.DOUBLE:
                return ValueExpression(target_typevalue, np.float64(value)), None

            if target_typevalue == ExpressionValueType.STRING:
                return ValueExpression(target_typevalue, str(value)), None
        except Exception as ex:
            return None, ValueError(f"failed while attempting to convert from \"{from_typename}\" value ({value}) to \"{normalize_enumname(target_typevalue.name)}\": {ex}")

        return None, TypeError(f"cannot convert \"{from_typename}\" value ({value}) to \"{normalize_enumname(target_typevalue)}\"")

    def _convert_fromboolean(self, target_typevalue: ExpressionValueType) -> Tuple[Optional["ValueExpression"], Optional[Exception]]:
        return self._convert_fromnumeric(self._booleanvalue_asint(), "Boolean", target_typevalue)

    def _convert_fromint32(self, target_typevalue: ExpressionValueType) -> Tuple[Optional["ValueExpression"], Optional[Exception]]:
        return self._convert_fromnumeric(self._int32value().item(), "Int32", target_typevalue)

    def _convert_fromint64(self, target_typevalue: ExpressionValueType) -> Tuple[Optional["ValueExpression"], Optional[Exception]]:
        return self._convert_fromnumeric(self._int64value().item(), "Int64", target_typevalue)

    def _convert_fromdecimal(self, target_typevalue: ExpressionValueType) -> Tuple[Optional["ValueExpression"], Optional[Exception]]:
        return self._convert_fromnumeric(self._decimalvalue(), "Decimal", target_typevalue)

    def _convert_fromdouble(self, target_typevalue: ExpressionValueType) -> Tuple[Optional["ValueExpression"], Optional[Exception]]:
        return self._convert_fromnumeric(self._doublevalue().item(), "Double", target_typevalue)

    def _convert_fromstring(self, target_typevalue: ExpressionValueType) -> Tuple[Optional["ValueExpression"], Optional[Exception]]:
        value = self._stringvalue()

        try:
            if target_typevalue == ExpressionValueType.BOOLEAN:
                return ValueExpression(target_typevalue, bool(value)), None

            if target_typevalue == ExpressionValueType.INT32:
                return ValueExpression(target_typevalue, np.int32(Decimal(value))), None

            if target_typevalue == ExpressionValueType.INT64:
                return ValueExpression(target_typevalue, np.int64(Decimal(value))), None

            if target_typevalue == ExpressionValueType.DECIMAL:
                return ValueExpression(target_typevalue, Decimal(value)), None

            if target_typevalue == ExpressionValueType.DOUBLE:
                return ValueExpression(target_typevalue, np.float64(value)), None

            if target_typevalue == ExpressionValueType.STRING:
                return ValueExpression(target_typevalue, value), None

            if target_typevalue == ExpressionValueType.GUID:
                return ValueExpression(target_typevalue, UUID(value)), None

            if target_typevalue == ExpressionValueType.DATETIME:
                return ValueExpression(target_typevalue, Convert.from_str(value, datetime)), None
        except Exception as ex:
            return None, ValueError(f"failed while attempting to convert \"String\" value ('{value}') to \"{normalize_enumname(target_typevalue)}\": {ex}")

        return None, TypeError(f"cannot convert \"String\" value ('{value}') to \"{normalize_enumname(target_typevalue)}\"")

    def _convert_fromguid(self, target_typevalue: ExpressionValueType) -> Tuple[Optional["ValueExpression"], Optional[Exception]]:
        value = self._guidvalue()

        if target_typevalue == ExpressionValueType.STRING:
            return ValueExpression(target_typevalue, str(value)), None

        if target_typevalue == ExpressionValueType.GUID:
            return ValueExpression(target_typevalue, value), None

        return None, TypeError(f"cannot convert \"Guid\" to \"{normalize_enumname(target_typevalue)}\"")

    def _convert_fromdatetime(self, target_typevalue: ExpressionValueType) -> Tuple[Optional["ValueExpression"], Optional[Exception]]:
        value = self._datetimevalue()

        if target_typevalue == ExpressionValueType.STRING:
            return ValueExpression(target_typevalue, xsdformat(value)), None

        if target_typevalue == ExpressionValueType.DATETIME:
            return ValueExpression(target_typevalue, value), None

        return self._convert_fromnumeric(value.timestamp(), "DateTime", target_typevalue)

    @staticmethod
    def nullvalue(target_valuetype: ExpressionValueType) -> "ValueExpression":
        """
        Gets a `ValueExpression` that represents a null, i.e., `None`, value of the specified `ExpressionValueType`.
        """

        return ValueExpression(target_valuetype, None)


TRUEVALUE = ValueExpression(ExpressionValueType.BOOLEAN, True)
"""
Defines a `ValueExpression` that represents `True`.
"""

FALSEVALUE = ValueExpression(ExpressionValueType.BOOLEAN, False)
"""
Defines a `ValueExpression` that represents `False`.
"""

NULLVALUE = ValueExpression.nullvalue(ExpressionValueType.UNDEFINED)
"""
Defines a `ValueExpression` that represents a null, i.e., `None`, value, value type `Undefined`.
"""

NULLBOOLVALUE = ValueExpression.nullvalue(ExpressionValueType.BOOLEAN)
"""
Defines a `ValueExpression` that represents a null, i.e., `None`, value of type `Boolean`.
"""

NULLINT32VALUE = ValueExpression.nullvalue(ExpressionValueType.INT32)
"""
Defines a `ValueExpression` that represents a null, i.e., `None`, value of type `Int32`.
"""

NULLDATETIMEVALUE = ValueExpression.nullvalue(ExpressionValueType.DATETIME)
"""
Defines a `ValueExpression` that represents a null, i.e., `None`, value of type `DateTime`.
"""

NULLSTRINGVALUE = ValueExpression.nullvalue(ExpressionValueType.STRING)
"""
Defines a `ValueExpression` that represents a null, i.e., `None`, value of type `String`.
"""

EMPTYSTRINGVALUE = ValueExpression(ExpressionValueType.STRING, Empty.STRING)
"""
Defines a `ValueExpression` that represents an empty string.
"""
