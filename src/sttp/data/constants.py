# ******************************************************************************************************
#  constants.py - Gbtc
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

from gsf import normalize_enumname
from .errors import EvaluateError
from enum import IntEnum
from typing import Optional, Tuple


class ExpressionType(IntEnum):
    """
    Defines an enumeration of possible expression types.
    """

    VALUE = 0
    """
    Value expression type.
    """

    UNARY = 1
    """
    Unary expression type.
    """

    COLUMN = 2
    """
    Column expression type.
    """

    INLIST = 3
    """
    In-list expression type.
    """

    FUNCTION = 4
    """
    Function expression type.
    """

    OPERATOR = 5
    """
    Operator expression type.
    """


class ExpressionValueType(IntEnum):
    """
    Defines an enumeration of possible expression value data types. These expression value
    data types are reduced to a reasonable set of possible types that can be represented in
    a filter expression. All data table column values will be mapped to these types.
    """

    BOOLEAN = 0
    """
    Boolean value type for an expression, i.e., `bool`.
    """

    INT32 = 1
    """
    32-bit integer value type for an expression, i.e., `numpy.int32`.
    """

    INT64 = 2
    """
    64-bit integer value type for an expression, i.e., `numpy.int64`.
    """

    DECIMAL = 3
    """
    Decimal value type for an expression, i.e., `decimal.Decimal`.
    """

    DOUBLE = 4
    """
    Double value type for an expression, i.e., `numpy.float64`.
    """

    STRING = 5
    """
    String value type for an expression, i.e., `str`.
    """

    GUID = 6
    """
    GUID value type for an expression, i.e., `uuid.UUID`.
    """
    
    DATETIME = 7
    """
    DateTime value type for an expression, i.e., `datetime.datetime`.
    """

    UNDEFINED = 8
    """
    Undefined value type for an expression, i.e., `None`.
    """

EXPRESSIONVALUETYPELEN = ExpressionValueType.UNDEFINED + 1
"""
Defines the number of elements in the `ExpressionValueType` enumeration.
"""

def is_integertype(type: ExpressionValueType) -> bool:
    """
    Determines if the specified expression value type is an integer type.
    """

    return type in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32, ExpressionValueType.INT64]

def is_numerictype(type: ExpressionValueType) -> bool:
    """
    Determines if the specified expression value type is a numeric type.
    """

    return type in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32, ExpressionValueType.INT64, ExpressionValueType.DECIMAL, ExpressionValueType.DOUBLE]

class ExpressionUnaryType(IntEnum):
    """
    Defines an enumeration of possible unary expression types.
    """

    PLUS = 0
    """
    Positive unary expression type, i.e., `+`.
    """
    
    MINUS = 1
    """
    Negative unary expression type, i.e., `-`.
    """

    NOT = 2
    """
    Not unary expression type, i.e., `~` or `!`.
    """

    def __str__(self):
        return "+" if self.value == 0 else \
               "-" if self.value == 1 else "~"


class ExpressionFunctionType(IntEnum):
    """
    Defines an enumeration of possible expression function types.
    """

    ABS = 0
    """
    Defines a function type that returns the absolute value of the specified numeric expression.
    """

    CEILING = 1
    """
    Defines a function type that returns the smallest integer greater than or equal to the specified numeric expression.
    """

    COALESCE = 2
    """
    Defines a function type that returns the first non-null value from the specified expression list.
    """

    CONVERT = 3
    """
    Defines a function type that converts the specified expression to the specified data type.
    """

    CONTAINS = 4
    """
    Defines a function type that returns a boolean value indicating whether the specified string expression contains the specified substring.
    """

    DATEADD = 5
    """
    Defines a function type that adds the specified number of specified `TimeInterval` units to the specified date expression.
    """

    DATEDIFF = 6
    """
    Defines a function type that returns the number of specified `TimeInterval` units between the specified date expressions.
    """

    DATEPART = 7
    """
    Defines a function type that returns the specified `TimeInterval` unit from the specified date expression.
    """

    ENDSWITH = 8
    """
    Defines a function type that returns a boolean value indicating whether the specified string expression ends with the specified substring.
    """

    FLOOR = 9
    """
    Defines a function type that returns the largest integer less than or equal to the specified numeric expression.
    """

    IIF = 10
    """
    Defines a function type that returns the first expression if the specified boolean expression is true, otherwise returns the second expression.
    """

    INDEXOF = 11
    """
    Defines a function type that returns the zero-based index of the first occurrence of the specified substring in the specified string expression.
    """

    ISDATE = 12
    """
    Defines a function type that returns a boolean value indicating whether the specified string expression is a valid date.
    """

    ISINTEGER = 13
    """
    Defines a function type that returns a boolean value indicating whether the specified string expression is a valid integer.
    """

    ISGUID = 14
    """
    Defines a function type that returns a boolean value indicating whether the specified string expression is a valid GUID.
    """

    ISNULL = 15
    """
    Defines a function type that returns a boolean value indicating whether the specified expression is null.
    """

    ISNUMERIC = 16
    """
    Defines a function type that returns a boolean value indicating whether the specified string expression is a valid numeric value.
    """

    LASTINDEXOF = 17
    """
    Defines a function type that returns the zero-based index of the last occurrence of the specified substring in the specified string expression.
    """

    LEN = 18
    """
    Defines a function type that returns the length of the specified string expression.
    """

    LOWER = 19
    """
    Defines a function type that returns the specified string expression converted to lower if self.value ==.
    """

    MAXOF = 20
    """
    Defines a function type that returns the maximum value of the specified expression list.
    """

    MINOF = 21
    """
    Defines a function type that returns the minimum value of the specified expression list.
    """

    NOW = 22
    """
    Defines a function type that returns the current local date and time.
    """

    NTHINDEXOF = 23
    """
    Defines a function type that returns the zero-based index of the nth occurrence of the specified substring in the specified string expression.
    """

    POWER = 24
    """
    Defines a function type that returns the specified numeric expression raised to the specified power.
    """

    REGEXMATCH = 25
    """
    Defines a function type that returns a boolean value indicating whether the specified string expression matches the specified regular expression.
    """

    REGEXVAL = 26
    """
    Defines a function type that returns the first matching value of the specified regular expression in the specified string expression.
    """

    REPLACE = 27
    """
    Defines a function type that returns the specified string expression with the specified substring replaced with the specified replacement string.
    """

    REVERSE = 28
    """
    Defines a function type that returns the specified string expression with the characters in reverse order.
    """

    ROUND = 29
    """
    Defines a function type that returns the specified numeric expression rounded to the specified number of decimal places.
    """

    SPLIT = 30
    """
    Defines a function type that returns the zero-based nth string value from the specified string expression split by the specified delimiter, or null if out of range.
    """

    SQRT = 31
    """
    Defines a function type that returns the square root of the specified numeric expression.
    """

    STARTSWITH = 32
    """
    Defines a function type that returns a boolean value indicating whether the specified string expression starts with the specified substring.
    """

    STRCOUNT = 33
    """
    Defines a function type that returns the number of occurrences of the specified substring in the specified string expression.
    """

    STRCMP = 34
    """
    Defines a function type that returns an integer value representing the comparision of the specified left and right strings.
    Returned value will be -1 if left is less-than right, 1 if left is greater-than right, or 0 if left equals right.
    """

    SUBSTR = 35
    """
    Defines a function type that returns a substring of the specified string expression starting at the specified index and with the specified length.
    """

    TRIM = 36
    """
    Defines a function type that returns the specified string expression with the specified characters trimmed from the beginning and end.
    """

    TRIMLEFT = 37
    """
    Defines a function type that returns the specified string expression with the specified characters trimmed from the beginning.
    """

    TRIMRIGHT = 38
    """
    Defines a function type that returns the specified string expression with the specified characters trimmed from the end.
    """

    UPPER = 39
    """
    Defines a function type that returns the specified string expression converted to upper if self.value ==.
    """

    UTCNOW = 40
    """
    Defines a function type that returns the current date and time in UTC.
    """


class ExpressionOperatorType(IntEnum):
    """
    Defines an enumeration of possible expression operator types.
    """

    MULTIPLY = 0
    """
    Defines an operator type `*` that multiplies the left and right expressions.
    """

    DIVIDE = 1
    """
    Defines an operator type `/` that divides the left and right expressions.
    """

    MODULUS = 2
    """
    Defines an operator type `%` that returns the remainder of the left and right expressions.
    """

    ADD = 3
    """
    Defines an operator type `+` that adds the left and right expressions.
    """

    SUBTRACT = 4
    """
    Defines an operator type `-` that subtracts the right expression from the left expression.
    """

    BITSHIFTLEFT = 5
    """
    Defines an operator type `<<` that shifts the left expression left by the number of bits specified by the right expression.
    """

    BITSHIFTRIGHT = 6
    """
    Defines an operator type `>>` that shifts the left expression right by the number of bits specified by the right expression.
    """

    BITWISEAND = 7
    """
    Defines an operator type `&` that performs a bitwise AND operation on the left and right expressions.
    """

    BITWISEOR = 8
    """
    Defines an operator type `|` that performs a bitwise OR operation on the left and right expressions.
    """

    BITWISEXOR = 9
    """        
    Defines an operator type `^` that performs a bitwise XOR operation on the left and right expressions.
    """

    LESSTHAN = 10
    """
    Defines an operator type `<` that returns a boolean value indicating whether the left expression is less than the right expression.
    """

    LESSTHANOREQUAL = 11
    """
    Defines an operator type `<=` that returns a boolean value indicating whether the left expression is less than or equal to the right expression.
    """

    GREATERTHAN = 12
    """
    Defines an operator type `>` that returns a boolean value indicating whether the left expression is greater than the right expression.
    """

    GREATERTHANOREQUAL = 13
    """
    Defines an operator type `>=` that returns a boolean value indicating whether the left expression is greater than or equal to the right expression.
    """

    EQUAL = 14
    """
    Defines an operator type `=` or `==` that returns a boolean value indicating whether the left expression is equal to the right expression.
    """

    EQUALEXACTMATCH = 15
    """
    Defines an operator type `===` that returns a boolean value indicating whether the left expression is equal to the right expression, case-sensitive.
    """

    NOTEQUAL = 16
    """
    Defines an operator type `<>` or `!=` that returns a boolean value indicating whether the left expression is not equal to the right expression.
    """

    NOTEQUALEXACTMATCH = 17
    """
    Defines an operator type `!==` that returns a boolean value indicating whether the left expression is not equal to the right expression, case-sensitive.
    """

    ISNULL = 18
    """
    Defines an operator type `IS NULL` that returns a boolean value indicating whether the left expression is null.
    """

    ISNOTNULL = 19
    """
    Defines an operator type `IS NOT NULL` that returns a boolean value indicating whether the left expression is not null.
    """

    LIKE = 20
    """
    Defines an operator type `LIKE` that returns a boolean value indicating whether the left expression matches the right expression patten.
    """

    LIKEEXACTMATCH = 21
    """
    Defines an operator type `LIKE BINARY` or `LIKE ===` that returns a boolean value indicating whether the left expression matches the right expression patten, case-sensitive.
    """

    NOTLIKE = 22
    """
    Defines an operator type `NOT LIKE` that returns a boolean value indicating whether the left expression does not match the right expression patten.
    """

    NOTLIKEEXACTMATCH = 23
    """
    Defines an operator type `NOT LIKE BINARY` or `NOT LIKE ===` that returns a boolean value indicating whether the left expression does not match the right expression patten, case-sensitive.
    """

    AND = 24
    """
    Defines an operator type `AND` that returns a boolean value indicating whether the left expression and the right expression are both true.
    """

    OR = 25
    """
    Defines an operator type `OR` that returns a boolean value indicating whether the left expression or the right expression is true.
    """

    def __str__(self):
        if self.value == ExpressionOperatorType.MULTIPLY:
            return "*"
        if self.value == ExpressionOperatorType.DIVIDE:
            return "/"
        if self.value == ExpressionOperatorType.MODULUS:
            return "%"
        if self.value == ExpressionOperatorType.ADD:
            return "+"
        if self.value == ExpressionOperatorType.SUBTRACT:
            return "-"
        if self.value == ExpressionOperatorType.BITSHIFTLEFT:
            return "<<"
        if self.value == ExpressionOperatorType.BITSHIFTRIGHT:
            return ">>"
        if self.value == ExpressionOperatorType.BITWISEAND:
            return "&"
        if self.value == ExpressionOperatorType.BITWISEOR:
            return "|"
        if self.value == ExpressionOperatorType.BITWISEXOR:
            return "^"
        if self.value == ExpressionOperatorType.LESSTHAN:
            return "<"
        if self.value == ExpressionOperatorType.LESSTHANOREQUAL:
            return "<="
        if self.value == ExpressionOperatorType.GREATERTHAN:
            return ">"
        if self.value == ExpressionOperatorType.GREATERTHANOREQUAL:
            return ">="
        if self.value == ExpressionOperatorType.EQUAL:
            return "="
        if self.value == ExpressionOperatorType.EQUALEXACTMATCH:
            return "==="
        if self.value == ExpressionOperatorType.NOTEQUAL:
            return "<>"
        if self.value == ExpressionOperatorType.NOTEQUALEXACTMATCH:
            return "!=="
        if self.value == ExpressionOperatorType.ISNULL:
            return "IS NULL"
        if self.value == ExpressionOperatorType.ISNOTNULL:
            return "IS NOT NULL"
        if self.value == ExpressionOperatorType.LIKE:
            return "LIKE"
        if self.value == ExpressionOperatorType.LIKEEXACTMATCH:
            return "LIKE BINARY"
        if self.value == ExpressionOperatorType.NOTLIKE:
            return "NOT LIKE"
        if self.value == ExpressionOperatorType.NOTLIKEEXACTMATCH:
            return "NOT LIKE BINARY"
        if self.value == ExpressionOperatorType.AND:
            return "AND"
        if self.value == ExpressionOperatorType.OR:
            return "OR"


class TimeInterval(IntEnum):
    """
    Defines enumeration of possible DateTime intervals.
    """

    YEAR = 0
    """
    Represents the year part of a DateTime expression.
    """

    MONTH = 1
    """
    Represents the month part (1-12) of a DateTime expression.
    """
    DAYOFYEAR = 2
    """
    Represents the day of the year (1-366) of a DateTime expression.
    """

    DAY = 3
    """
    Represents the day part (1-31) of a DateTime expression.
    """

    WEEK = 4
    """
    Represents the week part (1-53) of a DateTime expression.
    """

    WEEKDAY = 5
    """
    Represents the weekday part (0-6) of a DateTime expression.
    """

    HOUR = 6
    """
    Represents the hour part (0-23) of a DateTime expression.
    """

    MINUTE = 7
    """
    Represents the minute part (0-59) of a DateTime expression.
    """

    SECOND = 8
    """
    Represents the second part (0-59) of a DateTime expression.
    """

    MILLISECOND = 9
    """
    Represents the millisecond part (0-999) of a DateTime expression.
    """

    @classmethod
    def parse(cls, name: str) -> Optional["TimeInterval"]:
        return getattr(cls, name.upper(), None)


# Operation Value Type Selectors


# sourcery skip
def derive_operationvaluetype(operationtype: ExpressionOperatorType, leftvaluetype: ExpressionValueType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:
    if operationtype in [
            ExpressionOperatorType.MULTIPLY,
            ExpressionOperatorType.DIVIDE,
            ExpressionOperatorType.ADD,
            ExpressionOperatorType.SUBTRACT]:
        return derive_arithmetic_operationvaluetype(operationtype, leftvaluetype, rightvaluetype)

    if operationtype in [
            ExpressionOperatorType.MODULUS,
            ExpressionOperatorType.BITWISEAND,
            ExpressionOperatorType.BITWISEOR,
            ExpressionOperatorType.BITWISEXOR]:
        return derive_integer_operationvaluetype(operationtype, leftvaluetype, rightvaluetype)

    if operationtype in [
            ExpressionOperatorType.LESSTHAN,
            ExpressionOperatorType.LESSTHANOREQUAL,
            ExpressionOperatorType.GREATERTHAN,
            ExpressionOperatorType.GREATERTHANOREQUAL,
            ExpressionOperatorType.EQUAL,
            ExpressionOperatorType.EQUALEXACTMATCH,
            ExpressionOperatorType.NOTEQUAL,
            ExpressionOperatorType.NOTEQUALEXACTMATCH]:
        return derive_comparison_operationvaluetype(operationtype, leftvaluetype, rightvaluetype)

    if operationtype in [
            ExpressionOperatorType.AND,
            ExpressionOperatorType.OR]:
        return derive_boolean_operationvaluetype(operationtype, leftvaluetype, rightvaluetype)

    return leftvaluetype, None


# sourcery skip
def derive_arithmetic_operationvaluetype(operationtype: ExpressionOperatorType, leftvaluetype: ExpressionValueType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:
    if leftvaluetype == ExpressionValueType.BOOLEAN:
        return derive_arithmetic_operationvaluetype_fromboolean(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.INT32:
        return derive_arithmetic_operationvaluetype_fromint32(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.INT64:
        return derive_arithmetic_operationvaluetype_fromint64(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.DECIMAL:
        return derive_arithmetic_operationvaluetype_fromdecimal(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.DOUBLE:
        return derive_arithmetic_operationvaluetype_fromdouble(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.STRING and operationtype == ExpressionOperatorType.ADD:
        return ExpressionValueType.STRING, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"{normalize_enumname(leftvaluetype)}\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_arithmetic_operationvaluetype_fromboolean(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype == ExpressionValueType.BOOLEAN:
        return ExpressionValueType.BOOLEAN, None
    if rightvaluetype == ExpressionValueType.INT32:
        return ExpressionValueType.INT32, None
    if rightvaluetype == ExpressionValueType.INT64:
        return ExpressionValueType.INT64, None
    if rightvaluetype == ExpressionValueType.DECIMAL:
        return ExpressionValueType.DECIMAL, None
    if rightvaluetype == ExpressionValueType.DOUBLE:
        return ExpressionValueType.DOUBLE, None
    if rightvaluetype == ExpressionValueType.STRING and operationtype == ExpressionOperatorType.ADD:
        return ExpressionValueType.STRING, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Boolean\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_arithmetic_operationvaluetype_fromint32(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32]:
        return ExpressionValueType.INT32, None
    if rightvaluetype == ExpressionValueType.INT64:
        return ExpressionValueType.INT64, None
    if rightvaluetype == ExpressionValueType.DECIMAL:
        return ExpressionValueType.DECIMAL, None
    if rightvaluetype == ExpressionValueType.DOUBLE:
        return ExpressionValueType.DOUBLE, None
    if rightvaluetype == ExpressionValueType.STRING and operationtype == ExpressionOperatorType.ADD:
        return ExpressionValueType.STRING, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Int32\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_arithmetic_operationvaluetype_fromint64(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32, ExpressionValueType.INT64]:
        return ExpressionValueType.INT64, None
    if rightvaluetype == ExpressionValueType.DECIMAL:
        return ExpressionValueType.DECIMAL, None
    if rightvaluetype == ExpressionValueType.DOUBLE:
        return ExpressionValueType.DOUBLE, None
    if rightvaluetype == ExpressionValueType.STRING and operationtype == ExpressionOperatorType.ADD:
        return ExpressionValueType.STRING, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Int64\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_arithmetic_operationvaluetype_fromdecimal(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32, ExpressionValueType.INT64, ExpressionValueType.DECIMAL]:
        return ExpressionValueType.DECIMAL, None
    if rightvaluetype == ExpressionValueType.DOUBLE:
        return ExpressionValueType.DOUBLE, None
    if rightvaluetype == ExpressionValueType.STRING and operationtype == ExpressionOperatorType.ADD:
        return ExpressionValueType.STRING, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Decimal\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_arithmetic_operationvaluetype_fromdouble(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32, ExpressionValueType.INT64, ExpressionValueType.DECIMAL, ExpressionValueType.DOUBLE]:
        return ExpressionValueType.DOUBLE, None
    if rightvaluetype == ExpressionValueType.STRING and operationtype == ExpressionOperatorType.ADD:
        return ExpressionValueType.STRING, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Double\" and \"{normalize_enumname(rightvaluetype)}\"")


# sourcery skip
def derive_integer_operationvaluetype(operationtype: ExpressionOperatorType, leftvaluetype: ExpressionValueType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:
    if leftvaluetype == ExpressionValueType.BOOLEAN:
        return derive_integer_operationvaluetype_fromboolean(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.INT32:
        return derive_integer_operationvaluetype_fromint32(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.INT64:
        return derive_integer_operationvaluetype_fromint64(operationtype, rightvaluetype)

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"{normalize_enumname(leftvaluetype)}\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_integer_operationvaluetype_fromboolean(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype == ExpressionValueType.BOOLEAN:
        return ExpressionValueType.BOOLEAN, None
    if rightvaluetype == ExpressionValueType.INT32:
        return ExpressionValueType.INT32, None
    if rightvaluetype == ExpressionValueType.INT64:
        return ExpressionValueType.INT64, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Boolean\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_integer_operationvaluetype_fromint32(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32]:
        return ExpressionValueType.INT32, None
    if rightvaluetype == ExpressionValueType.INT64:
        return ExpressionValueType.INT64, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Int32\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_integer_operationvaluetype_fromint64(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32, ExpressionValueType.INT64]:
        return ExpressionValueType.INT64, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Int64\" and \"{normalize_enumname(rightvaluetype)}\"")


# sourcery skip
def derive_comparison_operationvaluetype(operationtype: ExpressionOperatorType, leftvaluetype: ExpressionValueType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:
    if leftvaluetype == ExpressionValueType.BOOLEAN:
        return derive_comparison_operationvaluetype_fromboolean(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.INT32:
        return derive_comparison_operationvaluetype_fromint32(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.INT64:
        return derive_comparison_operationvaluetype_fromint64(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.DECIMAL:
        return derive_comparison_operationvaluetype_fromdecimal(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.DOUBLE:
        return derive_comparison_operationvaluetype_fromdouble(operationtype, rightvaluetype)
    if leftvaluetype == ExpressionValueType.STRING:
        return leftvaluetype, None
    if leftvaluetype == ExpressionValueType.GUID:
        return derive_comparison_operationvaluetype_fromguid(operationtype, rightvaluetype)

    return derive_comparison_operationvaluetype_fromdatetime(operationtype, rightvaluetype)


def derive_comparison_operationvaluetype_fromboolean(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.STRING]:
        return ExpressionValueType.BOOLEAN, None
    if rightvaluetype == ExpressionValueType.INT32:
        return ExpressionValueType.INT32, None
    if rightvaluetype == ExpressionValueType.INT64:
        return ExpressionValueType.INT64, None
    if rightvaluetype == ExpressionValueType.DECIMAL:
        return ExpressionValueType.DECIMAL, None
    if rightvaluetype == ExpressionValueType.DOUBLE:
        return ExpressionValueType.DOUBLE, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Boolean\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_comparison_operationvaluetype_fromint32(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32]:
        return ExpressionValueType.INT32, None
    if rightvaluetype == ExpressionValueType.INT64:
        return ExpressionValueType.INT64, None
    if rightvaluetype in [ExpressionValueType.STRING, ExpressionValueType.DECIMAL]:
        return ExpressionValueType.DECIMAL, None
    if rightvaluetype == ExpressionValueType.DOUBLE:
        return ExpressionValueType.DOUBLE, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Int32\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_comparison_operationvaluetype_fromint64(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32, ExpressionValueType.INT64]:
        return ExpressionValueType.INT64, None
    if rightvaluetype in [ExpressionValueType.STRING, ExpressionValueType.DECIMAL]:
        return ExpressionValueType.DECIMAL, None
    if rightvaluetype == ExpressionValueType.DOUBLE:
        return ExpressionValueType.DOUBLE, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Int64\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_comparison_operationvaluetype_fromdecimal(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32, ExpressionValueType.INT64, ExpressionValueType.DECIMAL, ExpressionValueType.STRING]:
        return ExpressionValueType.DECIMAL, None
    if rightvaluetype == ExpressionValueType.DOUBLE:
        return ExpressionValueType.DOUBLE, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Decimal\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_comparison_operationvaluetype_fromdouble(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.BOOLEAN, ExpressionValueType.INT32, ExpressionValueType.INT64, ExpressionValueType.DECIMAL, ExpressionValueType.DOUBLE, ExpressionValueType.STRING]:
        return ExpressionValueType.DOUBLE, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Double\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_comparison_operationvaluetype_fromguid(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.GUID, ExpressionValueType.STRING]:
        return ExpressionValueType.GUID, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"Guid\" and \"{normalize_enumname(rightvaluetype)}\"")


def derive_comparison_operationvaluetype_fromdatetime(operationtype: ExpressionOperatorType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:  # sourcery skip
    if rightvaluetype in [ExpressionValueType.DATETIME, ExpressionValueType.STRING]:
        return ExpressionValueType.DATETIME, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"DateTime\" and \"{normalize_enumname(rightvaluetype)}\"")


# sourcery skip
def derive_boolean_operationvaluetype(operationtype: ExpressionOperatorType, leftvaluetype: ExpressionValueType, rightvaluetype: ExpressionValueType) -> Tuple[ExpressionValueType, Optional[Exception]]:
    if leftvaluetype == ExpressionValueType.BOOLEAN and rightvaluetype == ExpressionValueType.BOOLEAN:
        return ExpressionValueType.BOOLEAN, None

    return ExpressionValueType.UNDEFINED, EvaluateError(f"cannot perform \"{normalize_enumname(operationtype)}\" operation on \"{normalize_enumname(leftvaluetype)}\" and \"{normalize_enumname(rightvaluetype)}\"")
