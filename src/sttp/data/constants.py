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

from enum import IntEnum


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

ZEROEXPRESSIONVALUETYPE = 0
"""
Defines the zero value for the `ExpressionValueType` enumeration.
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
