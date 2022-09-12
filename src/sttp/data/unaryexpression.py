# ******************************************************************************************************
#  unaryexpression.py - Gbtc
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

from gsf import override, normalize_enumname
from .expression import Expression
from .valueexpression import ValueExpression
from .constants import ExpressionType, ExpressionUnaryType, ExpressionValueType
from .errors import EvaluateError
from decimal import Decimal
from typing import Optional, Tuple
import numpy as np

class UnaryExpression(Expression):
    """
    Represents a unary expression.
    """

    def __init__(self, unarytype: ExpressionUnaryType, value: Expression):
        self._unarytype = unarytype
        self._value = value

    @override
    @property
    def expressiontype(self) -> ExpressionType:
        """
        Gets the type of this `UnaryExpression`.
        """

        return ExpressionType.UNARY

    @property
    def unarytype(self) -> ExpressionUnaryType:
        """
        Gets the type of unary operation of this `UnaryExpression`.
        """

        return self._unarytype

    @property
    def value(self) -> Expression:
        """
        Gets the value of this `UnaryExpression`.
        """

        return self._value

    def applyto_bool(self, value: bool) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        """
        Applies the `UnaryExpression` prefix to a boolean and returns `ValueExpression` of result, if possible.
        """

        if self._unarytype == ExpressionUnaryType.NOT:
            return ValueExpression(ExpressionValueType.BOOLEAN, not value), None

        return None, EvaluateError(f"cannot apply unary type \"{normalize_enumname(self._unarytype)}\" to \"Boolean\" value")

    def applyto_int32(self, value: np.int32) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        """
        Applies the `UnaryExpression` prefix to a 32-bit integer value and returns `ValueExpression` of result.
        """

        if self._unarytype == ExpressionUnaryType.PLUS:
            return ValueExpression(ExpressionValueType.INT32, +value), None

        if self._unarytype == ExpressionUnaryType.MINUS:
            return ValueExpression(ExpressionValueType.INT32, -value), None

        return ValueExpression(ExpressionValueType.INT32, ~value), None

    def applyto_int64(self, value: np.int64) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        """
        Applies the `UnaryExpression` prefix to a 64-bit integer value and returns `ValueExpression` of result.
        """

        if self._unarytype == ExpressionUnaryType.PLUS:
            return ValueExpression(ExpressionValueType.INT64, +value), None

        if self._unarytype == ExpressionUnaryType.MINUS:
            return ValueExpression(ExpressionValueType.INT64, -value), None

        return ValueExpression(ExpressionValueType.INT64, ~value), None

    def applyto_decimal(self, value: Decimal) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        """
        Applies the `UnaryExpression` prefix to a `Decimal` value and returns `ValueExpression` of result, if possible.
        """

        if self._unarytype == ExpressionUnaryType.PLUS:
            return ValueExpression(ExpressionValueType.DECIMAL, +value), None

        if self._unarytype == ExpressionUnaryType.MINUS:
            return ValueExpression(ExpressionValueType.DECIMAL, -value), None

        return None, EvaluateError(f"cannot apply unary type \"~\" to \"Decimal\" value")

    def applyto_double(self, value: np.float64) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        """
        Applies the `UnaryExpression` prefix to a `Double` value and returns `ValueExpression` of result, if possible.
        """

        if self._unarytype == ExpressionUnaryType.PLUS:
            return ValueExpression(ExpressionValueType.DOUBLE, +value), None

        if self._unarytype == ExpressionUnaryType.MINUS:
            return ValueExpression(ExpressionValueType.DOUBLE, -value), None

        return None, EvaluateError(f"cannot apply unary type \"~\" to \"Double\" value")
