# ******************************************************************************************************
#  inlistexpression.py - Gbtc
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

from gsf import override
from .expression import Expression
from .constants import ExpressionType


class InListExpression(Expression):
    """
    Represents an in-list expression.
    """

    def __init__(self, value: Expression, arguments: list[Expression], has_notkeyword: bool, exactmatch: bool):
        self._value = value
        self._arguments = arguments
        self._has_notkeyword = has_notkeyword
        self._exactmatch = exactmatch

    @override
    @property
    def expressiontype(self) -> ExpressionType:
        """
        Gets the type of this `InListExpression`.
        """

        return ExpressionType.INLIST

    @property
    def value(self) -> Expression:
        """
        Gets the value of this `InListExpression`.
        """

        return self._value

    @property
    def arguments(self) -> list[Expression]:
        """
        Gets the arguments of this `FunctionExpression`.
        """

        return self._arguments

    @property
    def has_notkeyword(self) -> bool:
        """
        Gets a flag that indicates whether this `InListExpression` has a `NOT` keyword.
        """

        return self._has_notkeyword

    @property
    def exactmatch(self) -> bool:
        """
        Gets a flag that indicates whether this `InListExpression` is an exact match. i.e., has the `BINARY` or `===` keyword.
        """

        return self._exactmatch
