# ******************************************************************************************************
#  functionexpression.py - Gbtc
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
from .constants import ExpressionFunctionType, ExpressionType


class FunctionExpression(Expression):
    """
    Represents a function expression.
    """

    def __init__(self, functiontype: ExpressionFunctionType, arguments: list[Expression]):
        self._functiontype = functiontype
        self._arguments = arguments

    @override
    @property
    def expressiontype(self) -> ExpressionType:
        """
        Gets the type of this `FunctionExpression`.
        """

        return ExpressionType.FUNCTION

    @property
    def functiontype(self) -> ExpressionFunctionType:
        """
        Gets the type of function of this `FunctionExpression`.
        """

        return self._functiontype

    @property
    def arguments(self) -> list[Expression]:
        """
        Gets the arguments of this `FunctionExpression`.
        """

        return self._arguments
