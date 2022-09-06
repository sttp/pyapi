# ******************************************************************************************************
#  operatorexpression.py - Gbtc
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
#  09/04/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from gsf import override
from .expression import Expression
from .constants import ExpressionType, ExpressionOperatorType


class OperatorExpression(Expression):
    """
    Represents an operator expression.
    """

    def __init__(self, operatortype: ExpressionOperatorType, leftvalue: Expression, rightvalue: Expression):
        self._operatortype = operatortype
        self._leftvalue = leftvalue
        self._rightvalue = rightvalue

    @override
    @property
    def expressiontype(self) -> ExpressionType:
        """
        Gets the type of this `OperatorExpression`.
        """

        return ExpressionType.OPERATOR

    @property
    def operatortype(self) -> ExpressionOperatorType:
        """
        Gets the operator type for this `OperatorExpression`.
        """

        return self._operatortype

    @property
    def leftvalue(self) -> Expression:
        """
        Gets the left value expression for this `OperatorExpression`.
        """

        return self._leftvalue

    @property
    def rightvalue(self) -> Expression:
        """
        Gets the right value expression for this `OperatorExpression`.
        """

        return self._rightvalue
