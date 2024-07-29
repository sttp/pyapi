# ******************************************************************************************************
#  expressiontree.py - Gbtc
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

from gsf import Empty, Limits, normalize_enumname
from .datarow import DataRow
from .datatable import DataTable
from .datatype import DataType
from .orderbyterm import OrderByTerm
from .expression import Expression
from .valueexpression import ValueExpression, \
    TRUEVALUE, FALSEVALUE, EMPTYSTRINGVALUE, \
    NULLBOOLVALUE, NULLSTRINGVALUE, \
    NULLINT32VALUE, NULLDATETIMEVALUE
from .unaryexpression import UnaryExpression
from .columnexpression import ColumnExpression
from .inlistexpression import InListExpression
from .functionexpression import FunctionExpression
from .operatorexpression import OperatorExpression
from .constants import ExpressionType, \
    ExpressionValueType, ExpressionFunctionType, \
    ExpressionOperatorType, TimeInterval, \
    is_integertype, is_numerictype, \
    derive_operationvaluetype, \
    derive_comparison_operationvaluetype, \
    derive_arithmetic_operationvaluetype
from .errors import EvaluateError
from typing import Callable, List, Optional, Tuple, Union
from functools import cmp_to_key
from datetime import datetime, timezone
from dateutil import parser
from dateutil.relativedelta import relativedelta
from uuid import UUID
import numpy as np
import math
import re
import sys

INTSIZE = 64 if sys.maxsize > 2**32 else 32

UMAXINT64 = np.uint64(Limits.MAXINT64)


def _find_nthindex(source: str, test: str, index: int) -> int:
    result = 0

    for _ in range(index + 1):
        location = source.find(test, result)

        if location == -1:
            result = 0
            break

        result = location + 1

    return result - 1


def _split_nthindex(source: str, test: str, index: int) -> Tuple[int, int, bool]:
    firstindex = _find_nthindex(source, test, index - 1)
    secondindex = _find_nthindex(source, test, index)

    if firstindex <= 0:
        return (-1, -1, False) if secondindex <= 0 else (0, secondindex, True)

    if secondindex <= 0:
        return firstindex + len(test), len(source), True

    return firstindex + len(test), secondindex, True


class ExpressionTree:
    """
    Represents a tree of expressions for evaluation.
    """

    def __init__(self):
        """
        Creates a new expression tree.
        """

        self._currentrow: Optional[DataRow] = None

        self.tablename: str = Empty.STRING
        """
        Defines the associated table name parsed from "FILTER" statement, if any.
        """

        self.toplimit: int = -1
        """
        Defined the parsed value associated with the "TOP" keyword, if any.
        """

        self.orderbyterms: List[OrderByTerm] = []
        """
        Represents the order by elements parsed from the "ORDER BY" keyword, if any.
        """

        self.root: Optional[Expression] = None
        """
        Defines the starting `Expression` for evaluation of the expression tree, or None if there
        is not one. This is the root expression of the `ExpressionTree`. Value is automatically
        managed by the `FilterExpressionParser`.
        """

    def select(self, table: DataTable) -> Tuple[List[DataRow], Optional[Exception]]:
        """
        Returns the rows matching the the `ExpressionTree`.

        The expression tree result type is expected to be a boolean for this filtering operation.
        This works like the "WHERE" clause of a SQL expression. Any "TOP" limit and "ORDER BY"
        sorting clauses found in filter expressions will be respected. An error will be returned
        if the table parameter is None, the expression tree does not yield a boolean value or any
        row expresssion evaluation fails.
        """

        def predicate(result_expression: ValueExpression) -> Tuple[bool, Optional[Exception]]:
            # Final expression should have a boolean data type (operates as a WHERE clause)
            if result_expression.valuetype != ExpressionValueType.BOOLEAN:
                return False, EvaluateError(f"cannot execute select operation, final expression tree evaluation result must be a boolean value, not \"{normalize_enumname(result_expression.valuetype)}\"")

            # If final result is Null, i.e., has no value due to Null propagation, treat result as False
            return result_expression._booleanvalue(), None

        return self.selectwhere(table, predicate, True, True)

    def selectwhere(self, table: DataTable, predicate: Callable[[ValueExpression], Tuple[bool, Optional[Exception]]], applylimit: bool, applysort: bool) -> Tuple[List[DataRow], Optional[Exception]]:
        """
        Returns each table row evaluated from the `ExpressionTree` that matches the specified predicate.

        The `applylimit` and `applysort` parameters determine if any encountered "TOP" limit and "ORDER BY"
        sorting clauses will be respected. An error will be returned if the table parameter is nil or any
        row expresssion evaluation fails.
        """

        if table is None:
            return [], TypeError("cannot execute select operation, table parameter cannot be None")

        matchedrows: List[DataRow] = []

        # Find rows that match the expression tree
        for row in table:
            if applylimit and self.toplimit > -1 and len(matchedrows) >= self.toplimit:
                break

            if row is None:
                continue

            result_expression, err = self.evaluate(row)

            if err is not None:
                return [], err

            # If value result for row matches predicate expression, add it to matching rows
            result, err = predicate(result_expression)

            if err is not None:
                return [], err

            if result:
                matchedrows.append(row)

        # Sort matching rows if requested
        if applysort and matchedrows and self.orderbyterms:
            def compare_rows(leftmatchedrow: DataRow, rightmatchedrow: DataRow) -> int:
                for orderbyterm in self.orderbyterms:
                    if orderbyterm.ascending:
                        leftrow = leftmatchedrow
                        rightrow = rightmatchedrow
                    else:
                        leftrow = rightmatchedrow
                        rightrow = leftmatchedrow

                    result, err = DataRow.compare_datarowcolumns(leftrow, rightrow, orderbyterm.column.index, orderbyterm.extactmatch)

                    if err is not None:
                        raise EvaluateError(f"cannot execute select operation, failed while comparing rows for sorting: {err}")

                    # If last compare result was equal, continue sort based on next order-by term
                    if result != 0:
                        return result

                return 0

            matchedrows.sort(key=cmp_to_key(compare_rows))

        return matchedrows, None

    def evaluate(self, row: Optional[DataRow] = None) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        """
        Traverses the the `ExpressionTree` for the provided data row to produce a `ValueExpression`.
        Root expression should be assigned before calling `evaluate`; otherwise result will be a Null
        `ValueExpression`. The `datarow` parameter can be None if there are no columns referenced in
        expression tree. An error will be returned if the expresssion evaluation fails.
        """

        self._currentrow = row
        return self._evaluate(self.root)

    def _evaluate(self, expression: Optional[Union[Expression, ValueExpression]], target_valuetype: ExpressionValueType = ExpressionValueType.BOOLEAN) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if expression is None:
            return ValueExpression.nullvalue(target_valuetype), None

        expressiontype = expression.expressiontype

        if expressiontype == ExpressionType.VALUE:
            # Change Undefined NULL values to Nullable of target type
            if expression.valuetype == ExpressionValueType.UNDEFINED:
                return ValueExpression.nullvalue(target_valuetype), None

            return expression, None
        if expressiontype == ExpressionType.UNARY:
            return self._evaluate_unary(expression)
        if expressiontype == ExpressionType.COLUMN:
            return self._evaluate_column(expression)
        if expressiontype == ExpressionType.INLIST:
            return self._evaluate_in_list(expression)
        if expressiontype == ExpressionType.FUNCTION:
            return self._evaluate_function(expression)
        if expressiontype == ExpressionType.OPERATOR:
            return self._evaluate_operator(expression)

        return None, TypeError("unexpected expression type encountered")

    def _evaluate_unary(self, unary_expression: UnaryExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        unary_value, err = self._evaluate(unary_expression.value)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating unary expression value: {err}")

        unary_valuetype = unary_value.valuetype

        # If unary value is Null, result is Null
        if unary_value.is_null():
            return ValueExpression.nullvalue(unary_valuetype), None

        if unary_valuetype == ExpressionValueType.BOOLEAN:
            return unary_expression.applyto_bool(unary_value._booleanvalue())
        if unary_valuetype == ExpressionValueType.INT32:
            return unary_expression.applyto_int32(unary_value._int32value())
        if unary_valuetype == ExpressionValueType.INT64:
            return unary_expression.applyto_int64(unary_value._int64value())
        if unary_valuetype == ExpressionValueType.DECIMAL:
            return unary_expression.applyto_decimal(unary_value._decimalvalue())
        if unary_valuetype == ExpressionValueType.DOUBLE:
            return unary_expression.applyto_double(unary_value._doublevalue())

        return None, EvaluateError(f"cannot apply unary \"{normalize_enumname(unary_expression.unarytype)}\" operator to \"{normalize_enumname(unary_valuetype)}\"")

    def _evaluate_column(self, column_expression: ColumnExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:  # sourcery skip
        if self._currentrow is None:
            return None, TypeError("cannot evaluate column expression, current data row reference is not defined")

        column = column_expression.datacolumn

        if column is None:
            return None, TypeError("cannot evaluate column expression, data column reference is not defined")

        columnindex = column.index
        columndatatype = column.datatype

        if columndatatype == DataType.STRING:
            valuetype = ExpressionValueType.STRING
            value, isnull, err = self._currentrow.stringvalue(columnindex)
        elif columndatatype == DataType.BOOLEAN:
            valuetype = ExpressionValueType.BOOLEAN
            value, isnull, err = self._currentrow.booleanvalue(columnindex)
        elif columndatatype == DataType.DATETIME:
            valuetype = ExpressionValueType.DATETIME
            value, isnull, err = self._currentrow.datetimevalue(columnindex)
        elif columndatatype == DataType.SINGLE:
            valuetype = ExpressionValueType.DOUBLE
            f32, isnull, err = self._currentrow.singlevalue(columnindex)
            value = np.float64(f32)
        elif columndatatype == DataType.DOUBLE:
            valuetype = ExpressionValueType.DOUBLE
            value, isnull, err = self._currentrow.doublevalue(columnindex)
        elif columndatatype == DataType.DECIMAL:
            valuetype = ExpressionValueType.DECIMAL
            value, isnull, err = self._currentrow.decimalvalue(columnindex)
        elif columndatatype == DataType.GUID:
            valuetype = ExpressionValueType.GUID
            value, isnull, err = self._currentrow.guidvalue(columnindex)
        elif columndatatype == DataType.INT8:
            valuetype = ExpressionValueType.INT32
            i8, isnull, err = self._currentrow.int8value(columnindex)
            value = np.int32(i8)
        elif columndatatype == DataType.INT16:
            valuetype = ExpressionValueType.INT32
            i16, isnull, err = self._currentrow.int16value(columnindex)
            value = np.int32(i16)
        elif columndatatype == DataType.INT32:
            valuetype = ExpressionValueType.INT32
            value, isnull, err = self._currentrow.int32value(columnindex)
        elif columndatatype == DataType.INT64:
            valuetype = ExpressionValueType.INT64
            value, isnull, err = self._currentrow.int64value(columnindex)
        elif columndatatype == DataType.UINT8:
            valuetype = ExpressionValueType.INT32
            ui8, isnull, err = self._currentrow.uint8value(columnindex)
            value = np.int32(ui8)
        elif columndatatype == DataType.UINT16:
            valuetype = ExpressionValueType.INT32
            ui16, isnull, err = self._currentrow.uint16value(columnindex)
            value = np.int32(ui16)
        elif columndatatype == DataType.UINT32:
            valuetype = ExpressionValueType.INT64
            ui32, isnull, err = self._currentrow.uint32value(columnindex)
            value = np.int64(ui32)
        elif columndatatype == DataType.UINT64:
            ui64, isnull, err = self._currentrow.uint64value(columnindex)

            if ui64 > UMAXINT64:
                valuetype = ExpressionValueType.DOUBLE
                value = np.float64(ui64)
            else:
                valuetype = ExpressionValueType.INT64
                value = np.int64(ui64)
        else:
            return None, TypeError("unexpected column data type encountered")

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"{column.name}\" column \"{normalize_enumname(columndatatype)}\" value for current row: {err}")

        if isnull:
            return ValueExpression.nullvalue(valuetype), None

        return ValueExpression(valuetype, value), None

    def _evaluate_in_list(self, inlist_expression: InListExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        inlist_value, err = self._evaluate(inlist_expression.value)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IN\" expression source value: {err}")

        # If in-list test value is Null, result is Null
        if inlist_value.is_null():
            return ValueExpression.nullvalue(inlist_value.valuetype), None

        has_notkeyword = inlist_expression.has_notkeyword
        exactmatch = inlist_expression.exactmatch
        arguments = inlist_expression.arguments

        for i in range(len(arguments)):
            argument_value, err = self._evaluate(arguments[i])

            if err is not None:
                return None, EvaluateError(f"failed while evaluating \"IN\" expression argument {i} value: {err}")

            valuetype, err = derive_comparison_operationvaluetype(ExpressionOperatorType.EQUAL, inlist_value.valuetype, argument_value.valuetype)

            if err is not None:
                return None, EvaluateError(f"failed while deriving \"IN\" expression argument {i} comparison operation value type: {err}")

            result, err = self._equal_op(inlist_value, argument_value, valuetype, exactmatch)

            if err is not None:
                return None, EvaluateError(f"failed while comparing \"IN\" expression source value to argument {i} for equality: {err}")

            if result._booleanvalue():
                return FALSEVALUE if has_notkeyword else TRUEVALUE, None

        return TRUEVALUE if has_notkeyword else FALSEVALUE, None

    def _evaluate_function(self, function_expression: FunctionExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        functiontype = function_expression.functiontype
        arguments = function_expression.arguments

        if functiontype == ExpressionFunctionType.ABS:
            return self._evaluate_abs(arguments)
        if functiontype == ExpressionFunctionType.CEILING:
            return self._evaluate_ceiling(arguments)
        if functiontype == ExpressionFunctionType.COALESCE:
            return self._evaluate_coalesce(arguments)
        if functiontype == ExpressionFunctionType.CONVERT:
            return self._evaluate_convert(arguments)
        if functiontype == ExpressionFunctionType.CONTAINS:
            return self._evaluate_contains(arguments)
        if functiontype == ExpressionFunctionType.DATEADD:
            return self._evaluate_dateadd(arguments)
        if functiontype == ExpressionFunctionType.DATEDIFF:
            return self._evaluate_datediff(arguments)
        if functiontype == ExpressionFunctionType.DATEPART:
            return self._evaluate_datepart(arguments)
        if functiontype == ExpressionFunctionType.ENDSWITH:
            return self._evaluate_endswith(arguments)
        if functiontype == ExpressionFunctionType.FLOOR:
            return self._evaluate_floor(arguments)
        if functiontype == ExpressionFunctionType.IIF:
            return self._evaluate_iif(arguments)
        if functiontype == ExpressionFunctionType.INDEXOF:
            return self._evaluate_indexof(arguments)
        if functiontype == ExpressionFunctionType.ISDATE:
            return self._evaluate_is_date(arguments)
        if functiontype == ExpressionFunctionType.ISINTEGER:
            return self._evaluate_is_integer(arguments)
        if functiontype == ExpressionFunctionType.ISGUID:
            return self._evaluate_is_guid(arguments)
        if functiontype == ExpressionFunctionType.ISNULL:
            return self._evaluate_is_null(arguments)
        if functiontype == ExpressionFunctionType.ISNUMERIC:
            return self._evaluate_is_numeric(arguments)
        if functiontype == ExpressionFunctionType.LASTINDEXOF:
            return self._evaluate_lastindexof(arguments)
        if functiontype == ExpressionFunctionType.LEN:
            return self._evaluate_len(arguments)
        if functiontype == ExpressionFunctionType.LOWER:
            return self._evaluate_lower(arguments)
        if functiontype == ExpressionFunctionType.MAXOF:
            return self._evaluate_maxof(arguments)
        if functiontype == ExpressionFunctionType.MINOF:
            return self._evaluate_minof(arguments)
        if functiontype == ExpressionFunctionType.NTHINDEXOF:
            return self._evaluate_nthindexof(arguments)
        if functiontype == ExpressionFunctionType.NOW:
            return self._evaluate_now(arguments)
        if functiontype == ExpressionFunctionType.POWER:
            return self._evaluate_power(arguments)
        if functiontype == ExpressionFunctionType.REGEXMATCH:
            return self._evaluate_regexmatch(arguments)
        if functiontype == ExpressionFunctionType.REGEXVAL:
            return self._evaluate_regexval(arguments)
        if functiontype == ExpressionFunctionType.REPLACE:
            return self._evaluate_replace(arguments)
        if functiontype == ExpressionFunctionType.REVERSE:
            return self._evaluate_reverse(arguments)
        if functiontype == ExpressionFunctionType.ROUND:
            return self._evaluate_round(arguments)
        if functiontype == ExpressionFunctionType.SPLIT:
            return self._evaluate_split(arguments)
        if functiontype == ExpressionFunctionType.SQRT:
            return self._evaluate_sqrt(arguments)
        if functiontype == ExpressionFunctionType.STARTSWITH:
            return self._evaluate_startswith(arguments)
        if functiontype == ExpressionFunctionType.STRCOUNT:
            return self._evaluate_strcount(arguments)
        if functiontype == ExpressionFunctionType.STRCMP:
            return self._evaluate_strcmp(arguments)
        if functiontype == ExpressionFunctionType.SUBSTR:
            return self._evaluate_substr(arguments)
        if functiontype == ExpressionFunctionType.TRIM:
            return self._evaluate_trim(arguments)
        if functiontype == ExpressionFunctionType.TRIMLEFT:
            return self._evaluate_trimleft(arguments)
        if functiontype == ExpressionFunctionType.TRIMRIGHT:
            return self._evaluate_trimright(arguments)
        if functiontype == ExpressionFunctionType.UPPER:
            return self._evaluate_upper(arguments)
        if functiontype == ExpressionFunctionType.UTCNOW:
            return self._evaluate_utcnow(arguments)

        return None, TypeError("unexpected function type encountered")

    def _evaluate_abs(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"Abs\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.DOUBLE)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Abs\" function source value, first argument: {err}")

        return self._abs(sourcevalue)

    def _evaluate_ceiling(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"Ceiling\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.DOUBLE)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Ceiling\" function source value, first argument: {err}")

        return self._ceiling(sourcevalue)

    def _evaluate_coalesce(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2:
            return None, ValueError(f"\"Coalesce\" function expects at least 2 arguments, received {len(arguments)}")

        # Not pre-evaluating Coalesce arguments - arguments will be evaluated only up to first non-null value
        return self._coalesce(arguments)

    def _evaluate_convert(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 2:
            return None, ValueError(f"\"Convert\" function expects 2 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0])

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Convert\" function source value, first argument: {err}")

        targettype, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Convert\" function target type, second argument: {err}")

        return self._convert(sourcevalue, targettype)

    def _evaluate_contains(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2 or len(arguments) > 3:
            return None, ValueError(f"\"Contains\" function expects 2 or 3 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Contains\" function source value, first argument: {err}")

        testvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Contains\" function test value, second argument: {err}")

        if len(arguments) == 2:
            return self._contains(sourcevalue, testvalue, NULLBOOLVALUE)

        ignorecase, err = self._evaluate(arguments[2], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Contains\" function optional ignore case value, third argument: {err}")

        return self._contains(sourcevalue, testvalue, ignorecase)

    def _evaluate_dateadd(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 3:
            return None, ValueError(f"\"DateAdd\" function expects 3 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.DATETIME)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"DateAdd\" function source value, first argument: {err}")

        addvalue, err = self._evaluate(arguments[1], ExpressionValueType.INT32)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"DateAdd\" function add value, second argument: {err}")

        intervaltype, err = self._evaluate(arguments[2], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"DateAdd\" function interval type value, third argument: {err}")

        return self._dateadd(sourcevalue, addvalue, intervaltype)

    def _evaluate_datediff(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 3:
            return None, ValueError(f"\"DateDiff\" function expects 3 arguments, received {len(arguments)}")

        leftvalue, err = self._evaluate(arguments[0], ExpressionValueType.DATETIME)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"DateDiff\" function left value, first argument: {err}")

        rightvalue, err = self._evaluate(arguments[1], ExpressionValueType.DATETIME)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"DateDiff\" function right value, second argument: {err}")

        intervaltype, err = self._evaluate(arguments[2], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"DateDiff\" function interval type value, third argument: {err}")

        return self._datediff(leftvalue, rightvalue, intervaltype)

    def _evaluate_datepart(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 2:
            return None, ValueError(f"\"DatePart\" function expects 2 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.DATETIME)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"DatePart\" function source value, first argument: {err}")

        intervaltype, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"DatePart\" function interval type value, second argument: {err}")

        return self._datepart(sourcevalue, intervaltype)

    def _evaluate_endswith(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2 or len(arguments) > 3:
            return None, ValueError(f"\"EndsWith\" function expects 2 or 3 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"EndsWith\" function source value, first argument: {err}")

        testvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"EndsWith\" function test value, second argument: {err}")

        if len(arguments) == 2:
            return self._endswith(sourcevalue, testvalue, NULLBOOLVALUE)

        ignorecase, err = self._evaluate(arguments[2], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"EndsWith\" function optional ignore case value, third argument: {err}")

        return self._endswith(sourcevalue, testvalue, ignorecase)

    def _evaluate_floor(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"Floor\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.DOUBLE)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Floor\" function source value, first argument: {err}")

        return self._floor(sourcevalue)

    def _evaluate_iif(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 3:
            return None, ValueError(f"\"IIf\" function expects 3 arguments, received {len(arguments)}")

        testvalue, err = self._evaluate(arguments[0], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IIf\" function test value, first argument: {err}")

        # Not pre-evaluating IIf result value arguments - only evaluating desired path
        return self._iif(testvalue, arguments[1], arguments[2])

    def _evaluate_indexof(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2 or len(arguments) > 3:
            return None, ValueError(f"\"IndexOf\" function expects 2 or 3 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IndexOf\" function source value, first argument: {err}")

        testvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IndexOf\" function test value, second argument: {err}")

        if len(arguments) == 2:
            return self._indexof(sourcevalue, testvalue, NULLBOOLVALUE)

        ignorecase, err = self._evaluate(arguments[2], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IndexOf\" function optional ignore case value, third argument: {err}")

        return self._indexof(sourcevalue, testvalue, ignorecase)

    def _evaluate_is_date(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"IsDate\" function expects 1 argument, received {len(arguments)}")

        testvalue, err = self._evaluate(arguments[0])

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IsDate\" function test value, first argument: {err}")

        return self._is_date(testvalue)

    def _evaluate_is_integer(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"IsInteger\" function expects 1 argument, received {len(arguments)}")

        testvalue, err = self._evaluate(arguments[0])

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IsInteger\" function test value, first argument: {err}")

        return self._is_integer(testvalue)

    def _evaluate_is_guid(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"IsGuid\" function expects 1 argument, received {len(arguments)}")

        testvalue, err = self._evaluate(arguments[0])

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IsGuid\" function test value, first argument: {err}")

        return self._is_guid(testvalue)

    def _evaluate_is_null(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 2:
            return None, ValueError(f"\"IsNull\" function expects 2 arguments, received {len(arguments)}")

        testvalue, err = self._evaluate(arguments[0])

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IsNull\" function test value, first argument: {err}")

        defaultvalue, err = self._evaluate(arguments[1])

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IsNull\" function default value, second argument: {err}")

        return self._is_null(testvalue, defaultvalue)

    def _evaluate_is_numeric(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"IsNumeric\" function expects 1 argument, received {len(arguments)}")

        testvalue, err = self._evaluate(arguments[0])

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IsNumeric\" function test value, first argument: {err}")

        return self._is_numeric(testvalue)

    def _evaluate_lastindexof(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2 or len(arguments) > 3:
            return None, ValueError(f"\"LastIndexOf\" function expects 2 or 3 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"LastIndexOf\" function source value, first argument: {err}")

        testvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"LastIndexOf\" function test value, second argument: {err}")

        if len(arguments) == 2:
            return self._lastindexof(sourcevalue, testvalue, NULLBOOLVALUE)

        ignorecase, err = self._evaluate(arguments[2], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"LastIndexOf\" function optional ignore case value, third argument: {err}")

        return self._lastindexof(sourcevalue, testvalue, ignorecase)

    def _evaluate_len(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"Len\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Len\" function source value, first argument: {err}")

        return self._len(sourcevalue)

    def _evaluate_lower(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"Lower\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Lower\" function source value, first argument: {err}")

        return self._lower(sourcevalue)

    def _evaluate_maxof(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2:
            return None, ValueError(f"\"MaxOf\" function expects at least 2 arguments, received {len(arguments)}")

        return self._maxof(arguments)

    def _evaluate_minof(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2:
            return None, ValueError(f"\"MinOf\" function expects at least 2 arguments, received {len(arguments)}")

        return self._minof(arguments)

    def _evaluate_nthindexof(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 3 or len(arguments) > 4:
            return None, ValueError(f"\"NthIndexOf\" function expects 3 or 4 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"NthIndexOf\" function source value, first argument: {err}")

        testvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"NthIndexOf\" function test value, second argument: {err}")

        indexvalue, err = self._evaluate(arguments[2], ExpressionValueType.INT32)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"NthIndexOf\" function index value, third argument: {err}")

        if len(arguments) == 3:
            return self._nthindexof(sourcevalue, testvalue, indexvalue, NULLBOOLVALUE)

        ignorecase, err = self._evaluate(arguments[3], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"NthIndexOf\" function optional ignore case value, fourth argument: {err}")

        return self._nthindexof(sourcevalue, testvalue, indexvalue, ignorecase)

    def _evaluate_now(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if arguments:
            return None, ValueError(f"\"Now\" function expects 0 arguments, received {len(arguments)}")

        return self._now()

    def _evaluate_power(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 2:
            return None, ValueError(f"\"Power\" function expects 2 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.DOUBLE)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Power\" function source value, first argument: {err}")

        exponentvalue, err = self._evaluate(arguments[1], ExpressionValueType.INT32)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Power\" function exponent value, second argument: {err}")

        return self._power(sourcevalue, exponentvalue)

    def _evaluate_regexmatch(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 2:
            return None, ValueError(f"\"RegExMatch\" function expects 2 arguments, received {len(arguments)}")

        regexvalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"RegExMatch\" function expression value, first argument: {err}")

        testvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"RegExMatch\" function test value, second argument: {err}")

        return self._regexmatch(regexvalue, testvalue)

    def _evaluate_regexval(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 2:
            return None, ValueError(f"\"RegExValue\" function expects 2 arguments, received {len(arguments)}")

        regexvalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"RegExValue\" function expression value, first argument: {err}")

        testvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"RegExValue\" function test value, second argument: {err}")

        return self._regexval(regexvalue, testvalue)

    def _evaluate_replace(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 3 or len(arguments) > 4:
            return None, ValueError(f"\"Replace\" function expects 3 or 4 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Replace\" function source value, first argument: {err}")

        testvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Replace\" function test value, second argument: {err}")

        replacevalue, err = self._evaluate(arguments[2], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Replace\" function replace value, third argument: {err}")

        if len(arguments) == 3:
            return self._replace(sourcevalue, testvalue, replacevalue, NULLBOOLVALUE)

        ignorecase, err = self._evaluate(arguments[3], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Replace\" function optional ignore case value, fourth argument: {err}")

        return self._replace(sourcevalue, testvalue, replacevalue, ignorecase)

    def _evaluate_reverse(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"Reverse\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Reverse\" function source value, first argument: {err}")

        return self._reverse(sourcevalue)

    def _evaluate_round(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"Round\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.DOUBLE)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Round\" function source value, first argument: {err}")

        return self._round(sourcevalue)

    def _evaluate_split(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 3 or len(arguments) > 4:
            return None, ValueError(f"\"Split\" function expects 3 or 4 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Split\" function source value, first argument: {err}")

        delimitervalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Split\" function delimiter value, second argument: {err}")

        indexvalue, err = self._evaluate(arguments[2], ExpressionValueType.INT32)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Split\" function index value, third argument: {err}")

        if len(arguments) == 3:
            return self._split(sourcevalue, delimitervalue, indexvalue, NULLBOOLVALUE)

        ignorecase, err = self._evaluate(arguments[3], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Split\" function optional ignore case value, fourth argument: {err}")

        return self._split(sourcevalue, delimitervalue, indexvalue, ignorecase)

    def _evaluate_sqrt(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"Sqrt\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.DOUBLE)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Sqrt\" function source value, first argument: {err}")

        return self._sqrt(sourcevalue)

    def _evaluate_startswith(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2 or len(arguments) > 3:
            return None, ValueError(f"\"StartsWith\" function expects 2 or 3 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"StartsWith\" function source value, first argument: {err}")

        testvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"StartsWith\" function test value, second argument: {err}")

        if len(arguments) == 2:
            return self._startswith(sourcevalue, testvalue, NULLBOOLVALUE)

        ignorecase, err = self._evaluate(arguments[2], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"StartsWith\" function optional ignore case value, third argument: {err}")

        return self._startswith(sourcevalue, testvalue, ignorecase)

    def _evaluate_strcount(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2 or len(arguments) > 3:
            return None, ValueError(f"\"StrCount\" function expects 2 or 3 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"StrCount\" function source value, first argument: {err}")

        testvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"StrCount\" function test value, second argument: {err}")

        if len(arguments) == 2:
            return self._strcount(sourcevalue, testvalue, NULLBOOLVALUE)

        ignorecase, err = self._evaluate(arguments[2], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"StrCount\" function optional ignore case value, third argument: {err}")

        return self._strcount(sourcevalue, testvalue, ignorecase)

    def _evaluate_strcmp(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2 or len(arguments) > 3:
            return None, ValueError(f"\"StrCmp\" function expects 2 or 3 arguments, received {len(arguments)}")

        leftvalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"StrCmp\" function left value, first argument: {err}")

        rightvalue, err = self._evaluate(arguments[1], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"StrCmp\" function right value, second argument: {err}")

        if len(arguments) == 2:
            return self._strcmp(leftvalue, rightvalue, NULLBOOLVALUE)

        ignorecase, err = self._evaluate(arguments[2], ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"StrCmp\" function optional ignore case value, third argument: {err}")

        return self._strcmp(leftvalue, rightvalue, ignorecase)

    def _evaluate_substr(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) < 2 or len(arguments) > 3:
            return None, ValueError(f"\"SubStr\" function expects 2 or 3 arguments, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"SubStr\" function source value, first argument: {err}")

        indexvalue, err = self._evaluate(arguments[1], ExpressionValueType.INT32)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"SubStr\" function index value, second argument: {err}")

        if len(arguments) == 2:
            return self._substr(sourcevalue, indexvalue, NULLINT32VALUE)

        lengthvalue, err = self._evaluate(arguments[2], ExpressionValueType.INT32)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"SubStr\" function optional length value, third argument: {err}")

        return self._substr(sourcevalue, indexvalue, lengthvalue)

    def _evaluate_trim(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"Trim\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Trim\" function source value, first argument: {err}")

        return self._trim(sourcevalue)

    def _evaluate_trimleft(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"TrimLeft\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"TrimLeft\" function source value, first argument: {err}")

        return self._trimleft(sourcevalue)

    def _evaluate_trimright(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"TrimRight\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"TrimRight\" function source value, first argument: {err}")

        return self._trimright(sourcevalue)

    def _evaluate_upper(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if len(arguments) != 1:
            return None, ValueError(f"\"Upper\" function expects 1 argument, received {len(arguments)}")

        sourcevalue, err = self._evaluate(arguments[0], ExpressionValueType.STRING)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Upper\" function source value, first argument: {err}")

        return self._upper(sourcevalue)

    def _evaluate_utcnow(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if arguments:
            return None, ValueError(f"\"UtcNow\" function expects 0 arguments, received {len(arguments)}")

        return self._utcnow()

    def _evaluate_operator(self, operator_expression: OperatorExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        operatortype = operator_expression.operatortype

        leftvalue, err = self._evaluate(operator_expression.leftvalue)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"{normalize_enumname(operatortype)}\" operator left operand: {err}")

        rightvalue, err = self._evaluate(operator_expression.rightvalue)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"{normalize_enumname(operatortype)}\" operator right operand: {err}")

        valuetype, err = derive_operationvaluetype(operatortype, leftvalue.valuetype, rightvalue.valuetype)

        if err is not None:
            return None, EvaluateError(f"failed while deriving \"{normalize_enumname(operatortype)}\" operator value type: {err}")

        if operatortype == ExpressionOperatorType.MULTIPLY:
            return self._multiply_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.DIVIDE:
            return self._divide_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.MODULUS:
            return self._modulus_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.ADD:
            return self._add_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.SUBTRACT:
            return self._subtract_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.BITSHIFTLEFT:
            return self._bitshiftleft_op(leftvalue, rightvalue)
        if operatortype == ExpressionOperatorType.BITSHIFTRIGHT:
            return self._bitshiftright_op(leftvalue, rightvalue)
        if operatortype == ExpressionOperatorType.BITWISEAND:
            return self._bitwiseand_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.BITWISEOR:
            return self._bitwiseor_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.BITWISEXOR:
            return self._bitwisexor_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.LESSTHAN:
            return self._lessthan_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.LESSTHANOREQUAL:
            return self._lessthanorequal_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.GREATERTHAN:
            return self._greaterthan_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.GREATERTHANOREQUAL:
            return self._greaterthanorequal_op(leftvalue, rightvalue, valuetype)
        if operatortype == ExpressionOperatorType.EQUAL:
            return self._equal_op(leftvalue, rightvalue, valuetype, False)
        if operatortype == ExpressionOperatorType.EQUALEXACTMATCH:
            return self._equal_op(leftvalue, rightvalue, valuetype, True)
        if operatortype == ExpressionOperatorType.NOTEQUAL:
            return self._notequal_op(leftvalue, rightvalue, valuetype, False)
        if operatortype == ExpressionOperatorType.NOTEQUALEXACTMATCH:
            return self._notequal_op(leftvalue, rightvalue, valuetype, True)
        if operatortype == ExpressionOperatorType.ISNULL:
            return self._isnull_op(leftvalue), None
        if operatortype == ExpressionOperatorType.ISNOTNULL:
            return self._isnotnull_op(leftvalue), None
        if operatortype == ExpressionOperatorType.LIKE:
            return self._like_op(leftvalue, rightvalue, False)
        if operatortype == ExpressionOperatorType.LIKEEXACTMATCH:
            return self._like_op(leftvalue, rightvalue, True)
        if operatortype == ExpressionOperatorType.NOTLIKE:
            return self._notlike_op(leftvalue, rightvalue, False)
        if operatortype == ExpressionOperatorType.NOTLIKEEXACTMATCH:
            return self._notlike_op(leftvalue, rightvalue, True)
        if operatortype == ExpressionOperatorType.AND:
            return self._and_op(leftvalue, rightvalue)
        if operatortype == ExpressionOperatorType.OR:
            return self._or_op(leftvalue, rightvalue)

        return None, TypeError("unexpected operator type encountered")

    # Filter Expression Function Implementations

    def _abs(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        sourcevaluetype = sourcevalue.valuetype

        if not is_numerictype(sourcevaluetype):
            return None, TypeError("\"Abs\" function source value, first argument, must be a numeric type")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return ValueExpression.nullvalue(sourcevaluetype), None

        if sourcevaluetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, sourcevalue._booleanvalue()), None
        if sourcevaluetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.INT32, abs(sourcevalue._int32value())), None
        if sourcevaluetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.INT64, abs(sourcevalue._int64value())), None
        if sourcevaluetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, abs(sourcevalue._decimalvalue())), None
        if sourcevaluetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, abs(sourcevalue._doublevalue())), None

        return None, TypeError("unexpected expression value type encountered")

    def _ceiling(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        sourcevaluetype = sourcevalue.valuetype

        if not is_numerictype(sourcevaluetype):
            return None, TypeError("\"Ceiling\" function source value, first argument, must be a numeric type")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return ValueExpression.nullvalue(sourcevaluetype), None

        if is_integertype(sourcevaluetype):
            return sourcevalue, None

        if sourcevaluetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, math.ceil(sourcevalue._decimalvalue())), None
        if sourcevaluetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, math.ceil(sourcevalue._doublevalue())), None

        return None, TypeError("unexpected expression value type encountered")

    def _coalesce(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        testvalue, err = self._evaluate(arguments[0])

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"Coalesce\" function argument 0: {err}")

        if not testvalue.is_null():
            return testvalue, None

        for i in range(1, len(arguments)):
            listvalue, err = self._evaluate(arguments[i])

            if err is not None:
                return None, EvaluateError(f"failed while evaluating \"Coalesce\" function argument {i}: {err}")

            if not listvalue.is_null():
                return listvalue, None

        return testvalue, None

    def _convert(self, sourcevalue: ValueExpression, targettype: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if targettype.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Convert\" function target type, second argument, must be a \"String\"")

        if targettype.is_null():
            return None, TypeError("\"Convert\" function target type, second argument, is null")

        targettypename = targettype._stringvalue().upper()

        # Remove any "System." prefix:
        targettypename = targettypename.removeprefix("SYSTEM.")

        targetvaluetype = ExpressionValueType.UNDEFINED
        foundvaluetype = False

        for valuetype in ExpressionValueType:
            if targettypename == valuetype.name:
                targetvaluetype = valuetype
                foundvaluetype = True
                break

        if not foundvaluetype:
            # Handle a few common aliases
            if targettypename == "SINGLE" or targettypename.startswith("FLOAT"):
                targetvaluetype = ExpressionValueType.DOUBLE
                foundvaluetype = True
            elif targettypename == "BOOL":
                targetvaluetype = ExpressionValueType.BOOLEAN
                foundvaluetype = True
            elif targettypename.startswith("INT") or targettypename.startswith("UINT"):
                targetvaluetype = ExpressionValueType.INT64
                foundvaluetype = True
            elif targettypename in ["DATE", "TIME"]:
                targetvaluetype = ExpressionValueType.DATETIME
                foundvaluetype = True
            elif targettypename == "UUID":
                targetvaluetype = ExpressionValueType.GUID
                foundvaluetype = True

        if not foundvaluetype or targetvaluetype == ExpressionValueType.UNDEFINED:
            return None, EvaluateError(f"specified \"Convert\" function target type \"{targettype._stringvalue()}\", second argument, is not supported")

        return sourcevalue.convert(targetvaluetype)

    def _contains(self, sourcevalue: ValueExpression, testvalue: ValueExpression, ignorecase: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Contains\" function source value, first argument, must be a \"String\"")

        if testvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Contains\" function test value, second argument, must be a \"String\"")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLBOOLVALUE, None

        if testvalue.is_null():
            return FALSEVALUE, None

        ignorecase, err = ignorecase.convert(ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"Contains\" function ignore case, third argument, to a \"Boolean\": {err}")

        if ignorecase._booleanvalue():
            return ValueExpression(ExpressionValueType.BOOLEAN, testvalue._stringvalue().upper() in sourcevalue._stringvalue().upper()), None

        return ValueExpression(ExpressionValueType.BOOLEAN, testvalue._stringvalue() in sourcevalue._stringvalue()), None

    def _dateadd(self, sourcevalue: ValueExpression, addvalue: ValueExpression, intervaltype: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype not in [ExpressionValueType.DATETIME, ExpressionValueType.STRING]:
            return None, TypeError("\"DateAdd\" function source value, first argument, must be a \"DateTime\" or a \"String\"")

        if not is_integertype(addvalue.valuetype):
            return None, TypeError("\"DateAdd\" function add value, second argument, must be an integer type")

        if intervaltype.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"DateAdd\" function interval type, third argument, must be a \"String\"")

        if addvalue.is_null():
            return None, TypeError("\"DateAdd\" function add value second argument, is null")

        if intervaltype.is_null():
            return None, TypeError("\"DateAdd\" function interval type, third argument, is null")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLDATETIMEVALUE, None

        # DateTime parameters should support strings as well as literals
        sourcevalue, err = sourcevalue.convert(ExpressionValueType.DATETIME)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"DateAdd\" function source value, first argument, to a \"DateTime\": {err}")

        interval = TimeInterval.parse(intervaltype._stringvalue())

        if interval is None:
            return None, EvaluateError("failed while parsing \"DateAdd\" function interval type, third argument, as a valid time interval")

        value: int = addvalue.integervalue()

        if interval == TimeInterval.YEAR:
            return ValueExpression(ExpressionValueType.DATETIME, sourcevalue._datetimevalue() + relativedelta(years=value)), None
        if interval == TimeInterval.MONTH:
            return ValueExpression(ExpressionValueType.DATETIME, sourcevalue._datetimevalue() + relativedelta(months=value)), None
        if interval in [TimeInterval.DAYOFYEAR, TimeInterval.DAY]:
            return ValueExpression(ExpressionValueType.DATETIME, sourcevalue._datetimevalue() + relativedelta(days=value)), None
        if interval == TimeInterval.WEEKDAY:
            return ValueExpression(ExpressionValueType.DATETIME, sourcevalue._datetimevalue() + relativedelta(weekday=value)), None
        if interval == TimeInterval.WEEK:
            return ValueExpression(ExpressionValueType.DATETIME, sourcevalue._datetimevalue() + relativedelta(weeks=value)), None
        if interval == TimeInterval.HOUR:
            return ValueExpression(ExpressionValueType.DATETIME, sourcevalue._datetimevalue() + relativedelta(hours=value)), None
        if interval == TimeInterval.MINUTE:
            return ValueExpression(ExpressionValueType.DATETIME, sourcevalue._datetimevalue() + relativedelta(minutes=value)), None
        if interval == TimeInterval.SECOND:
            return ValueExpression(ExpressionValueType.DATETIME, sourcevalue._datetimevalue() + relativedelta(seconds=value)), None
        if interval == TimeInterval.MILLISECOND:
            return ValueExpression(ExpressionValueType.DATETIME, sourcevalue._datetimevalue() + relativedelta(microseconds=value * 1000)), None

        return None, TypeError("unexpected time interval encountered")

    def _datediff(self, leftvalue: ValueExpression, rightvalue: ValueExpression, intervaltype: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # sourcery skip
        if leftvalue.valuetype not in [ExpressionValueType.DATETIME, ExpressionValueType.STRING]:
            return None, TypeError("\"DateDiff\" function left value, first argument, must be a \"DateTime\" or a \"String\"")

        if rightvalue.valuetype not in [ExpressionValueType.DATETIME, ExpressionValueType.STRING]:
            return None, TypeError("\"DateDiff\" function right value, second argument, must be a \"DateTime\" or a \"String\"")

        if intervaltype.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"DateDiff\" function interval type, third argument, must be a \"String\"")

        if intervaltype.is_null():
            return None, TypeError("\"DateDiff\" function interval type, third argument, is null")

        # If either test value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return NULLINT32VALUE, None

        # DateTime parameters should support strings as well as literals
        leftvalue, err = leftvalue.convert(ExpressionValueType.DATETIME)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"DateDiff\" function left value, first argument, to a \"DateTime\": {err}")

        rightvalue, err = rightvalue.convert(ExpressionValueType.DATETIME)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"DateDiff\" function right value, second argument, to a \"DateTime\": {err}")

        interval = TimeInterval.parse(intervaltype._stringvalue())

        if interval is None:
            return None, EvaluateError("failed while parsing \"DateDiff\" function interval type, third argument, as a valid time interval")

        if interval in [TimeInterval.YEAR, TimeInterval.MONTH]:
            delta = relativedelta(rightvalue._datetimevalue(), leftvalue._datetimevalue())

            if interval == TimeInterval.YEAR:
                return ValueExpression(ExpressionValueType.INT32, delta.years), None

            return ValueExpression(ExpressionValueType.INT32, delta.months + (12 * delta.years)), None

        delta = rightvalue._datetimevalue() - leftvalue._datetimevalue()

        if interval in [TimeInterval.DAYOFYEAR, TimeInterval.DAY, TimeInterval.WEEKDAY]:
            return ValueExpression(ExpressionValueType.INT32, delta.days), None
        if interval == TimeInterval.WEEK:
            return ValueExpression(ExpressionValueType.INT32, delta.days // 7), None
        if interval == TimeInterval.HOUR:
            return ValueExpression(ExpressionValueType.INT32, round(delta.total_seconds()) // 3600), None
        if interval == TimeInterval.MINUTE:
            return ValueExpression(ExpressionValueType.INT32, round(delta.total_seconds()) // 60), None
        if interval == TimeInterval.SECOND:
            return ValueExpression(ExpressionValueType.INT32, round(delta.total_seconds())), None
        if interval == TimeInterval.MILLISECOND:
            return ValueExpression(ExpressionValueType.INT32, int(delta.total_seconds()) * 1000 + delta.microseconds // 1000), None

        return None, TypeError("unexpected time interval encountered")

    def _datepart(self, sourcevalue: ValueExpression, intervaltype: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype not in [ExpressionValueType.DATETIME, ExpressionValueType.STRING]:
            return None, TypeError("\"DatePart\" function source value, first argument, must be a \"DateTime\" or a \"String\"")

        if intervaltype.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"DatePart\" function interval type, second argument, must be a \"String\"")

        if intervaltype.is_null():
            return None, TypeError("\"DatePart\" function interval type, second argument, is null")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLINT32VALUE, None

        # DateTime parameters should support strings as well as literals
        sourcevalue, err = sourcevalue.convert(ExpressionValueType.DATETIME)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"DatePart\" function source value, first argument, to a \"DateTime\": {err}")

        interval = TimeInterval.parse(intervaltype._stringvalue())

        if interval is None:
            return None, EvaluateError(f"failed while parsing \"DatePart\" function interval type, second argument, as a valid time interval")

        if interval == TimeInterval.YEAR:
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._datetimevalue().year), None
        if interval == TimeInterval.MONTH:
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._datetimevalue().month), None
        if interval == TimeInterval.DAYOFYEAR:
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._datetimevalue().timetuple().tm_yday), None
        if interval == TimeInterval.DAY:
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._datetimevalue().day), None
        if interval == TimeInterval.WEEKDAY:
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._datetimevalue().weekday() + 2), None  # Starts on Monday at zero
        if interval == TimeInterval.WEEK:
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._datetimevalue().isocalendar()[1]), None
        if interval == TimeInterval.HOUR:
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._datetimevalue().hour), None
        if interval == TimeInterval.MINUTE:
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._datetimevalue().minute), None
        if interval == TimeInterval.SECOND:
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._datetimevalue().second), None
        if interval == TimeInterval.MILLISECOND:
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._datetimevalue().microsecond / 1000), None

        return None, TypeError("unexpected time interval encountered")

    def _endswith(self, sourcevalue: ValueExpression, testvalue: ValueExpression, ignorecase: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"EndsWith\" function source value, first argument, must be a \"String\"")

        if testvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"EndsWith\" function test value, second argument, must be a \"String\"")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLBOOLVALUE, None

        if testvalue.is_null():
            return FALSEVALUE, None

        ignorecase, err = ignorecase.convert(ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"EndsWith\" function ignore case, third argument, to a \"Boolean\": {err}")

        if ignorecase._booleanvalue():
            return ValueExpression(ExpressionValueType.BOOLEAN, sourcevalue._stringvalue().upper().endswith(testvalue._stringvalue().upper())), None

        return ValueExpression(ExpressionValueType.BOOLEAN, sourcevalue._stringvalue().endswith(testvalue._stringvalue())), None

    def _floor(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        sourcevaluetype = sourcevalue.valuetype

        if not is_numerictype(sourcevaluetype):
            return None, TypeError("\"Floor\" function source value, first argument, must be a numeric type")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return ValueExpression.nullvalue(sourcevaluetype), None

        if is_integertype(sourcevaluetype):
            return sourcevalue, None

        if sourcevaluetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, math.floor(sourcevalue._decimalvalue())), None
        if sourcevaluetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, math.floor(sourcevalue._doublevalue())), None

        return None, TypeError("unexpected expression value type encountered")

    def _iif(self, testvalue: ValueExpression, truevalue: Expression, falsevalue: Expression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:  # sourcery skip
        if testvalue.valuetype != ExpressionValueType.BOOLEAN:
            return None, TypeError("\"IIf\" function test value, first argument, must be a \"Boolean\"")

        # Null test expression evaluates to false, that is, false value expression
        if testvalue._booleanvalue():
            result, err = self._evaluate(truevalue)

            if err is not None:
                return None, EvaluateError(f"failed while evaluating \"IIf\" function true value, second argument: {err}")

            return result, None

        result, err = self._evaluate(falsevalue)

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"IIf\" function false value, third argument: {err}")

        return result, None

    def _indexof(self, sourcevalue: ValueExpression, testvalue: ValueExpression, ignorecase: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"IndexOf\" function source value, first argument, must be a \"String\"")

        if testvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"IndexOf\" function test value, second argument, must be a \"String\"")

        if testvalue.is_null():
            return None, TypeError("\"IndexOf\" function test value, second argument, is null")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLINT32VALUE, None

        ignorecase, err = ignorecase.convert(ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"IndexOf\" function ignore case, third argument, to a \"Boolean\": {err}")

        if ignorecase._booleanvalue():
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._stringvalue().upper().find(testvalue._stringvalue().upper())), None

        return ValueExpression(ExpressionValueType.INT32, sourcevalue._stringvalue().find(testvalue._stringvalue())), None

    def _is_date(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.is_null():
            return FALSEVALUE, None

        if sourcevalue.valuetype == ExpressionValueType.DATETIME:
            return TRUEVALUE, None

        if sourcevalue.valuetype == ExpressionValueType.STRING:
            try:
                parser.parse(sourcevalue._stringvalue())
                return TRUEVALUE, None
            except Exception:
                return FALSEVALUE, None

        return FALSEVALUE, None

    def _is_integer(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.is_null():
            return FALSEVALUE, None

        if is_integertype(sourcevalue.valuetype):
            return TRUEVALUE, None

        if sourcevalue.valuetype == ExpressionValueType.STRING:
            value = sourcevalue._stringvalue()

            # Shortcut for unsigned ints
            if value.isnumeric():
                return TRUEVALUE, None

            try:
                if "X" in value.upper():
                    int(value, base=16)
                    return TRUEVALUE, None

                int(value)
                return TRUEVALUE, None
            except Exception:
                return FALSEVALUE, None

        return FALSEVALUE, None

    def _is_guid(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.is_null():
            return FALSEVALUE, None

        if sourcevalue.valuetype == ExpressionValueType.GUID:
            return TRUEVALUE, None

        if sourcevalue.valuetype == ExpressionValueType.STRING:
            try:
                UUID(sourcevalue._stringvalue())
                return TRUEVALUE, None
            except Exception:
                return FALSEVALUE, None

        return FALSEVALUE, None

    def _is_null(self, testvalue: ValueExpression, defaultvalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if defaultvalue.is_null():
            return None, TypeError("\"IsNull\" function default value, second argument, is null")

        return (defaultvalue, None) if testvalue.is_null() else (testvalue, None)

    def _is_numeric(self, testvalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if testvalue.is_null():
            return FALSEVALUE, None

        if is_numerictype(testvalue.valuetype):
            return TRUEVALUE, None

        if testvalue.valuetype == ExpressionValueType.STRING:
            try:
                float(testvalue._stringvalue())
                return TRUEVALUE, None
            except Exception:
                return FALSEVALUE, None

        return FALSEVALUE, None

    def _lastindexof(self, sourcevalue: ValueExpression, testvalue: ValueExpression, ignorecase: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"LastIndexOf\" function source value, first argument, must be a \"String\"")

        if testvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"LastIndexOf\" function test value, second argument, must be a \"String\"")

        if testvalue.is_null():
            return None, TypeError("\"LastIndexOf\" function test value, second argument, is null")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLINT32VALUE, None

        ignorecase, err = ignorecase.convert(ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"LastIndexOf\" function ignore case, third argument, to a \"Boolean\": {err}")

        if ignorecase._booleanvalue():
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._stringvalue().upper().rfind(testvalue._stringvalue().upper())), None

        return ValueExpression(ExpressionValueType.INT32, sourcevalue._stringvalue().rfind(testvalue._stringvalue())), None

    def _len(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Len\" function source value, first argument, must be a \"String\"")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLINT32VALUE, None

        return ValueExpression(ExpressionValueType.INT32, len(sourcevalue._stringvalue())), None

    def _lower(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Lower\" function source value, first argument, must be a \"String\"")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLSTRINGVALUE, None

        return ValueExpression(ExpressionValueType.STRING, sourcevalue._stringvalue().lower()), None

    def _maxof(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        testvalue, err = self._evaluate(arguments[0])

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"MaxOf\" function argument 0: {err}")

        for i in range(1, len(arguments)):
            nextvalue, err = self._evaluate(arguments[i])

            if err is not None:
                return None, EvaluateError(f"failed while evaluating \"MaxOf\" function argument {i}: {err}")

            valuetype, err = derive_comparison_operationvaluetype(ExpressionOperatorType.GREATERTHAN, testvalue.valuetype, nextvalue.valuetype)

            if err is not None:
                return None, EvaluateError(f"failed while deriving \"MaxOf\" function greater than comparison operator value type: {err}")

            result, err = self._greaterthan_op(nextvalue, testvalue, valuetype)

            if err is not None:
                return None, EvaluateError(f"failed while evaluating \"MaxOf\" function greater than comparison operator: {err}")

            if result._booleanvalue() or (testvalue.is_null() and not nextvalue.is_null()):
                testvalue = nextvalue

        return testvalue, None

    def _minof(self, arguments: List[Expression]) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        testvalue, err = self._evaluate(arguments[0])

        if err is not None:
            return None, EvaluateError(f"failed while evaluating \"MinOf\" function argument 0: {err}")

        for i in range(1, len(arguments)):
            nextvalue, err = self._evaluate(arguments[i])

            if err is not None:
                return None, EvaluateError(f"failed while evaluating \"MinOf\" function argument {i}: {err}")

            valuetype, err = derive_comparison_operationvaluetype(ExpressionOperatorType.LESSTHAN, testvalue.valuetype, nextvalue.valuetype)

            if err is not None:
                return None, EvaluateError(f"failed while deriving \"MinOf\" function greater than comparison operator value type: {err}")

            result, err = self._lessthan_op(nextvalue, testvalue, valuetype)

            if err is not None:
                return None, EvaluateError(f"failed while evaluating \"MinOf\" function greater than comparison operator: {err}")

            if result._booleanvalue() or (testvalue.is_null() and not nextvalue.is_null()):
                testvalue = nextvalue

        return testvalue, None

    def _now(self) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        return ValueExpression(ExpressionValueType.DATETIME, datetime.now()), None

    def _nthindexof(self, sourcevalue: ValueExpression, testvalue: ValueExpression, indexvalue: ValueExpression, ignorecase: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"NthIndexOf\" function source value, first argument, must be a \"String\"")

        if testvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"NthIndexOf\" function test value, second argument, must be a \"String\"")

        if testvalue.is_null():
            return None, TypeError("\"NthIndexOf\" function test value, second argument, is null")

        if indexvalue.is_null():
            return None, TypeError("\"NthIndexOf\" function index value, third argument, is null")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLINT32VALUE, None

        ignorecase, err = ignorecase.convert(ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"NthIndexOf\" function ignore case, fourth argument, to a \"Boolean\": {err}")

        if ignorecase._booleanvalue():
            source = sourcevalue._stringvalue().upper()
            test = testvalue._stringvalue().upper()
        else:
            source = sourcevalue._stringvalue()
            test = testvalue._stringvalue()

        return ValueExpression(ExpressionValueType.INT32, ExpressionTree._find_nthindex(source, test, indexvalue.integervalue(-1))), None

    def _power(self, sourcevalue: ValueExpression, exponentvalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if not is_numerictype(sourcevalue.valuetype):
            return None, TypeError("\"Power\" function source value, first argument, must be a numeric type")

        if not is_numerictype(exponentvalue.valuetype):
            return None, TypeError("\"Power\" function exponent value, second argument, must be a numeric type")

        # If source value or exponent value is Null, result is Null
        if sourcevalue.is_null() or exponentvalue.is_null():
            return ValueExpression.nullvalue(sourcevalue.valuetype), None

        valuetype, err = derive_arithmetic_operationvaluetype(ExpressionOperatorType.MULTIPLY, sourcevalue.valuetype, exponentvalue.valuetype)

        if err is not None:
            return None, EvaluateError(f"failed while deriving \"Power\" function multiplicative arithmetic operation value type: {err}")

        sourcevalue, err = sourcevalue.convert(valuetype)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"Power\" function source value, first argument, to \"{valuetype}\": {err}")

        exponentvalue, err = exponentvalue.convert(valuetype)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"Power\" function exponent value, second argument, to \"{valuetype}\": {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, math.pow(sourcevalue._booleanvalue_asint(), exponentvalue._booleanvalue_asint()) != 0.0), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.INT32, math.pow(sourcevalue._int32value(), exponentvalue._int32value())), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.INT64, math.pow(sourcevalue._int64value(), exponentvalue._int64value())), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, math.pow(sourcevalue._decimalvalue(), exponentvalue._decimalvalue())), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, math.pow(sourcevalue._doublevalue(), exponentvalue._doublevalue())), None

        return None, TypeError("unexpected expression value type encountered")

    def _regexmatch(self, regexvalue: ValueExpression, testvalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        return self._evaluateregex("RegExMatch", regexvalue, testvalue, False)

    def _regexval(self, regexvalue: ValueExpression, testvalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        return self._evaluateregex("RegExVal", regexvalue, testvalue, True)

    def _evaluateregex(self, functionname: str, regexvalue: ValueExpression, testvalue: ValueExpression, return_matchedvalue: bool) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if regexvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError(f"\"{functionname}\" function regular expression value, first argument, must be a \"String\"")

        if testvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError(f"\"{functionname}\" function test value, second argument, must be a \"String\"")

        # If regular expression value or test value is Null, result is Null
        if regexvalue.is_null() or testvalue.is_null():
            return (NULLSTRINGVALUE, None) if return_matchedvalue else (NULLBOOLVALUE, None)

        try:
            match = re.search(regexvalue._stringvalue(), testvalue._stringvalue())

            if return_matchedvalue:
                return (EMPTYSTRINGVALUE, None) if match is None else (ValueExpression(ExpressionValueType.STRING, match[0]), None)

            return (FALSEVALUE, None) if match is None else (TRUEVALUE, None)
        except Exception as ex:
            return None, EvaluateError(f"failed while evaluating \"{functionname}\" function expression value, first argument: {ex}")

    def _replace(self, sourcevalue: ValueExpression, testvalue: ValueExpression, replacevalue: ValueExpression, ignorecase: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Replace\" function source value, first argument, must be a \"String\"")

        if testvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Replace\" function test value, second argument, must be a \"String\"")

        if replacevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Replace\" function replace value, third argument, must be a \"String\"")

        if testvalue.is_null():
            return None, TypeError("\"Replace\" function test value, second argument, is null")

        if replacevalue.is_null():
            return None, TypeError("\"Replace\" function replace value, third argument, is null")

        # If source value, result is Null
        if sourcevalue.is_null():
            return NULLSTRINGVALUE, None

        ignorecase, err = ignorecase.convert(ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"Replace\" function ignore case value, fourth argument, to \"Boolean\": {err}")

        if ignorecase._booleanvalue():
            try:
                regex = re.compile(re.escape(testvalue._stringvalue()), re.IGNORECASE)
            except Exception as ex:
                return None, EvaluateError(f"failed while compiling \"Replace\" function case-insensitive RegEx replace expression for test value, second argument: {ex}")

            return ValueExpression(ExpressionValueType.STRING, regex.sub(replacevalue._stringvalue(), sourcevalue._stringvalue())), None

        return ValueExpression(ExpressionValueType.STRING, sourcevalue._stringvalue().replace(testvalue._stringvalue(), replacevalue._stringvalue())), None

    def _reverse(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Reverse\" function source value, first argument, must be a \"String\"")

        # If source value, result is Null
        if sourcevalue.is_null():
            return NULLSTRINGVALUE, None

        return ValueExpression(ExpressionValueType.STRING, sourcevalue._stringvalue()[::-1]), None

    def _round(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        sourcevaluetype = sourcevalue.valuetype

        if not is_numerictype(sourcevaluetype):
            return None, TypeError("\"Round\" function source value, first argument, must be a numeric type")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return ValueExpression.nullvalue(sourcevaluetype), None

        if is_integertype(sourcevaluetype):
            return sourcevalue, None

        if sourcevaluetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, sourcevalue._decimalvalue().quantize(0)), None
        if sourcevaluetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, round(sourcevalue._doublevalue())), None

        return None, TypeError("unexpected expression value type encountered")

    def _split(self, sourcevalue: ValueExpression, delimitervalue: ValueExpression, indexvalue: ValueExpression, ignorecase: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Split\" function source value, first argument, must be a \"String\"")

        if delimitervalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Split\" function delimiter value, second argument, must be a \"String\"")

        if not is_integertype(indexvalue.valuetype):
            return None, TypeError("\"Split\" function index value, third argument, must be an integer type")

        if delimitervalue.is_null():
            return None, TypeError("\"Split\" function delimiter value, second argument, is null")

        if indexvalue.is_null():
            return None, TypeError("\"Split\" function index value, third argument, is null")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLSTRINGVALUE, None

        ignorecase, err = ignorecase.convert(ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"Split\" function ignore case value, fourth argument, to \"Boolean\": {err}")

        sourcevalue = sourcevalue._stringvalue()
        index = indexvalue._integervalue()

        if ignorecase._booleanvalue():
            start, stop, success = _split_nthindex(sourcevalue.upper(), delimitervalue._stringvalue().upper(), index)
        else:
            start, stop, success = _split_nthindex(sourcevalue, delimitervalue._stringvalue(), index)

        return (ValueExpression(ExpressionValueType.STRING, sourcevalue[start:stop]), None) if success else (EMPTYSTRINGVALUE, None)

    def _sqrt(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        sourcevaluetype = sourcevalue.valuetype

        if not is_numerictype(sourcevaluetype):
            return None, TypeError("\"Sqrt\" function source value, first argument, must be a numeric type")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return ValueExpression.nullvalue(sourcevaluetype), None

        if is_integertype(sourcevaluetype):
            return ValueExpression(ExpressionValueType.DOUBLE, math.sqrt(sourcevalue.integervalue())), None

        if sourcevaluetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, sourcevalue._decimalvalue().sqrt()), None
        if sourcevaluetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, math.sqrt(sourcevalue._doublevalue())), None

        return None, TypeError("unexpected expression value type encountered")

    def _startswith(self, sourcevalue: ValueExpression, testvalue: ValueExpression, ignorecase: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"StartsWith\" function source value, first argument, must be a \"String\"")

        if testvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"StartsWith\" function test value, second argument, must be a \"String\"")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLSTRINGVALUE, None

        if testvalue.is_null():
            return FALSEVALUE, None

        ignorecase, err = ignorecase.convert(ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"StartsWith\" function ignore case value, third argument, to \"Boolean\": {err}")

        if ignorecase._booleanvalue():
            return ValueExpression(ExpressionValueType.BOOLEAN, sourcevalue._stringvalue().upper().startswith(testvalue._stringvalue().upper())), None

        return ValueExpression(ExpressionValueType.BOOLEAN, sourcevalue._stringvalue().startswith(testvalue._stringvalue())), None

    def _strcount(self, sourcevalue: ValueExpression, testvalue: ValueExpression, ignorecase: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"StrCount\" function source value, first argument, must be a \"String\"")

        if testvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"StrCount\" function test value, second argument, must be a \"String\"")

        if sourcevalue.is_null() or testvalue.is_null():
            return ValueExpression(ExpressionValueType.INT32, 0), None

        findvalue = testvalue._stringvalue()

        if len(findvalue) == 0:
            return ValueExpression(ExpressionValueType.INT32, 0), None

        ignorecase, err = ignorecase.convert(ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"StrCount\" function ignore case value, third argument, to \"Boolean\": {err}")

        if ignorecase._booleanvalue():
            return ValueExpression(ExpressionValueType.INT32, sourcevalue._stringvalue().upper().count(findvalue.upper())), None

        return ValueExpression(ExpressionValueType.INT32, sourcevalue._stringvalue().count(findvalue)), None

    def _strcmp(self, leftvalue: ValueExpression, rightvalue: ValueExpression, ignorecase: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if leftvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"StrCmp\" function left value, first argument, must be a \"String\"")

        if rightvalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"StrCmp\" function right value, second argument, must be a \"String\"")

        if leftvalue.is_null() or rightvalue.is_null():
            return NULLSTRINGVALUE, None

        ignorecase, err = ignorecase.convert(ExpressionValueType.BOOLEAN)

        if err is not None:
            return None, EvaluateError(f"failed while converting \"StrCmp\" function ignore case value, third argument, to \"Boolean\": {err}")

        def compare_str(left: str, right: str) -> int:
            return -1 if left < right else 1 if left > right else 0

        if ignorecase._booleanvalue():
            return ValueExpression(ExpressionValueType.INT32, compare_str(leftvalue._stringvalue().upper(), rightvalue._stringvalue().upper())), None

        return ValueExpression(ExpressionValueType.INT32, compare_str(leftvalue._stringvalue(), rightvalue._stringvalue())), None

    def _substr(self, sourcevalue: ValueExpression, indexvalue: ValueExpression, lengthvalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"SubStr\" function source value, first argument, must be a \"String\"")

        if not is_integertype(indexvalue.valuetype):
            return None, TypeError("\"SubStr\" function index value, second argument, must be an integer type")

        if not is_integertype(lengthvalue.valuetype):
            return None, TypeError("\"SubStr\" function length value, third argument, must be an integer type")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLSTRINGVALUE, None

        sourcetext = sourcevalue._stringvalue()
        index = indexvalue.integervalue()

        if index < 0 or index >= len(sourcetext):
            return EMPTYSTRINGVALUE, None

        if not lengthvalue.is_null():
            length = lengthvalue.integervalue()

            if length <= 0:
                return EMPTYSTRINGVALUE, None

            if index + length < len(sourcetext):
                return ValueExpression(ExpressionValueType.STRING, sourcetext[index:index + length]), None

        return ValueExpression(ExpressionValueType.STRING, sourcetext[index:]), None

    def _trim(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Trim\" function source value, first argument, must be a \"String\"")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLSTRINGVALUE, None

        return ValueExpression(ExpressionValueType.STRING, sourcevalue._stringvalue().strip()), None

    def _trimleft(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"TrimLeft\" function source value, first argument, must be a \"String\"")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLSTRINGVALUE, None

        return ValueExpression(ExpressionValueType.STRING, sourcevalue._stringvalue().lstrip()), None

    def _trimright(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"TrimRight\" function source value, first argument, must be a \"String\"")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLSTRINGVALUE, None

        return ValueExpression(ExpressionValueType.STRING, sourcevalue._stringvalue().rstrip()), None

    def _upper(self, sourcevalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        if sourcevalue.valuetype != ExpressionValueType.STRING:
            return None, TypeError("\"Upper\" function source value, first argument, must be a \"String\"")

        # If source value is Null, result is Null
        if sourcevalue.is_null():
            return NULLSTRINGVALUE, None

        return ValueExpression(ExpressionValueType.STRING, sourcevalue._stringvalue().upper()), None

    def _utcnow(self) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        return ValueExpression(ExpressionValueType.DATETIME, datetime.now(timezone.utc)), None

    # Filter Expression Operator Implementations

    def _convert_operands(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[ValueExpression], Optional[Exception]]:
        # sourcery skip

        leftvalue, err = leftvalue.convert(valuetype)

        if err is not None:
            return None, None, err

        rightvalue, err = rightvalue.convert(valuetype)

        if err is not None:
            return None, None, err

        return leftvalue, rightvalue, None

    def _multiply_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(valuetype), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"multiplication \"*\" operator {err}")

        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.INT32, leftvalue._int32value() * rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.INT64, leftvalue._int64value() * rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, leftvalue._decimalvalue() * rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, leftvalue._doublevalue() * rightvalue._doublevalue()), None

        return None, EvaluateError(f"cannot apply multiplication \"*\" operator to \"{valuetype}\"")

    def _divide_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(valuetype), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"division \"/\" operator {err}")

        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.INT32, leftvalue._int32value() / rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.INT64, leftvalue._int64value() / rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, leftvalue._decimalvalue() / rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, leftvalue._doublevalue() / rightvalue._doublevalue()), None

        return None, EvaluateError(f"cannot apply division \"/\" operator to \"{valuetype}\"")

    def _modulus_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(valuetype), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"modulus \"%\" operator {err}")

        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.INT32, leftvalue._int32value() % rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.INT64, leftvalue._int64value() % rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, leftvalue._decimalvalue() % rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, leftvalue._doublevalue() % rightvalue._doublevalue()), None

        return None, EvaluateError(f"cannot apply modulus \"%\" operator to \"{valuetype}\"")

    def _add_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(valuetype), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"addition \"+\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, (leftvalue._booleanvalue_asint() + rightvalue._booleanvalue_asint()) != 0), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.INT32, leftvalue._int32value() + rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.INT64, leftvalue._int64value() + rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, leftvalue._decimalvalue() + rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, leftvalue._doublevalue() + rightvalue._doublevalue()), None
        if valuetype == ExpressionValueType.STRING:
            return ValueExpression(ExpressionValueType.STRING, leftvalue._stringvalue() + rightvalue._stringvalue()), None

        return None, EvaluateError(f"cannot apply addition \"+\" operator to \"{valuetype}\"")

    def _subtract_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(valuetype), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"subtraction \"-\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, (leftvalue._booleanvalue_asint() - rightvalue._booleanvalue_asint()) != 0), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.INT32, leftvalue._int32value() - rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.INT64, leftvalue._int64value() - rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.DECIMAL, leftvalue._decimalvalue() - rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.DOUBLE, leftvalue._doublevalue() - rightvalue._doublevalue()), None

        return None, EvaluateError(f"cannot apply subtraction \"-\" operator to \"{valuetype}\"")

    def _bitshiftleft_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left is Null, result is Null
        if leftvalue.is_null():
            return leftvalue, None

        if not is_integertype(rightvalue.valuetype):
            return None, TypeError(f"left bit-shift \"<<\" operator right operand, shift value, must be an integer")

        if rightvalue.is_null():
            return None, TypeError(f"left bit-shift \"<<\" operator right operand, shift value, is Null")

        shiftamount = rightvalue.integervalue()

        if leftvalue.valuetype == ExpressionValueType.BOOLEAN:
            if shiftamount < 0:
                shiftamount = INTSIZE - (abs(shiftamount) % INTSIZE)

            return ValueExpression(ExpressionValueType.BOOLEAN, (leftvalue._booleanvalue_asint() << shiftamount) != 0), None
        if leftvalue.valuetype == ExpressionValueType.INT32:
            if shiftamount < 0:
                shiftamount = 32 - (abs(shiftamount) % 32)

            return ValueExpression(ExpressionValueType.INT32, leftvalue._int32value() << shiftamount), None
        if leftvalue.valuetype == ExpressionValueType.INT64:
            if shiftamount < 0:
                shiftamount = 64 - (abs(shiftamount) % 64)

            return ValueExpression(ExpressionValueType.INT64, leftvalue._int64value() << shiftamount), None

        return None, EvaluateError(f"cannot apply left bit-shift \"<<\" operator to \"{leftvalue.valuetype}\"")

    def _bitshiftright_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left is Null, result is Null
        if leftvalue.is_null():
            return leftvalue, None

        if not is_integertype(rightvalue.valuetype):
            return None, TypeError(f"right bit-shift \">>\" operator right operand, shift value, must be an integer")

        if rightvalue.is_null():
            return None, TypeError(f"right bit-shift \">>\" operator right operand, shift value, is Null")

        shiftamount = rightvalue.integervalue()

        if leftvalue.valuetype == ExpressionValueType.BOOLEAN:
            if shiftamount < 0:
                shiftamount = INTSIZE - (abs(shiftamount) % INTSIZE)

            return ValueExpression(ExpressionValueType.BOOLEAN, (leftvalue._booleanvalue_asint() >> shiftamount) != 0), None
        if leftvalue.valuetype == ExpressionValueType.INT32:
            if shiftamount < 0:
                shiftamount = 32 - (abs(shiftamount) % 32)

            return ValueExpression(ExpressionValueType.INT32, leftvalue._int32value() >> shiftamount), None
        if leftvalue.valuetype == ExpressionValueType.INT64:
            if shiftamount < 0:
                shiftamount = 64 - (abs(shiftamount) % 64)

            return ValueExpression(ExpressionValueType.INT64, leftvalue._int64value() >> shiftamount), None

        return None, EvaluateError(f"cannot apply right bit-shift \">>\" operator to \"{leftvalue.valuetype}\"")

    def _bitwiseand_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(valuetype), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"bitwise \"&\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, (leftvalue._booleanvalue_asint() & rightvalue._booleanvalue_asint()) != 0), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.INT32, leftvalue._int32value() & rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.INT64, leftvalue._int64value() & rightvalue._int64value()), None

        return None, EvaluateError(f"cannot apply bitwise \"&\" operator to \"{valuetype}\"")

    def _bitwiseor_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(valuetype), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"bitwise \"|\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, (leftvalue._booleanvalue_asint() | rightvalue._booleanvalue_asint()) != 0), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.INT32, leftvalue._int32value() | rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.INT64, leftvalue._int64value() | rightvalue._int64value()), None

        return None, EvaluateError(f"cannot apply bitwise \"|\" operator to \"{valuetype}\"")

    def _bitwisexor_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(valuetype), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"bitwise \"^\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, (leftvalue._booleanvalue_asint() ^ rightvalue._booleanvalue_asint()) != 0), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.INT32, leftvalue._int32value() ^ rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.INT64, leftvalue._int64value() ^ rightvalue._int64value()), None

        return None, EvaluateError(f"cannot apply bitwise \"^\" operator to \"{valuetype}\"")

    def _lessthan_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(ExpressionValueType.BOOLEAN), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"less-than \"<\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._booleanvalue_asint() < rightvalue._booleanvalue_asint()), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int32value() < rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int64value() < rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._decimalvalue() < rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._doublevalue() < rightvalue._doublevalue()), None
        if valuetype == ExpressionValueType.STRING:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._stringvalue().upper() < rightvalue._stringvalue().upper()), None
        if valuetype == ExpressionValueType.GUID:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._guidvalue() < rightvalue._guidvalue()), None
        if valuetype == ExpressionValueType.DATETIME:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._datetimevalue() < rightvalue._datetimevalue()), None

        return None, EvaluateError(f"cannot apply less-than \"<\" operator to \"{valuetype}\"")

    def _lessthanorequal_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(ExpressionValueType.BOOLEAN), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"less-than-or-equal \"<=\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._booleanvalue_asint() <= rightvalue._booleanvalue_asint()), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int32value() <= rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int64value() <= rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._decimalvalue() <= rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._doublevalue() <= rightvalue._doublevalue()), None
        if valuetype == ExpressionValueType.STRING:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._stringvalue().upper() <= rightvalue._stringvalue().upper()), None
        if valuetype == ExpressionValueType.GUID:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._guidvalue() <= rightvalue._guidvalue()), None
        if valuetype == ExpressionValueType.DATETIME:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._datetimevalue() <= rightvalue._datetimevalue()), None

        return None, EvaluateError(f"cannot apply less-than-or-equal \"<=\" operator to \"{valuetype}\"")

    def _greaterthan_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(ExpressionValueType.BOOLEAN), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"greater-than \">\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._booleanvalue_asint() > rightvalue._booleanvalue_asint()), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int32value() > rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int64value() > rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._decimalvalue() > rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._doublevalue() > rightvalue._doublevalue()), None
        if valuetype == ExpressionValueType.STRING:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._stringvalue().upper() > rightvalue._stringvalue().upper()), None
        if valuetype == ExpressionValueType.GUID:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._guidvalue() > rightvalue._guidvalue()), None
        if valuetype == ExpressionValueType.DATETIME:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._datetimevalue() > rightvalue._datetimevalue()), None

        return None, EvaluateError(f"cannot apply greater-than \">\" operator to \"{valuetype}\"")

    def _greaterthanorequal_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(ExpressionValueType.BOOLEAN), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"greater-than-or-equal \">=\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._booleanvalue_asint() >= rightvalue._booleanvalue_asint()), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int32value() >= rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int64value() >= rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._decimalvalue() >= rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._doublevalue() >= rightvalue._doublevalue()), None
        if valuetype == ExpressionValueType.STRING:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._stringvalue().upper() >= rightvalue._stringvalue().upper()), None
        if valuetype == ExpressionValueType.GUID:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._guidvalue() >= rightvalue._guidvalue()), None
        if valuetype == ExpressionValueType.DATETIME:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._datetimevalue() >= rightvalue._datetimevalue()), None

        return None, EvaluateError(f"cannot apply greater-than-or-equal \">=\" operator to \"{valuetype}\"")

    def _equal_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType, exactmatch: bool) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(ExpressionValueType.BOOLEAN), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"equal \"=\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._booleanvalue_asint() == rightvalue._booleanvalue_asint()), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int32value() == rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int64value() == rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._decimalvalue() == rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._doublevalue() == rightvalue._doublevalue()), None
        if valuetype == ExpressionValueType.STRING:
            if exactmatch:
                return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._stringvalue() == rightvalue._stringvalue()), None

            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._stringvalue().upper() == rightvalue._stringvalue().upper()), None
        if valuetype == ExpressionValueType.GUID:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._guidvalue() == rightvalue._guidvalue()), None
        if valuetype == ExpressionValueType.DATETIME:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._datetimevalue() == rightvalue._datetimevalue()), None

        return None, EvaluateError(f"cannot apply equal \"=\" operator to \"{valuetype}\"")

    def _notequal_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType, exactmatch: bool) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return ValueExpression.nullvalue(ExpressionValueType.BOOLEAN), None

        leftvalue, rightvalue, err = self._convert_operands(leftvalue, rightvalue, valuetype)

        if err is not None:
            return None, EvaluateError(f"not-equal \"!=\" operator {err}")

        if valuetype == ExpressionValueType.BOOLEAN:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._booleanvalue_asint() != rightvalue._booleanvalue_asint()), None
        if valuetype == ExpressionValueType.INT32:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int32value() != rightvalue._int32value()), None
        if valuetype == ExpressionValueType.INT64:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._int64value() != rightvalue._int64value()), None
        if valuetype == ExpressionValueType.DECIMAL:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._decimalvalue() != rightvalue._decimalvalue()), None
        if valuetype == ExpressionValueType.DOUBLE:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._doublevalue() != rightvalue._doublevalue()), None
        if valuetype == ExpressionValueType.STRING:
            if exactmatch:
                return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._stringvalue() != rightvalue._stringvalue()), None

            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._stringvalue().upper() != rightvalue._stringvalue().upper()), None
        if valuetype == ExpressionValueType.GUID:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._guidvalue() != rightvalue._guidvalue()), None
        if valuetype == ExpressionValueType.DATETIME:
            return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._datetimevalue() != rightvalue._datetimevalue()), None

        return None, EvaluateError(f"cannot apply not-equal \"!=\" operator to \"{valuetype}\"")

    def _isnull_op(self, value: ValueExpression) -> Optional[ValueExpression]:
        return ValueExpression(ExpressionValueType.BOOLEAN, value.is_null())

    def _isnotnull_op(self, value: ValueExpression) -> Optional[ValueExpression]:
        return ValueExpression(ExpressionValueType.BOOLEAN, not value.is_null())

    def _like_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, exactmatch: bool) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # sourcery skip

        # If left is Null, result is Null
        if leftvalue.is_null():
            return NULLBOOLVALUE, None

        if leftvalue.valuetype != ExpressionValueType.STRING or rightvalue.valuetype != ExpressionValueType.STRING:
            return None, EvaluateError(f"cannot perform \"LIKE\" operation on \"{leftvalue.valuetype}\" and \"{rightvalue.valuetype}\"")

        if rightvalue.is_null():
            return None, TypeError(f"right operand of \"LIKE\" operation is Null")

        leftoperand = leftvalue._stringvalue()
        rightoperand = rightvalue._stringvalue()
        testexpression = rightoperand.replace("%", "*")
        startswith_wildcard = testexpression.startswith("*")
        endswith_wildcard = testexpression.endswith("*")

        if startswith_wildcard:
            testexpression = testexpression[1:]

        if endswith_wildcard and len(testexpression) > 0:
            testexpression = testexpression[:-1]

        # "*" or "**" expression means match everything
        if len(testexpression) == 0:
            return TRUEVALUE, None

        # Wild cards in the middle of the string are not supported
        if "*" in testexpression:
            return None, EvaluateError(f"right operand of \"LIKE\" expression \"{rightoperand}\" has an invalid pattern")

        if startswith_wildcard:
            if exactmatch:
                if leftoperand.endswith(testexpression):
                    return TRUEVALUE, None
            else:
                if leftoperand.upper().endswith(testexpression.upper()):
                    return TRUEVALUE, None

        if endswith_wildcard:
            if exactmatch:
                if leftoperand.startswith(testexpression):
                    return TRUEVALUE, None
            else:
                if leftoperand.upper().startswith(testexpression.upper()):
                    return TRUEVALUE, None

        if startswith_wildcard and endswith_wildcard:
            if exactmatch:
                if testexpression in leftoperand:
                    return TRUEVALUE, None
            else:
                if testexpression.upper() in leftoperand.upper():
                    return TRUEVALUE, None

        return FALSEVALUE, None

    def _notlike_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression, valuetype: ExpressionValueType, exactmatch: bool) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left is Null, result is Null
        if leftvalue.is_null():
            return NULLBOOLVALUE, None

        likeresult, err = self._like_op(leftvalue, rightvalue, valuetype, exactmatch)

        if err is not None:
            return None, err

        return (FALSEVALUE, None) if likeresult._booleanvalue() else (TRUEVALUE, None)

    def _and_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return NULLBOOLVALUE, None

        if leftvalue.valuetype != ExpressionValueType.BOOLEAN or rightvalue.valuetype != ExpressionValueType.BOOLEAN:
            return None, EvaluateError(f"cannot perform \"AND\" operation on \"{leftvalue.valuetype}\" and \"{rightvalue.valuetype}\"")

        return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._booleanvalue() and rightvalue._booleanvalue()), None

    def _or_op(self, leftvalue: ValueExpression, rightvalue: ValueExpression) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        # If left or right value is Null, result is Null
        if leftvalue.is_null() or rightvalue.is_null():
            return NULLBOOLVALUE, None

        if leftvalue.valuetype != ExpressionValueType.BOOLEAN or rightvalue.valuetype != ExpressionValueType.BOOLEAN:
            return None, EvaluateError(f"cannot perform \"OR\" operation on \"{leftvalue.valuetype}\" and \"{rightvalue.valuetype}\"")

        return ValueExpression(ExpressionValueType.BOOLEAN, leftvalue._booleanvalue() or rightvalue._booleanvalue()), None
