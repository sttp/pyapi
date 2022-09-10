# ******************************************************************************************************
#  test_expressiontree.py.py - Gbtc
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
#  09/09/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

import unittest
from gsf import Limits, normalize_enumname
from src.sttp.data.filterexpressionparser import FilterExpressionParser
from src.sttp.data.constants import ExpressionValueType
from src.sttp.data.dataset import DataSet, xsdformat
from src.sttp.data.datarow import DataRow
from src.sttp.data.datatype import DataType
from src.sttp.data.tableidfields import TableIDFields
from decimal import Decimal
from uuid import UUID, uuid1
from dateutil import parser
from datetime import datetime, timezone
from .test_dataset import TestDataSet


class TestExpressionTree(unittest.TestCase):

    def _test_evaluate_literal_expression(self, name: str, expected: object, expr: str = ...):
        result, err = FilterExpressionParser.evaluate_expression(str(expected) if expr is ... else expr)

        if err is not None:
            self.fail(f"test_evaluate_{name}_literal_expression: error parsing expression: {err}")

        if result is None:
            self.fail(f"test_evaluate_{name}_literal_expression: received no result")

        if result.valuetype != getattr(ExpressionValueType, name.upper()):
            self.fail(f"test_evaluate_{name}_literal_expression: received unexpected type: {normalize_enumname(result.valuetype)}")

        ve, err = eval(f"result.{name}value")()

        if err is not None:
            self.fail(f"test_evaluate_{name}_literal_expression: failed to retrieve value: {err}")

        if ve != expected:
            self.fail(f"test_evaluate_{name}_literal_expression: retrieved value does not match source")

    def test_evaluate_boolean_literal_expression(self):
        self._test_evaluate_literal_expression("boolean", True)
        self._test_evaluate_literal_expression("boolean", False)

    def test_evaluate_int32_literal_expression(self):
        self._test_evaluate_literal_expression("int32", Limits.MININT32 + 1)  # Min int32 value interpreted as int64
        self._test_evaluate_literal_expression("int32", -1)
        self._test_evaluate_literal_expression("int32", 0)
        self._test_evaluate_literal_expression("int32", 1)
        self._test_evaluate_literal_expression("int32", Limits.MAXINT32)

    def test_evaluate_int64_literal_expression(self):
        self._test_evaluate_literal_expression("int64", Limits.MININT64 + 1)  # Min int64 value interpreted as Decimal
        self._test_evaluate_literal_expression("int64", Limits.MININT32)
        self._test_evaluate_literal_expression("int64", Limits.MAXINT32 + 1)
        self._test_evaluate_literal_expression("int64", Limits.MAXINT64)

    def test_evaluate_decimal_literal_expression(self):
        self._test_evaluate_literal_expression("decimal", Decimal(-9223372036854775809.87686876))
        self._test_evaluate_literal_expression("decimal", Decimal(Limits.MININT64 - 1.0))

    def test_evaluate_double_literal_expression(self):
        self._test_evaluate_literal_expression("double", 123.456e-6, "123.456e-6")

    def test_evaluate_string_literal_expression(self):
        value = "Hello, literal string expression"
        self._test_evaluate_literal_expression("string", value, f"'{value}'")

    def test_evaluate_guid_literal_expression(self):
        value = "F63E09B3-17A4-4B6F-9FA5-E359A5220E8F"
        self._test_evaluate_literal_expression("guid", UUID(value), f"{{{value}}}")
        self._test_evaluate_literal_expression("guid", UUID(value))
        self._test_evaluate_literal_expression("guid", uuid1())

    def test_evaluate_datetime_literal_expression(self):
        def get_datetime(literal: str) -> datetime:
            return parser.parse(literal).astimezone(timezone.utc)

        value = "2021-09-09T12:34:56.789Z"
        self._test_evaluate_literal_expression("datetime", get_datetime(value), f"#{value}#")

        value = "2006-01-01 00:00:00"
        self._test_evaluate_literal_expression("datetime", get_datetime(value), f"#{value}#")

        value = "2019-01-1 00:00:59.999"
        self._test_evaluate_literal_expression("datetime", get_datetime(value), f"#{value}#")

        value = get_datetime(xsdformat(datetime.now()))
        self._test_evaluate_literal_expression("datetime", value, f"#{xsdformat(value)}#")

        value = get_datetime(xsdformat(datetime.now(timezone.utc)))
        self._test_evaluate_literal_expression("datetime", value, f"#{xsdformat(value)}#")

    def test_signalidset_expressions(self):
        # sourcery skip
        dataset, _, _, statid, freqid = TestDataSet._create_dataset()

        idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER ActiveMeasurements WHERE SignalType = 'FREQ'", "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_signalidset_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

        if len(idset) != 1:
            self.fail(f"test_signalidset_expressions: expected 1 result, received {len(idset)}")

        if next(iter(idset)) != freqid:
            self.fail(f"test_signalidset_expressions: retrieve Guid value does not match source - expected {freqid}, received {idset[0]}")

        idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER ActiveMeasurements WHERE SignalType = 'STAT'", "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_signalidset_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

        if len(idset) != 1:
            self.fail(f"test_signalidset_expressions: expected 1 result, received {len(idset)}")

        if next(iter(idset)) != statid:
            self.fail(f"test_signalidset_expressions: retrieve Guid value does not match source - expected {statid}, received {idset[0]}")

        idset, err = FilterExpressionParser.select_signalidset(dataset, str(statid), "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_signalidset_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

        if len(idset) != 1:
            self.fail(f"test_signalidset_expressions: expected 1 result, received {len(idset)}")

        if next(iter(idset)) != statid:
            self.fail(f"test_signalidset_expressions: retrieve Guid value does not match source - expected {statid}, received {idset[0]}")

        idset, err = FilterExpressionParser.select_signalidset(dataset, f";;{statid};;;", "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_signalidset_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

        if len(idset) != 1:
            self.fail(f"test_signalidset_expressions: expected 1 result, received {len(idset)}")

        if next(iter(idset)) != statid:
            self.fail(f"test_signalidset_expressions: retrieve Guid value does not match source - expected {statid}, received {idset[0]}")

        idset, err = FilterExpressionParser.select_signalidset(dataset, "{" + str(freqid) + "}", "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_signalidset_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

        if len(idset) != 1:
            self.fail(f"test_signalidset_expressions: expected 1 result, received {len(idset)}")

        if next(iter(idset)) != freqid:
            self.fail(f"test_signalidset_expressions: retrieve Guid value does not match source - expected {freqid}, received {idset[0]}")

        idset, err = FilterExpressionParser.select_signalidset(dataset, f"{freqid};{statid};{statid}", "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_signalidset_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

        if len(idset) != 2:
            self.fail(f"test_signalidset_expressions: expected 1 result, received {len(idset)}")

        if not freqid in idset or not statid in idset:
            self.fail(f"test_signalidset_expressions: expected Guid values not found")

        idset, err = FilterExpressionParser.select_signalidset(dataset, f"{freqid};{statid};{statid}; FILTER ActiveMeasurements WHERE True", "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_signalidset_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

        if len(idset) != 2:
            self.fail(f"test_signalidset_expressions: expected 1 result, received {len(idset)}")

        if not freqid in idset or not statid in idset:
            self.fail(f"test_signalidset_expressions: expected Guid values not found")

        idset, err = FilterExpressionParser.select_signalidset(dataset, f"filter activemeasurements where signalID = '{freqid}'", "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_signalidset_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

        if len(idset) != 1:
            self.fail(f"test_signalidset_expressions: expected 1 result, received {len(idset)}")

        if next(iter(idset)) != freqid:
            self.fail(f"test_signalidset_expressions: retrieve Guid value does not match source - expected {freqid}, received {idset[0]}")

        idset, err = FilterExpressionParser.select_signalidset(dataset, f"FILTER ActiveMeasurements WHERE signalID = '{freqid}' Or SIGNALID = {{{statid}}}", "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_signalidset_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

        if len(idset) != 2:
            self.fail(f"test_signalidset_expressions: expected 1 result, received {len(idset)}")

        if not freqid in idset or not statid in idset:
            self.fail(f"test_signalidset_expressions: expected Guid values not found")

        _, err = FilterExpressionParser.select_signalidset(dataset, "", "")

        if err is None:
            self.fail(f"test_signalidset_expressions: error expected, received none")

        _, err = FilterExpressionParser.select_signalidset(dataset, "bad expression", "ActiveMeasurements")

        if err is None:
            self.fail(f"test_signalidset_expressions: error expected, received none")

    def _get_row_guid(self, row: DataRow, columnindex: int) -> UUID:
        value, null, err = row.guidvalue(columnindex)

        if null or err is not None:
            self.fail(f"error retrieving Guid value from row: {err}")

        return value

    def _get_row_string(self, row: DataRow, columnindex: int) -> str:
        value, null, err = row.stringvalue(columnindex)

        if null or err is not None:
            self.fail(f"error retrieving string value from row: {err}")

        return value

    def test_selectdatarows_expressions(self):
        # sourcery skip
        dataset, signalidfield, signaltypefield, statid, freqid = TestDataSet._create_dataset()

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType = 'FREQ'; FILTER ActiveMeasurements WHERE SignalType = 'STAT' ORDER BY SignalID",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        # FREQ should be before STAT because of multiple statement evaluation order
        if self._get_row_guid(rows[0], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType = 'FREQ' OR SignalType = 'STAT'",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        # Row with stat comes before row with freq (single expression statement)
        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType = 'FREQ' OR SignalType = 'STAT' ORDER BY BINARY SignalType",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        # FREQ should sort before STAT with order by
        if self._get_row_guid(rows[0], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType = 'STAT' OR SignalType = 'FREQ' ORDER BY SignalType DESC",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        # Now descending order should have STAT before FREQ
        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            f"FILTER ActiveMeasurements WHERE SignalID = {{{statid}}} OR SignalID = '{freqid}' ORDER BY SignalType",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        # FREQ should sort before STAT with order by
        if self._get_row_guid(rows[0], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            f"FILTER ActiveMeasurements WHERE SignalID = {statid} OR SignalID = '{freqid}' ORDER BY SignalType;{statid}",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        #  Because expression includes Guid statID as a literal (at the end), it will parse first
        #  regardless of order by in filter statement
        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE True",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE IsNull(NULL, False) OR Coalesce(Null, true)",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE IIf(IsNull(NULL, False) OR Coalesce(Null, true), Len(SignalType) == 4, false)",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType IS !NULL",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE Len(SubStr(Coalesce(Trim(SignalType), 'OTHER'), 0, 0X2)) = 2",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE LEN(SignalTYPE) > 3.5",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE Len(SignalType) & 0x4 == 4",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE RegExVal('ST.+', SignalType) == 'STAT'",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 1:
            self.fail(f"test_selectdatarows_expressions: expected 1 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE RegExMatch('FR.+', SignalType)",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 1:
            self.fail(f"test_selectdatarows_expressions: expected 1 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType IN ('FREQ', 'STAT') ORDER BY SignalType",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            f"FILTER ActiveMeasurements WHERE SignalID IN ({statid}, {freqid})",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType LIKE 'ST%'",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 1:
            self.fail(f"test_selectdatarows_expressions: expected 1 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType LIKE '*EQ'",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 1:
            self.fail(f"test_selectdatarows_expressions: expected 1 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType LIKE '*TA%'",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 1:
            self.fail(f"test_selectdatarows_expressions: expected 1 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE -Len(SignalType) <= 0",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        # number converted to string and compared
        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType == 0",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 0:
            self.fail(f"test_selectdatarows_expressions: expected 0 results, received {len(rows)}")

        # number converted to string and compared
        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE SignalType > 99",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        rows, err = FilterExpressionParser.select_datarows(
            dataset,
            "FILTER ActiveMeasurements WHERE Len(SignalType) / 0x2 = 2",
            "ActiveMeasurements")

        if err is not None:
            self.fail(f"test_selectdatarows_expressions: error executing FilterExpressionParser.select_datarows: {err}")

        if len(rows) != 2:
            self.fail(f"test_selectdatarows_expressions: expected 2 result, received {len(rows)}")

        if self._get_row_guid(rows[0], signalidfield) != statid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_guid(rows[1], signalidfield) != freqid:
            self.fail(f"test_selectdatarows_expressions: retrieve Guid value does not match source")

        if self._get_row_string(rows[0], signaltypefield) != "STAT":
            self.fail(f"test_selectdatarows_expressions: retrieve SignalType value does not match source")

        if self._get_row_string(rows[1], signaltypefield) != "FREQ":
            self.fail(f"test_selectdatarows_expressions: retrieve SignalType value does not match source")

    def test_metadata_expressions(self):  # sourcery skip
        # Two sample metadata files exist, test both
        for i in range(2):
            with open(f"test/MetadataSample{i + 1}.xml", "rb") as binary_file:
                data = binary_file.read()

            dataset, err = DataSet.from_xml(data)

            if err is not None:
                self.fail(f"test_metadata_expressions: error loading DataSet from XML document: {err}")

            if dataset.tablecount != 4:
                self.fail(f"test_metadata_expressions: expected 4 tables, received {dataset.tablecount}")

            table = dataset.table("MeasurementDetail")

            if table is None:
                self.fail("test_metadata_expressions: table not found in DataSet")

            if table.columncount != 11:
                self.fail(f"test_metadata_expressions: expected 11 columns, received {table.columncount}")

            if table.column_byname("ID") is None:
                self.fail("test_metadata_expressions: missing expected table column")

            if table.column_byname("id").datatype != DataType.STRING:
                self.fail("test_metadata_expressions: column type does not match expected")

            if table.column_byname("SignalID") is None:
                self.fail("test_metadata_expressions: missing expected table column")

            if table.column_byname("signalID").datatype != DataType.GUID:
                self.fail("test_metadata_expressions: column type does not match expected")

            if table.rowcount == 0:
                self.fail("test_metadata_expressions: unexpected empty table")

            table = dataset.table("DeviceDetail")

            if table is None:
                self.fail("test_metadata_expressions: table not found in DataSet")

            if table.columncount != 19 + i:  # Second test adds a computed column
                self.fail(f"test_metadata_expressions: expected {19 + i} columns, received {table.columncount}")

            if table.column_byname("ACRONYM") is None:
                self.fail("test_metadata_expressions: missing expected table column")

            if table.column_byname("Acronym").datatype != DataType.STRING:
                self.fail("test_metadata_expressions: column type does not match expected")

            if table.column_byname("Name") is None:
                self.fail("test_metadata_expressions: missing expected table column")

            if table.column_byname("Name").datatype != DataType.STRING:
                self.fail("test_metadata_expressions: column type does not match expected")

            if table.rowcount != 1:
                self.fail("test_metadata_expressions: unexpected empty table")

            datarow = table[0]

            acronym, null, err = datarow.stringvalue_byname("Acronym")

            if null or err is not None:
                self.fail("test_metadata_expressions: unexpected NULL column value in row")

            name, null, err = datarow.stringvalue_byname("Name")

            if null or err is not None:
                self.fail("test_metadata_expressions: unexpected NULL column value in row")

            if acronym.upper() != name.upper():
                self.fail("test_metadata_expressions: unexpected column value in row")

            # In test data set, DeviceDetail.OriginalSource is null
            _, null, _ = datarow.stringvalue_byname("OriginalSource")

            if not null:
                self.fail("test_metadata_expressions: unexpected non-NULL column value in row")

            # In test data set, DeviceDetail.ParentAcronym is not null, but is an empty string
            parent_acronym, null, _ = datarow.stringvalue_byname("ParentAcronym")

            if null or parent_acronym != "":
                self.fail("test_metadata_expressions: unexpected NULL or non-empty column value in row")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER MeasurementDetail WHERE SignalAcronym = 'FREQ'", "MeasurementDetail")

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER TOP 8 MeasurementDetail WHERE SignalAcronym = 'STAT'", "MeasurementDetail")

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 8:
                self.fail(f"test_metadata_expressions: expected 8 results, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER TOP 0 MeasurementDetail WHERE SignalAcronym = 'STAT'", "MeasurementDetail")

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 0:
                self.fail(f"test_metadata_expressions: expected 0 results, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER TOP -1 MeasurementDetail WHERE SignalAcronym = 'STAT'", "MeasurementDetail")

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) == 0:
                self.fail(f"test_metadata_expressions: expected non-zero result set, received {len(idset)}")

            devicedetail_id_fields = TableIDFields()
            devicedetail_id_fields.signalid_fieldname = "UniqueID"
            devicedetail_id_fields.measurementkey_fieldname = "Name"
            devicedetail_id_fields.pointtag_fieldname = "Acronym"

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER DeviceDetail WHERE Convert(Longitude, 'System.Int32') = -89", "DeviceDetail", devicedetail_id_fields)

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER DeviceDetail WHERE Convert(latitude, 'int16') = 35", "DeviceDetail", devicedetail_id_fields)

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER DeviceDetail WHERE Convert(Convert(Latitude, 'Int32'), 'String') = 35", "DeviceDetail", devicedetail_id_fields)

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER DeviceDetail WHERE Convert(Latitude, 'Single') > 35", "DeviceDetail", devicedetail_id_fields)

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER DeviceDetail WHERE Longitude < 0.0", "DeviceDetail", devicedetail_id_fields)

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER DeviceDetail WHERE Acronym IN ('Test', 'Shelby')", "DeviceDetail", devicedetail_id_fields)

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER DeviceDetail WHERE Acronym not IN ('Test', 'Apple')", "DeviceDetail", devicedetail_id_fields)

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER DeviceDetail WHERE NOT (Acronym IN ('Test', 'Apple'))", "DeviceDetail", devicedetail_id_fields)

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(idset)}")

            idset, err = FilterExpressionParser.select_signalidset(dataset, "FILTER DeviceDetail WHERE NOT Acronym !IN ('Shelby', 'Apple')", "DeviceDetail", devicedetail_id_fields)

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_signalidset: {err}")

            if len(idset) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(idset)}")

            rows, err = FilterExpressionParser.select_datarows(dataset, "Acronym LIKE 'Shel%'", "DeviceDetail", devicedetail_id_fields)

            if err is not None:
                self.fail(f"test_metadata_expressions: error executing FilterExpressionParser.select_datarows: {err}")

            if len(rows) != 1:
                self.fail(f"test_metadata_expressions: expected 1 result, received {len(rows)}")

    def test_basic_expressions(self):
        # sourcery skip
        with open("test/MetadataSample2.xml", "rb") as binary_file:
            data = binary_file.read()

        dataset, err = DataSet.from_xml(data)

        if err is not None:
            self.fail(f"test_basic_expressions: error loading DataSet from XML document: {err}")

        # datarows, err = dataset["MeasurementDetail"].select("SignalAcronym = 'STAT'")

        # if err is not None:
        #     self.fail(f"test_basic_expressions: error executing select: {err}")

        # if len(datarows) != 116:
        #     self.fail(f"test_basic_expressions: expected 116 results, received {len(datarows)}")
