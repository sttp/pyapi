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
from gsf import Limits, Convert, normalize_enumname
from src.sttp.data.filterexpressionparser import FilterExpressionParser
from src.sttp.data.constants import ExpressionValueType
from src.sttp.data.dataset import DataSet, xsdformat
from src.sttp.data.datarow import DataRow
from src.sttp.data.datatype import DataType
from src.sttp.data.tableidfields import TableIDFields
from decimal import Decimal
from uuid import UUID, uuid1
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
        value = "2021-09-09T12:34:56.789Z"
        self._test_evaluate_literal_expression("datetime", Convert.from_str(value, datetime), f"#{value}#")

        value = "2006-01-01 00:00:00"
        self._test_evaluate_literal_expression("datetime", Convert.from_str(value, datetime), f"#{value}#")

        value = "2019-01-1 00:00:59.999"
        self._test_evaluate_literal_expression("datetime", Convert.from_str(value, datetime), f"#{value}#")

        value = Convert.from_str(datetime.now().isoformat(), datetime)
        self._test_evaluate_literal_expression("datetime", value, f"#{value.isoformat()}#")

        value = Convert.from_str(datetime.now(timezone.utc).isoformat(), datetime)
        self._test_evaluate_literal_expression("datetime", value, f"#{value.isoformat()}#")

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

        _, err = FilterExpressionParser.select_signalidset(dataset, "BAD-expression", "ActiveMeasurements")

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

        datarows, err = dataset["MeasurementDetail"].select("SignalAcronym = 'STAT'")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing table select: {err}")

        if len(datarows) != 116:
            self.fail(f"test_basic_expressions: expected 116 results, received {len(datarows)}")

        datarows, err = dataset["PhasorDetail"].select("Type = 'V'")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing table select: {err}")

        if len(datarows) != 2:
            self.fail(f"test_basic_expressions: expected 2 results, received {len(datarows)}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(dataset["SchemaVersion"][0], "VersionNumber > 0")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        datarow = dataset["DeviceDetail"][0]

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "AccessID % 2 = 0 AND FramesPerSecond % 4 <> 2 OR AccessID % 1 = 0")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "AccessID % 2 = 0 AND (FramesPerSecond % 4 <> 2 OR -AccessID % 1 = 0)")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "AccessID % 2 = 0 AND (FramesPerSecond % 4 <> 2 AND AccessID % 1 = 0)")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected false, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "AccessID % 2 >= 0 || (FramesPerSecond % 4 <> 2 AND AccessID % 1 = 0)")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "AccessID % 2 = 0 OR FramesPerSecond % 4 != 2 && AccessID % 1 == 0")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "!AccessID % 2 = 0 || FramesPerSecond % 4 = 0x2 && AccessID % 1 == 0")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "NOT AccessID % 2 = 0 OR FramesPerSecond % 4 >> 0x1 = 1 && AccessID % 1 == 0x0")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "!AccessID % 2 = 0 OR FramesPerSecond % 4 >> 1 = 1 && AccessID % 3 << 1 & 4 >= 4")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "OriginalSource IS NULL")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "ParentAcronym IS NOT NULL")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "NOT ParentAcronym IS NULL && Len(parentAcronym) == 0")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "-FramesPerSecond")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != -30:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected -30, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "~FramesPerSecond")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != -31:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected -31, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "~FramesPerSecond * -1 - 1 << -2")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != -2147483648:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected -31, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "NOT True")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected false, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "!True")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected false, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "~True")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected false, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "Len(IsNull(OriginalSource, 'A')) = 1")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "RegExMatch('SH', Acronym)")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "RegExMatch('SH', Name)")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected false, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "RegExMatch('S[hH]', Name) && RegExMatch('S[hH]', Acronym)")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "RegExVal('Sh\\w+', Name)")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.STRING:
            self.fail(f"test_basic_expressions: expected value type of string, received {value_expression.valuetype}")

        result, err = value_expression.stringvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.stringvalue: {err}")

        if result != "Shelby":
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 'Shelby', received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "SubStr(RegExVal('Sh\\w+', Name), 2) == 'ElbY'")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "SubStr(RegExVal('Sh\\w+', Name), 3, 2) == 'lB'")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "RegExVal('Sh\\w+', Name) IN ('NT', Acronym, 'NT')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "RegExVal('Sh\\w+', Name) IN ===('NT', Acronym, 'NT')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected false, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "RegExVal('Sh\\w+', Name) IN BINARY ('NT', Acronym, 3.05)")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected false, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "Name IN===(0x9F, Acronym, 'Shelby')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "Acronym LIKE === 'Sh*'")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected false, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "name LiKe binaRY 'SH%'")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected false, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "Name === 'Shelby' && Name== 'SHelBy' && Name !=='SHelBy'")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "Now()")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        now = datetime.now()

        if result != now and not result < now:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected {now}, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "UtcNow()")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        now = datetime.now(timezone.utc)

        if result != now and not result < now:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected {now}, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "#2019-02-04T03:00:52.73-05:00#")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        if result.month != 2:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2, received {result.month}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "#2019-02-04T03:00:52.73-05:00#")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        if result.day != 4:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 4, received {result.day}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#2019-02-04T03:00:52.73-05:00#, 'Year')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 2019:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2019, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#2019/02/04 03:00:52.73-05:00#, 'Month')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 2:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#2019-02-04 03:00:52.73-05:00#, 'Day')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 4:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 4, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#2019-02-04 03:00#, 'Hour')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 3:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 3, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#2019-02-04 03:00:52.73-05:00#, 'Hour')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 8:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 8, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#2/4/2019 3:21:55#, 'Minute')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 21:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 21, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#02/04/2019 03:21:55.33#, 'Second')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 55:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 55, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#02/04/2019 03:21:5.033#, 'Millisecond')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 33:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 33, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(DateAdd('2019-02-04 03:00:52.73-05:00', 1, 'Year'), 'year')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 2020:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2020, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateAdd('2019-02-04', 2, 'Month')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        if result != datetime(2019, 4, 4):
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2019-04-04 00:00:00, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateAdd(#1/31/2019#, 1, 'Day')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        if result != datetime(2019, 2, 1):
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2019-02-01 00:00:00, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateAdd(#2019-01-31#, 2, 'Week')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        if result != datetime(2019, 2, 14):
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2019-02-14 00:00:00, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateAdd(#2019-01-31#, 25, 'Hour')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        if result != datetime(2019, 2, 1, 1):
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2019-02-01 01:00:00, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateAdd(#2018-12-31 23:58#, 3, 'Minute')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        if result != datetime(2019, 1, 1, 0, 1):
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2019-01-01 00:01:00, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateAdd('2019-01-1 00:59', 61, 'Second')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        if result != datetime(2019, 1, 1, 1, 0, 1):
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2019-01-01 01:00:01, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateAdd('2019-01-1 00:00:59.999', 2, 'Millisecond')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        if result != datetime(2019, 1, 1, 0, 1, 0, 1000):
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2019-01-01 00:01:00.001, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateAdd(#1/1/2019 0:0:1.029#, -FramesPerSecond, 'Millisecond')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DATETIME:
            self.fail(f"test_basic_expressions: expected value type of datetime, received {value_expression.valuetype}")

        result, err = value_expression.datetimevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.datetimevalue: {err}")

        if result != datetime(2019, 1, 1, 0, 0, 0, 999000):
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2019-01-01 00:00:00.999, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateDiff(#2006-01-01 00:00:00#, #2008-12-31 00:00:00#, 'Year')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 2:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateDiff(#2006-01-01 00:00:00#, #2008-12-31 00:00:00#, 'month')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 35:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 35, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateDiff(#2006-01-01 00:00:00#, #2008-12-31 00:00:00#, 'DAY')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 1095:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 1095, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateDiff(#2006-01-01 00:00:00#, #2008-12-31 00:00:00#, 'Week')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 156:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 156, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateDiff(#2006-01-01 00:00:00#, #2008-12-31 00:00:00#, 'WeekDay')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 1095:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 1095, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateDiff(#2006-01-01 00:00:00#, #2008-12-31 00:00:00#, 'Hour')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 26280:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 26280, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateDiff(#2006-01-01 00:00:00#, #2008-12-31 00:00:00#, 'Minute')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 1576800:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 1576800, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateDiff(#2006-01-01 00:00:00#, #2008-12-31 00:00:00#, 'Second')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 94608000:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 94608000, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DateDiff(#2008-12-30 00:02:50.546#, '2008-12-31', 'Millisecond')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 86229454:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 86229454, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#2019-02-04 03:00:52.73-05:00#, 'DayOfyear')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 35:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 35, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#2019-02-04 03:00:52.73-05:00#, 'Week')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 6:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 6, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "DatePart(#2019-02-04 03:00:52.73-05:00#, 'WeekDay')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_basic_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.int32value: {err}")

        if result != 2:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 2, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(
            datarow, "IsDate(#2019-02-04 03:00:52.73-05:00#) AND IsDate('2/4/2019') ANd isdate(updatedon) && !ISDATE(2.5) && !IsDate('ImNotADate')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(
            datarow, "IsInteger(32768) AND IsInteger('1024') and ISinTegeR(FaLsE) And isinteger(accessID) && !ISINTEGER(2.5) && !IsInteger('ImNotAnInteger')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(
            datarow, "IsGuid({9448a8b5-35c1-4dc7-8c42-8712153ac08a}) AND IsGuid('9448a8b5-35c1-4dc7-8c42-8712153ac08a') anD isGuid(9448a8b5-35c1-4dc7-8c42-8712153ac08a) AND IsGuid(Convert(9448a8b5-35c1-4dc7-8c42-8712153ac08a, 'string')) aND isguid(nodeID) && !ISGUID(2.5) && !IsGuid('ImNotAGuid')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(
            datarow, "IsNumeric(32768) && isNumeric(123.456e-67) AND IsNumeric(3.14159265) and ISnumeric(true) AND IsNumeric('1024' ) and IsNumeric(2.5) aNd isnumeric(longitude) && !ISNUMERIC(9448a8b5-35c1-4dc7-8c42-8712153ac08a) && !IsNumeric('ImNotNumeric')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.BOOLEAN:
            self.fail(f"test_basic_expressions: expected value type of boolean, received {value_expression.valuetype}")

        result, err = value_expression.booleanvalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.booleanvalue: {err}")

        if not result:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected true, received {result}")

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "Convert(maxof(12, '99.9', 99.99), 'Double')")

        if err is not None:
            self.fail(f"test_basic_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.DOUBLE:
            self.fail(f"test_basic_expressions: expected value type of double, received {value_expression.valuetype}")

        result, err = value_expression.doublevalue()

        if err is not None:
            self.fail(f"test_basic_expressions: error executing value_expression.doublevalue: {err}")

        if result != 99.99:
            self.fail(f"test_basic_expressions: unexpected value expression result, expected 99.99, received {result}")

    def test_negative_expressions(self):
        _, err = FilterExpressionParser.evaluate_expression("Convert(123, 'unknown')")

        if err is None:
            self.fail("test_negative_expressions: expected error executing FilterExpressionParser.evaluate_expression")

        _, err = FilterExpressionParser.evaluate_expression("I-Am-A-bad-Expression")

        if err is None:
            self.fail("test_negative_expressions: expected error executing FilterExpressionParser.evaluate_expression")

    def test_misc_expressions(self):
        with open("test/MetadataSample2.xml", "rb") as binary_file:
            data = binary_file.read()

        dataset, err = DataSet.from_xml(data)

        if err is not None:
            self.fail(f"test_misc_expressions: error executing DataSet.from_xml: {err}")

        datarow = dataset["DeviceDetail"][0]

        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "AccessID ^ 2 + FramesPerSecond XOR 4")

        if err is not None:
            self.fail(f"test_misc_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_misc_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_misc_expressions: error executing value_expression.int32value: {err}")

        if result != 38:
            self.fail(f"test_misc_expressions: unexpected value expression result, expected 38, received {result}")

        # test edge case of evaluating standalone Guid not used as a row identifier
        g = uuid1()
        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, str(g))

        if err is not None:
            self.fail(f"test_misc_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.GUID:
            self.fail(f"test_misc_expressions: expected value type of guid, received {value_expression.valuetype}")

        result, err = value_expression.guidvalue()

        if err is not None:
            self.fail(f"test_misc_expressions: error executing value_expression.guidvalue: {err}")

        if result != g:
            self.fail(f"test_misc_expressions: unexpected value expression result, expected {g}, received {result}")

        # test computed column with expression defined in schema
        value_expression, err = FilterExpressionParser.evaluate_datarowexpression(datarow, "ComputedCol")

        if err is not None:
            self.fail(f"test_misc_expressions: error executing FilterExpressionParser.evaluate_datarowexpression: {err}")

        if value_expression.valuetype != ExpressionValueType.INT32:
            self.fail(f"test_misc_expressions: expected value type of int32, received {value_expression.valuetype}")

        result, err = value_expression.int32value()

        if err is not None:
            self.fail(f"test_misc_expressions: error executing value_expression.int32value: {err}")

        if result != 32:
            self.fail(f"test_misc_expressions: unexpected value expression result, expected 32, received {result}")
