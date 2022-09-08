# ******************************************************************************************************
#  filterexpressionparser.py - Gbtc
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
#  09/07/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from gsf import Empty
from dataset import DataSet
from datatable import DataTable
from datarow import DataRow
from .tableidfields import TableIDFields, DEFAULT_TABLEIDFIELDS
from .expressiontree import ExpressionTree
from .callbackerrorlistener import CallbackErrorListener
from .parser.FilterExpressionSyntaxListener import FilterExpressionSyntaxListener
from .parser.FilterExpressionSyntaxLexer import FilterExpressionSyntaxLexer
from .parser.FilterExpressionSyntaxParser import FilterExpressionSyntaxParser
from antlr4 import CommonTokenStream, InputStream, ParseTreeWalker
from antlr4.ParserRuleContext import ParserRuleContext
from typing import Callable, Dict, List, Optional, Set, Tuple, Union
from datetime import datetime, timedelta, timezone
from uuid import UUID
import numpy as np


class FilterExpressionParser(FilterExpressionSyntaxListener):
    """
    Represents a parser for STTP filter expressions.
    """

    def __init__(self,
                 filterexpression: str,
                 suppress_consoleerroroutput: bool = False,
                 ):
        self._inputstream = InputStream(filterexpression)
        self._lexer = FilterExpressionSyntaxLexer(self._inputstream)
        self._tokens = CommonTokenStream(self._lexer)
        self._parser = FilterExpressionSyntaxParser(self._tokens)
        self._errorlistener: CallbackErrorListener = CallbackErrorListener()

        self._filtered_rows: List[DataRow] = []
        self._filtered_rowset: Set[DataRow] = {}

        self._filtered_signalids: List[UUID] = []
        self._filtered_signalidset: Set[UUID] = {}

        self._filterexpression_statementcount: int = 0

        self._active_expressiontree: Optional[ExpressionTree] = None
        self._expressiontrees: List[ExpressionTree] = []
        self._expressions: Dict[ParserRuleContext, ExpressionTree] = {}

        self.dataset: DataSet = None
        """
        Defines the source metadata used for parsing the filter expression.
        """

        self.primarytablename: str = Empty.STRING
        """
        Defines the name of the table to use in the DataSet when filter expressions do not specify
        a table name, e.g., direct signal identification. See:
        https://sttp.github.io/documentation/filter-expressions/#direct-signal-identification
        """

        self.tableidfields: Dict[str, TableIDFields] = {}
        """
        Defines a map of table ID fields associated with table names.
        """

        self.track_filteredrows = False
        """
        Defines a flag that enables tracking of matching rows during filter expression evaluation.
        Value defaults to True. Set value to False and set `track_filteredsignalids` to True if
        only signal IDs are needed post filter expression evaluation.
        """

        self.track_filteredsignalids = False
        """
        Defines a flag that enables tracking of matching signal IDs during filter expression evaluation.
        """

        if suppress_consoleerroroutput:
            self._parser.removeErrorListeners()

        self._parser.addErrorListener(self._errorlistener)

    @staticmethod
    def FromDataSet(dataset: DataSet,
                    filterexpression: str,
                    primarytable: str,
                    tableidfields: Dict[str, TableIDFields],
                    suppress_consoleerroroutput: bool = False,
                    ) -> Tuple["FilterExpressionParser", Optional[Exception]]:
        """
        Creates a new filter expression parser associated with the provided dataSet
        and provided table details. Error will be returned if dataSet parameter is
        None or the filterExpression is empty.
        """

        if dataset is None:
            return None, ValueError("DataSet in None")

        if not filterexpression:
            return None, ValueError("Filter expression is empty")

        parser = FilterExpressionParser(filterexpression, suppress_consoleerroroutput)
        parser.dataset = dataset

        if primarytable:
            parser.primarytablename = primarytable

            if tableidfields is None:
                parser.tableidfields[primarytable] = DEFAULT_TABLEIDFIELDS
            else:
                parser.tableidfields[primarytable] = tableidfields

        return parser, None

    def set_parsingexception_callback(self, callback: Callable[[str], None]):
        """
        Registers a callback for receiving parsing exception messages.
        """

        self._errorlistener.parsingexception_callback = callback

    def expressiontrees(self) -> Tuple[List[ExpressionTree], Optional[Exception]]:
        """
        Returns the list of expression trees parsed from the filter expression.
        """

        if len(self._expressiontrees) == 0:
            err = self.visit_parsetreenodes()

            if err is not None:
                return None, err

        return self._expressiontrees

    def filtered_rows(self) -> List[DataRow]:
        """
        Gets the rows matching the parsed filter expression.
        Results could contain duplicates if source `DataSet` has duplicated rows.
        """

        return self._filtered_rows

    def filtered_rowset(self) -> Set[DataRow]:
        """
        Gets the unique row set matching the parsed filter expression.
        """

        return self._filtered_rowset

    def filtered_signalids(self) -> List[UUID]:
        """
        Gets the Guid-based signal IDs matching the parsed filter expression.
        Results could contain duplicates if source `DataSet` has duplicated rows.
        """

        return self._filtered_signalids

    def filtered_signalidset(self) -> Set[UUID]:
        """
        Gets the unique Guid-based signal ID set matching the parsed filter expression.
        """

        self._initialize_set_operations()

        return self._filtered_signalidset

    def filterexpression_statementcount(self) -> int:
        """
        Gets the number of filter expression statements encountered while parsing.
        """

        return self._filterexpression_statementcount

    def table(self, tablename: str) -> Tuple[Optional[DataTable], Optional[Exception]]:
        """
        Gets the DataTable for the specified tableName from the FilterExpressionParser DataSet.
        An error will be returned if no DataSet has been defined or the tableName cannot be found.
        """

        if self.dataset is None:
            return None, ValueError("no DataSet has been defined")

        table = self.dataset.table(tablename)

        if table is None:
            return None, ValueError(f"failed to find table \"{tablename}\" in DataSet")

        return table, None

    def evaluate(self, applylimit: bool, applysort: bool) -> Optional[Exception]:
        """
        Evaluate parses each statement in the filter expression and tracks the results.
        Filter expressions can contain multiple statements, separated by semi-colons,
        where each statement results in a unique expression tree; this function returns
        the combined results of each encountered filter expression statement, yielding
        all filtered rows and/or signal IDs that match the target filter expression.
        The `applylimit` and `applysort` flags determine if any encountered "TOP" limit
        and "ORDER BY" sorting clauses will be respected. Access matching results via
        `filtered_rows` and/or `filtered_signalids`, or related set functions. An error
        will be returned if expression fails to parse or any row expression evaluation fails.
        """

        if self.dataset is None:
            return None, ValueError("no DataSet has been defined")

        if not self.track_filteredrows and not self.track_filteredsignalids:
            return None, ValueError("no use in evaluating filter expression, neither filtered rows nor signal IDs have been set for tracking")

        self._filterexpression_statementcount = 0
        self._filtered_rows = []
        self._filtered_rowset = {}
        self._filtered_signalids = []
        self._filtered_signalidset = {}
        self._expressiontrees = []
        self._expressions = {}

        # Visiting tree nodes will automatically add literals to the the filtered results
        err = self.visit_parsetreenodes()

        if err is not None:
            return err

        # Each statement in the filter expression will have its own expression tree, evaluate each
        for expressiontree in self._expressiontrees:
            tablename = expressiontree.tablename

            if len(tablename) == 0:
                if len(self.primarytablename) == 0:
                    return ValueError("no table name defined for expression tree nor is any PrimaryTableName defined")

                tablename = self.primarytablename

            table, err = self.table(tablename)

            if err is not None:
                return err

            signalid_columnindex = -1
