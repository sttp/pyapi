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
from datarow import DataRow
from .tableidfields import TableIDFields
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

        self.track_filteredrows: bool = False
        """
        Defines a flag that enables tracking of matching rows during filter expression evaluation.
        Value defaults to True. Set value to False and set `track_filteredsignalids` to True if
        only signal IDs are needed post filter expression evaluation.
        """

        self.track_filteredsignalids: bool = False
        """
        Defines a flag that enables tracking of matching signal IDs during filter expression evaluation.
        """

        if suppress_consoleerroroutput:
            self._parser.removeErrorListeners()

        self._parser.addErrorListener(self._errorlistener)
