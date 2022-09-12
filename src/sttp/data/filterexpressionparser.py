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

from gsf import Convert, Empty, Limits
from .dataset import DataSet
from .datatable import DataTable
from .datarow import DataRow
from .tableidfields import TableIDFields, DEFAULT_TABLEIDFIELDS
from .expressiontree import ExpressionTree
from .callbackerrorlistener import CallbackErrorListener
from .expression import Expression
from .columnexpression import ColumnExpression
from .inlistexpression import InListExpression
from .functionexpression import FunctionExpression
from .operatorexpression import OperatorExpression
from .unaryexpression import UnaryExpression
from .valueexpression import ValueExpression, TRUEVALUE, FALSEVALUE, NULLVALUE
from .constants import ExpressionValueType, ExpressionUnaryType
from .constants import ExpressionFunctionType, ExpressionOperatorType
from .orderbyterm import OrderByTerm
from .errors import EvaluateError
from .parser.FilterExpressionSyntaxListener import FilterExpressionSyntaxListener as ExpressionListener
from .parser.FilterExpressionSyntaxLexer import FilterExpressionSyntaxLexer as ExpressionLexer
from .parser.FilterExpressionSyntaxParser import FilterExpressionSyntaxParser as ExpressionParser
from antlr4 import CommonTokenStream, InputStream, ParseTreeWalker
from antlr4.ParserRuleContext import ParserRuleContext
from typing import Callable, Dict, List, Optional, Set, Tuple
from decimal import Decimal
from datetime import datetime
from uuid import UUID


class FilterExpressionParser(ExpressionListener):
    """
    Represents a parser for STTP filter expressions.
    """

    def __init__(self,
                 filterexpression: str,
                 suppress_console_erroroutput: bool = False,
                 ):
        self._inputstream = InputStream(filterexpression)
        self._lexer = ExpressionLexer(self._inputstream)
        self._tokens = CommonTokenStream(self._lexer)
        self._parser = ExpressionParser(self._tokens)
        self._errorlistener: CallbackErrorListener = CallbackErrorListener()

        self._filtered_rows: List[DataRow] = []
        self._filtered_rowset: Optional[Set[DataRow]] = None

        self._filtered_signalids: List[UUID] = []
        self._filtered_signalidset: Optional[Set[UUID]] = None

        self._filterexpression_statementcount: int = 0

        self._active_expressiontree: Optional[ExpressionTree] = None
        self._expressiontrees: List[ExpressionTree] = []
        self._expressions: Dict[ParserRuleContext, Expression] = {}

        self.dataset: DataSet = None
        """
        Defines the source metadata used for parsing the filter expression.
        """

        self.primary_tablename: str = Empty.STRING
        """
        Defines the name of the table to use in the DataSet when filter expressions do not specify
        a table name, e.g., direct signal identification. See:
        https://sttp.github.io/documentation/filter-expressions/#direct-signal-identification
        """

        self.tableidfields_map: Dict[str, TableIDFields] = {}
        """
        Defines a map of table ID fields associated with table names.
        """

        self.track_filteredrows = True
        """
        Defines a flag that enables tracking of matching rows during filter expression evaluation.
        Value defaults to True. Set value to False and set `track_filteredsignalids` to True if
        only signal IDs are needed post filter expression evaluation.
        """

        self.track_filteredsignalids = False
        """
        Defines a flag that enables tracking of matching signal IDs during filter expression evaluation.
        """

        if suppress_console_erroroutput:
            self._parser.removeErrorListeners()

        self._parser.addErrorListener(self._errorlistener)

    @staticmethod
    def from_dataset(dataset: DataSet,
                     filterexpression: str,
                     primary_table: str,
                     tableidfields: Optional[TableIDFields] = None,
                     suppress_console_erroroutput: bool = False,
                     ) -> Tuple[Optional["FilterExpressionParser"], Optional[Exception]]:
        """
        Creates a new filter expression parser associated with the provided `dataSet`
        and provided table details. Error will be returned if `dataset` parameter is
        None or the `filterexpression` is empty.
        """

        if dataset is None:
            return None, ValueError("dataset parameter is None")

        if not filterexpression:
            return None, ValueError("filterexpression parameter is empty")

        parser = FilterExpressionParser(filterexpression, suppress_console_erroroutput)
        parser.dataset = dataset

        if primary_table:
            parser.primary_tablename = primary_table

            if tableidfields is None:
                parser.tableidfields_map[primary_table] = DEFAULT_TABLEIDFIELDS
            else:
                parser.tableidfields_map[primary_table] = tableidfields

        return parser, None

    def set_parsingexception_callback(self, callback: Callable[[str], None]):
        """
        Registers a callback for receiving parsing exception messages.
        """

        self._errorlistener.parsingexception_callback = callback

    @property
    def expressiontrees(self) -> Tuple[List[ExpressionTree], Optional[Exception]]:
        """
        Returns the list of expression trees parsed from the filter expression.
        """

        if len(self._expressiontrees) == 0:
            err = self._visit_parsetreenodes()

            if err is not None:
                return None, err

        return self._expressiontrees, None

    @property
    def filtered_rows(self) -> List[DataRow]:
        """
        Gets the rows matching the parsed filter expression.

        Results could contain duplicates if source `DataSet` has duplicated rows.
        """

        return self._filtered_rows

    @property
    def filtered_rowset(self) -> Set[DataRow]:
        """
        Gets the unique row set matching the parsed filter expression.
        """

        self._initialize_set_operations()
        return self._filtered_rowset

    @property
    def filtered_signalids(self) -> List[UUID]:
        """
        Gets the Guid-based signal IDs matching the parsed filter expression.

        Results could contain duplicates if source `DataSet` has duplicated rows.
        """

        return self._filtered_signalids

    @property
    def filtered_signalidset(self) -> Set[UUID]:
        """
        Gets the unique Guid-based signal ID set matching the parsed filter expression.
        """

        self._initialize_set_operations()
        return self._filtered_signalidset

    @property
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

        Filter expressions can contain multiple statements, separated by semi-colons, where each
        statement results in a unique expression tree; this function returns the combined results
        of each encountered filter expression statement, yielding all filtered rows and/or signal
        IDs that match the target filter expression. The `applylimit` and `applysort` flags
        determine if any encountered "TOP" limit and "ORDER BY" sorting clauses will be respected.
        Access matching results via `filtered_rows` and/or `filtered_signalids`, or related set
        functions. An error will be returned if expression fails to parse or any row expression
        evaluation fails.
        """

        if self.dataset is None:
            return None, ValueError("no DataSet has been defined")

        if not self.track_filteredrows and not self.track_filteredsignalids:
            return None, ValueError("no use in evaluating filter expression, neither filtered rows nor signal IDs have been set for tracking")

        self._filterexpression_statementcount = 0
        self._filtered_rows = []
        self._filtered_rowset = None
        self._filtered_signalids = []
        self._filtered_signalidset = None
        self._expressiontrees = []
        self._expressions = {}

        # Visiting tree nodes will automatically add literals to the the filtered results
        err = self._visit_parsetreenodes()

        if err is not None:
            return err

        # Each statement in the filter expression will have its own expression tree, evaluate each
        for expressiontree in self._expressiontrees:
            tablename = expressiontree.tablename

            if len(tablename) == 0:
                if len(self.primary_tablename) == 0:
                    return ValueError("no table name defined for expression tree nor is any PrimaryTableName defined")

                tablename = self.primary_tablename

            table, err = self.table(tablename)

            if err is not None:
                return err

            def where_predicate(result_expression: ValueExpression) -> Tuple[bool, Optional[Exception]]:
                if result_expression.valuetype == ExpressionValueType.BOOLEAN:
                    return result_expression._booleanvalue(), None

                # Filtered results will already have any matched literals
                return FALSEVALUE._booleanvalue(), None

            # Select all matching boolean results from expression tree evaluated for each table row
            matchedrows, err = expressiontree.selectwhere(table, where_predicate, applylimit, applysort)

            if err is not None:
                return err

            signalid_columnindex = -1

            if self.track_filteredsignalids:
                primary_tableidfields = self.tableidfields_map.get(table.name)

                if primary_tableidfields is None:
                    return EvaluateError(f"failed to find ID fields record for table \"{table.name}\"")

                signalid_column = table.column_byname(primary_tableidfields.signalid_fieldname)

                if signalid_column is None:
                    return EvaluateError(f"failed to find signal ID column \"{primary_tableidfields.signalid_fieldname}\" in table \"{table.name}\"")

                signalid_columnindex = signalid_column.index

            for matchedrow in matchedrows:
                self._add_matchedrow(matchedrow, signalid_columnindex)

        return None

    def _visit_parsetreenodes(self) -> Optional[Exception]:
        err: Optional[Exception] = None

        try:
            # Create a parse tree and start visiting listener methods
            walker = ParseTreeWalker()
            parsetree = self._parser.parse()
            walker.walk(self, parsetree)
        except Exception as ex:
            err = ex

        return err

    def _initialize_set_operations(self):
        # As an optimization, set operations are not engaged until second filter expression statement
        # is encountered, only then will duplicate results be a concern. Note that only using a set
        # is not an option because results can be sorted with the "ORDER BY" clause.

        if self.track_filteredrows and self._filtered_rowset is None:
            self._filtered_rowset = set(self._filtered_rows)

        if self.track_filteredsignalids and self._filtered_signalidset is None:
            self._filtered_signalidset = set(self._filtered_signalids)

    def _add_matchedrow(self, matchedrow: DataRow, signalid_columnindex: int):
        if self._filterexpression_statementcount > 1:
            # Set operations
            if self.track_filteredrows:
                startlen = len(self._filtered_rowset)
                self._filtered_rowset.add(matchedrow)

                if len(self._filtered_rowset) > startlen:
                    self._filtered_rows.append(matchedrow)

            if self.track_filteredsignalids:
                signalidfield, null, err = matchedrow.guidvalue(signalid_columnindex)

                if not null and err is None and signalidfield != Empty.GUID:
                    startlen = len(self._filtered_signalidset)
                    self._filtered_signalidset.add(signalidfield)

                    if len(self._filtered_signalidset) > startlen:
                        self._filtered_signalids.append(signalidfield)
        else:
            # Vector only operations
            if self.track_filteredrows:
                self._filtered_rows.append(matchedrow)

            if self.track_filteredsignalids:
                signalidfield, null, err = matchedrow.guidvalue(signalid_columnindex)

                if not null and err is None and signalidfield != Empty.GUID:
                    self._filtered_signalids.append(signalidfield)

    def _map_matchedfieldrow(self, primarytable: DataTable, columnname: str, matchvalue: str, signalid_columnindex: int):
        column = primarytable.column_byname(columnname)

        if column is None:
            return

        matchvalue = matchvalue.upper()
        columnindex = column.index

        for row in primarytable:
            if row is None:
                continue

            value, null, err = row.stringvalue(columnindex)

            if not null and err is None and value.upper() == matchvalue:
                self._add_matchedrow(row, signalid_columnindex)
                return

    def _get_expr(self, ctx: ParserRuleContext) -> Optional[Expression]:
        return self._expressions.get(ctx)

    def _add_expr(self, ctx: ParserRuleContext, expression: Expression):
        # Track expression in parser rule context map
        self._expressions[ctx] = expression

        # Update active expression tree root
        self._active_expressiontree.root = expression

    #    filterExpressionStatement
    #     : identifierStatement
    #     | filterStatement
    #     | expression
    #     ;
    def enterFilterExpressionStatement(self, ctx: ExpressionParser.FilterExpressionStatementContext):
        # One filter expression can contain multiple filter statements separated by semi-colon,
        # so we track each as an independent expression tree

        self._expressions = {}
        self._active_expressiontree = None
        self._filterexpression_statementcount += 1

        # Encountering second filter expression statement necessitates the use of set operations
        # to prevent possible result duplications
        if self._filterexpression_statementcount == 2:
            self._initialize_set_operations()

    #    filterStatement
    #     : K_FILTER ( K_TOP topLimit )? tableName K_WHERE expression ( K_ORDER K_BY orderingTerm ( ',' orderingTerm )* )?
    #     ;

    #    topLimit
    #     : ( '-' | '+' )? INTEGER_LITERAL
    #     ;

    #    orderingTerm
    #     : exactMatchModifier? columnName ( K_ASC | K_DESC )?
    #     ;
    def enterFilterStatement(self, ctx: ExpressionParser.FilterStatementContext):
        tablename: str = ctx.tableName().getText()

        table, err = self.table(tablename)

        if err is not None:
            raise EvaluateError(f"cannot parse filter expression statement, {err}")

        self._active_expressiontree = ExpressionTree()
        self._active_expressiontree.tablename = tablename
        self._expressiontrees.append(self._active_expressiontree)

        if ctx.K_TOP() is not None:
            self._active_expressiontree.toplimit = int(ctx.topLimit().getText())

        if ctx.K_ORDER() is not None and ctx.K_BY() is not None:
            orderingterms = ctx.orderingTerm()

            for i in range(len(orderingterms)):
                orderingterm: ExpressionParser.OrderingTermContext = orderingterms[i]
                orderby_columnname: str = orderingterm.orderByColumnName().getText()
                orderby_column = table.column_byname(orderby_columnname)

                if orderby_column is None:
                    raise EvaluateError(f"cannot parse filter expression statement, failed to find order by field \"{orderby_columnname}\" for table \"{table.name}\"")

                self._active_expressiontree.orderbyterms.append(OrderByTerm(
                    orderby_column,
                    orderingterm.K_DESC() is None,
                    orderingterm.exactMatchModifier() is not None))

    #    identifierStatement
    #     : GUID_LITERAL
    #     | MEASUREMENT_KEY_LITERAL
    #     | POINT_TAG_LITERAL
    #     ;
    def exitIdentifierStatement(self, ctx: ExpressionParser.IdentifierStatementContext):
        # sourcery skip
        signalid = Empty.GUID

        if ctx.GUID_LITERAL() is not None:
            signalid = FilterExpressionParser._parse_guidliteral(ctx.GUID_LITERAL().getText())

            if not self.track_filteredrows and not self.track_filteredsignalids:
                # Handle edge case of encountering standalone Guid when not tracking rows or table identifiers.
                # In this scenario the filter expression parser would only be used to generate expression trees
                # for general expression parsing, e.g., for a DataColumn expression, so here the Guid should be
                # treated as a literal expression value instead of an identifier to track:
                self.enterExpression(None)
                self._active_expressiontree.root = ValueExpression(ExpressionValueType.GUID, signalid)
                return

            if self.track_filteredsignalids and signalid != Empty.GUID:
                if self._filterexpression_statementcount > 1:
                    startlen = len(self._filtered_signalidset)
                    self._filtered_signalidset.add(signalid)

                    if len(self._filtered_signalidset) > startlen:
                        self._filtered_signalids.append(signalid)
                else:
                    self._filtered_signalids.append(signalid)

            if not self.track_filteredrows:
                return

        if self.dataset is None:
            return

        primary_table = self.dataset[self.primary_tablename]

        if primary_table is None:
            return

        primary_tableidfields = self.tableidfields_map.get(self.primary_tablename)

        if primary_tableidfields is None:
            return

        signalidcolumn = primary_table.column_byname(primary_tableidfields.signalid_fieldname)

        if signalidcolumn is None:
            return

        signalid_columnindex = signalidcolumn.index

        if self.track_filteredrows and signalid != Empty.GUID:
            # Map matching row for manually specified Guid
            for row in primary_table:
                if row is None:
                    continue

                value, null, err = row.guidvalue(signalid_columnindex)

                if not null and err is None and value == signalid:
                    if self.filterexpression_statementcount > 1:
                        startlen = len(self._filtered_rowset)
                        self._filtered_rowset.add(row)

                        if len(self._filtered_rowset) > startlen:
                            self._filtered_rows.append(row)
                    else:
                        self._filtered_rows.append(row)

                    return

            return

        if ctx.MEASUREMENT_KEY_LITERAL() is not None:
            self._map_matchedfieldrow(primary_table, primary_tableidfields.measurementkey_fieldname, ctx.MEASUREMENT_KEY_LITERAL().getText(), signalid_columnindex)
            return

        if ctx.POINT_TAG_LITERAL() is not None:
            self._map_matchedfieldrow(primary_table, primary_tableidfields.pointtag_fieldname, ctx.POINT_TAG_LITERAL().getText(), signalid_columnindex)
            return

    #    expression
    #     : notOperator expression
    #     | expression logicalOperator expression
    #     | predicateExpression
    #     ;
    def enterExpression(self, ctx: ExpressionParser.ExpressionContext):
        # Handle case of encountering a standalone expression, i.e., an expression not
        # within a filter statement context
        if self._active_expressiontree is None:
            self._active_expressiontree = ExpressionTree()
            self._expressiontrees.append(self._active_expressiontree)

    #    expression
    #     : notOperator expression
    #     | expression logicalOperator expression
    #     | predicateExpression
    #     ;
    def exitExpression(self, ctx: ExpressionParser.ExpressionContext):
        value: Optional[Expression] = None

        # Check for predicate expressions (see explicit visit function)
        predicate_expression: ExpressionParser.PredicateExpressionContext = ctx.predicateExpression()

        if predicate_expression is not None:
            value = self._get_expr(predicate_expression)

            if value is None:
                raise EvaluateError(f"failed to parse predicate expression \"{predicate_expression.getText()}\"")

            self._add_expr(ctx, value)
            return

        # Check for not operator expressions
        not_operator = ctx.notOperator()

        if not_operator is not None:
            expressions: List[ExpressionParser.ExpressionContext] = ctx.expression()

            if len(expressions) != 1:
                raise EvaluateError(f"not operator expression is malformed: \"{ctx.getText()}\"")

            value = self._get_expr(expressions[0])

            if value is None:
                raise EvaluateError(f"failed to find not operator expression \"{ctx.getText()}\"")

            self._add_expr(ctx, UnaryExpression(ExpressionUnaryType.NOT, value))
            return

        # Check for logical operator expressions
        logical_operator: ExpressionParser.LogicalOperatorContext = ctx.logicalOperator()

        if logical_operator is not None:
            expressions: List[ExpressionParser.ExpressionContext] = ctx.expression()

            if len(expressions) != 2:
                raise EvaluateError(f"operator expression, in logical operator expression context, is malformed: \"{ctx.getText()}\"")

            left = self._get_expr(expressions[0])

            if left is None:
                raise EvaluateError(f"failed to find left logical operator expression \"{ctx.getText()}\"")

            right = self._get_expr(expressions[1])

            if right is None:
                raise EvaluateError(f"failed to find right logical operator expression \"{ctx.getText()}\"")

            operator_symbol = logical_operator.getText()

            if logical_operator.K_AND() is not None or operator_symbol == "&&":
                operatortype = ExpressionOperatorType.AND
            elif logical_operator.K_OR() is not None or operator_symbol == "||":
                operatortype = ExpressionOperatorType.OR
            else:
                raise EvaluateError(f"unexpected logical operator \"{operator_symbol}\"")

            self._add_expr(ctx, OperatorExpression(operatortype, left, right))
            return

        raise EvaluateError(f"unexpected expression \"{ctx.getText()}\"")

    #    predicateExpression
    #     : predicateExpression notOperator? K_IN exactMatchModifier? '(' expressionList ')'
    #     | predicateExpression K_IS notOperator? K_NULL
    #     | predicateExpression comparisonOperator predicateExpression
    #     | predicateExpression notOperator? K_LIKE exactMatchModifier? predicateExpression
    #     | valueExpression
    #     ;
    def exitPredicateExpression(self, ctx: ExpressionParser.PredicateExpressionContext):
        # sourcery skip
        value_expression: ExpressionParser.ValueExpressionContext = ctx.valueExpression()

        # Check for value expressions (see explicit visit function)
        if value_expression is not None:
            value = self._get_expr(value_expression)

            if value is None:
                raise EvaluateError(f"failed to find value expression \"{value_expression.getText()}\"")

            self._add_expr(ctx, value)
            return

        has_notkeyword = ctx.notOperator() is not None
        exactmatch = ctx.exactMatchModifier() is not None

        # Check for IN expressions
        if ctx.K_IN() is not None:
            predicates: List[ExpressionParser.PredicateExpressionContext] = ctx.predicateExpression()

            # IN expression expects one predicate
            if len(predicates) != 1:
                raise EvaluateError(f"\"IN\" expression is malformed: \"{ctx.getText()}\"")

            value = self._get_expr(predicates[0])

            if value is None:
                raise EvaluateError(f"failed to find \"IN\" predicate expression \"{ctx.getText()}\"")

            expressionlist: ExpressionParser.ExpressionListContext = ctx.expressionList()
            expressions: List[ExpressionParser.ExpressionContext] = expressionlist.expression()
            argumentcount = len(expressions)

            if argumentcount < 1:
                raise EvaluateError("not enough expressions found for \"IN\" operation")

            arguments: List[Expression] = []

            for i in range(argumentcount):
                argument = self._get_expr(expressions[i])

                if argument is None:
                    raise EvaluateError(f"failed to find argument expression {i} \"{expressions[i].getText()}\" for \"IN\" operation")

                arguments.append(argument)

            self._add_expr(ctx, InListExpression(value, arguments, has_notkeyword, exactmatch))
            return

        # Check for IS NULL expressions
        if ctx.K_IS() is not None and ctx.K_NULL() is not None:
            if has_notkeyword:
                operatortype = ExpressionOperatorType.ISNOTNULL
            else:
                operatortype = ExpressionOperatorType.ISNULL

            predicates: List[ExpressionParser.PredicateExpressionContext] = ctx.predicateExpression()

            # IS NULL expression expects one predicate
            if len(predicates) != 1:
                raise EvaluateError(f"\"IS NULL\" expression is malformed: \"{ctx.getText()}\"")

            value = self._get_expr(predicates[0])

            if value is None:
                raise EvaluateError(f"failed to find \"IS NULL\" predicate expression \"{ctx.getText()}\"")

            self._add_expr(ctx, OperatorExpression(operatortype, value, None))
            return

        # Remaining operators require two predicate expressions
        predicates: List[ExpressionParser.PredicateExpressionContext] = ctx.predicateExpression()

        if len(predicates) != 2:
            raise EvaluateError(f"operator expression, in predicate expression context, is malformed: \"{ctx.getText()}\"")

        left = self._get_expr(predicates[0])

        if left is None:
            raise EvaluateError(f"failed to find left operator predicate expression \"{ctx.getText()}\"")

        right = self._get_expr(predicates[1])

        if right is None:
            raise EvaluateError(f"failed to find right operator predicate expression \"{ctx.getText()}\"")

        # Check for comparison operator expressions
        comparison_operator: ExpressionParser.ComparisonOperatorContext = ctx.comparisonOperator()

        if comparison_operator is not None:
            operatorsymbol = comparison_operator.getText()

            if operatorsymbol == "<":
                operatortype = ExpressionOperatorType.LESSTHAN
            elif operatorsymbol == "<=":
                operatortype = ExpressionOperatorType.LESSTHANOREQUAL
            elif operatorsymbol == ">":
                operatortype = ExpressionOperatorType.GREATERTHAN
            elif operatorsymbol == ">=":
                operatortype = ExpressionOperatorType.GREATERTHANOREQUAL
            elif operatorsymbol in ["=", "=="]:
                operatortype = ExpressionOperatorType.EQUAL
            elif operatorsymbol == "===":
                operatortype = ExpressionOperatorType.EQUALEXACTMATCH
            elif operatorsymbol in ["!=", "<>"]:
                operatortype = ExpressionOperatorType.NOTEQUAL
            elif operatorsymbol == "!==":
                operatortype = ExpressionOperatorType.NOTEQUALEXACTMATCH
            else:
                raise EvaluateError(f"unexpected comparison operator \"{operatorsymbol}\"")

            self._add_expr(ctx, OperatorExpression(operatortype, left, right))
            return

        # Check for LIKE expressions
        if ctx.K_LIKE() is not None:
            if exactmatch:
                operatortype = ExpressionOperatorType.NOTLIKEEXACTMATCH if has_notkeyword is None else ExpressionOperatorType.LIKEEXACTMATCH
            else:
                operatortype = ExpressionOperatorType.NOTLIKE if has_notkeyword is None else ExpressionOperatorType.LIKE

            self._add_expr(ctx, OperatorExpression(operatortype, left, right))
            return

        raise EvaluateError(f"unexpected predicate expression \"{ctx.getText()}\"")

    #    valueExpression
    #     : literalValue
    #     | columnName
    #     | functionExpression
    #     | unaryOperator valueExpression
    #     | '(' expression ')'
    #     | valueExpression mathOperator valueExpression
    #     | valueExpression bitwiseOperator valueExpression
    # 	  ;
    def exitValueExpression(self, ctx: ExpressionParser.ValueExpressionContext):
        # sourcery skip
        literal_value: ExpressionParser.LiteralValueContext = ctx.literalValue()

        # Check for literal values (see explicit visit function)
        if literal_value is not None:
            value = self._get_expr(literal_value)

            if value is None:
                raise EvaluateError(f"failed to find literal value \"{literal_value.getText()}\"")

            self._add_expr(ctx, value)
            return

        # Check for column names (see explicit visit function)
        columnname: ExpressionParser.ColumnNameContext = ctx.columnName()

        if columnname is not None:
            value = self._get_expr(columnname)

            if value is None:
                raise EvaluateError(f"failed to find column name \"{columnname.getText()}\"")

            self._add_expr(ctx, value)
            return

        # Check for function expressions (see explicit visit function)
        function_expression: ExpressionParser.FunctionExpressionContext = ctx.functionExpression()

        if function_expression is not None:
            value = self._get_expr(function_expression)

            if value is None:
                raise EvaluateError(f"failed to find function expression \"{function_expression.getText()}\"")

            self._add_expr(ctx, value)
            return

        # Check for unary operators
        unary_operator: ExpressionParser.UnaryOperatorContext = ctx.unaryOperator()

        if unary_operator is not None:
            values: List[ExpressionParser.ValueExpressionContext] = ctx.valueExpression()

            # Unary operator expects one value expression
            if len(values) != 1:
                raise EvaluateError(f"unary operator value expression is malformed: \"{ctx.getText()}\"")

            value = self._get_expr(values[0])

            if value is None:
                raise EvaluateError(f"failed to find unary operator value expression \"{ctx.getText()}\"")

            if unary_operator.K_NOT() is None:
                operatorsymbol = unary_operator.getText()

                if operatorsymbol == "+":
                    unarytype = ExpressionUnaryType.PLUS
                elif operatorsymbol == "-":
                    unarytype = ExpressionUnaryType.MINUS
                elif operatorsymbol in ["~", "!"]:
                    unarytype = ExpressionUnaryType.NOT
                else:
                    raise EvaluateError(f"unexpected unary type \"{operatorsymbol}\"")
            else:
                unarytype = ExpressionUnaryType.NOT

            self._add_expr(ctx, UnaryExpression(unarytype, value))
            return

        # Check for sub-expressions, i.e., "(" expression ")"
        expression: ExpressionParser.ExpressionContext = ctx.expression()

        if expression is not None:
            value = self._get_expr(expression)

            if value is None:
                raise EvaluateError(f"failed to find sub-expression \"{ctx.getText()}\"")

            self._add_expr(ctx, value)
            return

        # Remaining operators require two value expressions
        values: List[ExpressionParser.ValueExpressionContext] = ctx.valueExpression()

        if len(values) != 2:
            raise EvaluateError(f"operator expression, in value expression context, is malformed: \"{ctx.getText()}\"")

        left = self._get_expr(values[0])

        if left is None:
            raise EvaluateError(f"failed to find left operator value expression \"{ctx.getText()}\"")

        right = self._get_expr(values[1])

        if right is None:
            raise EvaluateError(f"failed to find right operator value expression \"{ctx.getText()}\"")

        # Check for math operator expressions
        math_operator: ExpressionParser.MathOperatorContext = ctx.mathOperator()

        if math_operator is not None:
            operatorsymbol = math_operator.getText()

            if operatorsymbol == "+":
                operatortype = ExpressionOperatorType.ADD
            elif operatorsymbol == "-":
                operatortype = ExpressionOperatorType.SUBTRACT
            elif operatorsymbol == "*":
                operatortype = ExpressionOperatorType.MULTIPLY
            elif operatorsymbol == "/":
                operatortype = ExpressionOperatorType.DIVIDE
            elif operatorsymbol == "%":
                operatortype = ExpressionOperatorType.MODULUS
            else:
                raise EvaluateError(f"unexpected math operator \"{operatorsymbol}\"")

            self._add_expr(ctx, OperatorExpression(operatortype, left, right))
            return

        # Check for bitwise operator expressions
        bitwise_operator: ExpressionParser.BitwiseOperatorContext = ctx.bitwiseOperator()

        if bitwise_operator is not None:
            # Check for bitwise operators
            if bitwise_operator.K_XOR() is None:
                operatorsymbol = bitwise_operator.getText()

                if operatorsymbol == "&":
                    operatortype = ExpressionOperatorType.BITWISEAND
                elif operatorsymbol == "|":
                    operatortype = ExpressionOperatorType.BITWISEOR
                elif operatorsymbol == "^":
                    operatortype = ExpressionOperatorType.BITWISEXOR
                elif operatorsymbol == "<<":
                    operatortype = ExpressionOperatorType.BITSHIFTLEFT
                elif operatorsymbol == ">>":
                    operatortype = ExpressionOperatorType.BITSHIFTRIGHT
                else:
                    raise EvaluateError(f"unexpected bitwise operator \"{operatorsymbol}\"")
            else:
                operatortype = ExpressionOperatorType.BITWISEXOR

            self._add_expr(ctx, OperatorExpression(operatortype, left, right))
            return

        raise EvaluateError(f"unexpected value expression \"{ctx.getText()}\"")

    #    literalValue
    #     : INTEGER_LITERAL
    #     | NUMERIC_LITERAL
    #     | STRING_LITERAL
    #     | DATETIME_LITERAL
    #     | GUID_LITERAL
    #     | BOOLEAN_LITERAL
    #     | K_NULL
    #     ;
    def exitLiteralValue(self, ctx: ExpressionParser.LiteralValueContext):
        result: Optional[ValueExpression] = None

        # Literal numeric values will not be negative, unary operators will handle negative values
        if ctx.INTEGER_LITERAL() is not None:
            literal: str = ctx.INTEGER_LITERAL().getText()

            try:
                value = Convert.from_str(literal, int)

                if value > Limits.MAXINT32:
                    if value > Limits.MAXINT64:
                        result = FilterExpressionParser._parse_numericliteral(literal)
                    else:
                        result = ValueExpression(ExpressionValueType.INT64, value)
                else:
                    result = ValueExpression(ExpressionValueType.INT32, value)
            except Exception:
                result = FilterExpressionParser._parse_numericliteral(literal)
        elif ctx.NUMERIC_LITERAL() is not None:
            literal: str = ctx.NUMERIC_LITERAL().getText()

            try:
                # Real literals using scientific notation are parsed as double
                if "E" in literal.upper():
                    result = ValueExpression(ExpressionValueType.DOUBLE, float(literal))
                else:
                    result = FilterExpressionParser._parse_numericliteral(literal)
            except Exception:
                result = FilterExpressionParser._parse_numericliteral(literal)
        elif ctx.STRING_LITERAL() is not None:
            result = ValueExpression(ExpressionValueType.STRING, FilterExpressionParser._parse_stringliteral(ctx.STRING_LITERAL().getText()))
        elif ctx.DATETIME_LITERAL() is not None:
            result = ValueExpression(ExpressionValueType.DATETIME, FilterExpressionParser._parse_datetimeliteral(ctx.DATETIME_LITERAL().getText()))
        elif ctx.GUID_LITERAL() is not None:
            result = ValueExpression(ExpressionValueType.GUID, FilterExpressionParser._parse_guidliteral(ctx.GUID_LITERAL().getText()))
        elif ctx.BOOLEAN_LITERAL() is not None:
            literal: str = ctx.BOOLEAN_LITERAL().getText()
            result = TRUEVALUE if literal.upper() == "TRUE" else FALSEVALUE
        elif ctx.K_NULL() is not None:
            result = NULLVALUE

        if result is not None:
            self._add_expr(ctx, result)

    @staticmethod
    def _parse_numericliteral(literal: str) -> ValueExpression:
        try:
            value = Decimal(literal)
            return ValueExpression(ExpressionValueType.DECIMAL, value)
        except Exception:
            try:
                value = float(literal)
                return ValueExpression(ExpressionValueType.DOUBLE, value)
            except Exception:
                return ValueExpression(ExpressionValueType.STRING, literal)

    @staticmethod
    def _parse_stringliteral(literal: str) -> str:
        # Remove any surrounding quotes from string, ANTLR grammar already
        # ensures strings starting with quote also ends with one
        return literal[1:-1] if literal[0] == "'" else literal

    @staticmethod
    def _parse_guidliteral(literal: str) -> UUID:
        # Remove any quotes from GUID (boost currently only handles optional braces),
        # ANTLR grammar already ensures GUID starting with quote also ends with one
        return UUID(literal[1:-1] if literal[0] in ["{", "'"] else literal)

    @staticmethod
    def _parse_datetimeliteral(literal: str) -> datetime:
        # Remove any surrounding '#' symbols from date/time, ANTLR grammar already
        # ensures date/time starting with '#' symbol will also end with one
        literal = literal[1:-1] if literal[0] == "#" else literal

        try:
            return Convert.from_str(literal, datetime)
        except Exception as ex:
            raise EvaluateError(f"failed to parse datetime literal #{literal}#: {ex}") from ex

    #    columnName
    #     : IDENTIFIER
    #     ;
    def exitColumnName(self, ctx: ExpressionParser.ColumnNameContext):
        tablename = self._active_expressiontree.tablename

        if tablename is None or len(tablename) == 0:
            if self.primary_tablename is None or len(self.primary_tablename) == 0:
                raise EvaluateError("cannot parse column name in filter expression, no table name defined for expression tree nor is any PrimaryTableName defined.")

            tablename = self.primary_tablename

        table, err = self.table(tablename)

        if err is not None:
            raise EvaluateError(f"cannot parse column name in filter expression, {err}")

        columnname: str = ctx.IDENTIFIER().getText()
        datacolumn = table.column_byname(columnname)

        if datacolumn is None:
            raise EvaluateError(f"cannot parse column name in filter expression, failed to find column \"{columnname}\" in table \"{tablename}\"")

        self._add_expr(ctx, ColumnExpression(datacolumn))

    #    functionExpression
    #     : functionName '(' expressionList? ')'
    #     ;
    def exitFunctionExpression(self, ctx: ExpressionParser.FunctionExpressionContext):
        # sourcery skip
        functionname: ExpressionParser.FunctionNameContext = ctx.functionName()

        if functionname.K_ABS() is not None:
            functiontype = ExpressionFunctionType.ABS
        elif functionname.K_CEILING() is not None:
            functiontype = ExpressionFunctionType.CEILING
        elif functionname.K_COALESCE() is not None:
            functiontype = ExpressionFunctionType.COALESCE
        elif functionname.K_CONVERT() is not None:
            functiontype = ExpressionFunctionType.CONVERT
        elif functionname.K_CONTAINS() is not None:
            functiontype = ExpressionFunctionType.CONTAINS
        elif functionname.K_DATEADD() is not None:
            functiontype = ExpressionFunctionType.DATEADD
        elif functionname.K_DATEDIFF() is not None:
            functiontype = ExpressionFunctionType.DATEDIFF
        elif functionname.K_DATEPART() is not None:
            functiontype = ExpressionFunctionType.DATEPART
        elif functionname.K_ENDSWITH() is not None:
            functiontype = ExpressionFunctionType.ENDSWITH
        elif functionname.K_FLOOR() is not None:
            functiontype = ExpressionFunctionType.FLOOR
        elif functionname.K_IIF() is not None:
            functiontype = ExpressionFunctionType.IIF
        elif functionname.K_INDEXOF() is not None:
            functiontype = ExpressionFunctionType.INDEXOF
        elif functionname.K_ISDATE() is not None:
            functiontype = ExpressionFunctionType.ISDATE
        elif functionname.K_ISINTEGER() is not None:
            functiontype = ExpressionFunctionType.ISINTEGER
        elif functionname.K_ISGUID() is not None:
            functiontype = ExpressionFunctionType.ISGUID
        elif functionname.K_ISNULL() is not None:
            functiontype = ExpressionFunctionType.ISNULL
        elif functionname.K_ISNUMERIC() is not None:
            functiontype = ExpressionFunctionType.ISNUMERIC
        elif functionname.K_LASTINDEXOF() is not None:
            functiontype = ExpressionFunctionType.LASTINDEXOF
        elif functionname.K_LEN() is not None:
            functiontype = ExpressionFunctionType.LEN
        elif functionname.K_LOWER() is not None:
            functiontype = ExpressionFunctionType.LOWER
        elif functionname.K_MAXOF() is not None:
            functiontype = ExpressionFunctionType.MAXOF
        elif functionname.K_MINOF() is not None:
            functiontype = ExpressionFunctionType.MINOF
        elif functionname.K_NOW() is not None:
            functiontype = ExpressionFunctionType.NOW
        elif functionname.K_NTHINDEXOF() is not None:
            functiontype = ExpressionFunctionType.NTHINDEXOF
        elif functionname.K_POWER() is not None:
            functiontype = ExpressionFunctionType.POWER
        elif functionname.K_REGEXMATCH() is not None:
            functiontype = ExpressionFunctionType.REGEXMATCH
        elif functionname.K_REGEXVAL() is not None:
            functiontype = ExpressionFunctionType.REGEXVAL
        elif functionname.K_REPLACE() is not None:
            functiontype = ExpressionFunctionType.REPLACE
        elif functionname.K_REVERSE() is not None:
            functiontype = ExpressionFunctionType.REVERSE
        elif functionname.K_ROUND() is not None:
            functiontype = ExpressionFunctionType.ROUND
        elif functionname.K_SPLIT() is not None:
            functiontype = ExpressionFunctionType.SPLIT
        elif functionname.K_SQRT() is not None:
            functiontype = ExpressionFunctionType.SQRT
        elif functionname.K_STARTSWITH() is not None:
            functiontype = ExpressionFunctionType.STARTSWITH
        elif functionname.K_STRCOUNT() is not None:
            functiontype = ExpressionFunctionType.STRCOUNT
        elif functionname.K_STRCMP() is not None:
            functiontype = ExpressionFunctionType.STRCMP
        elif functionname.K_SUBSTR() is not None:
            functiontype = ExpressionFunctionType.SUBSTR
        elif functionname.K_TRIM() is not None:
            functiontype = ExpressionFunctionType.TRIM
        elif functionname.K_TRIMLEFT() is not None:
            functiontype = ExpressionFunctionType.TRIMLEFT
        elif functionname.K_TRIMRIGHT() is not None:
            functiontype = ExpressionFunctionType.TRIMRIGHT
        elif functionname.K_UPPER() is not None:
            functiontype = ExpressionFunctionType.UPPER
        elif functionname.K_UTCNOW() is not None:
            functiontype = ExpressionFunctionType.UTCNOW
        else:
            raise EvaluateError(f"unknown function \"{functionname.getText()}\"")

        expressionlist: ExpressionParser.ExpressionListContext = ctx.expressionList()
        arguments: List[Expression] = []

        if expressionlist is not None:
            expressions: List[ExpressionParser.ExpressionContext] = expressionlist.expression()
            argumentcount = len(expressions)

            for i in range(argumentcount):
                argument = self._get_expr(expressions[i])

                if argument is None:
                    raise EvaluateError(f"failed to find argument expression {i} \"{expressions[i].getText()}\" for function \"{functionname.getText()}\"")

                arguments.append(argument)

        self._add_expr(ctx, FunctionExpression(functiontype, arguments))

    @staticmethod
    def generate_expressiontrees(dataset: DataSet, primarytable: str, filterexpression: str, suppress_console_erroroutput: bool = False) -> Tuple[Optional[List[ExpressionTree]], Optional[Exception]]:
        """
        Produces a set of expression trees for the provided `filterexpression` and `dataset`.

        One expression tree will be produced per filter expression statement encountered in the specified `filterexpression`.
        If `primarytable` parameter is not defined, then filter expression should not contain directly defined signal IDs.
        An error will be returned if `dataSet` parameter is None, the `filterexpression` is empty or expression fails to parse.
        """

        parser, err = FilterExpressionParser.from_dataset(dataset, filterexpression, primarytable, None, suppress_console_erroroutput)

        if err is not None:
            return None, err

        parser.track_filteredrows = False

        return parser.expressiontrees

    @staticmethod
    def generate_expressiontrees_fromtable(datatable: DataTable, filterexpression: str, suppress_console_erroroutput: bool = False) -> Tuple[Optional[List[ExpressionTree]], Optional[Exception]]:
        """
        Produces a set of expression trees for the provided `filterexpression` and `datatable`.

        One expression tree will be produced per filter expression statement encountered in the specified `filterexpression`.
        An error will be returned if `datatable` parameter is None, the `filterexpression` is empty or expression fails to parse.
        """

        if datatable is None:
            return None, ValueError("datatable parameter cannot be None")

        return FilterExpressionParser.generate_expressiontrees(datatable.parent, datatable.name, filterexpression, suppress_console_erroroutput)

    @staticmethod
    def generate_expressiontree(datatable: DataTable, filterexpression: str, suppress_console_erroroutput: bool = False) -> Tuple[Optional[ExpressionTree], Optional[Exception]]:
        """
        Gets the first produced expression tree for the provided `filterexpression` and `datatable`.

        If `filterexpression` contains multiple semi-colon separated statements, only the first expression is returned.
        An error will be returned if `datatable` parameter is None, the `filterexpression` is empty or expression fails to parse.
        """

        expressiontrees, err = FilterExpressionParser.generate_expressiontrees_fromtable(datatable, filterexpression, suppress_console_erroroutput)

        if err is not None:
            return None, err

        if expressiontrees is not None and len(expressiontrees) > 0:
            return expressiontrees[0], None

        return None, EvaluateError(f"no expression trees generated with filter expression \"{filterexpression}\" for table \"{datatable.name}\"")

    @staticmethod
    def evaluate_expression(filterexpression: str, suppress_console_erroroutput: bool = False) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        """
        Returns the result of the evaluated `filterexpression`.

        This expression evaluation function is only for simple expressions that do not reference any `DataSet` columns.
        Use `evaluate_datarowexpression` for evaluating filter expressions that contain column references.
        If `filterexpression` contains multiple semi-colon separated statements, only the first expression is evaluated.
        An error will be returned if the `filterexpression` is empty or expression fails to parse.
        """

        if filterexpression is None or not filterexpression:
            return None, EvaluateError("filterexpression is empty")

        parser = FilterExpressionParser(filterexpression, suppress_console_erroroutput)
        parser.track_filteredrows = False

        expressiontrees, err = parser.expressiontrees

        if err is not None:
            return None, err

        if expressiontrees is not None and len(expressiontrees) > 0:
            return expressiontrees[0].evaluate()

        return None, EvaluateError(f"no expression trees generated with filter expression \"{filterexpression}\"")

    @staticmethod
    def evaluate_datarowexpression(datarow: DataRow, filterexpression: str, suppress_console_erroroutput: bool = False) -> Tuple[Optional[ValueExpression], Optional[Exception]]:
        """
        Returns the result of the evaluated `filterexpression` using the specified `datarow`.

        If `filterexpression` contains multiple semi-colon separated statements, only the first expression is evaluated.
        An error will be returned if `datarow` parameter is None, the `filterexpression` is empty, expression fails to
        parse or row expression evaluation fails.
        """

        if datarow is None:
            return None, ValueError("datarow is None")

        if filterexpression is None or not filterexpression:
            return None, EvaluateError("filterexpression is empty")

        expressiontree, err = FilterExpressionParser.generate_expressiontree(datarow.parent, filterexpression, suppress_console_erroroutput)

        return (None, err) if err is not None else expressiontree.evaluate(datarow)

    @staticmethod
    def select_datarows(dataset: DataSet, filterexpression: str, primarytable: str, tableidfields: Optional[TableIDFields] = None, suppress_console_erroroutput: bool = False) -> Tuple[Optional[List[DataRow]], Optional[Exception]]:
        """
        Returns all rows matching the provided `filterexpression` and `dataset`.

        Filter expressions can contain multiple statements, separated by semi-colons, where each statement results in a
        unique expression tree; this function returns the combined results of each encountered filter expression statement.
        Returned `DataRow` list will contain all matching rows, order preserved. If `dataset` includes duplicated rows, it
        will be possible for the result set to contain duplicates. Any encountered "TOP" limit or "ORDER BY" clauses will
        be respected. An error will be returned if `dataset` parameter is None, the `filterexpression` is empty, expression
        fails to parse or any row expression evaluation fails.
        """

        parser, err = FilterExpressionParser.from_dataset(dataset, filterexpression, primarytable, tableidfields, suppress_console_erroroutput)

        if err is not None:
            return None, err

        err = parser.evaluate(True, True)

        return (None, err) if err is not None else (parser.filtered_rows, None)

    @staticmethod
    def select_datarows_fromtable(datatable: DataTable, filterexpression: str, tableidfields: Optional[TableIDFields] = None, suppress_console_erroroutput: bool = False) -> Tuple[Optional[List[DataRow]], Optional[Exception]]:
        """
        Returns all rows matching the provided `filterexpression` and `datatable`.

        Filter expressions can contain multiple statements, separated by semi-colons, where each statement results
        in a unique expression tree; this function returns the combined results of each encountered filter expression
        statement. Returned `DataRow` list will contain all matching rows, order preserved. If dataSet includes
        duplicated rows, it will be possible for the result set to contain duplicates. Any encountered "TOP" limit or
        "ORDER BY" clauses will be respected. An error will be returned if `datatable` parameter (or its parent DataSet)
        is None, the `filterexpression` is empty, expression fails to parse or any row expression evaluation fails.
        """

        if datatable is None:
            return None, ValueError("datatable is None")

        return FilterExpressionParser.select_datarows(datatable.parent, filterexpression, datatable.name, tableidfields, suppress_console_erroroutput)

    @staticmethod
    def select_datarowset(dataset: DataSet, filterexpression: str, primarytable: str, tableidfields: Optional[TableIDFields] = None, suppress_console_erroroutput: bool = False) -> Tuple[Optional[Set[DataRow]], Optional[Exception]]:
        """
        Returns all unique rows matching the provided `filterexpression` and `dataset`.

        Filter expressions can contain multiple statements, separated by semi-colons, where each statement results
        in a unique expression tree; this function returns the combined results of each encountered filter expression
        statement. Returned `DataRow` set will contain only unique rows, in arbitrary order. Any encountered "TOP" limit
        clauses for individual filter expression statements will be respected, but "ORDER BY" clauses will be ignored.
        An error will be returned if `dataset` parameter is None, the `filterexpression` is empty, expression fails to
        parse or any row expression evaluation fails.
        """

        parser, err = FilterExpressionParser.from_dataset(dataset, filterexpression, primarytable, tableidfields, suppress_console_erroroutput)

        if err is not None:
            return None, err

        err = parser.evaluate(True, False)

        return (None, err) if err is not None else (parser.filtered_rowset, None)

    @staticmethod
    def select_datarowset_fromtable(datatable: DataTable, filterexpression: str, tableidfields: Optional[TableIDFields] = None, suppress_console_erroroutput: bool = False) -> Tuple[Optional[Set[DataRow]], Optional[Exception]]:
        """
        Returns all unique rows matching the provided `filterexpression` and `datatable`.

        Filter expressions can contain multiple statements, separated by semi-colons, where each statement results
        in a unique expression tree; this function returns the combined results of each encountered filter expression
        statement. Returned `DataRow` set will contain only unique rows, in arbitrary order. Any encountered "TOP" limit
        clauses for individual filter expression statements will be respected, but "ORDER BY" clauses will be ignored.
        An error will be returned if `datatable` parameter (or its parent DataSet) is None, the `filterexpression` is
        empty, expression fails to parse or any row expression evaluation fails.
        """

        if datatable is None:
            return None, ValueError("datatable is None")

        return FilterExpressionParser.select_datarowset(datatable.parent, filterexpression, datatable.name, tableidfields, suppress_console_erroroutput)

    @staticmethod
    def select_signalidset(dataset: DataSet, filterexpression: str, primarytable: str, tableidfields: Optional[TableIDFields] = None, suppress_console_erroroutput: bool = False) -> Tuple[Optional[Set[UUID]], Optional[Exception]]:
        """
        Returns all unique signal IDs matching the provided `filterexpression` and `dataset`.

        Filter expressions can contain multiple statements, separated by semi-colons, where each statement results
        in a unique expression tree; this function returns the combined results of each encountered filter expression
        statement. Returned `UUID` set will contain only unique values, in arbitrary order. Any encountered "TOP" limit
        clauses for individual filter expression statements will be respected, but "ORDER BY" clauses will be ignored.
        An error will be returned if `dataset` parameter is None, the `filterexpression` is empty, expression fails to
        parse or any row expression evaluation fails.
        """

        parser, err = FilterExpressionParser.from_dataset(dataset, filterexpression, primarytable, tableidfields, suppress_console_erroroutput)

        if err is not None:
            return None, err

        parser.track_filteredrows = False
        parser.track_filteredsignalids = True

        err = parser.evaluate(True, False)

        return (None, err) if err is not None else (parser.filtered_signalidset, None)

    @staticmethod
    def select_signalidset_fromtable(datatable: DataTable, filterexpression: str, tableidfields: Optional[TableIDFields] = None, suppress_console_erroroutput: bool = False) -> Tuple[Optional[Set[UUID]], Optional[Exception]]:
        """
        Returns all unique signal IDs matching the provided `filterexpression` and `datatable`.

        Filter expressions can contain multiple statements, separated by semi-colons, where each statement results
        in a unique expression tree; this function returns the combined results of each encountered filter expression
        statement. Returned `UUID` set will contain only unique values, in arbitrary order. Any encountered "TOP" limit
        clauses for individual filter expression statements will be respected, but "ORDER BY" clauses will be ignored.
        An error will be returned if `datatable` parameter (or its parent DataSet) is None, the `filterexpression` is
        empty, expression fails to parse or any row expression evaluation fails.
        """

        if datatable is None:
            return None, ValueError("datatable is None")

        return FilterExpressionParser.select_signalidset(datatable.parent, filterexpression, datatable.name, tableidfields, suppress_console_erroroutput)
