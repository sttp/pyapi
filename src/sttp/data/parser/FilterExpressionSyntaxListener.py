# Generated from FilterExpressionSyntax.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .FilterExpressionSyntaxParser import FilterExpressionSyntaxParser
else:
    from FilterExpressionSyntaxParser import FilterExpressionSyntaxParser

# This class defines a complete listener for a parse tree produced by FilterExpressionSyntaxParser.
class FilterExpressionSyntaxListener(ParseTreeListener):

    # Enter a parse tree produced by FilterExpressionSyntaxParser#parse.
    def enterParse(self, ctx:FilterExpressionSyntaxParser.ParseContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#parse.
    def exitParse(self, ctx:FilterExpressionSyntaxParser.ParseContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#err.
    def enterErr(self, ctx:FilterExpressionSyntaxParser.ErrContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#err.
    def exitErr(self, ctx:FilterExpressionSyntaxParser.ErrContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#filterExpressionStatementList.
    def enterFilterExpressionStatementList(self, ctx:FilterExpressionSyntaxParser.FilterExpressionStatementListContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#filterExpressionStatementList.
    def exitFilterExpressionStatementList(self, ctx:FilterExpressionSyntaxParser.FilterExpressionStatementListContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#filterExpressionStatement.
    def enterFilterExpressionStatement(self, ctx:FilterExpressionSyntaxParser.FilterExpressionStatementContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#filterExpressionStatement.
    def exitFilterExpressionStatement(self, ctx:FilterExpressionSyntaxParser.FilterExpressionStatementContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#identifierStatement.
    def enterIdentifierStatement(self, ctx:FilterExpressionSyntaxParser.IdentifierStatementContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#identifierStatement.
    def exitIdentifierStatement(self, ctx:FilterExpressionSyntaxParser.IdentifierStatementContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#filterStatement.
    def enterFilterStatement(self, ctx:FilterExpressionSyntaxParser.FilterStatementContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#filterStatement.
    def exitFilterStatement(self, ctx:FilterExpressionSyntaxParser.FilterStatementContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#topLimit.
    def enterTopLimit(self, ctx:FilterExpressionSyntaxParser.TopLimitContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#topLimit.
    def exitTopLimit(self, ctx:FilterExpressionSyntaxParser.TopLimitContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#orderingTerm.
    def enterOrderingTerm(self, ctx:FilterExpressionSyntaxParser.OrderingTermContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#orderingTerm.
    def exitOrderingTerm(self, ctx:FilterExpressionSyntaxParser.OrderingTermContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#expressionList.
    def enterExpressionList(self, ctx:FilterExpressionSyntaxParser.ExpressionListContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#expressionList.
    def exitExpressionList(self, ctx:FilterExpressionSyntaxParser.ExpressionListContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#expression.
    def enterExpression(self, ctx:FilterExpressionSyntaxParser.ExpressionContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#expression.
    def exitExpression(self, ctx:FilterExpressionSyntaxParser.ExpressionContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#predicateExpression.
    def enterPredicateExpression(self, ctx:FilterExpressionSyntaxParser.PredicateExpressionContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#predicateExpression.
    def exitPredicateExpression(self, ctx:FilterExpressionSyntaxParser.PredicateExpressionContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#valueExpression.
    def enterValueExpression(self, ctx:FilterExpressionSyntaxParser.ValueExpressionContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#valueExpression.
    def exitValueExpression(self, ctx:FilterExpressionSyntaxParser.ValueExpressionContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#notOperator.
    def enterNotOperator(self, ctx:FilterExpressionSyntaxParser.NotOperatorContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#notOperator.
    def exitNotOperator(self, ctx:FilterExpressionSyntaxParser.NotOperatorContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#unaryOperator.
    def enterUnaryOperator(self, ctx:FilterExpressionSyntaxParser.UnaryOperatorContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#unaryOperator.
    def exitUnaryOperator(self, ctx:FilterExpressionSyntaxParser.UnaryOperatorContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#exactMatchModifier.
    def enterExactMatchModifier(self, ctx:FilterExpressionSyntaxParser.ExactMatchModifierContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#exactMatchModifier.
    def exitExactMatchModifier(self, ctx:FilterExpressionSyntaxParser.ExactMatchModifierContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#comparisonOperator.
    def enterComparisonOperator(self, ctx:FilterExpressionSyntaxParser.ComparisonOperatorContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#comparisonOperator.
    def exitComparisonOperator(self, ctx:FilterExpressionSyntaxParser.ComparisonOperatorContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#logicalOperator.
    def enterLogicalOperator(self, ctx:FilterExpressionSyntaxParser.LogicalOperatorContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#logicalOperator.
    def exitLogicalOperator(self, ctx:FilterExpressionSyntaxParser.LogicalOperatorContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#bitwiseOperator.
    def enterBitwiseOperator(self, ctx:FilterExpressionSyntaxParser.BitwiseOperatorContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#bitwiseOperator.
    def exitBitwiseOperator(self, ctx:FilterExpressionSyntaxParser.BitwiseOperatorContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#mathOperator.
    def enterMathOperator(self, ctx:FilterExpressionSyntaxParser.MathOperatorContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#mathOperator.
    def exitMathOperator(self, ctx:FilterExpressionSyntaxParser.MathOperatorContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#functionName.
    def enterFunctionName(self, ctx:FilterExpressionSyntaxParser.FunctionNameContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#functionName.
    def exitFunctionName(self, ctx:FilterExpressionSyntaxParser.FunctionNameContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#functionExpression.
    def enterFunctionExpression(self, ctx:FilterExpressionSyntaxParser.FunctionExpressionContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#functionExpression.
    def exitFunctionExpression(self, ctx:FilterExpressionSyntaxParser.FunctionExpressionContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#literalValue.
    def enterLiteralValue(self, ctx:FilterExpressionSyntaxParser.LiteralValueContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#literalValue.
    def exitLiteralValue(self, ctx:FilterExpressionSyntaxParser.LiteralValueContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#tableName.
    def enterTableName(self, ctx:FilterExpressionSyntaxParser.TableNameContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#tableName.
    def exitTableName(self, ctx:FilterExpressionSyntaxParser.TableNameContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#columnName.
    def enterColumnName(self, ctx:FilterExpressionSyntaxParser.ColumnNameContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#columnName.
    def exitColumnName(self, ctx:FilterExpressionSyntaxParser.ColumnNameContext):
        pass


    # Enter a parse tree produced by FilterExpressionSyntaxParser#orderByColumnName.
    def enterOrderByColumnName(self, ctx:FilterExpressionSyntaxParser.OrderByColumnNameContext):
        pass

    # Exit a parse tree produced by FilterExpressionSyntaxParser#orderByColumnName.
    def exitOrderByColumnName(self, ctx:FilterExpressionSyntaxParser.OrderByColumnNameContext):
        pass



del FilterExpressionSyntaxParser