@echo off
echo Compiling FilterExpressionSyntax grammar for Python...
java -jar antlr-4.10.1-complete.jar -Dlanguage=Python3 -o parser FilterExpressionSyntax.g4
echo Finished.