@echo off
:: https://towardsdatascience.com/documenting-python-code-with-sphinx-554e1d6c4f6d
echo Make sure Sphinx is installed:
echo pip install sphinx sphinx_rtd_theme
pause

sphinx-apidoc -o docs src/

echo Ready to start generating HTML
pause

cd docs
call ./make html

cd ..
echo Ready to deploy new HTML
pause
xcopy /e /i /y docs\_build\html docs

