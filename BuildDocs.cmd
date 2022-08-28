@echo off
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
rmdir /s /q docs\html
xcopy /e /i docs\_build\html docs\html