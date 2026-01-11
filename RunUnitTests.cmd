@echo off
set PYTHONPATH=%~dp0src;%PYTHONPATH%
if exist "%~dp0.venv\Scripts\python.exe" (
    "%~dp0.venv\Scripts\python.exe" -m pytest test -v
) else (
    python -m pytest test -v
)