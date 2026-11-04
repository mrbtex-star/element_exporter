@echo off
REM Element (Matrix) Chat Exporter - Windows Launcher
REM Usage: run.bat [--list-rooms] [--export-all] [--media] [--formats markdown,json] [--limit N] [--room ROOM] [--verbose]

set SCRIPT_DIR=%~dp0
REM Auto-detect Python path (handles asterisks in path)
for /f "delims=" %%i in ('where python3 2^>nul') do set PYTHON=%%i
if not defined PYTHON (
    echo ERROR: python3 not found in PATH
    echo Please ensure Python 3.10+ is installed and on PATH
    pause
    exit /b 1
)

echo.
echo ================================ Element (Matrix) Exporter =================================
echo Python: %PYTHON%
echo Script: %SCRIPT_DIR%main.py
echo ==========================================================================================
echo.

"%PYTHON%" "%SCRIPT_DIR%main.py" %*

echo.
pause
