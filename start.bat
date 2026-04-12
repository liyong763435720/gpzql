@echo off
chcp 65001 >nul

set PYTHON=python

echo ========================================
echo   Yongjinge v1.0.0
echo ========================================
echo.
echo Checking Python...
%PYTHON% --version
if %errorlevel% neq 0 goto :no_python

echo Starting service at http://localhost:8588
echo Press Ctrl+C to stop.
echo.

%PYTHON% start_prod.py
pause
exit /b 0

:no_python
echo [ERROR] Python not found. Please install Python 3.11+
pause
exit /b 1
