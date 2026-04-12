@echo off
chcp 65001 >nul

echo ========================================
echo   Yongjinge v1.0.0 - Install
echo ========================================
echo.

REM Check Python
python --version >nul 2>&1
if %errorlevel% neq 0 goto :no_python

echo Python version:
python --version

REM Check pip
python -m pip --version >nul 2>&1
if %errorlevel% neq 0 goto :no_pip

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip -q

REM Install dependencies
echo Installing dependencies...
python -m pip install -r requirements.txt
if %errorlevel% neq 0 goto :install_failed

echo.
echo [OK] Installation complete!
echo.
echo To start: run start.bat
echo.
pause
exit /b 0

:no_python
echo [ERROR] Python not found. Please install Python 3.11+
pause
exit /b 1

:no_pip
echo [ERROR] pip not found.
pause
exit /b 1

:install_failed
echo [ERROR] Dependency installation failed.
pause
exit /b 1
