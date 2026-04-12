@echo off
chcp 65001 >nul
title 涌金阁 - 启动中...

:: 检查服务是否存在
sc query YongJinGe >nul 2>&1
if errorlevel 1 (
    echo [错误] 涌金阁服务未安装，请重新运行安装程序。
    pause
    exit /b 1
)

:: 如果服务未运行则尝试启动
sc query YongJinGe | find "STOPPED" >nul 2>&1
if not errorlevel 1 (
    echo [信息] 正在启动涌金阁服务...
    net start YongJinGe >nul 2>&1
)

:: 等待服务启动（最多 20 秒）
set /a tries=0
:wait_loop
set /a tries+=1
if %tries% gtr 20 goto timeout_err
echo [%tries%/20] 等待服务就绪...
timeout /t 1 /nobreak >nul
powershell -Command "try { (New-Object Net.WebClient).DownloadString('http://localhost:8588/api/auth/current-user') | Out-Null; exit 0 } catch { exit 1 }" >nul 2>&1
if errorlevel 1 goto wait_loop
goto open_browser

:timeout_err
echo.
echo [错误] 服务启动超时！请检查：
echo   1. 以管理员身份重新运行此程序
echo   2. 检查服务状态：sc query YongJinGe
echo   3. 手动运行查看报错：
echo      "C:\Program Files\涌金阁\python\python.exe" "C:\Program Files\涌金阁\start_prod.py"
pause
exit /b 1

:open_browser
start "" "http://localhost:8588"
