@echo off
setlocal enabledelayedexpansion

echo ========================================================
echo   YongJinGe - Installer Build Script
echo ========================================================
echo.

set PYTHON_VERSION=3.11.9
set PYTHON_EMBED_URL=https://www.python.org/ftp/python/%PYTHON_VERSION%/python-%PYTHON_VERSION%-embed-amd64.zip
set PYTHON_EMBED_ZIP=python-embed.zip
set PYTHON_EMBED_DIR=python-embed
set GET_PIP_URL=https://bootstrap.pypa.io/get-pip.py
set ISCC_PATH=C:\Program Files (x86)\Inno Setup 6\ISCC.exe

:: ── 1. Check Inno Setup ──────────────────────────────────
if not exist "%ISCC_PATH%" (
    echo [ERROR] Inno Setup 6 not found. Please install it first:
    echo         https://jrsoftware.org/isdl.php
    pause
    exit /b 1
)
echo [OK] Inno Setup found

:: ── 2. Download embedded Python ──────────────────────────
if not exist "%PYTHON_EMBED_DIR%\python.exe" (
    echo [INFO] Downloading Python %PYTHON_VERSION% embeddable...
    if not exist "%PYTHON_EMBED_ZIP%" (
        powershell -Command "Invoke-WebRequest -Uri '%PYTHON_EMBED_URL%' -OutFile '%PYTHON_EMBED_ZIP%'"
        if errorlevel 1 ( echo [ERROR] Download failed. Check network. & pause & exit /b 1 )
    )
    echo [INFO] Extracting Python...
    powershell -Command "Expand-Archive -Path '%PYTHON_EMBED_ZIP%' -DestinationPath '%PYTHON_EMBED_DIR%' -Force"
    del "%PYTHON_EMBED_ZIP%"
    echo [OK] Python extracted
) else (
    echo [OK] Python embeddable already exists
)

:: ── 3. Enable import in embedded Python ──────────────────
set PTH_FILE=%PYTHON_EMBED_DIR%\python311._pth
powershell -Command "(Get-Content '%PTH_FILE%') -replace '#import site','import site' | Set-Content '%PTH_FILE%'"
echo [OK] Python pth file patched

:: ── 4. Install pip ────────────────────────────────────────
if not exist "%PYTHON_EMBED_DIR%\Scripts\pip.exe" (
    echo [INFO] Installing pip...
    if not exist "get-pip.py" (
        powershell -Command "Invoke-WebRequest -Uri '%GET_PIP_URL%' -OutFile 'get-pip.py'"
    )
    %PYTHON_EMBED_DIR%\python.exe get-pip.py --no-warn-script-location -q
    del "get-pip.py"
    echo [OK] pip installed
) else (
    echo [OK] pip already installed
)

:: ── 5. Download packages offline ─────────────────────────
echo [INFO] Downloading packages (this may take a few minutes)...
if not exist "%PYTHON_EMBED_DIR%\packages" mkdir "%PYTHON_EMBED_DIR%\packages"
%PYTHON_EMBED_DIR%\python.exe -m pip download -r ..\requirements.txt -d "%PYTHON_EMBED_DIR%\packages" -q
if errorlevel 1 (
    echo [ERROR] Package download failed.
    pause
    exit /b 1
)
echo [OK] Packages downloaded

:: ── 5b. Pre-build source distributions (.tar.gz) to wheels ──
:: 部分包（multitasking / jsonpath / thriftpy2）只有源码包，离线安装时
:: pip 需要 setuptools/wheel 才能编译，这里提前把它们编译成 .whl，
:: 这样目标机器完全不需要任何构建工具。
echo [INFO] Pre-building source distributions to wheels...
%PYTHON_EMBED_DIR%\python.exe -m pip install setuptools wheel --quiet --no-warn-script-location
for %%f in ("%PYTHON_EMBED_DIR%\packages\*.tar.gz") do (
    echo   Building: %%~nxf
    %PYTHON_EMBED_DIR%\python.exe -m pip wheel "%%f" -w "%PYTHON_EMBED_DIR%\packages" --no-deps -q
    if not errorlevel 1 (
        del "%%f"
    ) else (
        echo   [WARN] Failed to build %%~nxf, keeping source dist
    )
)
echo [OK] All source distributions pre-built

:: ── 6. Download NSSM ──────────────────────────────────────
if not exist "assets\nssm.exe" (
    echo [INFO] Downloading NSSM...
    powershell -Command "Invoke-WebRequest -Uri 'https://nssm.cc/release/nssm-2.24.zip' -OutFile 'nssm.zip'"
    powershell -Command "Expand-Archive -Path 'nssm.zip' -DestinationPath 'nssm-tmp' -Force"
    copy "nssm-tmp\nssm-2.24\win64\nssm.exe" "assets\nssm.exe"
    rmdir /s /q "nssm-tmp"
    del "nssm.zip"
    echo [OK] NSSM downloaded
) else (
    echo [OK] NSSM already exists
)

:: ── 7. Chinese language file ─────────────────────────────
if not exist "ChineseSimplified.isl" (
    echo [INFO] Downloading Chinese language file...
    powershell -Command "Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/jrsoftware/issrc/main/Files/Languages/Unofficial/ChineseSimplified.isl' -OutFile 'ChineseSimplified.isl'"
)
echo [OK] Language file ready

:: ── 9. Icon placeholder ───────────────────────────────────
if not exist "assets\icon.ico" (
    echo [INFO] Generating default icon...
    python -c "import struct; size=32; r,g,b,a=0x25,0x63,0xeb,0xff; pixels=bytes([b,g,r,a]*size*size); bi=struct.pack('<IiiHHIIiiII',40,size,size*2,1,32,0,0,0,0,0,0); img=bi+pixels+bytes(size*size//8); h=struct.pack('<HHH',0,1,1); d=struct.pack('<BBBBHHII',size,size,0,0,1,32,len(img),22); open('assets/icon.ico','wb').write(h+d+img)" 2>nul
)
echo [OK] Icon ready

:: ── 8. Compile installer ──────────────────────────────────
echo.
:: ── 10. Compile installer ─────────────────────────────────
echo [INFO] Compiling installer with Inno Setup...
"%ISCC_PATH%" yongjinge.iss
if errorlevel 1 (
    echo [ERROR] Inno Setup compilation failed.
    pause
    exit /b 1
)

echo.
echo ========================================================
echo [DONE] Installer built successfully!
echo        Output: installer\dist\
echo ========================================================
pause
