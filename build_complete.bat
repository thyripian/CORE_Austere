@echo off
echo Building Complete CORE Scout Application...

REM Step 1: Build React frontend
echo.
echo [1/4] Building React frontend...
cd user_interface
call npm install
call npm run build
if %ERRORLEVEL% NEQ 0 (
    echo ❌ React build failed!
    pause
    exit /b 1
)
cd ..
echo ✅ React frontend built successfully!

REM Step 2: Build Python backend EXE
echo.
echo [2/4] Building Python backend EXE...
call build_backend.bat
if %ERRORLEVEL% NEQ 0 (
    echo ❌ Backend build failed!
    pause
    exit /b 1
)
echo ✅ Backend EXE built successfully!

REM Step 3: Build Electron app
echo.
echo [3/4] Building Electron application...
call npm run build
if %ERRORLEVEL% NEQ 0 (
    echo ❌ Electron build failed!
    pause
    exit /b 1
)
echo ✅ Electron app built successfully!

REM Step 4: Verify build
echo.
echo [4/4] Verifying build...
if exist "dist\win-unpacked\Scout.exe" (
    echo ✅ Electron executable found: dist\win-unpacked\Scout.exe
) else (
    echo ❌ Electron executable not found!
    pause
    exit /b 1
)

if exist "dist\win-unpacked\resources\backend\core-scout-backend.exe" (
    echo ✅ Backend EXE found: dist\win-unpacked\resources\backend\core-scout-backend.exe
) else (
    echo ❌ Backend EXE not found!
    pause
    exit /b 1
)

echo.
echo 🎉 Complete build successful!
echo.
echo 📁 Output location: dist\win-unpacked\
echo 🚀 Run: dist\win-unpacked\Scout.exe
echo.
echo The application includes:
echo   - Electron frontend with React UI
echo   - Packaged Python backend EXE
echo   - All dependencies included
echo   - No external Python installation required
echo.
pause
