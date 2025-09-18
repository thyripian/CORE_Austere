@echo off
echo Building CORE Scout Backend with PyInstaller...

REM Clean previous builds
if exist "dist" rmdir /s /q "dist"
if exist "build" rmdir /s /q "build"

REM Build the backend EXE
pyinstaller backend.spec --clean --noconfirm

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✅ Backend build successful!
    echo Backend EXE location: dist\core-scout-backend.exe
    echo.
) else (
    echo.
    echo ❌ Backend build failed!
    echo Check the output above for errors.
    echo.
    pause
    exit /b 1
)

echo Done!
