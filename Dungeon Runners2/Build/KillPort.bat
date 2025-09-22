@echo off
REM ==========================================
REM KillPort.bat  --  Free a TCP port on Windows
REM Usage:
REM   KillPort.bat 2110
REM   (or run without args and it will prompt)
REM Requires: Administrator privileges (to kill other processes)
REM ==========================================

setlocal ENABLEDELAYEDEXPANSION

set "PORT=%~1"
if "%PORT%"=="" (
  set /p PORT=Enter TCP port to free: 
)

if "%PORT%"=="" (
  echo No port provided. Exiting.
  exit /b 1
)

echo.
echo Scanning for processes using TCP port %PORT% ...
set "FOUND=0"

REM The output of netstat: Proto  Local Address  Foreign Address  State  PID
for /f "tokens=5" %%P in ('netstat -ano ^| find ":%PORT%"') do (
  set "PID=%%P"
  set "FOUND=1"
  echo.
  echo Found PID: !PID! using port %PORT%
  echo Process info:
  tasklist /FI "PID eq !PID!"
  echo Attempting to kill PID !PID! ...
  taskkill /PID !PID! /F >nul 2>&1
  if !ERRORLEVEL! EQU 0 (
    echo   Killed PID !PID! successfully.
  ) else (
    echo   Failed to kill PID !PID!. Try running this script as Administrator.
  )
)

if "!FOUND!"=="0" (
  echo No process found using port %PORT% (nothing to kill).
) else (
  echo.
  echo Verifying port %PORT% is now free...
  for /f "tokens=5" %%P in ('netstat -ano ^| find ":%PORT%"') do (
    echo   Still detected PID %%P on port %PORT%.
    set "STILL=1"
  )
  if defined STILL (
    echo Some processes still hold the port. You may need to rerun as Administrator.
  ) else (
    echo Port %PORT% appears to be free.
  )
)

echo.
pause
