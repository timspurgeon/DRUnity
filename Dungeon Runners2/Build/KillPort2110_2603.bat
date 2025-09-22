@echo off
REM ==========================================
REM Auto Kill Ports 2110 and 2603
REM Automatically kills processes on auth and game server ports
REM Requires: Administrator privileges
REM ==========================================

setlocal ENABLEDELAYEDEXPANSION

echo.
echo Auto-killing processes on ports 2110 (Auth) and 2603 (Game)...
echo.

REM Kill port 2110 (Auth Server)
echo Checking port 2110 (Auth Server)...
set "FOUND_2110=0"
for /f "tokens=5" %%P in ('netstat -ano ^| find ":2110"') do (
  set "PID=%%P"
  set "FOUND_2110=1"
  echo   Found PID: !PID! using port 2110
  taskkill /PID !PID! /F >nul 2>&1
  if !ERRORLEVEL! EQU 0 (
    echo   Killed PID !PID! successfully.
  ) else (
    echo   Failed to kill PID !PID!. Try running as Administrator.
  )
)
if "!FOUND_2110!"=="0" (
  echo   Port 2110 is free.
)

echo.

REM Kill port 2603 (Game Server)
echo Checking port 2603 (Game Server)...
set "FOUND_2603=0"
for /f "tokens=5" %%P in ('netstat -ano ^| find ":2603"') do (
  set "PID=%%P"
  set "FOUND_2603=1"
  echo   Found PID: !PID! using port 2603
  taskkill /PID !PID! /F >nul 2>&1
  if !ERRORLEVEL! EQU 0 (
    echo   Killed PID !PID! successfully.
  ) else (
    echo   Failed to kill PID !PID!. Try running as Administrator.
  )
)
if "!FOUND_2603!"=="0" (
  echo   Port 2603 is free.
)

echo.
echo Done! Both ports should now be free.
echo.
pause