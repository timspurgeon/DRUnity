@echo off
setlocal

REM =============================================
REM RunServer_tailed.bat  -- Unity server (tailed)
REM =============================================
REM This runs your Unity headless server in the background
REM and live-tails the log to this window.
REM Edit EXE if you rename your server build.

REM --- Your server EXE name (NO quotes here) ---
set EXE=Dungeon Runners2.exe

REM --- Log file path ---
set LOGDIR=logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set LOGFILE=%LOGDIR%\server.log
if exist "%LOGFILE%" del "%LOGFILE%" >nul 2>&1

REM --- Sanity checks ---
if not exist "%EXE%" (
  echo ERROR: "%EXE%" not found in %cd%
  echo Make sure this BAT sits in the same folder as your server EXE.
  pause
  exit /b 1
)

echo ============================================
echo  Starting "%EXE%" -batchmode -nographics
echo  Writing logs to: %LOGFILE%
echo  (This window will show live logs; Ctrl+C to stop tail)
echo ============================================

REM --- Launch Unity server in background, write to server.log ---
start "Unity Server" /B "%EXE%" -batchmode -nographics -stackTraceLogType Full -logFile "%LOGFILE%"

REM --- Live tail the log so you see lines like:
REM     [Auth] Listening on 0.0.0.0:2110
powershell -NoProfile -Command "Get-Content -Path '%LOGFILE%' -Wait"
