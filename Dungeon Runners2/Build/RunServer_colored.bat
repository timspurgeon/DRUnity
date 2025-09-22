@echo off
setlocal enabledelayedexpansion

REM --- Go to your build folder ---
cd /d "C:\UnityProjects\Dungeon Runners2\Build\"

REM --- EXE name (auto-detect with/without space) ---
set "EXE=Dungeon Runners2.exe"
if not exist "%EXE%" if exist "DungeonRunners2.exe" set "EXE=DungeonRunners2.exe"

REM --- sanity checks ---
if not exist "%EXE%" (
  echo [DR][ERROR] Server EXE not found in:
  echo   C:\UnityProjects\Dungeon Runners2\Build\
  echo Tried "Dungeon Runners2.exe" and "DungeonRunners2.exe"
  pause
  exit /b 1
)

if not exist "serverconfig.json" (
  echo [DR][WARN] serverconfig.json not found; the server may quit immediately.
)

REM --- logs setup ---
set "LOGDIR=logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set "LOGFILE=%LOGDIR%\server.log"
del "%LOGFILE%" >nul 2>&1
type nul > "%LOGFILE%"

REM --- Start colorized tail in its own window (falls back to plain tail) ---
if exist "%~dp0tail-dr.ps1" (
  start "DR Logs" powershell -NoExit -NoProfile -ExecutionPolicy Bypass -File "%~dp0tail-dr.ps1" -Path "%LOGFILE%"
) else (
  start "DR Logs" powershell -NoExit -NoProfile -ExecutionPolicy Bypass -Command "Get-Content -Path '%LOGFILE%' -Wait"
)

REM --- Start the server in its OWN window and keep it open even after exit ---
set "PS_CMD=$ErrorActionPreference='Stop';"
set "PS_CMD=%PS_CMD% $exe = '%CD%\%EXE%';"
set "PS_CMD=%PS_CMD% $args = @('-batchmode','-nographics','-stackTraceLogType','Full','-logFile','%CD%\%LOGFILE%');"
set "PS_CMD=%PS_CMD% Write-Host ('[DR] Launching: {0}' -f $exe);"
set "PS_CMD=%PS_CMD% $p = Start-Process -FilePath $exe -ArgumentList $args -PassThru -WorkingDirectory '%CD%';"
set "PS_CMD=%PS_CMD% Write-Host ('[DR] PID {0} started' -f $p.Id);"
set "PS_CMD=%PS_CMD% Wait-Process -Id $p.Id;"
set "PS_CMD=%PS_CMD% try { $code = (Get-CimInstance Win32_Process -Filter ('ProcessId={0}' -f $p.Id)).ExitCode } catch { $code = $p.ExitCode }"
set "PS_CMD=%PS_CMD% Write-Host ('[DR] Server exited with code {0}' -f $code) -ForegroundColor Yellow;"
set "PS_CMD=%PS_CMD% Write-Host ('[DR] Log: %CD%\%LOGFILE%');"
set "PS_CMD=%PS_CMD% Write-Host 'This window will stay open. Press Ctrl+C or close it.' -ForegroundColor Gray;"
set "PS_CMD=%PS_CMD% Start-Sleep -Seconds 999999;"

start "DR Server" powershell -NoExit -NoProfile -ExecutionPolicy Bypass -Command "%PS_CMD%"

echo [DR] Launched: "DR Server" window + "DR Logs" window.
echo You can close this launcher window now.
endlocal
