param(
  [string]$Path = ".\logs\server.log",
  [int]$Tail = 80
)

# Wait until the log file exists
while (-not (Test-Path $Path)) { Start-Sleep -Milliseconds 200 }

Write-Host "[TAIL] Following $Path`n" -ForegroundColor DarkGray

# Stream the log and color-code lines by tag/keywords
Get-Content -Path $Path -Wait -Tail $Tail | ForEach-Object {
  $line = $_
  if ($line -match '\[Auth\]') {
    Write-Host $line -ForegroundColor Green
  }
  elseif ($line -match '\[Game\]') {
    Write-Host $line -ForegroundColor Cyan
  }
  elseif ($line -match 'Listening on') {
    Write-Host $line -ForegroundColor Yellow
  }
  elseif ($line -match 'ERROR|Exception|fail|closed mid-frame') {
    Write-Host $line -ForegroundColor Red
  }
  else {
    Write-Host $line
  }
}
