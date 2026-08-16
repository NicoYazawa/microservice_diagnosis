# 拉取 M0 基础设施所需镜像（带超时与重试，适配不稳定网络）。
# 用法: powershell -File scripts/pull-images.ps1
$ErrorActionPreference = 'Continue'

$images = @(
  'postgres:18-alpine',
  'hashicorp/consul:2.0.3',
  'prom/prometheus:v3.13.2',
  'apache/kafka:4.3.1'
)

function Pull-WithTimeout {
  param([string]$Image, [int]$TimeoutSec = 300)
  $out = Join-Path $env:TEMP "docker-pull-$([guid]::NewGuid()).log"
  $err = Join-Path $env:TEMP "docker-pull-$([guid]::NewGuid()).err.log"
  $p = Start-Process -FilePath 'docker' -ArgumentList @('pull', $Image) `
    -NoNewWindow -PassThru -RedirectStandardOutput $out -RedirectStandardError $err
  if (-not $p.WaitForExit($TimeoutSec * 1000)) {
    Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
    Write-Output "TIMEOUT: $Image"
    return $false
  }
  if ($p.ExitCode -eq 0) {
    Write-Output "OK: $Image"
    return $true
  }
  $last = (Get-Content $err -ErrorAction SilentlyContinue | Select-Object -Last 2) -join ' | '
  Write-Output "FAILED: $Image ($last)"
  return $false
}

foreach ($img in $images) {
  $ok = $false
  for ($i = 1; $i -le 8; $i++) {
    Write-Output "=== pull $img (attempt $i/8) ==="
    if (Pull-WithTimeout $img) { $ok = $true; break }
    Start-Sleep -Seconds 3
  }
  if (-not $ok) { Write-Output "GIVE-UP: $img" }
}
