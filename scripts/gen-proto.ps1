#Requires -Version 5.1
<#
.SYNOPSIS
  M1 contract generation pipeline: api/proto/v1/*.proto -> api/gen/ (Go code + OpenAPI)

.DESCRIPTION
  - Self-contained toolchain bootstrap: protoc and Go plugins (protoc-gen-go,
    -go-grpc, -grpc-gateway, -openapiv2) are downloaded/installed into <repo>/bin/
    (gitignored) when missing. No system-wide installation is required.
  - Idempotent: already-installed pinned versions are skipped; safe to re-run.
  - Outputs:
      api/gen/observation/v1, orchestrator/v1, agent/v1   *.pb.go / *.pb.gw.go
      api/gen/openapi/mfdh.swagger.json                   (merged OpenAPI spec)
  - Generated layout aligns with go_package (module= strips the module prefix).
    Commit the generated files.

.PARAMETER SkipBootstrap
  Skip toolchain bootstrap (for quick regeneration once the toolchain is ready).

.PARAMETER OnlyBootstrap
  Only install/verify the toolchain, do not generate.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts/gen-proto.ps1
  powershell -ExecutionPolicy Bypass -File scripts/gen-proto.ps1 -SkipBootstrap
#>
param(
  [switch]$SkipBootstrap,
  [switch]$OnlyBootstrap
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$binDir = Join-Path $repoRoot 'bin'
$modulePath = 'github.com/NicoYazawa/microservice_diagnosis'

# ---- Pinned toolchain versions (verified 2026-08-16) ----
$protocVersion = '35.1'                                                      # protocolbuffers/protobuf
$protocUrl = "https://github.com/protocolbuffers/protobuf/releases/download/v$protocVersion/protoc-$protocVersion-win64.zip"
$protocDir = Join-Path $binDir "protoc-$protocVersion"
$protocExe = Join-Path $protocDir 'bin/protoc.exe'

$pluginVersions = [ordered]@{
  'protoc-gen-go'           = @{ Pkg = 'google.golang.org/protobuf/cmd/protoc-gen-go';                 Version = 'v1.36.12' }
  'protoc-gen-go-grpc'      = @{ Pkg = 'google.golang.org/grpc/cmd/protoc-gen-go-grpc'; Version = 'v1.6.2' }
  'protoc-gen-grpc-gateway' = @{ Pkg = 'github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway'; Version = 'v2.30.0' }
  'protoc-gen-openapiv2'    = @{ Pkg = 'github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2'; Version = 'v2.30.0' }
}

function New-Dir([string]$Path) {
  if (-not (Test-Path $Path)) { New-Item -ItemType Directory -Path $Path -Force | Out-Null }
}

function Install-Protoc {
  if (-not (Test-Path $protocExe)) {
    New-Dir $binDir
    Write-Host "[gen-proto] downloading protoc $protocVersion ..."
    $zip = Join-Path $binDir "protoc-$protocVersion-win64.zip"
    Invoke-WebRequest -Uri $protocUrl -OutFile $zip
    New-Dir $protocDir
    Expand-Archive -Path $zip -DestinationPath $protocDir -Force
    Remove-Item $zip -Force
    Write-Host "[gen-proto] protoc installed: $protocExe"
  } else {
    Write-Host "[gen-proto] protoc $protocVersion ready"
  }
}

function Install-Plugins {
  $env:GOBIN = $binDir
  New-Dir $binDir
  foreach ($name in $pluginVersions.Keys) {
    $info = $pluginVersions[$name]
    $exe = Join-Path $binDir "$name.exe"
    $marker = Join-Path $binDir "$name.version"
    $installed = (Test-Path $exe) -and (Test-Path $marker) -and ((Get-Content $marker -Raw).Trim() -eq $info.Version)
    if ($installed) {
      Write-Host "[gen-proto] $name $($info.Version) ready"
      continue
    }
    Write-Host "[gen-proto] go install $($info.Pkg)@$($info.Version) ..."
    & go install "$($info.Pkg)@$($info.Version)"
    if ($LASTEXITCODE -ne 0) { throw "go install $name failed (exit=$LASTEXITCODE)" }
    Set-Content -Path $marker -Value $info.Version -Encoding ascii
  }
}

function Invoke-Generate {
  $protoDir = Join-Path $repoRoot 'api/proto'
  $thirdPartyDir = Join-Path $repoRoot 'third_party'
  $openapiOut = Join-Path $repoRoot 'api/gen/openapi'
  New-Dir $openapiOut

  $files = @('v1/observation.proto', 'v1/orchestrator.proto', 'v1/agent.proto')

  $oldPath = $env:PATH
  $env:PATH = "$binDir;$protocDir\bin;$oldPath"

  Write-Host "[gen-proto] generating Go code + OpenAPI ..."
  & $protocExe `
    "-I$protoDir" `
    "-I$thirdPartyDir" `
    "--go_out=$repoRoot" "--go_opt=module=$modulePath" `
    "--go-grpc_out=$repoRoot" "--go-grpc_opt=module=$modulePath" `
    "--grpc-gateway_out=$repoRoot" "--grpc-gateway_opt=module=$modulePath" `
    "--openapiv2_out=$openapiOut" "--openapiv2_opt=allow_merge=true" "--openapiv2_opt=merge_file_name=mfdh" `
    $files
  $exitCode = $LASTEXITCODE
  $env:PATH = $oldPath
  if ($exitCode -ne 0) { throw "protoc generation failed (exit=$exitCode)" }

  Write-Host "[gen-proto] generated files:"
  Get-ChildItem (Join-Path $repoRoot 'api/gen') -Recurse -File |
    Sort-Object FullName |
    ForEach-Object { Write-Host "  + $($_.FullName.Substring($repoRoot.Length + 1))" }
}

# ---- main ----
if (-not $SkipBootstrap) {
  Install-Protoc
  Install-Plugins
} else {
  if (-not (Test-Path $protocExe)) { throw "protoc missing: run once without -SkipBootstrap first" }
  foreach ($name in $pluginVersions.Keys) {
    if (-not (Test-Path (Join-Path $binDir "$name.exe"))) { throw "plugin $name missing: run once without -SkipBootstrap first" }
  }
  Write-Host "[gen-proto] toolchain verified (bootstrap skipped)"
}

if ($OnlyBootstrap) {
  Write-Host "[gen-proto] toolchain ready"
  exit 0
}

Invoke-Generate
