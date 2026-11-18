<#
.SYNOPSIS
  Offline BladeBridge DataStage XML converter wrapper for Windows.

.DESCRIPTION
  Runs run_dbxconv.py with uv and Python 3.12. Accepts either one .xml file or a folder
  of .xml files. A single file is staged in an isolated temporary folder because dbxconv
  expects a directory of staged inputs.

.EXAMPLE
  .\run_dbxconv.ps1 -XmlPath .\migrated_batches\batch_001\xml\J01_Land_Orders.xml -OutputDir .\tmp\bladebridge -Target PYSPARK

.EXAMPLE
  .\run_dbxconv.ps1 -XmlPath .\migrated_batches\batch_001\xml -OutputDir .\migrated_batches\batch_001\bladebridge_pyspark -Target PYSPARK
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$XmlPath,

    [Parameter()]
    [string]$OutputDir = "",

    [Parameter()]
    [ValidateSet("PYSPARK", "SPARKSQL", "DBSQL")]
    [string]$Target = "PYSPARK",

    [Parameter()]
    [string]$PluginDir = "bb_plugin",

    [Parameter()]
    [string]$PythonVersion = "3.12",

    [Parameter()]
    [switch]$ContinueOnError,

    [Parameter()]
    [switch]$Quiet
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$script = Join-Path $root "src\lakebridge_workflow\run_dbxconv.py"

if (-not (Test-Path -LiteralPath $script -PathType Leaf)) {
    throw "run_dbxconv.py not found at $script"
}
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    throw "uv was not found in PATH. Install uv before running this wrapper."
}

# Resolve the plugin relative to the current directory, the script directory, or the supplied path.
$pluginCandidates = @(
    (Join-Path (Get-Location) $PluginDir),
    (Join-Path $root $PluginDir),
    $PluginDir
) | Select-Object -Unique
$plugin = $null
foreach ($candidate in $pluginCandidates) {
    if (Test-Path -LiteralPath (Join-Path $candidate "databricks\labs\bladebridge\Converter\bin\Windows\dbxconv.exe") -PathType Leaf) {
        $plugin = [System.IO.Path]::GetFullPath($candidate)
        break
    }
}
if (-not $plugin) {
    throw "BladeBridge dbxconv.exe not found. Unpack the wheel under $PluginDir (expected $PluginDir\databricks\labs\bladebridge\Converter\bin\Windows\dbxconv.exe)."
}

$xml = [System.IO.Path]::GetFullPath($XmlPath)
if (-not (Test-Path -LiteralPath $xml)) {
    throw "XmlPath not found: $xml"
}

if (-not $OutputDir) {
    $leaf = [System.IO.Path]::GetFileNameWithoutExtension($xml)
    if (Test-Path -LiteralPath $xml -PathType Container) {
        $OutputDir = Join-Path $xml "bladebridge_$($Target.ToLowerInvariant())"
    } else {
        $OutputDir = Join-Path ([System.IO.Path]::GetDirectoryName($xml)) "bladebridge_$($Target.ToLowerInvariant())"
    }
}
$out = [System.IO.Path]::GetFullPath($OutputDir)
New-Item -ItemType Directory -Path $out -Force | Out-Null

$staging = $null
$pythonInput = $xml
if (Test-Path -LiteralPath $xml -PathType Leaf) {
    $staging = Join-Path ([System.IO.Path]::GetTempPath()) ("lakebridge_dbxconv_" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $staging -Force | Out-Null
    Copy-Item -LiteralPath $xml -Destination (Join-Path $staging ([System.IO.Path]::GetFileName($xml)))
    $pythonInput = $staging
}

try {
    $arguments = @("run", "--python", $PythonVersion, $script, $plugin, $pythonInput, $out, $Target)
    if ($ContinueOnError) { $arguments += "--continue-on-error" }
    if ($Quiet) { $arguments += "--quiet" }

    Write-Host "BladeBridge target: $Target"
    Write-Host "Plugin:             $plugin"
    Write-Host "Input:              $pythonInput"
    Write-Host "Output:             $out"
    & uv @arguments
    exit $LASTEXITCODE
}
finally {
    if ($staging -and (Test-Path -LiteralPath $staging)) {
        Remove-Item -LiteralPath $staging -Recurse -Force
    }
}
