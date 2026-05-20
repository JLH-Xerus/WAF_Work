[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $ServerInstance,

    [Parameter(Mandatory = $true)]
    [string] $Database,

    [string] $SqlFolder      = (Join-Path $PSScriptRoot 'sql'),
    [string] $ResultsRoot    = (Join-Path $PSScriptRoot 'results'),

    [int]    $WarmupRuns           = 1,
    [int]    $MeasurementRuns      = 3,
    [int]    $CommandTimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $SqlFolder)) {
    throw "SQL folder not found: $SqlFolder"
}

if ($MeasurementRuns -lt 1) {
    throw "MeasurementRuns must be at least 1 (got $MeasurementRuns)."
}
if ($WarmupRuns -lt 0) {
    throw "WarmupRuns cannot be negative (got $WarmupRuns)."
}

$runStamp  = (Get-Date).ToString('yyyy-MM-dd_HH-mm-ss')
$runFolder = Join-Path $ResultsRoot $runStamp
New-Item -ItemType Directory -Path $runFolder -Force | Out-Null

Write-Host "Server           : $ServerInstance"
Write-Host "Database         : $Database"
Write-Host "SQL folder       : $SqlFolder"
Write-Host "Results          : $runFolder"
Write-Host "Warmup runs      : $WarmupRuns"
Write-Host "Measurement runs : $MeasurementRuns"
Write-Host ""

$scripts = Get-ChildItem -LiteralPath $SqlFolder -Filter '*.sql' -File |
           Sort-Object Name

if (-not $scripts) {
    Write-Warning "No .sql files found in $SqlFolder"
    return
}

Write-Host ("Found {0} script(s):" -f $scripts.Count)
$scripts | ForEach-Object { Write-Host "  - $($_.Name)" }
Write-Host ""

$connStr = "Server=$ServerInstance;Database=$Database;Integrated Security=SSPI;Application Name=Run-SqlPerfTests"

$statisticsPrologue = @"
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SET STATISTICS XML ON;
"@

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

foreach ($script in $scripts) {

    Write-Host ("--- {0} ---" -f $script.Name)

    $userSql  = Get-Content -LiteralPath $script.FullName -Raw
    $batch    = $statisticsPrologue + [Environment]::NewLine + $userSql
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($script.Name)

    $connection = New-Object System.Data.SqlClient.SqlConnection $connStr

    try {
        $connection.Open()

        $totalRuns = $WarmupRuns + $MeasurementRuns

        for ($i = 1; $i -le $totalRuns; $i++) {

            $isWarmup = ($i -le $WarmupRuns)
            if ($isWarmup) {
                $phase    = 'warmup'
                $phaseIdx = $i
                $phaseTot = $WarmupRuns
            } else {
                $phase    = 'measurement'
                $phaseIdx = $i - $WarmupRuns
                $phaseTot = $MeasurementRuns
            }

            Write-Host ("  {0,-11} {1}/{2}" -f $phase, $phaseIdx, $phaseTot)

            $messages = New-Object 'System.Collections.Generic.List[string]'
            $plans    = New-Object 'System.Collections.Generic.List[string]'

            $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {
                param($eventSender, $eventArgs)
                foreach ($err in $eventArgs.Errors) {
                    $messages.Add($err.Message)
                }
            }
            $connection.add_InfoMessage($handler)

            $iterationStart = [System.Diagnostics.Stopwatch]::StartNew()

            try {
                $command = $connection.CreateCommand()
                $command.CommandText    = $batch
                $command.CommandTimeout = $CommandTimeoutSeconds

                $reader = $command.ExecuteReader()
                try {
                    do {
                        $isShowplan = ($reader.FieldCount -eq 1) -and
                                      ($reader.GetName(0) -like '*Showplan*')

                        if ($isShowplan) {
                            while ($reader.Read()) {
                                $plans.Add($reader.GetString(0))
                            }
                        } else {
                            while ($reader.Read()) { }
                        }
                    } while ($reader.NextResult())
                }
                finally {
                    $reader.Dispose()
                }
            }
            catch {
                $messages.Add("*** EXCEPTION: $($_.Exception.Message)")
                Write-Warning $_.Exception.Message
            }
            finally {
                $iterationStart.Stop()
                $connection.remove_InfoMessage($handler)
            }

            if (-not $isWarmup) {
                $messageFile = Join-Path $runFolder ("{0}.run{1}.messages.txt" -f $baseName, $phaseIdx)

                $header = @(
                    "Script         : $($script.Name)"
                    "Server         : $ServerInstance"
                    "Database       : $Database"
                    "Run at         : $runStamp"
                    "Iteration      : measurement $phaseIdx of $MeasurementRuns (after $WarmupRuns warmup run(s))"
                    "Wall-clock ms  : $([int]$iterationStart.Elapsed.TotalMilliseconds)"
                    "Plans captured : $($plans.Count)"
                    ('-' * 60)
                    ''
                ) -join [Environment]::NewLine

                ($header + ($messages -join [Environment]::NewLine)) |
                    Set-Content -LiteralPath $messageFile -Encoding UTF8

                Write-Host ("              -> {0}" -f (Split-Path $messageFile -Leaf))

                if ($plans.Count -eq 1) {
                    $planFile = Join-Path $runFolder ("{0}.run{1}.sqlplan" -f $baseName, $phaseIdx)
                    [System.IO.File]::WriteAllText($planFile, $plans[0], $utf8NoBom)
                    Write-Host ("              -> {0}" -f (Split-Path $planFile -Leaf))
                }
                elseif ($plans.Count -gt 1) {
                    for ($p = 0; $p -lt $plans.Count; $p++) {
                        $planFile = Join-Path $runFolder ("{0}.run{1}.stmt{2}.sqlplan" -f $baseName, $phaseIdx, ($p + 1))
                        [System.IO.File]::WriteAllText($planFile, $plans[$p], $utf8NoBom)
                        Write-Host ("              -> {0}" -f (Split-Path $planFile -Leaf))
                    }
                }
            }
        }
    }
    finally {
        $connection.Dispose()
    }
}

Write-Host ""
Write-Host "Done."
