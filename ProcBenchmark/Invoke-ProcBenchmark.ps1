<#
.SYNOPSIS
    Benchmark stored procedure / T-SQL refactors across many SQL Server sites.

.DESCRIPTION
    Reads a config CSV of sites, runs an "original" and a "refactored" T-SQL batch
    against each one using Windows authentication, and captures:
      * STATISTICS TIME  (CPU + elapsed ms, per statement, totalled per batch)
      * STATISTICS IO    (logical / physical / read-ahead / LOB reads, per table)
      * Actual execution plan XML (saved as .sqlplan files, openable in SSMS)
      * Row count and a checksum of the result set so you can spot data drift

    Emits a single results.xlsx with:
      * Summary       - one row per site/iteration with deltas and % change,
                        green = improvement, red = regression
      * IO Detail     - per-table reads for every site/version/iteration
      * Time Detail   - per-statement CPU + elapsed for every site/version/iteration
      * Plans Index   - hyperlinks to each .sqlplan file
      * Errors        - any sites/versions that blew up, with the SQL exception

    Requires Windows PowerShell 5.1 or PowerShell 7+ on Windows, and the ImportExcel
    module (Install-Module ImportExcel -Scope CurrentUser). If ImportExcel is not
    available the script falls back to writing CSVs and prints a warning.

.PARAMETER ConfigPath
    Path to sites.csv. See sites.csv.template for the expected schema.

.PARAMETER OutputDir
    Directory where results.xlsx and the plans/ subfolder are written.
    Defaults to a timestamped folder beside the script.

.PARAMETER Iterations
    How many times to run BOTH the original and the refactored batch per site.
    Default 3. The summary reports each iteration; analyze in Excel to take median.

.PARAMETER WarmupRuns
    How many "throwaway" runs to perform first (results discarded). Useful to
    pre-warm the buffer pool so iteration 1 isn't penalized for cold cache.
    Default 1. Set to 0 if you want to see the cold-cache cost.

.PARAMETER ConnectionTimeout
    Seconds to wait when opening a connection. Default 15.

.PARAMETER CommandTimeout
    Seconds to wait for a batch to finish. Default 300.

.PARAMETER CapturePlans
    Capture actual execution plan XML via SET STATISTICS XML ON. Default $true.
    Set to $false if a proc misbehaves with showplan on (rare).

.PARAMETER SkipChecksum
    Skip computing a checksum of the result rows. The checksum is used to verify
    that the refactor returns the same data as the original. Skipping it speeds
    up very large result sets but you lose the correctness check.

.EXAMPLE
    .\Invoke-ProcBenchmark.ps1 -ConfigPath .\sites.csv -Iterations 5

.EXAMPLE
    .\Invoke-ProcBenchmark.ps1 -ConfigPath .\sites.csv -CapturePlans $false -WarmupRuns 0
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$ConfigPath,

    [string]$OutputDir = (Join-Path $PSScriptRoot ("results_" + (Get-Date -Format 'yyyyMMdd_HHmmss'))),

    [int]$Iterations = 3,
    [int]$WarmupRuns = 1,
    [int]$ConnectionTimeout = 15,
    [int]$CommandTimeout = 300,
    [bool]$CapturePlans = $true,
    [switch]$SkipChecksum
)

$ErrorActionPreference = 'Stop'

# ----------------------------------------------------------------------------
# Setup
# ----------------------------------------------------------------------------

if (-not (Test-Path $ConfigPath)) {
    throw "Config file not found: $ConfigPath"
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$plansDir = Join-Path $OutputDir 'plans'
New-Item -ItemType Directory -Force -Path $plansDir | Out-Null

$logPath = Join-Path $OutputDir 'run.log'
function Write-Log {
    param([string]$Message, [string]$Level = 'INFO')
    $line = "{0} [{1}] {2}" -f (Get-Date -Format 'HH:mm:ss'), $Level, $Message
    Write-Host $line
    Add-Content -Path $logPath -Value $line
}

Write-Log "Config: $ConfigPath"
Write-Log "Output: $OutputDir"
Write-Log "Iterations: $Iterations  Warmup: $WarmupRuns  CapturePlans: $CapturePlans"

# Check ImportExcel availability up front so user sees the warning early.
$haveImportExcel = $null -ne (Get-Module -ListAvailable -Name ImportExcel)
if (-not $haveImportExcel) {
    Write-Log "ImportExcel module not found - will write CSV output. Install with:" 'WARN'
    Write-Log "    Install-Module ImportExcel -Scope CurrentUser" 'WARN'
}

# ----------------------------------------------------------------------------
# Parsers - convert STATISTICS TIME / STATISTICS IO InfoMessage text into objects
# ----------------------------------------------------------------------------

# STATISTICS TIME emits two kinds of messages, both repeated for each statement
# in the batch:
#   "SQL Server parse and compile time:
#       CPU time = N ms, elapsed time = N ms."
#   "SQL Server Execution Times:
#       CPU time = N ms,  elapsed time = N ms."
# We only care about Execution Times; compile time is reported separately
# but we ignore it for the summary (it's a rounding error in most refactors).
function Parse-StatisticsTime {
    param([string[]]$Messages)

    $exec = @()
    $compile = @()
    foreach ($msg in $Messages) {
        # Each "Execution Times" message can span lines. Use single-line regex
        # because each InfoMessage is one logical string with embedded \n.
        $pattern = 'SQL Server Execution Times:\s*CPU time\s*=\s*(\d+)\s*ms,\s*elapsed time\s*=\s*(\d+)\s*ms'
        $m = [regex]::Matches($msg, $pattern)
        foreach ($match in $m) {
            $exec += [PSCustomObject]@{
                CpuMs     = [int]$match.Groups[1].Value
                ElapsedMs = [int]$match.Groups[2].Value
            }
        }
        $compilePattern = 'SQL Server parse and compile time:\s*CPU time\s*=\s*(\d+)\s*ms,\s*elapsed time\s*=\s*(\d+)\s*ms'
        $cm = [regex]::Matches($msg, $compilePattern)
        foreach ($match in $cm) {
            $compile += [PSCustomObject]@{
                CpuMs     = [int]$match.Groups[1].Value
                ElapsedMs = [int]$match.Groups[2].Value
            }
        }
    }
    return [PSCustomObject]@{
        Statements      = $exec
        CompileMessages = $compile
        TotalCpuMs      = ($exec | Measure-Object -Property CpuMs -Sum).Sum
        TotalElapsedMs  = ($exec | Measure-Object -Property ElapsedMs -Sum).Sum
        StatementCount  = $exec.Count
    }
}

# STATISTICS IO emits one message per accessed table per statement:
#   "Table 'TableName'. Scan count N, logical reads N, physical reads N,
#    page server reads N, read-ahead reads N, page server read-ahead reads N,
#    lob logical reads N, lob physical reads N, lob page server reads N,
#    lob read-ahead reads N, lob page server read-ahead reads N."
# Column set varies by SQL version - we use named capture groups and tolerate
# missing columns by defaulting to 0.
function Parse-StatisticsIO {
    param([string[]]$Messages)

    $rows = @()
    foreach ($msg in $Messages) {
        # Each Table '...' fact is one message, but sometimes multiple are
        # concatenated. Match all occurrences.
        $tableMatches = [regex]::Matches(
            $msg,
            "Table\s+'([^']+)'\.\s+(.+?)(?=Table\s+'|$)",
            [System.Text.RegularExpressions.RegexOptions]::Singleline
        )
        foreach ($tm in $tableMatches) {
            $tableName = $tm.Groups[1].Value
            $body = $tm.Groups[2].Value

            function _Cap([string]$body, [string]$label) {
                $rx = [regex]::Match($body, "$label\s*=?\s*(\d+)", 'IgnoreCase')
                if ($rx.Success) { return [int]$rx.Groups[1].Value } else { return 0 }
            }

            $rows += [PSCustomObject]@{
                TableName             = $tableName
                ScanCount             = _Cap $body 'Scan count'
                LogicalReads          = _Cap $body 'logical reads'
                PhysicalReads         = _Cap $body 'physical reads'
                ReadAheadReads        = _Cap $body 'read-ahead reads'
                LobLogicalReads       = _Cap $body 'lob logical reads'
                LobPhysicalReads      = _Cap $body 'lob physical reads'
                LobReadAheadReads     = _Cap $body 'lob read-ahead reads'
            }
        }
    }

    # Aggregate per-table across statements: callers can choose to look at the
    # raw rows (one per Table mention) or the aggregated totals.
    $aggregated = $rows | Group-Object TableName | ForEach-Object {
        $g = $_
        [PSCustomObject]@{
            TableName         = $g.Name
            ScanCount         = ($g.Group | Measure-Object ScanCount -Sum).Sum
            LogicalReads      = ($g.Group | Measure-Object LogicalReads -Sum).Sum
            PhysicalReads     = ($g.Group | Measure-Object PhysicalReads -Sum).Sum
            ReadAheadReads    = ($g.Group | Measure-Object ReadAheadReads -Sum).Sum
            LobLogicalReads   = ($g.Group | Measure-Object LobLogicalReads -Sum).Sum
            LobPhysicalReads  = ($g.Group | Measure-Object LobPhysicalReads -Sum).Sum
            LobReadAheadReads = ($g.Group | Measure-Object LobReadAheadReads -Sum).Sum
        }
    }

    return [PSCustomObject]@{
        Rows              = $rows
        Aggregated        = @($aggregated)
        TotalLogicalReads = ($rows | Measure-Object LogicalReads -Sum).Sum
        TotalPhysicalReads= ($rows | Measure-Object PhysicalReads -Sum).Sum
        TotalReadAhead    = ($rows | Measure-Object ReadAheadReads -Sum).Sum
    }
}

# ----------------------------------------------------------------------------
# Execution - run one batch, return parsed metrics
# ----------------------------------------------------------------------------

function Invoke-Batch {
    param(
        [string]$Server,
        [string]$Database,
        [string]$Sql,
        [bool]$CapturePlans,
        [bool]$SkipChecksum,
        [int]$ConnectionTimeout,
        [int]$CommandTimeout
    )

    $cs = "Server=$Server;Database=$Database;Integrated Security=SSPI;" +
          "Application Name=ProcBenchmark;Connect Timeout=$ConnectionTimeout"

    $conn = New-Object System.Data.SqlClient.SqlConnection $cs
    $messages = New-Object System.Collections.Generic.List[string]
    $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler]{
        param($sender, $e)
        # $e.Message has all info-level messages including PRINT and STATISTICS
        $messages.Add($e.Message)
    }
    $conn.add_InfoMessage($handler)
    # Suppress firing of InfoMessage for low-severity errors by not setting
    # FireInfoMessageEventOnUserErrors - default false is what we want.

    $plans = New-Object System.Collections.Generic.List[string]
    $rowCount = 0
    $checksum = $null
    $checksumAccum = New-Object System.Security.Cryptography.SHA1Managed
    $checksumAny = $false

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $conn.Open()

        # Prefix with the STATISTICS toggles. These are session-scoped and
        # tolerant of being set on already.
        $prefix = "SET STATISTICS TIME ON;`r`nSET STATISTICS IO ON;`r`n"
        if ($CapturePlans) { $prefix += "SET STATISTICS XML ON;`r`n" }

        $cmd = $conn.CreateCommand()
        $cmd.CommandText = $prefix + $Sql
        $cmd.CommandTimeout = $CommandTimeout

        $reader = $cmd.ExecuteReader()
        try {
            do {
                # Detect plan result set: SET STATISTICS XML ON returns a
                # single-column result set named "Microsoft SQL Server ... XML Showplan".
                $isPlan = $false
                if ($CapturePlans -and $reader.FieldCount -eq 1) {
                    $colName = $reader.GetName(0)
                    if ($colName -like '*Showplan*' -or $colName -like '*XML Showplan*') {
                        $isPlan = $true
                    }
                }

                if ($isPlan) {
                    while ($reader.Read()) {
                        $val = $reader.GetValue(0)
                        if ($null -ne $val -and $val -ne [DBNull]::Value) {
                            $plans.Add([string]$val)
                        }
                    }
                } else {
                    while ($reader.Read()) {
                        $rowCount++
                        if (-not $SkipChecksum) {
                            # Build a single string from the row's values
                            $sb = New-Object System.Text.StringBuilder
                            for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                                $v = $reader.GetValue($i)
                                [void]$sb.Append([string]$v)
                                [void]$sb.Append([char]31) # unit separator
                            }
                            [void]$sb.Append([char]30) # record separator
                            $bytes = [System.Text.Encoding]::UTF8.GetBytes($sb.ToString())
                            [void]$checksumAccum.TransformBlock($bytes, 0, $bytes.Length, $null, 0)
                            $checksumAny = $true
                        }
                    }
                }
            } while ($reader.NextResult())
        } finally {
            $reader.Dispose()
        }

        if ($checksumAny) {
            [void]$checksumAccum.TransformFinalBlock(@(), 0, 0)
            $checksum = ([System.BitConverter]::ToString($checksumAccum.Hash) -replace '-','').ToLower()
        }

        $sw.Stop()

        $time = Parse-StatisticsTime -Messages $messages
        $io   = Parse-StatisticsIO   -Messages $messages

        return [PSCustomObject]@{
            Success         = $true
            WallClockMs     = [int]$sw.Elapsed.TotalMilliseconds
            RowCount        = $rowCount
            Checksum        = $checksum
            CpuMs           = $time.TotalCpuMs
            ElapsedMs       = $time.TotalElapsedMs
            StatementCount  = $time.StatementCount
            Statements      = $time.Statements
            LogicalReads    = $io.TotalLogicalReads
            PhysicalReads   = $io.TotalPhysicalReads
            ReadAheadReads  = $io.TotalReadAhead
            IORows          = $io.Aggregated
            Plans           = @($plans)
            RawMessages     = @($messages)
            ErrorMessage    = $null
        }
    } catch {
        $sw.Stop()
        return [PSCustomObject]@{
            Success         = $false
            WallClockMs     = [int]$sw.Elapsed.TotalMilliseconds
            RowCount        = $null
            Checksum        = $null
            CpuMs           = $null
            ElapsedMs       = $null
            StatementCount  = $null
            Statements      = @()
            LogicalReads    = $null
            PhysicalReads   = $null
            ReadAheadReads  = $null
            IORows          = @()
            Plans           = @()
            RawMessages     = @($messages)
            ErrorMessage    = $_.Exception.Message
        }
    } finally {
        if ($conn.State -ne 'Closed') { $conn.Close() }
        $conn.Dispose()
        $checksumAccum.Dispose()
    }
}

# ----------------------------------------------------------------------------
# Driver - walk the config, run iterations, collect results
# ----------------------------------------------------------------------------

$sites = Import-Csv -Path $ConfigPath
Write-Log "Loaded $($sites.Count) site(s) from config."

$summaryRows  = New-Object System.Collections.Generic.List[object]
$ioDetailRows = New-Object System.Collections.Generic.List[object]
$timeDetailRows = New-Object System.Collections.Generic.List[object]
$plansIndex   = New-Object System.Collections.Generic.List[object]
$errorRows    = New-Object System.Collections.Generic.List[object]

function Get-RowValue {
    param($Row, [string]$Column, $Default = $null)
    if ($Row.PSObject.Properties.Name -contains $Column -and -not [string]::IsNullOrWhiteSpace($Row.$Column)) {
        return $Row.$Column
    }
    return $Default
}

function Save-Plans {
    param(
        [string[]]$PlanXmls,
        [string]$SitePrefix,
        [string]$Version,
        [int]$Iteration
    )
    $paths = @()
    for ($i = 0; $i -lt $PlanXmls.Count; $i++) {
        $filename = "{0}_{1}_iter{2}_stmt{3:00}.sqlplan" -f $SitePrefix, $Version, $Iteration, ($i + 1)
        $path = Join-Path $plansDir $filename
        Set-Content -LiteralPath $path -Value $PlanXmls[$i] -Encoding UTF8
        $paths += $path
    }
    return $paths
}

function Run-Version {
    param(
        [string]$SiteName,
        [string]$SitePrefix,
        [string]$Server,
        [string]$Database,
        [string]$Sql,
        [string]$Version,
        [int]$Iterations,
        [int]$WarmupRuns
    )
    $results = @()

    # Warmups - results discarded
    for ($w = 0; $w -lt $WarmupRuns; $w++) {
        Write-Log "  [$SiteName] $Version warmup $($w+1)/$WarmupRuns"
        [void](Invoke-Batch -Server $Server -Database $Database -Sql $Sql `
            -CapturePlans $false -SkipChecksum $true `
            -ConnectionTimeout $ConnectionTimeout -CommandTimeout $CommandTimeout)
    }

    for ($i = 1; $i -le $Iterations; $i++) {
        Write-Log "  [$SiteName] $Version iter $i/$Iterations"
        $r = Invoke-Batch -Server $Server -Database $Database -Sql $Sql `
            -CapturePlans $CapturePlans -SkipChecksum $SkipChecksum `
            -ConnectionTimeout $ConnectionTimeout -CommandTimeout $CommandTimeout

        $planPaths = @()
        if ($r.Success -and $r.Plans.Count -gt 0) {
            $planPaths = Save-Plans -PlanXmls $r.Plans -SitePrefix $SitePrefix `
                -Version $Version -Iteration $i
            foreach ($p in $planPaths) {
                $plansIndex.Add([PSCustomObject]@{
                    SiteName  = $SiteName
                    Version   = $Version
                    Iteration = $i
                    PlanFile  = $p
                })
            }
        }

        if (-not $r.Success) {
            $errorRows.Add([PSCustomObject]@{
                SiteName  = $SiteName
                Server    = $Server
                Database  = $Database
                Version   = $Version
                Iteration = $i
                Error     = $r.ErrorMessage
            })
            Write-Log "    FAILED: $($r.ErrorMessage)" 'ERROR'
        }

        foreach ($io in $r.IORows) {
            $ioDetailRows.Add([PSCustomObject]@{
                SiteName          = $SiteName
                Version           = $Version
                Iteration         = $i
                TableName         = $io.TableName
                ScanCount         = $io.ScanCount
                LogicalReads      = $io.LogicalReads
                PhysicalReads     = $io.PhysicalReads
                ReadAheadReads    = $io.ReadAheadReads
                LobLogicalReads   = $io.LobLogicalReads
                LobPhysicalReads  = $io.LobPhysicalReads
                LobReadAheadReads = $io.LobReadAheadReads
            })
        }

        for ($s = 0; $s -lt $r.Statements.Count; $s++) {
            $timeDetailRows.Add([PSCustomObject]@{
                SiteName       = $SiteName
                Version        = $Version
                Iteration      = $i
                StatementIndex = $s + 1
                CpuMs          = $r.Statements[$s].CpuMs
                ElapsedMs      = $r.Statements[$s].ElapsedMs
            })
        }

        $results += $r
    }

    return $results
}

function _NullSafe { param($a, $b, [scriptblock]$op)
    if ($null -eq $a -or $null -eq $b) { return $null }
    return & $op $a $b
}

$siteIdx = 0
foreach ($site in $sites) {
    $siteIdx++
    $siteName = Get-RowValue $site 'SiteName' "site$siteIdx"
    $server   = Get-RowValue $site 'ServerName'
    $database = Get-RowValue $site 'DatabaseName'
    $origSql  = Get-RowValue $site 'OriginalSql'
    $newSql   = Get-RowValue $site 'RefactoredSql'

    if (-not $server -or -not $database) {
        Write-Log "Skipping row $siteIdx ($siteName): missing ServerName or DatabaseName" 'WARN'
        continue
    }
    if (-not $origSql -or -not $newSql) {
        Write-Log "Skipping row $siteIdx ($siteName): missing OriginalSql or RefactoredSql" 'WARN'
        continue
    }

    Write-Log "[$siteIdx/$($sites.Count)] $siteName ($server / $database)"

    # Build a filesystem-safe prefix for plan filenames
    $sitePrefix = ($siteName -replace '[^A-Za-z0-9_-]','_')

    # Wrap with @() so a single-iteration result is still treated as an array.
    $origResults = @(Run-Version -SiteName $siteName -SitePrefix $sitePrefix `
        -Server $server -Database $database -Sql $origSql -Version 'Original' `
        -Iterations $Iterations -WarmupRuns $WarmupRuns)

    $newResults = @(Run-Version -SiteName $siteName -SitePrefix $sitePrefix `
        -Server $server -Database $database -Sql $newSql -Version 'Refactored' `
        -Iterations $Iterations -WarmupRuns $WarmupRuns)

    # Build paired summary rows by iteration index.
    for ($i = 0; $i -lt $Iterations; $i++) {
        $o = $origResults[$i]
        $n = $newResults[$i]

        $deltaCpu      = _NullSafe $n.CpuMs        $o.CpuMs        { param($a,$b) $a - $b }
        $deltaElapsed  = _NullSafe $n.ElapsedMs    $o.ElapsedMs    { param($a,$b) $a - $b }
        $deltaLogical  = _NullSafe $n.LogicalReads $o.LogicalReads { param($a,$b) $a - $b }

        $pctCpu = $null
        if ($null -ne $o.CpuMs -and $o.CpuMs -gt 0 -and $null -ne $n.CpuMs) {
            $pctCpu = [math]::Round((($n.CpuMs - $o.CpuMs) / $o.CpuMs) * 100, 2)
        }
        $pctElapsed = $null
        if ($null -ne $o.ElapsedMs -and $o.ElapsedMs -gt 0 -and $null -ne $n.ElapsedMs) {
            $pctElapsed = [math]::Round((($n.ElapsedMs - $o.ElapsedMs) / $o.ElapsedMs) * 100, 2)
        }
        $pctLogical = $null
        if ($null -ne $o.LogicalReads -and $o.LogicalReads -gt 0 -and $null -ne $n.LogicalReads) {
            $pctLogical = [math]::Round((($n.LogicalReads - $o.LogicalReads) / $o.LogicalReads) * 100, 2)
        }

        $rowsMatch = if ($null -ne $o.RowCount -and $null -ne $n.RowCount) { $o.RowCount -eq $n.RowCount } else { $null }
        $dataMatch = $null
        if (-not $SkipChecksum -and $o.Checksum -and $n.Checksum) {
            $dataMatch = ($o.Checksum -eq $n.Checksum)
        }

        $status = if ($o.Success -and $n.Success) { 'OK' } else { 'ERROR' }

        $summaryRows.Add([PSCustomObject]@{
            SiteName             = $siteName
            Server               = $server
            Database             = $database
            Iteration            = $i + 1
            Status               = $status
            Original_CpuMs       = $o.CpuMs
            Refactored_CpuMs     = $n.CpuMs
            Delta_CpuMs          = $deltaCpu
            Pct_CpuChange        = $pctCpu
            Original_ElapsedMs   = $o.ElapsedMs
            Refactored_ElapsedMs = $n.ElapsedMs
            Delta_ElapsedMs      = $deltaElapsed
            Pct_ElapsedChange    = $pctElapsed
            Original_LogicalReads   = $o.LogicalReads
            Refactored_LogicalReads = $n.LogicalReads
            Delta_LogicalReads      = $deltaLogical
            Pct_LogicalChange       = $pctLogical
            Original_PhysicalReads   = $o.PhysicalReads
            Refactored_PhysicalReads = $n.PhysicalReads
            Original_RowCount    = $o.RowCount
            Refactored_RowCount  = $n.RowCount
            RowCount_Match       = $rowsMatch
            Data_Match           = $dataMatch
            Original_WallClockMs   = $o.WallClockMs
            Refactored_WallClockMs = $n.WallClockMs
        })
    }
}

# ----------------------------------------------------------------------------
# Output
# ----------------------------------------------------------------------------

$xlsxPath = Join-Path $OutputDir 'results.xlsx'

if ($haveImportExcel -and $summaryRows.Count -gt 0) {
    Import-Module ImportExcel
    Write-Log "Writing $xlsxPath"

    # Summary sheet with conditional formatting
    $summaryRows | Export-Excel -Path $xlsxPath -WorksheetName 'Summary' `
        -AutoSize -FreezeTopRow -BoldTopRow -AutoFilter -TableName 'Summary' `
        -ConditionalText @(
            New-ConditionalText -Text 'ERROR' -ConditionalTextColor White -BackgroundColor Red
        )

    # Color the delta and pct columns: green if <=0 (improvement), red if >0 (regression)
    $excel = Open-ExcelPackage -Path $xlsxPath
    $ws = $excel.Workbook.Worksheets['Summary']
    $lastRow = $ws.Dimension.End.Row
    $deltaCols = @('Delta_CpuMs','Pct_CpuChange','Delta_ElapsedMs','Pct_ElapsedChange','Delta_LogicalReads','Pct_LogicalChange')
    foreach ($colName in $deltaCols) {
        $colIdx = $null
        for ($c = 1; $c -le $ws.Dimension.End.Column; $c++) {
            if ($ws.Cells[1, $c].Text -eq $colName) { $colIdx = $c; break }
        }
        if (-not $colIdx) { continue }
        $colLetter = [OfficeOpenXml.ExcelCellAddress]::GetColumnLetter($colIdx)
        $range = "${colLetter}2:${colLetter}${lastRow}"
        Add-ConditionalFormatting -Worksheet $ws -Address $range -RuleType LessThanOrEqual `
            -ConditionValue 0 -BackgroundColor LightGreen
        Add-ConditionalFormatting -Worksheet $ws -Address $range -RuleType GreaterThan `
            -ConditionValue 0 -BackgroundColor LightPink
    }

    # RowCount_Match / Data_Match: red on FALSE
    foreach ($flagCol in @('RowCount_Match','Data_Match')) {
        $colIdx = $null
        for ($c = 1; $c -le $ws.Dimension.End.Column; $c++) {
            if ($ws.Cells[1, $c].Text -eq $flagCol) { $colIdx = $c; break }
        }
        if (-not $colIdx) { continue }
        $colLetter = [OfficeOpenXml.ExcelCellAddress]::GetColumnLetter($colIdx)
        $range = "${colLetter}2:${colLetter}${lastRow}"
        Add-ConditionalFormatting -Worksheet $ws -Address $range -RuleType ContainsText `
            -ConditionValue 'False' -BackgroundColor Salmon
    }
    Close-ExcelPackage $excel

    if ($ioDetailRows.Count -gt 0) {
        $ioDetailRows | Export-Excel -Path $xlsxPath -WorksheetName 'IO Detail' `
            -AutoSize -FreezeTopRow -BoldTopRow -AutoFilter -TableName 'IODetail'
    }
    if ($timeDetailRows.Count -gt 0) {
        $timeDetailRows | Export-Excel -Path $xlsxPath -WorksheetName 'Time Detail' `
            -AutoSize -FreezeTopRow -BoldTopRow -AutoFilter -TableName 'TimeDetail'
    }
    if ($plansIndex.Count -gt 0) {
        $plansIndex | Export-Excel -Path $xlsxPath -WorksheetName 'Plans Index' `
            -AutoSize -FreezeTopRow -BoldTopRow -AutoFilter -TableName 'PlansIndex'
    }
    if ($errorRows.Count -gt 0) {
        $errorRows | Export-Excel -Path $xlsxPath -WorksheetName 'Errors' `
            -AutoSize -FreezeTopRow -BoldTopRow -AutoFilter -TableName 'Errors'
    }

    Write-Log "DONE: $xlsxPath"
} else {
    # CSV fallback
    Write-Log "Writing CSV output (no ImportExcel)"
    $summaryRows  | Export-Csv -NoTypeInformation -Path (Join-Path $OutputDir 'summary.csv')
    $ioDetailRows | Export-Csv -NoTypeInformation -Path (Join-Path $OutputDir 'io_detail.csv')
    $timeDetailRows | Export-Csv -NoTypeInformation -Path (Join-Path $OutputDir 'time_detail.csv')
    $plansIndex   | Export-Csv -NoTypeInformation -Path (Join-Path $OutputDir 'plans_index.csv')
    $errorRows    | Export-Csv -NoTypeInformation -Path (Join-Path $OutputDir 'errors.csv')
    Write-Log "DONE: CSV files in $OutputDir"
}

if ($errorRows.Count -gt 0) {
    Write-Log "$($errorRows.Count) failure(s) recorded - see Errors sheet" 'WARN'
}
