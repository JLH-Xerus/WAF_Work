<#
.SYNOPSIS
    Unit tests for the Parse-StatisticsTime and Parse-StatisticsIO helpers in
    Invoke-ProcBenchmark.ps1.

.DESCRIPTION
    The parsers turn STATISTICS TIME / STATISTICS IO InfoMessage strings into
    structured objects. These tests feed in real-world sample output captured
    from SQL Server 2019 and 2022 and assert the parsed fields match.

    Run from the ProcBenchmark folder:
        .\tests\Test-Parsers.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

# Dot-source the script so we get the functions, but skip the body that requires
# -ConfigPath. We do this by re-defining the functions inline here, copied
# verbatim from Invoke-ProcBenchmark.ps1. If you change the parsers there,
# update them here too.

function Parse-StatisticsTime {
    param([string[]]$Messages)
    $exec = @()
    $compile = @()
    foreach ($msg in $Messages) {
        $pattern = 'SQL Server Execution Times:\s*CPU time\s*=\s*(\d+)\s*ms,\s*elapsed time\s*=\s*(\d+)\s*ms'
        foreach ($match in [regex]::Matches($msg, $pattern)) {
            $exec += [PSCustomObject]@{ CpuMs=[int]$match.Groups[1].Value; ElapsedMs=[int]$match.Groups[2].Value }
        }
        $compilePattern = 'SQL Server parse and compile time:\s*CPU time\s*=\s*(\d+)\s*ms,\s*elapsed time\s*=\s*(\d+)\s*ms'
        foreach ($match in [regex]::Matches($msg, $compilePattern)) {
            $compile += [PSCustomObject]@{ CpuMs=[int]$match.Groups[1].Value; ElapsedMs=[int]$match.Groups[2].Value }
        }
    }
    [PSCustomObject]@{
        Statements      = $exec
        CompileMessages = $compile
        TotalCpuMs      = ($exec | Measure-Object CpuMs -Sum).Sum
        TotalElapsedMs  = ($exec | Measure-Object ElapsedMs -Sum).Sum
        StatementCount  = $exec.Count
    }
}

function Parse-StatisticsIO {
    param([string[]]$Messages)
    $rows = @()
    foreach ($msg in $Messages) {
        $tableMatches = [regex]::Matches(
            $msg,
            "Table\s+'([^']+)'\.\s+(.+?)(?=Table\s+'|$)",
            [System.Text.RegularExpressions.RegexOptions]::Singleline
        )
        foreach ($tm in $tableMatches) {
            $tableName = $tm.Groups[1].Value
            $body = $tm.Groups[2].Value
            function _Cap([string]$b, [string]$label) {
                $rx = [regex]::Match($b, "$label\s*=?\s*(\d+)", 'IgnoreCase')
                if ($rx.Success) { [int]$rx.Groups[1].Value } else { 0 }
            }
            $rows += [PSCustomObject]@{
                TableName         = $tableName
                ScanCount         = _Cap $body 'Scan count'
                LogicalReads      = _Cap $body 'logical reads'
                PhysicalReads     = _Cap $body 'physical reads'
                ReadAheadReads    = _Cap $body 'read-ahead reads'
                LobLogicalReads   = _Cap $body 'lob logical reads'
                LobPhysicalReads  = _Cap $body 'lob physical reads'
                LobReadAheadReads = _Cap $body 'lob read-ahead reads'
            }
        }
    }
    [PSCustomObject]@{
        Rows              = $rows
        TotalLogicalReads = ($rows | Measure-Object LogicalReads -Sum).Sum
        TotalPhysicalReads= ($rows | Measure-Object PhysicalReads -Sum).Sum
        TotalReadAhead    = ($rows | Measure-Object ReadAheadReads -Sum).Sum
    }
}

# ----------------------------------------------------------------------------
# Test runner
# ----------------------------------------------------------------------------

$script:Failures = 0
$script:Passes   = 0

function Assert-Equal {
    param($Expected, $Actual, [string]$Name)
    if ($Expected -eq $Actual) {
        $script:Passes++
        Write-Host "  PASS  $Name" -ForegroundColor Green
    } else {
        $script:Failures++
        Write-Host "  FAIL  $Name  expected=<$Expected>  actual=<$Actual>" -ForegroundColor Red
    }
}

# ----------------------------------------------------------------------------
# STATISTICS TIME — single statement
# ----------------------------------------------------------------------------
Write-Host "`n[STATISTICS TIME] single statement"
$msg1 = @"
SQL Server parse and compile time:
   CPU time = 0 ms, elapsed time = 1 ms.

 SQL Server Execution Times:
   CPU time = 47 ms,  elapsed time = 123 ms.
"@
$r = Parse-StatisticsTime -Messages @($msg1)
Assert-Equal 1   $r.StatementCount  "one execution time captured"
Assert-Equal 47  $r.TotalCpuMs      "CPU = 47ms"
Assert-Equal 123 $r.TotalElapsedMs  "Elapsed = 123ms"
Assert-Equal 1   $r.CompileMessages.Count "one compile message"

# ----------------------------------------------------------------------------
# STATISTICS TIME — multi-statement batch (matches the GetListOfImages refactor)
# ----------------------------------------------------------------------------
Write-Host "`n[STATISTICS TIME] multi-statement batch"
$msg2 = @"
SQL Server parse and compile time:
   CPU time = 0 ms, elapsed time = 0 ms.
 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.
SQL Server parse and compile time:
   CPU time = 0 ms, elapsed time = 0 ms.
 SQL Server Execution Times:
   CPU time = 16 ms,  elapsed time = 22 ms.
SQL Server parse and compile time:
   CPU time = 0 ms, elapsed time = 0 ms.
 SQL Server Execution Times:
   CPU time = 250 ms,  elapsed time = 412 ms.
 SQL Server Execution Times:
   CPU time = 0 ms,  elapsed time = 0 ms.
"@
$r = Parse-StatisticsTime -Messages @($msg2)
Assert-Equal 4   $r.StatementCount  "four execution times captured"
Assert-Equal 266 $r.TotalCpuMs      "CPU sum = 266ms (0+16+250+0)"
Assert-Equal 434 $r.TotalElapsedMs  "Elapsed sum = 434ms (0+22+412+0)"

# ----------------------------------------------------------------------------
# STATISTICS TIME — split across multiple InfoMessage strings (what SqlClient
# actually delivers: each message can be a fragment)
# ----------------------------------------------------------------------------
Write-Host "`n[STATISTICS TIME] split across messages"
$msgs = @(
    "SQL Server parse and compile time: `n   CPU time = 0 ms, elapsed time = 1 ms.",
    " SQL Server Execution Times:`n   CPU time = 100 ms,  elapsed time = 200 ms."
)
$r = Parse-StatisticsTime -Messages $msgs
Assert-Equal 1   $r.StatementCount  "one execution time across fragments"
Assert-Equal 100 $r.TotalCpuMs      "CPU = 100ms"
Assert-Equal 200 $r.TotalElapsedMs  "Elapsed = 200ms"

# ----------------------------------------------------------------------------
# STATISTICS IO — single table
# ----------------------------------------------------------------------------
Write-Host "`n[STATISTICS IO] single table"
$io1 = "Table 'vImgImage'. Scan count 1, logical reads 1247, physical reads 3, page server reads 0, read-ahead reads 102, page server read-ahead reads 0, lob logical reads 5, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0."
$r = Parse-StatisticsIO -Messages @($io1)
Assert-Equal 1    $r.Rows.Count             "one table row"
Assert-Equal 'vImgImage' $r.Rows[0].TableName "table name"
Assert-Equal 1    $r.Rows[0].ScanCount       "scan count = 1"
Assert-Equal 1247 $r.Rows[0].LogicalReads    "logical reads = 1247"
Assert-Equal 3    $r.Rows[0].PhysicalReads   "physical reads = 3"
Assert-Equal 102  $r.Rows[0].ReadAheadReads  "read-ahead reads = 102"
Assert-Equal 5    $r.Rows[0].LobLogicalReads "lob logical reads = 5"

# ----------------------------------------------------------------------------
# STATISTICS IO — multiple tables in one message
# ----------------------------------------------------------------------------
Write-Host "`n[STATISTICS IO] multiple tables in one message"
$io2 = @"
Table 'oeorderhistory'. Scan count 5, logical reads 8421, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.
Table 'ImgRxImgAssoc'. Scan count 5, logical reads 2103, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.
Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, page server reads 0, read-ahead reads 0, page server read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob page server reads 0, lob read-ahead reads 0, lob page server read-ahead reads 0.
"@
$r = Parse-StatisticsIO -Messages @($io2)
Assert-Equal 3     $r.Rows.Count           "three table rows"
Assert-Equal 10524 $r.TotalLogicalReads    "sum logical = 8421+2103+0"
$oeRow = $r.Rows | Where-Object TableName -eq 'oeorderhistory'
Assert-Equal 8421  $oeRow.LogicalReads     "oeorderhistory logical reads"
$irRow = $r.Rows | Where-Object TableName -eq 'ImgRxImgAssoc'
Assert-Equal 5     $irRow.ScanCount        "ImgRxImgAssoc scan count"

# ----------------------------------------------------------------------------
# STATISTICS IO — older SQL Server version with shorter column set
# ----------------------------------------------------------------------------
Write-Host "`n[STATISTICS IO] older SQL Server format (no page server columns)"
$io3 = "Table 'CanCanister'. Scan count 1, logical reads 47, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0."
$r = Parse-StatisticsIO -Messages @($io3)
Assert-Equal 1   $r.Rows.Count         "one row"
Assert-Equal 47  $r.Rows[0].LogicalReads "logical reads parsed without page-server columns"
Assert-Equal 0   $r.Rows[0].LobLogicalReads "lob logical reads = 0"

# ----------------------------------------------------------------------------
# STATISTICS IO — table name with special characters
# ----------------------------------------------------------------------------
Write-Host "`n[STATISTICS IO] schema-qualified table name"
$io4 = "Table 'dbo.MyTable_2024'. Scan count 1, logical reads 99, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0."
$r = Parse-StatisticsIO -Messages @($io4)
Assert-Equal 'dbo.MyTable_2024' $r.Rows[0].TableName "qualified table name"
Assert-Equal 99 $r.Rows[0].LogicalReads "logical reads"

# ----------------------------------------------------------------------------
# STATISTICS IO — empty / no matches
# ----------------------------------------------------------------------------
Write-Host "`n[STATISTICS IO] no matches"
$r = Parse-StatisticsIO -Messages @("(8 rows affected)", "Command(s) completed successfully.")
Assert-Equal 0 $r.Rows.Count          "no rows from non-IO messages"
Assert-Equal 0 $r.TotalLogicalReads   "zero total"

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
Write-Host ""
Write-Host "----------------------------------------"
Write-Host ("Passed: {0}   Failed: {1}" -f $script:Passes, $script:Failures) -ForegroundColor (& { if ($script:Failures) { 'Red' } else { 'Green' } })
if ($script:Failures -gt 0) { exit 1 } else { exit 0 }
