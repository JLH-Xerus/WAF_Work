<#
.SYNOPSIS
    Runs the WAF SQL Server configuration-audit script collection against a
    list of instances and produces one Excel workbook per instance.

.DESCRIPTION
    For each instance in $Instances, this script:
        1. Opens a connection using the supplied connection string.
        2. Executes every .sql file in $ScriptFolder (sorted by name), one at
           a time, capturing ALL result sets the script returns.
        3. Writes each result set to its own worksheet in a workbook named
           after the instance (e.g., PROD-DB01.xlsx).
        4. Writes a Summary worksheet at the top of each workbook with one
           row per script: rows captured, sheets produced, errors.

    The script is read-only - it only runs the SELECT-based audit scripts in
    this folder. No data is modified on the target instance.

    Requires:
        * Windows PowerShell 5.1+ or PowerShell 7+
        * ImportExcel module (Install-Module ImportExcel -Scope CurrentUser)
        * Network connectivity to each target instance

.PARAMETER ScriptFolder
    Folder containing the *.sql files. Defaults to the folder this script
    lives in.

.PARAMETER OutputFolder
    Where workbooks are written. Defaults to .\output next to this script.

.PARAMETER CommandTimeoutSec
    Per-script timeout. Default 600 seconds (10 min). Wait-stats and Query
    Store can be slow on busy instances.

.PARAMETER IncludePreflight
    Switch. If set, runs 00_preflight_validate_dmvs.sql for each instance and
    writes its results to the workbook. Useful the first time you run against
    a new build to catch DMV/column mismatches.

.PARAMETER ScriptNamePattern
    Wildcard pattern for which .sql files to run. Default '[0-9][0-9]_*.sql'
    picks up the numbered scripts and skips 00_run_all.sql (which is a
    SQLCMD driver, not a collection script). 00_preflight is excluded
    unless -IncludePreflight is supplied.

.EXAMPLE
    .\Run-WafConfigAudit.ps1

    Runs every numbered audit script against every instance in $Instances
    and produces output\<InstanceName>.xlsx for each.

.EXAMPLE
    .\Run-WafConfigAudit.ps1 -IncludePreflight -CommandTimeoutSec 1200

    Includes the preflight DMV validation and gives each script up to 20
    minutes to complete.

.NOTES
    Edit the $Instances array below to add the 14 instances. The
    InstanceName drives the workbook filename; the ConnectionString is what
    SqlClient connects with. Trusted connections and SQL auth are both
    supported - it's just a standard ADO.NET connection string.
#>
[CmdletBinding()]
param(
    [string] $ScriptFolder       = $PSScriptRoot,
    [string] $OutputFolder       = (Join-Path $PSScriptRoot 'output'),
    [int]    $CommandTimeoutSec  = 600,
    [switch] $IncludePreflight,
    [string] $ScriptNamePattern  = '[0-9][0-9]_*.sql'
)

#region ==== Instance list ====================================================
#
# Edit this array. One entry per instance you want to audit.
#
#   InstanceName       - free-form label; drives the workbook filename. Avoid
#                        characters not legal in Windows filenames (\ / : * ? " < > |).
#   ConnectionString   - any valid ADO.NET connection string. Examples:
#                          Integrated:
#                            "Server=PROD-LISTENER;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit"
#                          SQL auth:
#                            "Server=PROD-LISTENER,1433;Database=master;User Id=svc_dba;Password=...;TrustServerCertificate=true;Application Name=WAF Config Audit"
#
# Tips:
#   * Point Server= at the listener (FCI VNN or AG listener) rather than a
#     node hostname so you always read from the active node.
#   * Always set Application Name so it shows up in sp_who2 / activity monitor
#     while the audit runs.
#   * Set TrustServerCertificate=true if your SQL cert is self-signed.
#
$Instances = @(
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER01'; ConnectionString = 'Server=PROD-CLUSTER01;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER02'; ConnectionString = 'Server=PROD-CLUSTER02;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER03'; ConnectionString = 'Server=PROD-CLUSTER03;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER04'; ConnectionString = 'Server=PROD-CLUSTER04;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER05'; ConnectionString = 'Server=PROD-CLUSTER05;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER06'; ConnectionString = 'Server=PROD-CLUSTER06;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER07'; ConnectionString = 'Server=PROD-CLUSTER07;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER08'; ConnectionString = 'Server=PROD-CLUSTER08;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER09'; ConnectionString = 'Server=PROD-CLUSTER09;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER10'; ConnectionString = 'Server=PROD-CLUSTER10;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER11'; ConnectionString = 'Server=PROD-CLUSTER11;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER12'; ConnectionString = 'Server=PROD-CLUSTER12;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER13'; ConnectionString = 'Server=PROD-CLUSTER13;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
    [pscustomobject]@{ InstanceName = 'PROD-CLUSTER14'; ConnectionString = 'Server=PROD-CLUSTER14;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit' }
)
#endregion ====================================================================

#region ==== Prereqs and helpers =============================================
$ErrorActionPreference = 'Stop'

if (-not (Get-Module -ListAvailable -Name ImportExcel)) {
    throw "The ImportExcel module is required. Install with:  Install-Module -Name ImportExcel -Scope CurrentUser"
}
Import-Module ImportExcel -ErrorAction Stop

# SqlClient is in the .NET framework on PS 5.1 and shipped with PS 7. No
# import needed - it's available via the [System.Data.SqlClient] namespace.

if (-not (Test-Path -Path $ScriptFolder)) {
    throw "Script folder not found: $ScriptFolder"
}
if (-not (Test-Path -Path $OutputFolder)) {
    New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null
}

function Get-AuditSqlFiles {
    param(
        [string] $Folder,
        [string] $Pattern,
        [switch] $IncludePreflight
    )
    $files = Get-ChildItem -Path $Folder -Filter $Pattern -File |
             Sort-Object Name |
             Where-Object { $_.Name -ne '00_run_all.sql' }   # SQLCMD driver, skip

    if ($IncludePreflight) {
        $pre = Get-ChildItem -Path $Folder -Filter '00_preflight_validate_dmvs.sql' -File -ErrorAction SilentlyContinue
        if ($pre) {
            $files = @($pre) + $files
        }
    }
    return $files
}

function ConvertTo-SafeFileName {
    param([string] $Name)
    $invalid = [IO.Path]::GetInvalidFileNameChars()
    $out = $Name
    foreach ($c in $invalid) { $out = $out.Replace($c, '_') }
    return $out -replace '[\\/:*?"<>|]', '_'
}

function ConvertTo-SafeSheetName {
    <#
        Excel rules:
            * max 31 chars
            * cannot contain : \ / ? * [ ]
            * cannot be blank
    #>
    param(
        [string]   $Name,
        [hashtable]$UsedNames
    )
    if ([string]::IsNullOrWhiteSpace($Name)) { $Name = 'Sheet' }
    $clean = $Name -replace '[:\\/\?\*\[\]]', '_'
    if ($clean.Length -gt 31) { $clean = $clean.Substring(0, 31) }

    # Enforce uniqueness within a workbook
    $candidate = $clean
    $suffix = 2
    while ($UsedNames.ContainsKey($candidate.ToLowerInvariant())) {
        $tag = "~$suffix"
        $base = $clean
        if (($base.Length + $tag.Length) -gt 31) {
            $base = $base.Substring(0, 31 - $tag.Length)
        }
        $candidate = "$base$tag"
        $suffix++
    }
    $UsedNames[$candidate.ToLowerInvariant()] = $true
    return $candidate
}

function Get-SectionLabel {
    <#
        Many of the audit scripts emit a column named 'section' as the first
        column of every result set. If present, use that for the sheet name.
        Otherwise fall back to a generated label.
    #>
    param(
        [System.Data.DataTable] $Table,
        [string] $ScriptStem,
        [int]    $ResultIndex
    )
    if ($Table.Columns.Contains('section') -and $Table.Rows.Count -gt 0) {
        $val = $Table.Rows[0]['section']
        if ($val -and "$val" -ne [System.DBNull]::Value.ToString()) {
            return "$ScriptStem $val"
        }
    }
    return ('{0} rs{1}' -f $ScriptStem, $ResultIndex)
}

function Invoke-SqlScriptAllResults {
    <#
        Runs a script against a SQL Server instance using
        SqlDataAdapter.Fill so that ALL result sets are captured (one
        DataTable per result set). Also collects any PRINT / INFO messages.
        Returns a hashtable: @{ Tables = [...]; Messages = [...]; Elapsed = TimeSpan }
    #>
    param(
        [string] $ConnectionString,
        [string] $ScriptText,
        [int]    $TimeoutSec
    )

    $messages = New-Object System.Collections.Generic.List[string]
    $ds = New-Object System.Data.DataSet
    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    $conn = New-Object System.Data.SqlClient.SqlConnection $ConnectionString
    try {
        # Capture PRINT / RAISERROR-with-no-severity / informational messages
        $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {
            param($eventSender, $e)
            foreach ($err in $e.Errors) {
                $messages.Add("[$($err.Number)] $($err.Message)") | Out-Null
            }
        }
        $conn.add_InfoMessage($handler)
        $conn.FireInfoMessageEventOnUserErrors = $false

        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandText    = $ScriptText
        $cmd.CommandTimeout = $TimeoutSec
        $cmd.CommandType    = 'Text'

        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $cmd
        $null = $adapter.Fill($ds)
        $sw.Stop()

        return [pscustomobject]@{
            Tables   = @($ds.Tables)
            Messages = $messages.ToArray()
            Elapsed  = $sw.Elapsed
        }
    }
    finally {
        if ($conn.State -ne 'Closed') { $conn.Close() }
        $conn.Dispose()
    }
}
#endregion ====================================================================

#region ==== Main loop =======================================================
$scripts = Get-AuditSqlFiles -Folder $ScriptFolder -Pattern $ScriptNamePattern -IncludePreflight:$IncludePreflight
if (-not $scripts) {
    throw "No audit scripts matched pattern '$ScriptNamePattern' in $ScriptFolder"
}

Write-Host ""
Write-Host ("Found {0} audit scripts in {1}:" -f $scripts.Count, $ScriptFolder) -ForegroundColor Cyan
$scripts | ForEach-Object { Write-Host "    $($_.Name)" }
Write-Host ""
Write-Host ("Will audit {0} instances. Output -> {1}" -f $Instances.Count, $OutputFolder) -ForegroundColor Cyan
Write-Host ""

$overallSw = [System.Diagnostics.Stopwatch]::StartNew()
$instanceIndex = 0

foreach ($inst in $Instances) {
    $instanceIndex++
    $instStart = Get-Date
    $safeName  = ConvertTo-SafeFileName -Name $inst.InstanceName
    $workbook  = Join-Path $OutputFolder ($safeName + '.xlsx')

    Write-Host ("[{0}/{1}] {2}" -f $instanceIndex, $Instances.Count, $inst.InstanceName) -ForegroundColor Yellow
    Write-Host ("       -> {0}" -f $workbook)

    if (Test-Path $workbook) {
        Remove-Item $workbook -Force
    }

    $usedSheetNames = @{}
    $summaryRows    = New-Object System.Collections.Generic.List[object]

    # Quick connectivity test before we burn time on script execution
    try {
        $testConn = New-Object System.Data.SqlClient.SqlConnection $inst.ConnectionString
        $testConn.Open()
        $testCmd = $testConn.CreateCommand()
        $testCmd.CommandText = "SELECT @@SERVERNAME AS server_name, @@VERSION AS version_text"
        $testCmd.CommandTimeout = 30
        $reader = $testCmd.ExecuteReader()
        $null = $reader.Read()
        $serverName = $reader['server_name']
        $reader.Close()
        $testConn.Close()
        Write-Host ("       Connected. @@SERVERNAME = {0}" -f $serverName) -ForegroundColor DarkGray
    }
    catch {
        Write-Warning ("       Connection failed: {0}" -f $_.Exception.Message)
        $summaryRows.Add([pscustomobject]@{
            Script           = '(connection)'
            Status           = 'FAILED'
            ResultSets       = 0
            TotalRows        = 0
            ElapsedSeconds   = 0
            Error            = $_.Exception.Message
            Messages         = ''
        }) | Out-Null
        $summaryRows | Export-Excel -Path $workbook -WorksheetName 'Summary' -AutoSize -AutoFilter -BoldTopRow
        continue
    }

    # Open the workbook once for the whole instance. Every Export-Excel call
    # passes -ExcelPackage so writes happen in memory; we save once at the
    # end. This is ~10x faster than reopening per result set.
    $pkg = Open-ExcelPackage -Path $workbook -Create

    foreach ($script in $scripts) {
        $stem  = [IO.Path]::GetFileNameWithoutExtension($script.Name)
        $short = $stem.Substring(0, [Math]::Min(2, $stem.Length))    # "01", "02" etc.
        Write-Host ("       running {0} ..." -f $script.Name) -NoNewline

        $status = 'OK'
        $errMsg = $null
        $rsCount = 0
        $rowCount = 0
        $elapsed = [TimeSpan]::Zero
        $msgs = @()

        try {
            $scriptText = Get-Content -LiteralPath $script.FullName -Raw
            $result = Invoke-SqlScriptAllResults -ConnectionString $inst.ConnectionString `
                                                 -ScriptText       $scriptText `
                                                 -TimeoutSec       $CommandTimeoutSec
            $elapsed = $result.Elapsed
            $msgs    = $result.Messages
            $rsIdx   = 0

            foreach ($table in $result.Tables) {
                $rsIdx++
                $rowCount += $table.Rows.Count

                # Skip empty result sets - they add no value to the workbook
                if ($table.Rows.Count -eq 0) { continue }

                $label = Get-SectionLabel -Table $table -ScriptStem $short -ResultIndex $rsIdx
                $sheet = ConvertTo-SafeSheetName -Name $label -UsedNames $usedSheetNames

                # Convert DataTable to PSObjects so ImportExcel can serialize cleanly
                $rows = foreach ($r in $table.Rows) {
                    $obj = [ordered]@{}
                    foreach ($col in $table.Columns) {
                        $v = $r[$col]
                        if ($v -is [DBNull]) { $v = $null }
                        $obj[$col.ColumnName] = $v
                    }
                    [pscustomobject]$obj
                }

                $rows | Export-Excel -ExcelPackage $pkg `
                                     -WorksheetName $sheet `
                                     -AutoSize `
                                     -AutoFilter `
                                     -BoldTopRow `
                                     -FreezeTopRow `
                                     -PassThru | Out-Null
                $rsCount++
            }
            Write-Host ("  done ({0} result sets, {1} rows, {2:n1}s)" -f $rsCount, $rowCount, $elapsed.TotalSeconds) -ForegroundColor Green
        }
        catch {
            $status = 'FAILED'
            $errMsg = $_.Exception.Message
            Write-Host ""
            Write-Warning ("       {0}: {1}" -f $script.Name, $errMsg)
        }

        $summaryRows.Add([pscustomobject]@{
            Script           = $script.Name
            Status           = $status
            ResultSets       = $rsCount
            TotalRows        = $rowCount
            ElapsedSeconds   = [math]::Round($elapsed.TotalSeconds, 2)
            Error            = $errMsg
            Messages         = ($msgs -join "  |  ")
        }) | Out-Null
    }

    # Write summary sheet, then move it to the front
    $summaryRows | Export-Excel -ExcelPackage $pkg `
                                -WorksheetName 'Summary' `
                                -AutoSize -AutoFilter -BoldTopRow -FreezeTopRow `
                                -PassThru | Out-Null

    try {
        if ($pkg.Workbook.Worksheets['Summary'] -and
            $pkg.Workbook.Worksheets[1].Name -ne 'Summary') {
            $pkg.Workbook.Worksheets.MoveToStart('Summary')
        }
    } catch {
        Write-Verbose "Could not reorder Summary sheet: $($_.Exception.Message)"
    }

    Close-ExcelPackage $pkg

    $instElapsed = (Get-Date) - $instStart
    Write-Host ("       instance done in {0:n1}s ({1} sheets)" -f $instElapsed.TotalSeconds, ($usedSheetNames.Count + 1)) -ForegroundColor Cyan
    Write-Host ""
}

$overallSw.Stop()
Write-Host ""
Write-Host ("All instances complete in {0:n1} minutes. Workbooks in:" -f ($overallSw.Elapsed.TotalMinutes)) -ForegroundColor Cyan
Write-Host ("    {0}" -f $OutputFolder) -ForegroundColor Cyan
#endregion ====================================================================
