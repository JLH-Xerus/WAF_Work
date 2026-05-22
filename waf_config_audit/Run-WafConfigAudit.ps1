[CmdletBinding()]
param(
    [string] $ScriptFolder       = $PSScriptRoot,
    [string] $OutputFolder       = (Join-Path $PSScriptRoot 'output'),
    [int]    $CommandTimeoutSec  = 600,
    [switch] $IncludePreflight,
    [string] $ScriptNamePattern  = '[0-9][0-9]_*.sql',
    [string] $InstanceName,
    [switch] $ListInstances
)

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

$ErrorActionPreference = 'Stop'

if (-not (Get-Module -ListAvailable -Name ImportExcel)) {
    throw "The ImportExcel module is required. Install with:  Install-Module -Name ImportExcel -Scope CurrentUser"
}
Import-Module ImportExcel -ErrorAction Stop

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
    $files = Get-ChildItem -Path $Folder -Filter '*.sql' -File |
             Where-Object { $_.Name -like $Pattern -and $_.Name -ne '00_run_all.sql' -and $_.Name -ne '00_preflight_validate_dmvs.sql' } |
             Sort-Object Name

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
    param(
        [string]   $Name,
        [hashtable]$UsedNames
    )
    if ([string]::IsNullOrWhiteSpace($Name)) { $Name = 'Sheet' }
    $clean = $Name -replace '[:\\/\?\*\[\]]', '_'
    if ($clean.Length -gt 31) { $clean = $clean.Substring(0, 31) }

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

if ($ListInstances) {
    Write-Host ""
    Write-Host ("Configured instances ({0}):" -f $Instances.Count) -ForegroundColor Cyan
    $i = 0
    foreach ($inst in $Instances) {
        $i++
        Write-Host ("  [{0,2}] {1}" -f $i, $inst.InstanceName)
        Write-Host ("       {0}" -f $inst.ConnectionString) -ForegroundColor DarkGray
    }
    Write-Host ""
    return
}

if ($PSBoundParameters.ContainsKey('InstanceName') -and $InstanceName) {
    $matched = @($Instances | Where-Object { $_.InstanceName -like $InstanceName })
    if (-not $matched) {
        Write-Host ""
        Write-Warning ("No instance in `$Instances matched '{0}'. Available instances:" -f $InstanceName)
        $Instances | ForEach-Object { Write-Host ("    {0}" -f $_.InstanceName) }
        Write-Host ""
        throw "Aborting - InstanceName filter did not match any configured instance."
    }
    $Instances = $matched
    Write-Host ""
    Write-Host ("Filter -InstanceName '{0}' matched {1} instance(s):" -f $InstanceName, $Instances.Count) -ForegroundColor Yellow
    $Instances | ForEach-Object { Write-Host ("    {0}" -f $_.InstanceName) }
}

$scripts = Get-AuditSqlFiles -Folder $ScriptFolder -Pattern $ScriptNamePattern -IncludePreflight:$IncludePreflight
if (-not $scripts) {
    throw "No audit scripts matched pattern '$ScriptNamePattern' in $ScriptFolder"
}

Write-Host ""
Write-Host ("Found {0} audit scripts in {1}:" -f $scripts.Count, $ScriptFolder) -ForegroundColor Cyan
$scripts | ForEach-Object { Write-Host "    $($_.Name)" }
Write-Host ""
Write-Host ("Will audit {0} instance(s). Output -> {1}" -f $Instances.Count, $OutputFolder) -ForegroundColor Cyan
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

    $pkg = Open-ExcelPackage -Path $workbook -Create

    foreach ($script in $scripts) {
        $stem  = [IO.Path]::GetFileNameWithoutExtension($script.Name)
        $short = $stem.Substring(0, [Math]::Min(2, $stem.Length))
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

                if ($table.Rows.Count -eq 0) { continue }

                $label = Get-SectionLabel -Table $table -ScriptStem $short -ResultIndex $rsIdx
                $sheet = ConvertTo-SafeSheetName -Name $label -UsedNames $usedSheetNames

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
