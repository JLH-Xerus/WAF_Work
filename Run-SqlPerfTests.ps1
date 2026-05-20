<#
.SYNOPSIS
    Run SQL scripts against a SQL Server instance and capture performance
    diagnostics (messages today; STATISTICS IO/TIME and XML plans in later
    iterations).

.DESCRIPTION
    PIECE 1 of N -- scaffolding only.

    For each .sql file in the ./sql subfolder, this script:
      1. Opens a SqlConnection with an InfoMessage handler attached.
      2. Executes the script.
      3. Writes any captured messages to ./results/<timestamp>/<name>.messages.txt

    It does NOT yet enable STATISTICS IO/TIME or capture XML plans -- those
    come in the next iteration. The purpose of this slice is to prove that
    file discovery, the connection, and message capture all work end-to-end.

.PARAMETER ServerInstance
    SQL Server instance, e.g. "localhost", "SERVER01\SQL2019", "myserver,1433"

.PARAMETER Database
    Initial database context for the connection.

.PARAMETER SqlFolder
    Folder containing .sql files to run. Defaults to ./sql next to this script.

.PARAMETER ResultsRoot
    Root folder for results. A timestamped subfolder is created under it per run.
    Defaults to ./results next to this script.

.EXAMPLE
    .\Run-SqlPerfTests.ps1 -ServerInstance "localhost" -Database "AdventureWorks"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $ServerInstance,

    [Parameter(Mandatory = $true)]
    [string] $Database,

    [string] $SqlFolder    = (Join-Path $PSScriptRoot 'sql'),
    [string] $ResultsRoot  = (Join-Path $PSScriptRoot 'results'),

    [int]    $CommandTimeoutSeconds = 300
)

# ----- Setup ----------------------------------------------------------------

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $SqlFolder)) {
    throw "SQL folder not found: $SqlFolder"
}

$runStamp  = (Get-Date).ToString('yyyy-MM-dd_HH-mm-ss')
$runFolder = Join-Path $ResultsRoot $runStamp
New-Item -ItemType Directory -Path $runFolder -Force | Out-Null

Write-Host "SQL folder : $SqlFolder"
Write-Host "Results    : $runFolder"
Write-Host ""

# Discover scripts (sorted so order is deterministic).
$scripts = Get-ChildItem -LiteralPath $SqlFolder -Filter '*.sql' -File |
           Sort-Object Name

if (-not $scripts) {
    Write-Warning "No .sql files found in $SqlFolder"
    return
}

Write-Host ("Found {0} script(s):" -f $scripts.Count)
$scripts | ForEach-Object { Write-Host "  - $($_.Name)" }
Write-Host ""

# ----- Connection string ----------------------------------------------------

# Windows authentication. To switch to SQL auth later, swap Integrated Security
# for User ID / Password (we'll parameterize that in a later iteration).
$connStr = "Server=$ServerInstance;Database=$Database;Integrated Security=SSPI;Application Name=Run-SqlPerfTests"

# ----- Process each script --------------------------------------------------

foreach ($script in $scripts) {

    Write-Host ("--- {0} ---" -f $script.Name)

    $sqlText = Get-Content -LiteralPath $script.FullName -Raw

    # Buffer for InfoMessage output. Using a typed list keeps things tidy.
    $messages = New-Object 'System.Collections.Generic.List[string]'

    $connection = New-Object System.Data.SqlClient.SqlConnection $connStr

    # Wire up the InfoMessage handler BEFORE opening the connection. The handler
    # fires asynchronously as messages stream back from the server, so it must
    # be attached before any work is done on the connection.
    $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {
        param($eventSender, $eventArgs)
        foreach ($err in $eventArgs.Errors) {
            # Each SqlError has Class (severity), Number, Message, etc.
            # STATISTICS IO/TIME and PRINT all come through here at Class 0.
            $messages.Add($err.Message)
        }
    }
    $connection.add_InfoMessage($handler)

    try {
        $connection.Open()

        $command = $connection.CreateCommand()
        $command.CommandText    = $sqlText
        $command.CommandTimeout = $CommandTimeoutSeconds

        # We don't care about returned rows -- ExecuteNonQuery is enough for
        # this slice. (We'll switch to ExecuteReader in the next iteration so
        # we can pick the XML plan result set off the wire.)
        [void] $command.ExecuteNonQuery()
    }
    catch {
        $messages.Add("*** EXCEPTION: $($_.Exception.Message)")
        Write-Warning $_.Exception.Message
    }
    finally {
        $connection.remove_InfoMessage($handler)
        $connection.Dispose()
    }

    # Write captured messages to disk.
    $baseName    = [System.IO.Path]::GetFileNameWithoutExtension($script.Name)
    $messageFile = Join-Path $runFolder ("$baseName.messages.txt")

    $header = @(
        "Script   : $($script.Name)"
        "Server   : $ServerInstance"
        "Database : $Database"
        "Run at   : $runStamp"
        ('-' * 60)
        ''
    ) -join [Environment]::NewLine

    ($header + ($messages -join [Environment]::NewLine)) |
        Set-Content -LiteralPath $messageFile -Encoding UTF8

    Write-Host ("  messages -> {0}  ({1} line(s))" -f $messageFile, $messages.Count)
}

Write-Host ""
Write-Host "Done."
