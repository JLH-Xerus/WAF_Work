# Run-SqlPerfTests

A small PowerShell harness for running SQL Server scripts repeatedly under controlled conditions and capturing the diagnostic output you need to compare query versions during performance tuning.

The harness is being built incrementally. Today it discovers `.sql` files, runs each one against a SQL Server instance with a configurable number of warmup and measurement iterations, and writes captured Messages-pane output (PRINT, info messages) to disk per measurement run. Upcoming iterations will layer in `STATISTICS IO` / `STATISTICS TIME` capture and per-iteration `.sqlplan` files (actual execution plans, openable in SSMS).

---

## Folder layout

```
WAF_Work/
├─ Run-SqlPerfTests.ps1     the harness
├─ README.md                this file
├─ sql/                     drop your .sql files here, one query per file
│  └─ 01_smoke_test.sql
└─ results/                 created on first run; a timestamped subfolder per run
   └─ 2026-05-20_08-42-17/
      ├─ 01_smoke_test.run1.messages.txt
      ├─ 01_smoke_test.run2.messages.txt
      └─ 01_smoke_test.run3.messages.txt
```

You place SQL scripts into `sql/`. Each script is treated as a single batch — do not include `GO` separators. The harness discovers them in alphabetical order, which is why the sample is prefixed `01_`; numbering the files keeps run order predictable when you're comparing v1 vs v2 of the same query.

---

## Prerequisites

The script targets Windows PowerShell 5.1 (the in-box version that ships with Windows) and uses `System.Data.SqlClient`, which is loaded by default — no module install required. It also runs on PowerShell 7+ with the same code path.

You need:

- Network reach from the machine running the script to the SQL Server instance (TCP, default port 1433 unless your instance is custom).
- A login with permission to execute the queries in `sql/`. For the upcoming `.sqlplan` capture you will also need `SHOWPLAN` permission on the database, which is granted with `GRANT SHOWPLAN TO [your_login]`.
- If you're running from a Parallels (or any non-domain-joined) Windows VM, see [Running from Parallels](#running-from-parallels) below.

---

## Running the script

From a PowerShell window in the script folder:

```powershell
.\Run-SqlPerfTests.ps1 -ServerInstance "your_server" -Database "your_db"
```

Common variations:

```powershell
# Named instance
.\Run-SqlPerfTests.ps1 -ServerInstance "MYSERVER\SQL2019" -Database "AdventureWorks"

# Remote server on a non-default port
.\Run-SqlPerfTests.ps1 -ServerInstance "sqlbox.corp.local,1433" -Database "AdventureWorks"

# Tighter loop: skip warmup, take 5 measurements
.\Run-SqlPerfTests.ps1 -ServerInstance "localhost" -Database "AdventureWorks" -WarmupRuns 0 -MeasurementRuns 5

# Long-running query: extend command timeout to 30 minutes
.\Run-SqlPerfTests.ps1 -ServerInstance "localhost" -Database "AdventureWorks" -CommandTimeoutSeconds 1800
```

If you hit `cannot be loaded because running scripts is disabled on this system`, relax the policy for just the current window and retry:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

---

## Parameters

| Parameter | Default | Purpose |
|---|---|---|
| `-ServerInstance` | *(required)* | SQL Server instance: `localhost`, `SERVER\INSTANCE`, or `host,port`. |
| `-Database` | *(required)* | Initial database context for the connection. |
| `-SqlFolder` | `.\sql` | Folder scanned for `.sql` files. |
| `-ResultsRoot` | `.\results` | Root for output; a timestamped subfolder is created per run. |
| `-WarmupRuns` | `1` | Iterations executed before measurement, with no output captured. |
| `-MeasurementRuns` | `3` | Iterations whose output is captured to disk. Must be at least 1. |
| `-CommandTimeoutSeconds` | `300` | Per-iteration query timeout. Raise this for slow queries. |

---

## Output

For every measurement iteration of every script, the harness writes a file named:

```
<script_basename>.run<N>.messages.txt
```

Each file has a header block (script, server, database, timestamp, iteration number, wall-clock milliseconds for the iteration) followed by everything SQL Server emitted on the message channel — every `PRINT`, every low-severity info message, and (once we wire it up in the next iteration) the full `STATISTICS IO` and `STATISTICS TIME` blocks.

Warmup iterations are *executed* but their output is *not* written; their job is to populate the buffer pool and plan cache so the measurement runs reflect steady-state performance.

The wall-clock timing in the header is captured in PowerShell with a `Stopwatch` and includes round-trip overhead. Trust the `STATISTICS TIME` numbers (once enabled) as the authoritative server-side timing; the wall-clock value is a useful sanity check.

---

## Why warmup runs matter

Cold-cache and warm-cache executions of the same query can differ by an order of magnitude. The first run pays for:

- Physical I/O to read data pages from disk into the buffer pool.
- Query compilation and plan caching.
- Stored-procedure and function compilation, if referenced.

A "refactored" query that runs faster than the original can be entirely misleading if the original ran cold and the refactored ran warm — you might just be measuring the buffer pool, not the rewrite. Likewise, comparing two cold runs introduces variance from disk-subsystem behavior that has nothing to do with your SQL.

The harness defends against this by running each script `WarmupRuns + MeasurementRuns` times against a single open connection. The buffer pool and plan cache are server-wide, so by the time the first measurement iteration starts, both have been primed by the warmup. Subsequent measurements should show low variance — if they don't, that itself is a finding worth investigating (recompiles, parameter sniffing, blocking, etc.).

When comparing two versions of a query, the honest workflow is:

1. Put `original.sql` and `refactored.sql` side by side in `sql/`.
2. Run the harness.
3. Compare the median of the measurement runs for each, not the minimum and not the first run.

---

## Running from Parallels

If your Windows VM is **not** joined to the corporate domain, you need to launch PowerShell with `runas /netonly` so your domain credentials are used for the SQL Server connection without Windows trying to validate them against a local domain controller it can't reach:

```
runas /netonly /user:DOMAIN\your.username "powershell.exe"
```

Type your domain password when prompted (it won't echo). The new window will look like it's running as your local Parallels user — that's expected; `/netonly` means "use these credentials only for network traffic." Then `cd` into the script folder and run as usual.

A quick sanity check before running the script:

```powershell
Test-NetConnection -ComputerName your_sql_server -Port 1433
```

`TcpTestSucceeded : True` confirms network reach.

If your VM *is* domain-joined, regular Shift + right-click → "Run as different user" works the same as it does for SSMS, and you can skip `/netonly`.

---

## Troubleshooting

**`SQL folder not found`** — the harness expects a `sql\` folder next to the script. Create it or override with `-SqlFolder`.

**`Login failed for user`** — the connection is reaching SQL Server but authentication is failing. If you're using Windows auth from a non-domain-joined VM, you almost certainly need `runas /netonly` (see above).

**`A network-related or instance-specific error occurred`** — the connection can't reach the server at all. Check firewall, port, and that the SQL Server Browser service is running if you're connecting to a named instance.

**`Execution Timeout Expired`** — your query took longer than `-CommandTimeoutSeconds` (default 300). Increase the timeout, or investigate why the query is slow (which is presumably what you're here for).

**Messages file is empty** — the script ran but produced no PRINT or info messages. Once `STATISTICS IO`/`STATISTICS TIME` capture is wired up in the next iteration, you should always see output here.

---

## Roadmap

The current scaffold proves the plumbing. Next iterations will add:

1. `SET STATISTICS IO ON; SET STATISTICS TIME ON` at the head of each iteration so the messages file fills with logical-reads, physical-reads, and CPU/elapsed numbers per statement.
2. `SET STATISTICS XML ON` plus a switch from `ExecuteNonQuery` to `ExecuteReader`, so each measurement iteration also produces a `<script>.run<N>.sqlplan` file — double-click in SSMS to see the graphical plan.
3. A small summary file per run that pulls the per-iteration numbers into a table for at-a-glance comparison.
