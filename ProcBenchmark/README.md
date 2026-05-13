# ProcBenchmark — portable SQL Server refactor harness

A single-file PowerShell script that runs an "original" and a "refactored" T-SQL batch
against every site in a CSV config and produces one Excel workbook comparing them.

No CMS server, no special tooling, no DB-side install — just PowerShell + the
ImportExcel module on whatever workstation you happen to be borrowing.

## What you get

For every site × iteration the harness captures:

- **STATISTICS TIME** — CPU and elapsed ms, summed across all statements in the batch
- **STATISTICS IO** — logical, physical, read-ahead, and LOB reads, per table
- **Actual execution plan** — saved as `.sqlplan` files you can open straight into SSMS
- **Row count + SHA-1 checksum of the result set** — so you can prove the refactor returns the same data
- **Wall-clock time** — useful when STATISTICS TIME under-reports parallel work

Output is `results.xlsx` with five sheets:

| Sheet         | Contents |
| ------------- | -------- |
| Summary       | One row per site/iteration with side-by-side metrics, deltas, and % change. Greens = improvement, pinks = regression. Salmon flags = result mismatch. |
| IO Detail     | Per-table reads for every site/version/iteration. |
| Time Detail   | Per-statement CPU + elapsed for every site/version/iteration. |
| Plans Index   | File paths to each captured `.sqlplan`. |
| Errors        | Any site/version that threw a SQL exception, with the error message. |

Plans land in a sibling `plans/` folder.

## Prerequisites

1. Windows PowerShell 5.1 (built into Windows) or PowerShell 7+.
2. The ImportExcel module — one-time install:
   ```powershell
   Install-Module ImportExcel -Scope CurrentUser
   ```
   If you can't install modules on the client's box, the script falls back to writing
   CSVs in the same folder. You still get all the data, just no formatting.
3. Windows account with execute permission on the procs at every site.
4. Network reachability from the box you're running on to each SQL instance.

## Setup

1. Copy `sites.csv.template` to `sites.csv` and fill in your sites.
2. Each row has five columns:
   - `SiteName` — friendly label, used in filenames and Excel rows.
   - `ServerName` — `host\instance` or `host` for default instance.
   - `DatabaseName` — initial DB context.
   - `OriginalSql` — the T-SQL to run for the "before" version. Can be `EXEC dbo.myProc @x=1`, a multi-statement batch, or an entire inline script.
   - `RefactoredSql` — the T-SQL for the "after" version. Same shape.
3. Quote fields that contain commas, newlines, or quotes (standard CSV).

Tips:
- If a proc takes parameters, put the full `EXEC` statement with parameter values in the cell.
- For inline batches like `GetListOfImages_refactor.sql`, paste the entire body. Strip the
  `USE database` / `GO` separators — the script sets the database via the connection string
  and runs the whole cell as one batch.
- You can leave `SET STATISTICS IO/TIME ON` in the SQL or take them out — the harness sets
  them itself, and SQL Server tolerates redundant SETs.

## Running

```powershell
cd \path\to\ProcBenchmark
.\Invoke-ProcBenchmark.ps1 -ConfigPath .\sites.csv
```

Common variations:

```powershell
# More iterations for a tighter median
.\Invoke-ProcBenchmark.ps1 -ConfigPath .\sites.csv -Iterations 5

# See cold-cache cost (no warmup)
.\Invoke-ProcBenchmark.ps1 -ConfigPath .\sites.csv -WarmupRuns 0

# Skip plan capture (some procs misbehave with SET STATISTICS XML ON)
.\Invoke-ProcBenchmark.ps1 -ConfigPath .\sites.csv -CapturePlans $false

# Skip the result-set checksum (faster, but no data-drift detection)
.\Invoke-ProcBenchmark.ps1 -ConfigPath .\sites.csv -SkipChecksum

# Longer command timeout for slow sites
.\Invoke-ProcBenchmark.ps1 -ConfigPath .\sites.csv -CommandTimeout 1800
```

The script creates a timestamped output folder beside itself
(`results_20260512_153021\`) containing `results.xlsx`, `plans\`, and `run.log`.

## Interpreting the output

Open `results.xlsx` and start on **Summary**:

- **Pct_CpuChange / Pct_ElapsedChange / Pct_LogicalChange** are
  `(refactored - original) / original * 100`. **Negative is good** — green cells
  mean the refactor used less CPU / time / IO. Pink cells are regressions, look at them.
- **RowCount_Match** and **Data_Match** — salmon-highlighted **False** means the
  refactor doesn't return the same data. Fix that before celebrating any speedup.
- For a single trustworthy number per site, take the **median** across iterations
  (the warmup iteration is dropped, so all reported iterations should be warm).

When you need to see *why* a site regressed:

1. **IO Detail** — find the rows for that site/version. The big numbers tell you
   which tables the planner is hammering. Compare the same table between Original
   and Refactored to see if you traded one scan for many seeks, or vice versa.
2. **Plans Index** — open the `.sqlplan` for that site/iteration in SSMS to look
   at the graphical plan. Compare original vs. refactored side-by-side.
3. **Time Detail** — if one statement in a multi-statement batch dominates, the
   per-statement breakdown will surface it.

## Caveats

- **Buffer pool warming.** Iteration 2+ benefits from data already cached by
  iteration 1. The warmup iteration is meant to absorb that for the *first*
  recorded iteration, but iterations within the original-vs-refactored pair are
  not interleaved (we run original ×N then refactored ×N). If you want truly
  cold-vs-cold comparisons, that requires `DBCC DROPCLEANBUFFERS` which is risky
  on production — talk to the DBA before adding it.
- **Procedure cache.** Plans get cached and reused. If a refactor's first compile
  is expensive but subsequent runs are fast, that shows up as a low CPU number on
  iteration 2+ vs. iteration 1. Use the warmup iteration to absorb compile cost.
- **STATISTICS TIME under parallelism.** CPU time reported includes time on every
  worker thread, so a parallel query can show CPU > elapsed. That's not a bug.
- **Multi-statement batches.** STATISTICS TIME / IO totals are summed across all
  statements in the batch. Per-statement breakdown is on the Time Detail sheet.
- **Auth.** Windows auth only in this version. To benchmark against a server you
  can't reach with your AD identity, add a SQL-auth column and modify
  `Invoke-Batch` to switch connection strings — the change is small.

## Files

```
ProcBenchmark/
  Invoke-ProcBenchmark.ps1     # the harness
  sites.csv.template            # copy to sites.csv and edit
  README.md                     # this file
  tests/
    Test-Parsers.ps1            # unit tests for the STATISTICS parsers
```

Run `tests\Test-Parsers.ps1` to confirm the regex parsers still match your SQL
Server version's STATISTICS output if anything looks off in the Excel.
