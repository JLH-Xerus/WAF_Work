# SQL Server Configuration Audit Collection

A set of read-only T-SQL scripts that capture a comprehensive configuration
snapshot of a SQL Server 2019 instance running on a **physical host**,
**SAN-attached storage**, in an **Active-Passive cluster with a listener**.

Each script is self-contained and emits one or more labeled result sets in
SSMS. Together they produce the raw material we need to:

1. Build a catalog of every instance's configuration.
2. Write a human-readable interpretation of what the configuration *means*.
3. Make recommendations where deficiencies (apparent or latent) are detected.

## How to run

1. Open the script in SSMS connected to the **listener** (so we read from
   whichever node is currently active).
2. Set results to **Grid** (Ctrl+T) for readability, or **Text** (Ctrl+T then
   Ctrl+T again) if you want copy-pastable text.
3. **Run `00_preflight_validate_dmvs.sql` once first.** It checks every DMV
   and column referenced across the collection against the live schema on
   this instance. Any row with status `MISSING COLUMNS` or `OBJECT NOT FOUND`
   means a downstream script will throw on that build - fix or remove the
   reference before continuing.
4. Each script begins with a comment describing what it collects. They are
   all `SELECT`-only and safe to run on production.
5. Run scripts in numeric order, or pick the section(s) you need.

## File index

| #  | Script                                       | Captures                                              |
|----|----------------------------------------------|-------------------------------------------------------|
| 01 | `01_instance_identity_and_hardware.sql`      | Version, edition, hardware, services, startup, trace flags |
| 02 | `02_sp_configure_and_advanced.sql`           | Full sp_configure dump with non-default flags         |
| 03 | `03_memory_cpu_parallelism.sql`              | Memory, CPU, MAXDOP, NUMA, LPIM, IFI                  |
| 04 | `04_tempdb_configuration.sql`                | tempdb file layout, contention, equal-sizing checks   |
| 05 | `05_database_configuration.sql`              | Per-DB recovery model, compat level, options          |
| 06 | `06_database_files_and_vlfs.sql`             | File paths, growth, VLF counts, I/O stats             |
| 07 | `07_storage_and_volumes.sql`                 | Volume free space, mount points, I/O stalls by drive  |
| 08 | `08_ha_dr_detection.sql`                     | Auto-detect FCI, AG, mirroring, log shipping          |
| 09 | `09_backup_posture.sql`                      | Backup history, CHECKDB age, suspect pages            |
| 10 | `10_security_configuration.sql`              | Auth mode, sa state, sysadmin, audits, TDE, TLS       |
| 11 | `11_query_store_and_perf.sql`                | Query Store per DB, parameterization, ad-hoc workload |
| 12 | `12_sql_agent_configuration.sql`             | Agent service account, jobs, alerts, operators        |
| 13 | `13_wait_stats_baseline.sql`                 | Top waits, latch stats, signal-wait ratio             |

## Running across many instances (PowerShell orchestrator)

`Run-WafConfigAudit.ps1` automates the collection. For each instance in the
`$Instances` array at the top of the script it:

1. Opens a connection using the supplied ADO.NET connection string.
2. Runs every numbered audit script (00_run_all.sql is skipped - it's a
   SQLCMD driver, not a collection script).
3. Captures every result set the script returns.
4. Writes each result set to its own worksheet in `<InstanceName>.xlsx`.
5. Adds a `Summary` worksheet at the top of each workbook with script
   status, result-set count, row count, elapsed time, and any errors.

Prerequisites:

- PowerShell 5.1 or 7+.
- `Install-Module -Name ImportExcel -Scope CurrentUser`.

Edit the 14 instances at the top of the script (connection strings can be
trusted or SQL auth - any ADO.NET conn string works). Then:

```powershell
.\Run-WafConfigAudit.ps1
.\Run-WafConfigAudit.ps1 -IncludePreflight       # also runs 00_preflight first
.\Run-WafConfigAudit.ps1 -CommandTimeoutSec 1200 # raise per-script timeout
.\Run-WafConfigAudit.ps1 -OutputFolder C:\Audit  # change output dir
```

Workbooks land in `.\output\<InstanceName>.xlsx` by default. One workbook
per instance, ~70-100 worksheets per workbook.

## Assumptions and notes

- SQL Server 2019 target. A few DMVs (e.g. `sys.dm_os_buffer_pool_extension_configuration`)
  and columns vary by build; scripts are written to be tolerant.
- Scripts assume the executing login has at least `VIEW SERVER STATE`,
  `VIEW ANY DEFINITION`, and `VIEW SERVER PERFORMANCE STATE`. `sysadmin`
  produces the most complete output.
- Cluster/AG detection is automatic - if you're on an FCI, the FCI section
  will populate; if you're on an AG, that section will. Sections that don't
  apply will return zero rows with an explanatory message.
- All scripts are **read-only**. No `ALTER`, `DBCC`, `EXEC sp_configure`
  changes - everything is observation only.

## Next steps

After collection, the downstream workflow will:
- **Catalog**: load result sets into a structured catalog (one row per
  configuration setting per instance).
- **Document**: produce a narrative explaining each instance's posture and
  how it compares to its peers in the cluster.
- **Recommend**: flag deviations from best practice for the FCI+SAN+physical
  topology, with rationale for each recommendation.
