# SQL Server WAF Configuration Audit

A read-only collection of T-SQL scripts and a PowerShell orchestrator that
capture a complete configuration snapshot of one or more SQL Server 2019
instances, write the results to per-instance Excel workbooks, and produce
the raw material for a downstream catalog, narrative documentation, and
deficiency-recommendation pipeline.

The collection is built for the following environment topology:

- SQL Server 2019 (build 15.x).
- Physical hosts (not virtual).
- SAN-attached storage.
- Active-Passive Windows Server Failover Cluster, accessed via a listener.
  The scripts auto-detect whether that listener represents a Failover
  Cluster Instance (FCI), an Always On Availability Group (AG), a hybrid
  of the two, or neither, and dump the relevant configuration accordingly.

All scripts are read-only. No `ALTER`, no `RECONFIGURE`, no `DBCC` operations
that take maintenance locks. The orchestrator opens each connection with
`Application Name=WAF Config Audit` so the audit is identifiable in
`sp_who2`, Activity Monitor, and Extended Events.

---

## Folder layout

```
waf_config_audit/
    00_README.md                          this file
    00_preflight_validate_dmvs.sql        column-existence validator
    00_run_all.sql                        optional SQLCMD-mode driver
    01_instance_identity_and_hardware.sql
    02_sp_configure_and_advanced.sql
    03_memory_cpu_parallelism.sql
    04_tempdb_configuration.sql
    05_database_configuration.sql
    06_database_files_and_vlfs.sql
    07_storage_and_volumes.sql
    08_ha_dr_detection.sql
    09_backup_posture.sql
    10_security_configuration.sql
    11_query_store_and_perf.sql
    12_sql_agent_configuration.sql
    13_wait_stats_baseline.sql
    Run-WafConfigAudit.ps1                PowerShell orchestrator
    output/                               workbooks land here
```

---

## Quick start

1. Install the ImportExcel PowerShell module once on the workstation that
   will run the audit:

   ```powershell
   Install-Module -Name ImportExcel -Scope CurrentUser
   ```

2. Open `Run-WafConfigAudit.ps1` and replace the 14 placeholder entries in
   the `$Instances` array with your real instance names and ADO.NET
   connection strings. Point `Server=` at the listener so the read happens
   on whichever node is currently active.

3. Verify the array parsed correctly:

   ```powershell
   .\Run-WafConfigAudit.ps1 -ListInstances
   ```

4. Dry run against one instance with the preflight included:

   ```powershell
   .\Run-WafConfigAudit.ps1 -InstanceName 'PROD-CLUSTER01' -IncludePreflight
   ```

5. Inspect `.\output\PROD-CLUSTER01.xlsx`. Confirm the Summary sheet shows
   every script as `OK` and that the preflight section shows no missing
   columns and adequate permissions.

6. Run the full sweep:

   ```powershell
   .\Run-WafConfigAudit.ps1
   ```

One workbook is produced per instance, named after the instance, in
`.\output\`. Each workbook contains roughly 70 to 100 worksheets - one per
result set returned by the collection scripts.

---

## Required permissions

The login the orchestrator authenticates with should have at least the
following on every target instance. `sysadmin` covers all of it and is the
simplest path; if `sysadmin` is not possible, the explicit grant set below
is equivalent for audit purposes.

| Permission                                    | Why                                     |
|-----------------------------------------------|-----------------------------------------|
| `VIEW SERVER STATE` (server)                  | All `sys.dm_*` DMVs                     |
| `VIEW ANY DEFINITION` (server)                | Full visibility into `sys.databases`, `sys.server_principals`, certificates, audits |
| `SQLAgentReaderRole` on msdb                  | Read `sysjobs`, `sysjobhistory`, `sysalerts`, `syscategories`, `sysnotifications` |
| `DatabaseMailUserRole` on msdb                | Read `sysmail_*`                        |
| `sysadmin` (or rights to run `DBCC DBINFO`)   | Last-known-good CHECKDB date            |
| `sysadmin` (for `log_shipping_*` tables)      | Log shipping detection                  |

If permissions are short, the affected sections simply return empty
rowsets rather than throwing. The Summary sheet for each instance reports
exactly which scripts succeeded and which did not.

Grant example for a least-privilege audit login `AUDIT\sqlaudit`:

```sql
USE master;
GRANT VIEW SERVER STATE TO [AUDIT\sqlaudit];
GRANT VIEW ANY DEFINITION TO [AUDIT\sqlaudit];

USE msdb;
ALTER ROLE SQLAgentReaderRole  ADD MEMBER [AUDIT\sqlaudit];
ALTER ROLE DatabaseMailUserRole ADD MEMBER [AUDIT\sqlaudit];
```

---

## What each script captures

### 00_preflight_validate_dmvs.sql

Cross-checks every DMV, catalog view, and msdb table the collection uses
against the live schema on the current instance, and reports the current
login's effective permissions. Run this once per instance the first time
you point the orchestrator at it. It produces three result sets:

1. **Missing columns** - any column the collection scripts reference that
   does not exist on this build. Any row here is a real bug and must be
   fixed before running the rest of the collection.
2. **Per-object summary** - one row per inventoried object with status of
   `OK`, `MISSING COLUMNS`, or `OBJECT NOT FOUND`. `OBJECT NOT FOUND` on
   an msdb table is almost always a permissions issue, not a missing
   object.
3. **Current login permissions** - server-role flags, key server-level
   permissions, and msdb role membership for the executing login. Use
   this to explain any `OBJECT NOT FOUND` rows in the prior result set.

### 00_run_all.sql

Optional SQLCMD-mode driver that uses `:r` to inline every numbered script
into a single batch. Useful when you want to run the entire collection
from SSMS without the PowerShell orchestrator. Open the file in SSMS,
enable SQLCMD Mode under the Query menu, and execute. The PowerShell
orchestrator does **not** use this file.

### 01_instance_identity_and_hardware.sql

The "who and what" of the instance. Eight result sets:

1. Edition, build, product version, integrated-security mode, clustering
   and HADR flags from `SERVERPROPERTY` plus `@@VERSION`.
2. Host and OS information from `sys.dm_os_host_info` and
   `sys.dm_os_sys_info`: platform, distribution, uptime, sockets, cores
   per socket, NUMA node count, CPU count, hyperthread ratio, soft-NUMA
   configuration, physical memory, committed and target memory, scheduler
   count, affinity type, and the SQL memory model (conventional vs
   LOCK_PAGES vs LARGE_PAGES, which is the runtime indicator of LPIM).
3. SQL Server services: service account, start type, status, last
   startup, cluster awareness, and the Instant File Initialization flag.
4. Registered startup parameters read from the server registry, which is
   where `-T<flag>` and other `-` switches are persisted.
5. Active trace flags from `DBCC TRACESTATUS`, with inline guidance on
   the common ones (1117, 1118, 1204, 1222, 3226, 4199, 7412, 8048, 9024).
6. Process memory from `sys.dm_os_process_memory`: physical in use, large
   and locked page allocations, page fault count, memory utilization
   percentage, and the process memory pressure flags.
7. Scheduler summary aggregated by status.
8. NUMA detail in two views: a memory-side aggregation (one row per
   memory node, with SQLOS node counts and combined affinity collapsed
   via `OUTER APPLY`) and a SQLOS-side detail (one row per SQLOS node
   with state, affinity mask, online schedulers, active workers, and
   resource monitor state).

### 02_sp_configure_and_advanced.sql

Two result sets:

1. Full `sys.configurations` dump with `value`, `value_in_use`, computed
   `default_value` from a curated SQL 2019 defaults table, an `is_default`
   flag, a `pending_reconfigure` flag (set when `value <> value_in_use`),
   plus `minimum`, `maximum`, `is_dynamic`, `is_advanced`, and the engine
   description.
2. A curated set of consequential settings (the ones that most often
   drive performance, stability, or security on FCI-on-SAN-on-physical)
   with recommendation text and a why-it-matters explanation. Covers
   max/min server memory, MAXDOP, cost threshold for parallelism,
   optimize for ad hoc workloads, backup compression default, remote
   admin connections, default trace, CLR settings, ad-hoc distributed
   queries, OLE automation, xp_cmdshell, Agent XPs, Database Mail XPs,
   blocked process threshold, priority boost, lightweight pooling,
   contained database authentication, cross-DB ownership chaining,
   automatic soft-NUMA, tempdb metadata memory-optimization, and show
   advanced options.

### 03_memory_cpu_parallelism.sql

Six result sets covering memory and CPU posture:

1. Memory and parallelism summary with derived recommendations: physical
   memory, current min/max server memory, recommended max-memory based on
   physical capacity, current MAXDOP, recommended MAXDOP derived from
   cores-per-NUMA-node (capped at 16 for multi-NUMA, 8 for single-NUMA
   high-core), current and recommended cost threshold for parallelism,
   optimize-for-ad-hoc state, NUMA node count, and cores per socket.
2. Resource Governor workload groups joined to their resource pools.
   `is_system_group` is computed from the catalog view
   `sys.resource_governor_workload_groups` (which the DMV does not
   expose).
3. Database-scoped configurations (MAXDOP, legacy CE, parameter sniffing,
   QO hotfixes, accelerated database recovery, lightweight profiling,
   elevate online, elevate resumable, async stats wait) per database via
   dynamic SQL.
4. LPIM and Instant File Initialization status.
5. Plan cache breakdown by `objtype` with single-use plan totals -
   evidence for or against enabling optimize-for-ad-hoc.
6. Top 25 memory clerks by pages, with NUMA node attribution.

### 04_tempdb_configuration.sql

Six result sets:

1. tempdb's database-level options.
2. Per-file layout: drive, physical path, size, growth setting (with
   percent vs MB called out), max size, state.
3. Sanity check: data file count vs scheduler count, equal-sizing check,
   any percent-growth setting, count of distinct growth settings, and
   guidance text.
4. Live contention check via `sys.dm_exec_requests` filtered to
   `PAGELATCH%` waits on database_id 2.
5. tempdb metadata memory-optimization: the configured `sp_configure`
   value and a runtime indicator pulled from
   `sys.dm_xtp_system_memory_consumers` if non-zero (which is the
   authoritative signal that the feature is actually active on this
   instance).
6. tempdb I/O stats from `sys.dm_io_virtual_file_stats` aggregated per
   file, with average read and write stall in milliseconds.

### 05_database_configuration.sql

Three result sets:

1. Per-database configuration matrix - every meaningful column from
   `sys.databases`: recovery model, compatibility level, page verify
   option, collation, all `is_auto_*` flags, snapshot isolation state,
   read-committed snapshot, trustworthy, DB chaining, broker enabled,
   replication flags, encryption state, query store on, CDC enabled,
   containment, target recovery time, delayed durability, log reuse
   wait, parameterization mode, ANSI session defaults, supplemental
   logging.
2. Deviations from best practice as one row per finding: auto close on,
   auto shrink on, page verify other than CHECKSUM, auto-update or
   auto-create stats off, trustworthy on, non-sa owner, compatibility
   level below 150, simple recovery on a user DB, RCSI off, DB chaining
   on, target recovery time = 0 (legacy checkpoint), forced delayed
   durability. Each finding has a rationale.
3. Database owners with orphan-SID detection.

### 06_database_files_and_vlfs.sql

Three result sets:

1. Full file layout per database via dynamic SQL: drive, physical path,
   size, used and free, percent used, growth setting (percent vs MB),
   max size, state, read-only flag. Each per-DB block is wrapped in a
   `BEGIN TRY/CATCH` inside the dynamic batch so one bad database does
   not abort the collection for the rest.
2. File-layout findings: multiple log files, percent-growth files,
   data and log on the same drive, unequal-sized data files in
   multi-file user databases.
3. VLF counts per database via `sys.dm_db_log_info` with health rating:
   under 100 is OK, 100 to 500 is WATCH, over 500 is INVESTIGATE.

### 07_storage_and_volumes.sql

Four result sets:

1. Volumes hosting SQL files via `sys.dm_os_volume_stats`: mount point,
   logical volume name, file system type, total and available bytes,
   percent free, read-only and compression flags.
2. File-to-volume mapping. Which database files live on which volume.
3. I/O stalls aggregated by drive and file type, with average read and
   write stall and a health rating (under 10 ms OK, 10 to 20 ms WATCH,
   over 20 ms INVESTIGATE).
4. NTFS allocation-unit reminder. The engine cannot read this from
   inside SQL; the row points at `fsutil fsinfo ntfsinfo <drive>:` for
   a host-side confirmation that data and log drives are 64 KB.

### 08_ha_dr_detection.sql

Auto-detects which HA/DR technology is in use and dumps the relevant
configuration. Eight result sets, with sections returning a single
"not in use" note row when they do not apply:

1. HA/DR posture summary with computed interpretation: standalone, FCI
   only, AG only, or FCI plus AG hybrid.
2. FCI cluster nodes, FCI cluster properties (failure condition level,
   health check timeout, dump path), and FCI shared drives.
3. Always On endpoints: protocol, encryption, authentication mode,
   state, port.
4. Always On Availability Groups: AG properties (cluster type, version,
   distributed flag, automated backup preference, failure condition,
   sync secondaries required to commit), AG replicas (availability and
   failover mode, session timeout, primary and secondary connection
   modes, backup priority, seeding mode, read-only routing URL), AG
   listeners, AG replica state, and per-database synchronization state.
5. Database mirroring (legacy) per database if any DB still has a
   mirroring partner.
6. Log shipping primary and secondary configuration from msdb.
7. WSFC cluster info from `sys.dm_hadr_cluster`,
   `sys.dm_hadr_cluster_members`, and `sys.dm_hadr_cluster_networks`.
8. Recent failover and cluster mentions from the default trace,
   plus recent AG role/state-change events read from the
   `AlwaysOn_health` Extended Events session.

### 09_backup_posture.sql

Five result sets:

1. Backup history per database from msdb: last full, last diff, last
   log, hours and minutes since each, compression ratio on the most
   recent full, computed `posture` (no full on record, FULL recovery
   model but no log backup, log backup older than 60 minutes, last
   full > 48 hours).
2. Backup destinations seen in the last 7 days. Useful for spotting
   anyone backing up to a local node-specific path, which is invisible
   after failover.
3. Suspect pages from `msdb.dbo.suspect_pages` with event-type
   decoding.
4. Time since last successful CHECKDB per database, derived from
   `DBCC DBINFO` `dbi_dbccLastKnownGood`. Requires sysadmin.
5. The current `backup compression default` setting.

### 10_security_configuration.sql

Nine result sets:

1. Authentication mode, force-encryption flag, hide-instance flag, TCP
   dynamic and static ports.
2. The sa login state and a posture flag (sa enabled is a finding).
3. Server-role membership for every member of `sysadmin`,
   `securityadmin`, `serveradmin`, `setupadmin`, `processadmin`,
   `diskadmin`, `dbcreator`, and `bulkadmin`.
4. SQL logins with weak password policies: `CHECK_POLICY` off,
   `CHECK_EXPIRATION` off, blank password, password equal to login
   name, password equal to `password`.
5. SQL Server Audit objects with their `dm_server_audit_status`
   runtime status (`status`, `status_desc`, `status_time`,
   `audit_file_path`).
6. TDE-encrypted databases with encryption state and certificate
   thumbprint, plus master-database certificates.
7. Databases with cross-DB ownership chaining enabled.
8. Notable server-level permissions granted to non-default principals
   (CONTROL SERVER, ALTER ANY LOGIN, ALTER ANY SERVER ROLE,
   IMPERSONATE ANY LOGIN, etc.).
9. Linked servers with their RPC, data access, and DTC promotion
   settings - the column is `is_remote_proc_transaction_promotion_enabled`.

### 11_query_store_and_perf.sql

Three result sets:

1. Query Store per database via dynamic SQL: actual state, desired
   state, read-only reason (with decode), current and max storage,
   percent used, capture mode, flush interval, interval length, stale
   threshold, max plans per query, cleanup mode, wait stats capture
   mode, plus a computed posture line. Each per-DB block is wrapped
   in TRY/CATCH so a single problematic database does not abort the
   batch.
2. Curated instance-wide perf toggles from `sys.configurations` with
   recommendations.
3. Forced parameterization per database.

### 12_sql_agent_configuration.sql

Eight result sets:

1. SQL Agent service: account, start type, status, last startup,
   cluster awareness.
2. Agent-related `sp_configure` settings.
3. SQL Agent properties read directly from the server registry via
   `sys.dm_server_registry`.
4. Job inventory: owner (with orphan-SID detection), category, dates,
   last-run timestamp and status, notification settings. Date math
   uses `msdb.dbo.agent_datetime()` so it is language-safe.
5. Recent failed job steps (last 7 days).
6. Operators and their notification configuration.
7. Alerts with severity, message ID, performance condition, and the
   operator each notifies.
8. Database Mail profiles and accounts.

### 13_wait_stats_baseline.sql

Four result sets - a snapshot of waits since instance startup:

1. The wait-stats window (instance start time and uptime), so the
   following totals can be interpreted.
2. Top 40 non-idle waits with waiting task count, total wait, signal
   wait, average wait per task, percentage of all waits, and an
   inline hint for common dominant waits.
3. Top 20 latch waits.
4. Signal-wait ratio across all non-idle waits as a CPU-pressure
   indicator (over 25 percent sustained is investigate).

---

## The PowerShell orchestrator

`Run-WafConfigAudit.ps1` is the production driver. It is the recommended
way to run the collection across many instances.

### What it does

For each instance in `$Instances`:

1. Performs a connectivity smoke test: connects, runs
   `SELECT @@SERVERNAME`, prints the server name to the console.
   If the smoke test fails the instance is marked failed in the
   workbook and the loop moves on.
2. Opens the per-instance Excel workbook once via
   `Open-ExcelPackage -Create`.
3. For each numbered audit script (alphabetical order), reads the
   script text, executes it through `SqlDataAdapter.Fill` so that
   every result set in the batch is captured as a `DataTable`, and
   writes each non-empty `DataTable` to its own worksheet.
4. Captures `PRINT` and informational messages from the SQL connection
   via the `InfoMessage` event handler and records them on the
   Summary sheet.
5. At the end of each instance, writes a Summary worksheet at the top
   of the workbook with one row per script (status, result-set count,
   row count, elapsed seconds, errors, captured messages) and saves
   the workbook with `Close-ExcelPackage`.

### Worksheet naming

Each result set returned by the collection scripts has a `section`
column with a label like `01 - Version and edition`. The orchestrator
uses that label as the worksheet name. It:

- Sanitizes characters Excel disallows in sheet names (`: \ / ? * [ ]`).
- Truncates to Excel's 31-character maximum.
- Enforces uniqueness within a workbook using a `~2`, `~3` suffix when
  two result sets collide on the same sanitized name.

### Parameters

| Parameter             | Default                  | Purpose                                          |
|-----------------------|--------------------------|--------------------------------------------------|
| `-ScriptFolder`       | script's own folder      | Where the .sql files live                        |
| `-OutputFolder`       | `.\output`               | Where workbooks are written                      |
| `-CommandTimeoutSec`  | `600`                    | Per-script timeout                               |
| `-IncludePreflight`   | (off)                    | Adds `00_preflight_validate_dmvs.sql` to the run |
| `-ScriptNamePattern`  | `[0-9][0-9]_*.sql`       | Wildcard to filter which scripts run             |
| `-InstanceName`       | (none)                   | Filter `$Instances` to one or a wildcard subset  |
| `-ListInstances`      | (off)                    | Print the instance list and exit                 |

### Recipes

```powershell
.\Run-WafConfigAudit.ps1 -ListInstances
.\Run-WafConfigAudit.ps1 -InstanceName 'PROD-CLUSTER01' -IncludePreflight
.\Run-WafConfigAudit.ps1 -InstanceName 'PROD-*'
.\Run-WafConfigAudit.ps1 -CommandTimeoutSec 1200
.\Run-WafConfigAudit.ps1 -ScriptNamePattern '0[12]_*.sql'
.\Run-WafConfigAudit.ps1 -OutputFolder C:\Audit\2026-05
.\Run-WafConfigAudit.ps1
```

### Performance characteristics

The orchestrator keeps each workbook open in memory and writes all
sheets via `Export-Excel -ExcelPackage $pkg -PassThru`, saving once at
the end with `Close-ExcelPackage`. This is roughly an order of
magnitude faster than reopening the .xlsx file for every sheet. Expect
30 to 90 seconds per healthy instance; long Query Store reads and
slow wait-stats reads will push that higher on busy boxes.

### Connection string conventions

Both Windows authentication and SQL authentication are supported - the
`ConnectionString` value is a standard ADO.NET connection string.

Windows authentication:

```
Server=PROD-LISTENER;Database=master;Integrated Security=true;TrustServerCertificate=true;Application Name=WAF Config Audit
```

SQL authentication:

```
Server=PROD-LISTENER,1433;Database=master;User Id=svc_dba;Password=...;TrustServerCertificate=true;Application Name=WAF Config Audit
```

Conventions:

- Point `Server=` at the listener so the audit reads from whichever node
  is currently active in the cluster.
- Set `Application Name=WAF Config Audit` so the audit shows up clearly
  in `sp_who2` and Activity Monitor.
- Set `TrustServerCertificate=true` if your SQL TLS certificate is
  self-signed; otherwise omit it.

---

## Output structure

One Excel workbook is produced per instance, named after the instance.
For 14 instances you get 14 workbooks. Each workbook has the layout
below:

```
PROD-CLUSTER01.xlsx
    Summary                            one row per script: status, rows, elapsed, errors
    01 - Version and edition
    01 - Host and OS
    01 - SQL services
    01 - Registry startup parameters
    01 - Active trace flags
    01 - Process memory
    01 - Scheduler summary
    01 - NUMA nodes (memory side)
    01 - SQLOS nodes
    02 - sys.configurations
    02 - Consequential settings
    03 - Memory and parallelism
    03 - Resource Governor groups
    03 - Database scoped configurations
    03 - LPIM and IFI
    03 - LPIM/IFI per service
    03 - Plan cache by objtype
    03 - Top memory clerks
    04 - tempdb database options
    04 - tempdb files
    04 - tempdb data file sanity
    04 - tempdb allocation latch waits
    04 - tempdb metadata memory-opt
    04 - tempdb XTP memory consumers
    04 - tempdb I/O stats
    05 - Database configuration
    05 - Database deviations
    05 - Database owners
    06 - Database file layout
    06 - File-layout findings
    06 - VLF counts
    07 - Volumes hosting SQL files
    07 - File-to-volume mapping
    07 - I/O stalls by drive
    07 - NTFS allocation-unit reminder
    08 - HA/DR posture summary
    08 - FCI cluster nodes                 (present only on FCI)
    08 - FCI cluster properties            (present only on FCI)
    08 - FCI shared drives                 (present only on FCI)
    08 - Always On endpoints
    08 - Availability Groups               (present only on AG)
    08 - AG replicas                       (present only on AG)
    08 - AG listeners                      (present only on AG)
    08 - AG replica current state          (present only on AG)
    08 - AG database synchronization       (present only on AG)
    08 - Database mirroring                (present only if mirroring)
    08 - Log shipping primary              (present only if log shipping)
    08 - Log shipping secondary            (present only if log shipping)
    08 - WSFC cluster
    08 - WSFC members
    08 - WSFC network subnets
    08 - Default trace failover/cluster events
    08 - AlwaysOn_health role changes      (present only if AG)
    09 - Backup history per database
    09 - Backup destinations
    09 - Suspect pages
    09 - Time since last CHECKDB
    09 - Backup compression default
    10 - Authentication and encryption
    10 - sa login
    10 - Server-role membership
    10 - SQL logins with weak password policy
    10 - SQL Server Audits
    10 - Server Audit specifications
    10 - TDE encrypted databases
    10 - Certificates in master
    10 - Cross-DB ownership chaining
    10 - Server-level permissions
    10 - Linked servers
    11 - Query Store per database
    11 - Query/perf toggles
    11 - Parameterization per database
    12 - SQL Agent service
    12 - Agent-related sp_configure
    12 - SQL Agent properties
    12 - SQL Agent jobs
    12 - Failed job steps (7 days)
    12 - Operators
    12 - Alerts
    12 - Database Mail profiles
    13 - Wait stats window
    13 - Top non-idle waits
    13 - Top latch waits
    13 - Signal wait ratio
```

Sheets for sections that do not apply (for example, AG sheets on an
FCI-only instance) are simply absent from the workbook rather than
present and empty.

---

## Design choices, briefly

- **Read-only by construction.** Nothing in the collection issues
  `ALTER`, `RECONFIGURE`, `DBCC` maintenance commands, or any DML.
  Every result is from a `SELECT`, a DMV, a catalog view, an msdb
  table, or a read-only `DBCC TRACESTATUS`/`DBCC DBINFO` call.
- **Defensive across builds.** The preflight script validates every
  DMV column reference against the live schema before any collection
  runs. Per-database dynamic SQL is wrapped in TRY/CATCH so one bad
  database does not abort the rest.
- **Auto-detect HA/DR topology.** Whether the listener you connect to
  fronts an FCI, an AG, a hybrid, or a standalone instance, the
  relevant sections populate and the irrelevant ones produce a single
  "not in use" note row.
- **Result-set-per-section.** Each collection script emits multiple
  labeled result sets with a `section` column, so the orchestrator
  can route each one to its own worksheet without parsing the SQL.
- **One workbook per instance.** For 14 instances you get 14 files
  rather than one large workbook. This makes per-instance archiving,
  emailing, and side-by-side comparison straightforward.

---

## Known limitations

- The preflight cannot tell you about *data quality* problems on the
  target instance - only about column existence and login permissions.
  An OK preflight means the audit will execute cleanly; it does not
  mean the instance is healthy.
- I/O stall and wait-stats numbers are cumulative since instance
  startup. If the instance was restarted recently the numbers will not
  reflect a representative workload. The Summary sheet flags uptime
  via script 01 / section 02 / uptime_days; treat anything under a
  week as a young baseline.
- `DBCC DBINFO` reads (script 09 section 4) require sysadmin. Without
  it, the last-CHECKDB section reports no data rather than throwing.
- The audit reads from whichever node it connects to via the listener.
  On an active-passive FCI that is by definition the active node;
  there is no meaningful per-node configuration drift to worry about
  for FCI. On an AG, run the orchestrator against the listener for
  the primary's view, then optionally re-run with `Server=<replica>`
  in the connection string to capture each secondary's local view.

---

## Next steps

After the collection produces the 14 workbooks, the downstream
workflow is:

1. **Catalog**. Load each workbook's result sets into a structured
   per-instance configuration catalog (one row per setting per
   instance) so the data is queryable.
2. **Document**. Produce a per-instance narrative that explains the
   posture and a per-cluster narrative that compares instances within
   the same cluster.
3. **Recommend**. Flag deviations from best practice for the
   FCI-on-SAN-on-physical topology with a rationale for each, and
   distinguish apparent issues (something is clearly wrong) from
   latent issues (the configuration is acceptable today but will not
   age well).
