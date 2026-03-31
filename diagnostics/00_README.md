# Performance Diagnostics Toolkit

**Purpose:** Deployable diagnostic kit for rapid site assessment across all 15 instances. Run these scripts to gather evidence for performance recommendations.

**Requirements:** SQL Server 2016+ with Query Store enabled. Read-only — no data modification.

---

## Recommended Execution Order

Run the scripts in numbered order. Each builds on the context of the previous:

| # | Script | Time | What It Answers |
|---|--------|------|-----------------|
| **01** | `01_QueryStore_TopOffenders.sql` | ~5s | "Which procs are burning the most I/O?" |
| **02** | `02_PlanInstability_Detector.sql` | ~5s | "Which procs have parameter sniffing problems?" |
| **03** | `03_ForcedPlan_FailureAudit.sql` | ~2s | "Are any forced plans silently failing?" |
| **04** | `04_WaitStats_Snapshot.sql` | ~1s | "What is the server waiting on — I/O, locks, CPU, memory?" |
| **05** | `05_IndexUsage_And_MissingIndexes.sql` | ~10s | "Which indexes are unused? Which are missing?" |
| **06** | `06_ShipTote_SkewAnalysis.sql` | ~15s | "Is SrtShipToteShipmentAssoc data skewed?" (proc-specific) |
| **07** | `07_Statistics_Staleness.sql` | ~5s | "Are any table statistics stale enough to cause bad plans?" |
| **08** | `08_TempDB_And_MemoryPressure.sql` | ~5s | "Are we under resource pressure — tempdb, buffer pool, memory grants?" |
| **09** | `09_Skew_Analysis_Generic.sql` | ~15s | "Is column X in table Y skewed?" (reusable, parameterized) |
| **10** | `10_DataVelocity_And_Capacity.sql` | ~30s | "How fast is each table growing? When do we hit the wall?" |

**Total time: ~90 seconds for a full site assessment.**

---

## Quick Start: New Site Assessment

```
-- Step 1: Identify the worst offenders
-- Run 01, 02, 03 in sequence. Export results.

-- Step 2: Understand the server environment
-- Run 04, 05, 07, 08. Export results.

-- Step 3: Deep-dive on top offenders
-- For each proc from Step 1:
--   a. Read the proc code — look for anti-patterns
--   b. Run 09 with the relevant table/column for skew analysis
--   c. Check 07 for stale stats on tables the proc touches

-- Step 4: Measure data velocity
-- Run 10 to understand growth rates per table.
-- Compare across sites to identify which instances are under
-- the most pressure and where archival policies are needed.

-- Step 5: Prioritize
-- Rank by TotalReads from 01. Fix the top 3-5 first.
-- Deploy, re-run 01, verify improvement, repeat.
-- Use velocity data from 10 to predict which fixes are
-- most time-sensitive (fast-growing tables degrade faster).
```

---

## Configurable Parameters

| Script | Variable | Default | Purpose |
|--------|----------|---------|---------|
| 01 | `@LookbackHours` | 24 | Query Store analysis window |
| 02 | `@LookbackHours` | 24 | Plan instability analysis window |
| 02 | `@MinReadRatio` | 2.0 | Minimum read ratio to surface |
| 07 | `@MinModPct` | 10.0 | Minimum modification % to surface |
| 09 | `@TableName` | *(set before running)* | Target table for skew analysis |
| 09 | `@ColumnName` | *(set before running)* | Grouping column for skew analysis |
| 10 | `@Tables` insert list | *(configure per site)* | Table/DateColumn pairs to measure velocity |

---

## Anti-Pattern Quick Reference

When reviewing a proc flagged by Script 01, look for these patterns:

| Anti-Pattern | Detection Signal | Fix |
|--------------|-----------------|-----|
| **Parameter sniffing** | Script 02: ReadRatio > 10x | Local variable assignment |
| **Correlated subqueries** | Same table has reads >> its size in STATISTICS IO | CTE with GROUP BY + LEFT JOIN back |
| **Scalar UDF** | `dbo.FunctionName()` in SELECT; CPU ≈ Duration | Inline with STRING_AGG / CTE |
| **LEFT JOIN + OR** | LEFT JOINs with OR across different join paths in WHERE | UNION of INNER JOIN branches |
| **Table variables** | `Declare @T Table (...)` joined to large tables | Convert to #temp table with index |
| **Non-SARGable** | `IsNull(col,0)`, `Convert(...)`, `Year(col)` in WHERE | Rewrite predicate or filtered index |
| **UNION ALL views** | `v` prefix on table name; check view definition | Keep view, fix surrounding pattern |
| **Stale statistics** | Script 07: ModPct > 20% | UPDATE STATISTICS |
| **Forced plan failures** | Script 03: FailureCount > 0 | UNFORCE plan, fix root cause |

---

## Notes

- Scripts 01-03 require **Query Store** to be enabled. If it's not, start with 04, 05, 07, 08.
- Script 06 is specific to `lsp_SrtGetShipToteIfExists` tables. Use Script 09 for general-purpose skew analysis.
- Wait stats (04) are cumulative since last restart. For delta analysis, run twice with a gap.
- Index usage (05) counters reset on restart. Allow a representative workload period before acting on "unused" indexes.
- All scripts use `WITH (NOLOCK)` or read from DMVs. Safe to run on production.
- Script 10 requires configuring table/date-column pairs for your schema. Defaults are set for the pharmacy fulfillment schema. Adjust the `@Tables` INSERT block for each site's specific tables.
- Script 10's growth projections are linear extrapolations. Useful for capacity planning but won't capture seasonal patterns. Use the trend data (Section C) to spot non-linear growth.
