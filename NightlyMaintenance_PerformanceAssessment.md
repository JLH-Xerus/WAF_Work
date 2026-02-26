# Nightly Maintenance Stored Procedures — Performance Assessment

**Author:** Justin Hunter / Claude
**Date:** 2026-02-25
**Scope:** `lsp_DbPerformNightlyMaintenance` and all 19 child stored procedures
**Objective:** Identify performance bottlenecks, bugs, and refactoring opportunities — with an eye toward integration with the table partitioning POC (Modules 1–5)

---

## Executive Summary

The nightly maintenance suite is a well-structured, defensive codebase that has grown organically over 26 versions. It handles database integrity checks, backups, index rebuilds, and a large number of data purge operations across 30+ tables. The procedures are generally sound in intent, but several systemic patterns severely limit throughput — particularly at the scale described in the partitioning POC (hundreds of millions to billions of rows). The single biggest opportunity is replacing the iterative block-delete loops with partition-aware `SWITCH` operations on the high-volume tables, which would reduce multi-hour purge windows to seconds.

Below are the findings organized by severity and category.

---

## 1. Critical Bugs

### 1.1 SmartShelf Delete Loops Never Execute (lsp_DbDeleteOldSmartShelfErrors & Events)

**Severity:** Critical — these procedures silently do nothing.

Both `SmartShelf.lsp_DbDeleteOldSmartShelfErrors` (line 81) and `SmartShelf.lsp_DbDeleteOldSmartShelfEvents` (line 78) have an identical logic bug in their `WHILE` condition:

```sql
While Exists(Select * From SmartShelf.EvtErrorLog Where DtTm < @CutoffDtTm)
  And @NumOfRowsDeleted > @MaxToDelete   -- BUG: should be "<"
```

`@NumOfRowsDeleted` is initialized to `0` and `@MaxToDelete` defaults to `25000000`. The condition `0 > 25000000` is immediately `FALSE`, so the `WHILE` loop is never entered. Old SmartShelf errors and events are **never actually deleted**.

**Fix:** Change `>` to `<` in both procedures, matching the pattern used in every other delete procedure.

### 1.2 Double-Delete in lsp_DbDeleteOldDiagEvents

**Severity:** High — wasted I/O and potential for t-log inflation.

`lsp_DbDeleteOldDiagEvents` issues two delete statements within each loop iteration — first a `DELETE TOP (50000)` and then immediately a second delete using a self-join on `Top 50000`:

```sql
Delete Top (50000) From EvtDiagEvent Where EventDtTm < @CutOffDate

Delete EvtDiagEvent
From EvtDiagEvent As AllEvts
Join (Select Top 50000 TrxId From EvtDiagEvent Where EventDtTm < @CutOffDate) As DelEvts
   On AllEvts.TrxId = DelEvts.TrxId
```

The first statement deletes 50,000 rows. The second statement then deletes another 50,000 rows (or fewer if running out) — but both are wrapped in an explicit transaction. This means each iteration deletes up to 100,000 rows in a single transaction rather than the intended 50,000, and the `Exists` check loop has no `@MaxToDelete` cap, so it will run unbounded until the table is fully purged.

**Fix:** Remove one of the two delete statements (the second appears to be the original, the first an incomplete refactor). Add `@MaxToDelete` and `@NumOfRowsBlockSize` parameters consistent with the newer procedures.

### 1.3 lsp_DbDeleteOldErrors — Same Double-Delete Pattern

**Severity:** High — same issue as 1.2.

`lsp_DbDeleteOldErrors` has commented-out code for a `DELETE TOP (50000)` pattern but then uses the self-join approach. The commented code suggests this was mid-refactor. Additionally, like `lsp_DbDeleteOldDiagEvents`, it wraps each block in an explicit `BEGIN TRAN / COMMIT TRAN`, which is unnecessary given that each `DELETE` is already an atomic operation. The explicit transaction just holds locks longer.

Also lacks `@MaxToDelete` — the loop runs until all qualifying rows are gone, which could inflate the t-log enormously on first activation if millions of old error rows exist.

---

## 2. Structural / Architectural Issues

### 2.1 Unbounded Delete Loops (Older Procedures)

**Affected:** `lsp_DbDeleteOldErrors`, `lsp_DbDeleteOldDiagEvents`, `lsp_SsDeleteOldEvents`

These three older-generation procedures lack the `@MaxToDelete` safety parameter that the newer procedures have. They will loop until every qualifying row is deleted, regardless of t-log impact. The newer procedures (e.g., `lsp_DbDeleteOldEvents`, `lsp_DbDeleteOldRxData`, `lsp_DbDeleteOldPmssData`) all correctly implement `@MaxToDelete`.

**Recommendation:** Refactor these three procedures to match the modern parameter pattern with `@OlderThanXDays`, `@NumOfRowsBlockSize`, and `@MaxToDelete`.

### 2.2 Iterative Block Deletion Becomes Slow at Scale

**Affected:** All 14 deletion procedures.

Every delete procedure follows the same fundamental pattern: a `WHILE EXISTS` loop that deletes N rows per iteration using set-based `DELETE TOP (N)` or CTE-based block deletes. **Note:** These are not row-by-row (RBAR) operations — each individual delete is a proper set-based operation removing thousands of rows at once. This is a well-established and reasonable pattern for managing t-log growth during large purges.

However, the **iterative looping overhead** becomes significant at scale on the high-volume tables identified in the partitioning POC (EvtEvent, OeOrderHistory, OeScriptImage, etc.) where hundreds of millions of rows may qualify for deletion. Each iteration of the loop:
1. Runs an `EXISTS` subquery (full scan until a qualifying row is found)
2. Identifies the block of rows to delete (`TOP N` — requires an index seek or scan)
3. Deletes those rows as a set (generating t-log entries for every row)
4. Repeats

For a table with 500M qualifying rows at 100,000 rows per block, that's 5,000 loop iterations, each with its own query plan execution. Even at 2 seconds per iteration, that's nearly 3 hours for a single table — not because the deletes are row-by-row, but because the cumulative overhead of thousands of iterations adds up.

**Recommendation (ties to partitioning POC):** For tables that are partitioned on their date column, replace the iterative block-delete loop with `ALTER TABLE ... SWITCH PARTITION` to move old partitions to a staging table, then `TRUNCATE` the staging table. This is a metadata-only operation that completes in milliseconds regardless of row count. Module 3 (Switch Operations) and Module 4 (Maintenance Plans) of the POC already detail this approach. For non-partitioned tables, the current block-delete pattern remains appropriate.

### 2.3 EXISTS(SELECT *) as Loop Condition Is Expensive

**Affected:** All delete procedures using `WHILE EXISTS(SELECT * FROM Table WHERE DateCol < @Cutoff)`.

On large tables without a covering index on the date column, this can trigger a full table scan on every loop iteration just to check if more work remains. Combined with the block-delete approach, the cumulative cost of these existence checks can be significant.

**Recommendation:** Use `@@ROWCOUNT` from the previous delete to determine if there's more work. If the previous delete returned fewer rows than `@NumOfRowsBlockSize`, there's nothing left. This eliminates the need for the `EXISTS` check entirely:

```sql
WHILE @ContinueLoop = 1 AND @NumOfRowsDeleted < @MaxToDelete
BEGIN
    DELETE ... -- block delete
    SET @RowsThisBatch = @@ROWCOUNT
    SET @NumOfRowsDeleted += @RowsThisBatch
    IF @RowsThisBatch < @NumOfRowsBlockSize
        SET @ContinueLoop = 0
END
```

### 2.4 Inline SQL Deletes in the Parent Procedure

**Affected:** `lsp_DbPerformNightlyMaintenance` Step 05 (Purge Temp Data).

The parent procedure contains approximately 10 inline delete statements (wrapped in dynamic SQL strings executed via `sp_ExecuteSql`) for tables like `OrdLabelPrintStream`, `ShpShipmentPkgLabelData`, `TcdCommDumpLog`, `SysActionQueue`, `SysSystemIsActiveTimeLog`, `CvyRxRoute`, `SysStatHistory`, `ToteLog`, `InvWkstnNdcLotCode`, and `EvtAPIHistoryEvent`.

These inline deletes have no block-size control, no `@MaxToDelete` cap, and no row-count tracking. On tables like `CvyRxRoute` (which uses a `LEFT JOIN` to `OeOrder`), the delete can be expensive if the tables are large.

Additionally, the `EvtAPIHistoryEvent` delete (around line 1210) is executed **directly** (not via `sp_ExecuteSql`), which means a failure here could potentially affect the execution of subsequent steps — contradicting the defensive `sp_ExecuteSql` pattern used everywhere else.

**Recommendation:** Either extract these into dedicated child procedures following the modern block-delete pattern, or at minimum wrap the `EvtAPIHistoryEvent` delete in `sp_ExecuteSql` for consistency.

### 2.5 `NOT IN` Subqueries for Orphan Detection

**Affected:** Parent procedure — `OrdLabelPrintStream` delete and `ShpShipmentPkgLabelData` delete.

```sql
Delete From OrdLabelPrintStream
Where ReceivedDtTm < DateAdd(Hour, -24, GetDate())
  And OrderId Not In (Select OrderId From OeOrder With (NoLock))
```

`NOT IN` with a subquery is problematic because: (a) if any `OrderId` in `OeOrder` is `NULL`, the entire `NOT IN` returns no rows (SQL Server three-valued logic), and (b) the optimizer often converts this to a less efficient anti-join plan compared to `NOT EXISTS` or `LEFT JOIN ... IS NULL`.

**Recommendation:** Rewrite using `NOT EXISTS`:
```sql
Delete L From OrdLabelPrintStream L
Where L.ReceivedDtTm < DateAdd(Hour, -24, GetDate())
  And Not Exists (Select 1 From OeOrder O With (NoLock) Where O.OrderId = L.OrderId)
```

---

## 3. Index Rebuild Performance

### 3.1 lsp_DbRebuildFragmentedIndexes — Full DMV Scan with LIMITED Mode

The procedure queries `sys.dm_db_index_physical_stats` with `NULL` for the last parameter, which defaults to `LIMITED` mode. `LIMITED` mode only examines leaf-level pages for heaps and parent-level (non-leaf) pages for B-trees. This is fast but can be inaccurate for fragmentation assessment.

More importantly, the cursor iterates through **every** fragmented index in the entire database and rebuilds them one at a time. With no page-count filter, this includes tiny indexes (< 1000 pages) where fragmentation is meaningless.

**Recommendation:**
- Add a minimum page count filter (e.g., `AND S.Page_Count > 1000`) to skip small indexes where rebuild is wasted effort.
- Consider `REORGANIZE` for indexes between 10–30% fragmentation and `REBUILD` only above 30%.
- For partitioned tables (per the POC), rebuild only specific partitions: `ALTER INDEX ... REBUILD PARTITION = N`. The existing procedure rebuilds the entire index, which is unnecessary if only the hot partition is fragmented.
- Module 4, Section 4.2 of the partitioning POC already contains a partition-aware index maintenance procedure that addresses all of these points.

### 3.2 Hard-Coded EvtEvent PK Exclusion

The procedure has a hard-coded bypass for `PK_EvtEvent`:

```sql
If @TableName = 'EvtEvent' And @IndexName = 'PK_EvtEvent'
    Print 'Bypassing index ' + @TableName + '.' + @IndexName
```

The comment explains this was due to a one-time data migration fragmentation issue. This is tech debt — the check runs on every index rebuild cycle indefinitely. If the EvtEvent table is partitioned (as suggested in the POC), this bypass becomes unnecessary because partition-level rebuilds would target only the affected partitions.

---

## 4. lsp_DbDeleteOldRxData — The Most Complex Procedure

### 4.1 Temp Table Recreated Every Loop Iteration

Each loop iteration drops and recreates `#BlkOfRxsToDel` with a `SELECT INTO` followed by `CREATE INDEX`. On SQL Server, the temp table metadata (and any cached plan) must be rebuilt every iteration.

**Recommendation:** Create the temp table once before the loop with `CREATE TABLE #BlkOfRxsToDel (OrderId INT, HistoryDtTm DATETIME)` and the index, then use `INSERT INTO ... SELECT TOP` + `TRUNCATE TABLE` within the loop. This allows SQL Server to reuse the temp table's cached execution plans.

### 4.2 Sequential Deletes from ~20 Related Tables Per Block

Each block iteration deletes from approximately 20 tables sequentially (OeOrderSecondaryData, OeOrderCurrHistoryDtTm, CaAudit, CvyRxRoute, OeDurData, OeFlaggedRxs, OeLotCode, shipment associations, image associations, etc.). Each delete is a separate statement with its own plan compilation and execution.

While this is necessary for referential integrity ordering, the approach generates enormous t-log volume because each of the ~20 deletes per block is a separate transaction.

**Recommendation for partitioned environment:** With partitioned tables, the entire cascade of deletes becomes unnecessary for partitioned child tables. If the child tables are partitioned on the same key (or a key derivable from the parent's partition key), a coordinated `SWITCH` operation can move entire partitions of the parent and child tables in parallel. This is detailed in Module 3 of the partitioning POC.

---

## 5. lsp_DbDeleteOldWorkflowImages — Pre-Loads All IDs

### 5.1 Bulk Load of All Qualifying IDs Into Temp Table

This procedure loads **all** qualifying image IDs into `#OlderImageIds` before entering the delete loop:

```sql
Insert Into #OlderImageIds
Select I.Id From ImgImage I Join @ContentCodes C On C.Code = I.ContentCode
Where CapturedDtTm < @CutoffDtTm And ArchivedDtTm Is Null
Order By CapturedDtTm
```

If there are millions of qualifying images, this temp table itself becomes a performance problem (memory pressure, spill to tempdb). The subsequent loop then deletes blocks from `#OlderImageIds` using `DELETE WHERE Id <= (SELECT MAX(Id) FROM #BlkIds)`, which is efficient but unnecessary if the procedure just used the main table directly (as the other delete procedures do).

**Recommendation:** Follow the same CTE-based block-delete pattern used by the other procedures, querying the source table directly. The pre-load approach adds complexity and memory overhead without clear benefit.

---

## 6. lsp_DbDeleteOldAutoVerificationData — No Batching

### 6.1 Unbounded Full-Table Deletes

This procedure deletes from three tables (`InvAliasCodeAutoVerifyRxDirections`, `InvNdcAutoVerifyRxDirections`, `InvProductAutoVerifyRxDirections`) using simple `DELETE ... WHERE LastRxVerificationDtTm < @CutoffDtTm` with no block-size control:

```sql
Delete InvAliasCodeAutoVerifyRxDirections
   Where LastRxVerificationDtTm < @CutoffDtTm
```

Although the `@MaxToDelete` parameter exists in the procedure signature, it is **never referenced** in the procedure body. Each delete operates on the entire qualifying result set in a single transaction.

**Recommendation:** Implement block-size deletion or, given these tables may be relatively small, at minimum verify the row volumes to confirm that unbounded deletes are acceptable.

---

## 7. Utility and Support Procedures

### 7.1 lsp_DbGrowDbFilesIfNearFull — Cursor-Based

Uses a cursor to iterate over `DbFileSpaceStatsSnapshot`. This is fine since there are typically only a handful of database files. No performance concern.

### 7.2 lsp_DbLogDbFileSpaceStats — Cursor-Based

Same pattern as the grow procedure — a cursor over a small result set to log events. Also calls `lsp_DbPopulateTableDbFileSpaceStatsSnapshot` which we don't have in the repo (along with `lsp_DbGrowDbFile`, `lsp_DbLogSqlEvent`, `lsp_DbLogSqlError`). These appear to be utility procedures that are likely lightweight.

### 7.3 lsp_DbInsertOrUpdateSysProperty — Clean and Efficient

Simple upsert pattern. No issues.

### 7.4 lsp_SysMonthlyUpdateCentralFillClosedDates — Transaction Wrapped

Runs inside an explicit transaction with a `GOTO PROCERR` / `ROLLBACK` pattern. This is fine for a low-volume operation. One minor note: `@@ROWCOUNT` is captured after `COMMIT TRANSACTION` (line 143), but `@@ROWCOUNT` reflects the last statement's row count — which is the `COMMIT` itself (0 rows), not the update operation. The `@NumItemsUpdated` output parameter from `lsp_SysUpdateCentralFillClosedDates` should already have the correct count.

---

## 8. Connection to Partitioning POC

The performance issues identified above map directly to the partitioning POC modules:

| Issue | POC Module | Impact |
|-------|-----------|--------|
| Iterative block deletes on EvtEvent (billions of rows) | Module 3: Switch Operations | Hours → milliseconds |
| Iterative block deletes on OeOrderHistory + cascade | Module 3: Switch Operations | Hours → seconds (coordinated switch) |
| Full-table index rebuild on large tables | Module 4: Maintenance Plans | Hours → minutes (partition-level rebuild) |
| EXISTS scans on unpartitioned tables | Module 2: Indexing Strategy | Elimination via partition pruning |
| DBCC CHECKDB on entire database | Module 4: Maintenance Plans | Target only hot filegroups |
| Full DMV scan for fragmentation | Module 4: Maintenance Plans | Scan only hot partitions |

### Recommended Prioritization

1. **Fix the SmartShelf bugs** (items 1.1) — zero effort, immediate correctness fix.
2. **Fix the double-delete bugs** (items 1.2, 1.3) — quick win, prevents wasted I/O.
3. **Standardize older procedures** (item 2.1) — add `@MaxToDelete` to the three legacy procs.
4. **Partition the high-volume event tables** (EvtEvent, EvtInvEventLog, SmartShelf.EvtEventLog) — these are the biggest bang-for-buck partitioning candidates because they're append-only, date-keyed, and purged by age. Partition switch replaces the delete loops entirely.
5. **Partition OeOrderHistory and related tables** — more complex due to the cascade of ~20 child tables, but the payoff is enormous (Module 3 of the POC covers this).
6. **Implement partition-aware index maintenance** — replace `lsp_DbRebuildFragmentedIndexes` with the Module 4 procedure that only touches hot partitions.
7. **Refactor inline deletes in the parent procedure** — lower priority but improves maintainability.

---

## 9. Additional Stored Procedures Referenced (Not in Repo)

The following procedures are called but were not included in the repo. They should be reviewed as part of a complete assessment:

- `lsp_DbPopulateTableDbFileSpaceStatsSnapshot` — called by both `lsp_DbLogDbFileSpaceStats` and `lsp_DbGrowDbFilesIfNearFull`
- `lsp_DbGrowDbFile` — called by `lsp_DbGrowDbFilesIfNearFull`
- `lsp_DbLogSqlEvent` — called by most delete procedures for event logging
- `lsp_DbLogSqlError` — called by most delete procedures for error logging
- `lsp_SysGetCentralFillWeekdayAndHolidayClosedDates` — called by the closed dates procedure
- `lsp_SysUpdateCentralFillClosedDates` — called by the closed dates procedure
- `fStringListToVarCharTable` / `fStringListToIntTable` — table-valued functions used for parameter parsing

---

## 10. Summary of Findings

| Category | Count | Key Items |
|----------|-------|-----------|
| Critical Bugs | 3 | SmartShelf delete loops never execute; double-deletes in errors/diag events |
| Unbounded Operations | 4 | Three legacy procs + auto-verification lack @MaxToDelete |
| Scalability Bottlenecks | 14 | All delete procs use iterative block-delete loops — slow at scale on high-volume tables |
| Unused Parameters | 1 | @MaxToDelete in auto-verification proc is never used |
| Architectural Improvements | 5 | NOT IN → NOT EXISTS, EXISTS → @@ROWCOUNT, temp table reuse, inline SQL extraction, EvtEvent PK bypass removal |
| Partitioning Candidates | 6+ | EvtEvent, EvtInvEventLog, OeOrderHistory, OeScriptImage, ImgImage, SmartShelf tables |
