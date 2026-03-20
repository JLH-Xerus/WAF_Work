# LSP Purge Process — Low-Hanging Fruit Optimization Analysis

**Author:** Justin Hunter / Claude
**Date:** 2026-03-18
**Scope:** `lsp_DbPurgeHistoryData`, `lsp_DbPurgeHistoryData_Driver`, `lsp_DbDeleteOldEvents`
**Objective:** Identify quick-win optimizations that can be applied *now*, before table partitioning is in place

---

## Context

This analysis focuses exclusively on things that can be changed in the purge stored procedures themselves to improve throughput, reduce log pressure, and improve operational visibility — without requiring schema changes, new indexes on base tables, or the partitioning work from the POC modules. These are the "fix the code" improvements vs. the "fix the architecture" improvements.

The procedures are grouped by priority: things that are almost certainly hurting you right now at the top, followed by improvements that become more valuable as data volume grows.

---

## Priority 1 — Fix These First (Immediate Impact)

### 1.1 Replace `WHILE EXISTS(SELECT *)` with `@@ROWCOUNT`-Based Loop Control

**Affected:** `lsp_DbPurgeHistoryData` (lines 130, 1580), `lsp_DbDeleteOldEvents` (line 101)

Every iteration of the main delete loop begins by running an `EXISTS` subquery against the full base table:

```sql
While Exists (Select * From OeOrderHistory
   Where HistoryDtTm >= @HistoryDtTm_EffectiveFrom
     And HistoryDtTm < @HistoryDtTm_EffectiveTo)
   And @NumOfRowsDeleted < @MaxToDelete
```

On a table with hundreds of millions of rows, this `EXISTS` check must locate at least one qualifying row before proceeding. Even with an index on `HistoryDtTm`, this is wasted I/O that runs *every single iteration*. On a 500M-row table being purged at 100K rows/block, that's 5,000 unnecessary index seeks just to answer "is there more work?" — when you already know the answer from the previous delete's `@@ROWCOUNT`.

**Recommended pattern:**

```sql
Declare @ContinueLoop Bit = 1

While @ContinueLoop = 1 And @NumOfRowsDeleted < @MaxToDelete
Begin
   -- ... do the block delete ...

   Set @DeletedThisPass = @@ROWCOUNT
   Set @NumOfRowsDeleted += @DeletedThisPass

   If @DeletedThisPass < @NumOfRowsBlockSize
      Set @ContinueLoop = 0
End
```

The logic: if the delete returned fewer rows than the block size, the table is exhausted (or you've hit the end of the qualifying range). No `EXISTS` needed.

**Impact:** Eliminates one full index seek per loop iteration. On a purge run that takes 5,000 iterations, this saves 5,000 query executions. On the OeOrderHistory loop specifically, the savings compound because the `EXISTS` check happens at the *top* of the loop — meaning every one of those ~40 cascading deletes has to wait for the next `EXISTS` check before the next block starts.

**Applies identically to:** The `OeOrderAcceptReject` loop (line 1580) and `lsp_DbDeleteOldEvents` (line 101).

---

### 1.2 Stop Dropping and Recreating `#BlkOfRxsToDel` Every Iteration

**Affected:** `lsp_DbPurgeHistoryData` (lines 138–152)

Every loop iteration does this sequence:

```sql
If Object_Id('TempDb..#BlkOfRxsToDel') Is Not Null
   Drop Table #BlkOfRxsToDel

Select Top (@NumOfRowsBlockSize) OrderId, HistoryDtTm
Into #BlkOfRxsToDel
From OeOrderHistory ...

Create Index ByOrderIdHistoryDtTm On #BlkOfRxsToDel(OrderId, HistoryDtTm)
```

This forces SQL Server to:

1. Drop the temp table's metadata, statistics, and cached plans
2. Create a brand-new temp table via `SELECT INTO` (which generates schema lock overhead)
3. Create a new nonclustered index (builds a new B-tree from scratch)
4. **Recompile every downstream statement** that references `#BlkOfRxsToDel` — that's approximately 15 DELETE statements that all join to this temp table. Each one gets a fresh plan compilation on every loop iteration.

At 100K block size, the temp table is tiny (a few MB), so the index build is fast — but the **plan recompilation of 15+ statements per iteration** is the real cost. On a purge run with thousands of iterations, you're doing tens of thousands of unnecessary compilations.

**Recommended pattern:**

```sql
-- Before the loop:
Create Table #BlkOfRxsToDel (OrderId Int, HistoryDtTm DateTime)
Create Index ByOrderIdHistoryDtTm On #BlkOfRxsToDel(OrderId, HistoryDtTm)

-- Inside the loop:
Truncate Table #BlkOfRxsToDel

Insert Into #BlkOfRxsToDel (OrderId, HistoryDtTm)
Select Top (@NumOfRowsBlockSize) OrderId, HistoryDtTm
From OeOrderHistory
Where HistoryDtTm >= @HistoryDtTm_EffectiveFrom
  And HistoryDtTm < @HistoryDtTm_EffectiveTo
Order By HistoryDtTm
```

`TRUNCATE TABLE` is a metadata-only operation (instant, minimal logging). `INSERT INTO` a pre-existing temp table with a stable schema allows SQL Server to reuse cached plans. The index structure stays in place and gets populated by the insert.

**Impact:** Eliminates plan recompilation overhead for ~15 delete statements per iteration. On a 3,000-iteration purge run, this eliminates ~45,000 unnecessary compilations.

---

### 1.3 `SELECT *` in OeOrderAcceptReject CTE

**Affected:** `lsp_DbPurgeHistoryData` (line 1590)

```sql
;With Cte As
   (
      Select Top (@NumOfRowsBlockSize) *
      From OeOrderAcceptReject
      Where RejectDtTm >= @HistoryDtTm_EffectiveFrom ...
      Order By Id
   )
Delete From Cte
```

The CTE selects all columns (`SELECT *`), then deletes from the CTE. SQL Server needs to read every column from the clustered index for the TOP N rows, even though the delete only needs the key columns to identify which rows to remove.

**Fix:** Change `Select Top (@NumOfRowsBlockSize) *` to `Select Top (@NumOfRowsBlockSize) Id`.

The delete-from-CTE pattern works perfectly fine when the CTE selects only the key — SQL Server uses the key to locate and delete the full row. You just don't need to *read* the full row to delete it.

**Impact:** Reduces I/O per block by however wide the OeOrderAcceptReject row is. If the table has LOB columns or is particularly wide, this can be significant.

---

## Priority 2 — Add Operational Safety Nets

### 2.1 Add TRY/CATCH Error Handling to `lsp_DbPurgeHistoryData`

**Affected:** `lsp_DbPurgeHistoryData` — the entire procedure body has **no error handling**.

Compare to `lsp_DbDeleteOldEvents`, which has a proper `BEGIN TRY / BEGIN CATCH` with error logging via `lsp_DbLogSqlError` and `lsp_DbLogSqlEvent`. If any of the ~40 DELETE statements inside `lsp_DbPurgeHistoryData` fails — say, a deadlock, a lock timeout, or a constraint violation — the procedure crashes with an unhandled error. The caller (`lsp_DbPerformNightlyMaintenance`) wraps the call in `sp_ExecuteSql` which will trap the error, but:

- No error details are logged to the event table
- No "PurgeOldData End / Failed" event is written
- The print output (which is the only diagnostic) may be lost if the connection is closed
- The database is left in a partially-purged state (some child tables deleted, parent rows still present for that block)

**Fix:** Wrap the entire main body in `BEGIN TRY / BEGIN CATCH` matching the pattern in `lsp_DbDeleteOldEvents`:

```sql
Begin Try
   -- ... existing loop logic ...

   Set @EvtNotes = 'History=' + Cast(@HistoryRowsDeletedLocal As VarChar(20))
                  + '; AcceptReject=' + Cast(@AcceptRejectRowsDeletedLocal As VarChar(20))
   Exec lsp_DbLogSqlEvent 'A', 'PurgeOldData End', 'Successful', @EvtNotes, 'lsp_DbPurgeHistoryData'
   Return 0
End Try
Begin Catch
   Set @ErrNum = ERROR_NUMBER()
   Set @ErrMsg = ERROR_MESSAGE()
   Exec dbo.lsp_DbLogSqlError 'PurgeOldData Err', @ErrNum, @ErrMsg, 'lsp_DbPurgeHistoryData'

   Set @EvtNotes = Left('History=' + Cast(@HistoryRowsDeletedLocal As VarChar(20))
                  + '; AcceptReject=' + Cast(@AcceptRejectRowsDeletedLocal As VarChar(20))
                  + '; ' + @ErrMsg, 255)
   Exec lsp_DbLogSqlEvent 'A', 'PurgeOldData End', 'Failed', @EvtNotes, 'lsp_DbPurgeHistoryData'

   -- Still output the counts so the driver can see partial progress
   Set @HistoryRowsDeleted = @HistoryRowsDeletedLocal
   Set @AcceptRejectRowsDeleted = @AcceptRejectRowsDeletedLocal

   Return @ErrNum
End Catch
```

**Impact:** Operational visibility. When this proc fails at 2 AM, you'll know exactly what happened instead of having to hunt through SQL Server error logs.

---

### 2.2 Add Per-Block Progress Logging

**Affected:** `lsp_DbPurgeHistoryData`

The procedure currently prints progress messages (`Print 'Processing block of ...'`) but does **not** log the total rows deleted after each block completes. The only row-count logging happens at the very end in the "Successful" event note — and even that currently logs an empty string (`Set @EvtNotes = ''` on line 1613).

When a purge runs for 3 hours and fails at hour 2, you have no idea how far it got. Add a print after each OeOrderHistory block delete:

```sql
Print 'Block complete: ' + Cast(@DeletedThisPass As VarChar(12))
    + ' OeOrderHistory rows this block, '
    + Cast(@NumOfRowsDeleted As VarChar(12))
    + ' total.   (' + Convert(VarChar(30), GetDate(), 120) + ')'
```

Also fix line 1613 — the "Successful" event note is currently empty. It should log the final counts:

```sql
Set @EvtNotes = 'History=' + Cast(@HistoryRowsDeletedLocal As VarChar(20))
              + '; AcceptReject=' + Cast(@AcceptRejectRowsDeletedLocal As VarChar(20))
```

**Impact:** When investigating a slow or failed purge, you can see exactly how many rows were processed and how long each block took. This is essential for tuning `@NumOfRowsBlockSize`.

---

## Priority 3 — Reduce Unnecessary Work Inside the Loop

### 3.1 Add Indexes to Inner Temp Tables

**Affected:** `lsp_DbPurgeHistoryData` — all the cascade temp tables

Only `#BlkOfRxsToDel` gets an index. None of these other temp tables get indexed:

- `#ShpIds` (ShipmentId) — joined to by ~8 DELETE statements
- `#ManIds` (ManifestId) — joined to by ~3 DELETE statements
- `#PalIds` (PalletId) — joined to by ~2 DELETE statements
- `#Grps` (GroupNum) — joined to by ~9 DELETE statements
- `#Pats` (PatCustId) — joined to by ~7 DELETE statements
- `#Prescrs` (PrescrId) — joined to by ~3 DELETE statements
- `#RxDoseSchedIds` (RxDoseSchedId) — joined to by 3 DELETE statements
- `#PwkSetIds` (PaperworkSetId) — joined to by 2 DELETE statements
- `#CanReplens` (CanisterSn, ReplenDtTm) — joined to by ~5 DELETE statements

At a block size of 100K, these temp tables can have up to 100K rows (for #BlkOfRxsToDel) or potentially more for the cascade tables (a single order can have many shipments, images, etc.). SQL Server will table-scan these temp tables on every join if they're small enough, which is fine for a few hundred rows — but with 100K orders per block and potentially millions of cascade rows, these scans add up.

**Recommendation:** Add a clustered index to each temp table after population, on the join column. The highest-value ones to index first are `#ShpIds`, `#Grps`, and `#Pats` since they participate in the most joins.

If you adopt the "create table once, truncate+insert in loop" pattern from 1.2, you can create these indexes once before the loop too. However, note that most of these inner temp tables are already created via SELECT INTO and dropped at the end of each block — so the refactoring effort is higher. Start with the ones that participate in the most joins.

---

### 3.2 Consolidate the "OrderId-Only" Orphan Deletes

**Affected:** `lsp_DbPurgeHistoryData` — the 5 delete blocks after the OeOrderHistory delete (OeComments, OeOrderPoNumAssoc, OeProblemRxs, OeRequireCorrespondenceRxs, OeStatusTrail)

These 5 tables are keyed by `OrderId` alone (no `HistoryDtTm`). Each one runs the exact same pattern:

```sql
Delete [Target] From #BlkOfRxsToDel R
   Join [Target] T On T.OrderId = R.OrderId
   Left Join OeOrder O On O.OrderId = T.OrderId
   Left Join OeOrderHistory OH On OH.OrderId = T.OrderId
Where O.OrderId Is Null And OH.OrderId Is Null
```

That's 5 separate queries, each doing two LEFT JOINs back to `OeOrder` and `OeOrderHistory`. The `OeStatusTrail` one is even worse — it first materializes the qualifying OrderIds into *another* temp table (`#RxsToDeleteFromStatusTrail`) and then deletes from there.

**Optimization:** Compute the "orphaned OrderIds" list once and reuse it for all 5 deletes:

```sql
-- Compute orphaned OrderIds once
If Object_Id('TempDb..#OrphanedOrderIds') Is Not Null
   Drop Table #OrphanedOrderIds

Select R.OrderId
Into #OrphanedOrderIds
From #BlkOfRxsToDel R
   Left Join OeOrder O On O.OrderId = R.OrderId
   Left Join OeOrderHistory OH On OH.OrderId = R.OrderId
Where O.OrderId Is Null And OH.OrderId Is Null

-- Then use it 5 times with simple inner joins:
Delete OC From #OrphanedOrderIds X Join OeComments OC On OC.OrderId = X.OrderId
Delete OP From #OrphanedOrderIds X Join OeOrderPoNumAssoc OP On OP.OrderId = X.OrderId
Delete PR From #OrphanedOrderIds X Join OeProblemRxs PR On PR.OrderId = X.OrderId
Delete RC From #OrphanedOrderIds X Join OeRequireCorrespondenceRxs RC On RC.RxNumOrOrderId = X.OrderId
Delete ST From #OrphanedOrderIds X Join OeStatusTrail ST On ST.OrderId = X.OrderId

Drop Table #OrphanedOrderIds
```

**Impact:** Eliminates 8 LEFT JOIN operations against `OeOrder` and `OeOrderHistory` per block (from 10 down to 2). Those are large tables — hitting them repeatedly with LEFT JOINs is expensive. Also eliminates the redundant `#RxsToDeleteFromStatusTrail` temp table.

---

### 3.3 Consolidate the #RxPats / #GrpPats → #Pats Chain

**Affected:** `lsp_DbPurgeHistoryData` (lines 1076–1125)

The procedure builds the patient list through a 3-step process:

1. Create `#RxPats` — distinct `PatCustId` from `OeOrderHistory` for the block
2. Create `#GrpPats` — distinct `PatCustId` from `OeGroup` via `OeOrderHistory.GroupNum`
3. Create `#Pats` — `UNION` of #RxPats and #GrpPats

This creates 3 temp tables and runs 3 queries when one would do:

```sql
Select Distinct PatCustId
Into #Pats
From (
   Select OH.PatCustId
   From #BlkOfRxsToDel R
   Join OeOrderHistory OH On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm

   Union

   Select G.PatCustId
   From #BlkOfRxsToDel R
   Join OeOrderHistory OH On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm
   Join OeGroup G On G.GroupNum = OH.GroupNum
) As AllPats
```

**Impact:** Eliminates 2 temp table creations per iteration and reduces the number of joins to `OeOrderHistory` (which is the table you're trying to purge — so it's huge).

---

## Priority 4 — Driver Proc Improvements

### 4.1 Add Batch Timing to `lsp_DbPurgeHistoryData_Driver`

**Affected:** `lsp_DbPurgeHistoryData_Driver`

The driver already prints pass-level results, which is good. But it doesn't print timing per chunk or total elapsed time. On a wide-range purge (e.g., 2 years of data in 7-day chunks), knowing that chunk 1 took 45 minutes but chunk 52 took 3 minutes tells you a lot about data distribution.

**Fix:** Add `@ChunkStart DateTime` before the inner loop, set it to `GetDate()`, and print the elapsed time after each chunk:

```sql
Declare @ChunkStartTime DateTime

-- Inside the While loop, before the inner While:
Set @ChunkStartTime = GetDate()

-- After the inner While completes:
Print '--- Chunk elapsed: ' + Cast(DateDiff(Second, @ChunkStartTime, GetDate()) As VarChar(12)) + 's ---'
```

---

### 4.2 Consider Adding WAITFOR DELAY Between Chunks

**Affected:** `lsp_DbPurgeHistoryData_Driver`

When running a wide-range purge during business hours or on a busy system, the driver hammers the database with continuous delete operations. A configurable delay between chunks lets the system breathe:

```sql
-- New parameter:
, @DelayBetweenChunks VarChar(12) = '00:00:00'  -- hh:mm:ss

-- After each chunk completes:
If @DelayBetweenChunks <> '00:00:00'
   WaitFor Delay @DelayBetweenChunks
```

This is low-effort and can make the difference between a purge that causes application timeouts and one that runs invisibly in the background.

---

## Priority 5 — Minor Fixes

### 5.1 Wrong Comment on AcceptReject WHILE Loop End

**Affected:** `lsp_DbPurgeHistoryData` (line 1601)

```sql
End  -- While Exists (Select * From OeOrderHistory Where ...
```

This comment references `OeOrderHistory`, but this `End` closes the `OeOrderAcceptReject` loop. Cosmetic, but confusing during debugging.

### 5.2 `#CanImgIntIds` Object_Id Check Missing TempDb Prefix

**Affected:** `lsp_DbPurgeHistoryData` (line 686)

```sql
If Object_Id('#CanImgIntIds') Is Not Null   -- Missing 'TempDb..' prefix
```

Every other temp table check in the procedure uses `Object_Id('TempDb..#TableName')`. This one is missing the prefix. It will still work (SQL Server resolves `#name` to the correct temp table), but it's inconsistent and could behave differently in edge cases.

### 5.3 The "Successful" Event Note Is Empty

**Affected:** `lsp_DbPurgeHistoryData` (line 1613)

```sql
Set @EvtNotes = ''
Exec lsp_DbLogSqlEvent 'A', 'PurgeOldData End', 'Successful', @EvtNotes, 'lsp_DbPurgeHistoryData'
```

After doing all that work tracking `@HistoryRowsDeletedLocal` and `@AcceptRejectRowsDeletedLocal`, the "Successful" event logs an empty string. This is clearly an oversight — the row counts should be in the event note.

---

## Summary — Estimated Impact

| # | Fix | Effort | Impact | Iterations Saved |
|---|-----|--------|--------|-----------------|
| 1.1 | `@@ROWCOUNT` loop control | 30 min | High | 1 EXISTS query per iteration × 2 loops |
| 1.2 | Pre-create `#BlkOfRxsToDel` | 45 min | High | ~15 plan recompilations per iteration |
| 1.3 | `SELECT Id` in AcceptReject CTE | 5 min | Low-Med | Reduced I/O per AcceptReject block |
| 2.1 | Add TRY/CATCH | 30 min | Critical (safety) | N/A — prevents silent failures |
| 2.2 | Progress logging + fix empty note | 15 min | Med (operational) | N/A — diagnostics |
| 3.1 | Index inner temp tables | 1 hr | Med-High at scale | Faster joins in cascade deletes |
| 3.2 | Consolidate orphan deletes | 45 min | Med | 8 fewer LEFT JOINs per iteration |
| 3.3 | Consolidate patient list | 20 min | Low-Med | 2 fewer temp tables per iteration |
| 4.1 | Driver timing | 10 min | Low (operational) | N/A — diagnostics |
| 4.2 | Driver delay option | 10 min | Low (operational) | N/A — throttling |

**The top-3 bang-for-buck items are 1.1, 1.2, and 2.1.** If you fix nothing else, fix those three. The `@@ROWCOUNT` change and the temp table reuse together should measurably reduce wall-clock time on every purge run, and the TRY/CATCH will save you hours of debugging the next time something goes wrong at 2 AM.

---

## What This Analysis Does NOT Cover

These are explicitly out of scope here (covered by the partitioning POC and the broader performance assessment):

- Adding indexes to base tables (covered by Module 2)
- Partitioning OeOrderHistory or EvtEvent (covered by Modules 1 & 3)
- Replacing delete loops with `SWITCH PARTITION` (covered by Module 3)
- Refactoring the nightly maintenance parent procedure (covered by the main assessment)
- The SmartShelf and DiagEvents bugs (covered by the main assessment item 1.1–1.3)
