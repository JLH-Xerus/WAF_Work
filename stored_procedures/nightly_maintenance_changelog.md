# Nightly Maintenance Stored Procedures — Change Journal

**Date:** 2026-03-05
**Reviewed by:** Justin Hunter

---
---

# lsp_DbDeleteOldInvAuditEvents — v1 to v2

**Procedure:** dbo.lsp_DbDeleteOldInvAuditEvents
**Affected Table:** EvtInvEventLog

---

## Summary

Performance and observability improvements to the inventory audit event purge procedure. The overall structure and behavior of the procedure are preserved. These changes reduce unnecessary I/O during block deletions, improve the accuracy of row count tracking, and add deletion totals to the event log for diagnostic visibility.

---

## Change 1: Narrowed CTE Select (Line 104)

**Before:**
```sql
Select Top (@NumOfRowsBlockSize) * From EvtInvEventLog Where EventDtTm < @CutoffDtTm Order By Id
```

**After:**
```sql
Select Top (@NumOfRowsBlockSize) Id From EvtInvEventLog Where EventDtTm < @CutoffDtTm Order By Id
```

**Rationale:**
The CTE is used solely to identify rows for deletion. SQL Server only needs the row locator (the `Id` clustering key) to perform the delete — it does not need the values of every column in the table. By selecting `*`, the original query forced the engine to read all columns for every row in each block, even though none of that data was used. Narrowing the select to `Id` allows the query to be satisfied from the clustered index alone, reducing the amount of data read per iteration.

For a procedure that may iterate hundreds of times in a single run (e.g., 25M rows / 100K block size = 250 iterations), this reduction compounds significantly.

---

## Change 2: Accurate Row Count Tracking (Line 111)

**Before:**
```sql
Set @NumOfRowsDeleted = @NumOfRowsDeleted + @NumOfRowsBlockSize
```

**After:**
```sql
Set @NumOfRowsDeleted = @NumOfRowsDeleted + @@ROWCOUNT
```

**Rationale:**
The original code added the configured block size to the running total after each deletion, regardless of how many rows were actually deleted. This made the count approximate — particularly inaccurate on the final iteration where the remaining qualifying rows are typically fewer than a full block.

`@@ROWCOUNT` returns the exact number of rows affected by the immediately preceding statement. Since the `Set` assignment follows the `Delete From Cte` with no intervening statements, `@@ROWCOUNT` reliably captures the true count.

This also makes the `@MaxToDelete` exit condition more precise: the loop will now exit based on actual deletions rather than an inflated estimate.

**Note:** The corresponding comment in the `@MaxToDelete` parameter description ("If this value is not a multiple of @NumOfRowsBlockSize, the effect will be approximate.") was removed since the tracking is now exact.

---

## Change 3: Deletion Count Logged on Success (Line 119)

**Before:**
```sql
Set @EvtNotes = ''
```

**After:**
```sql
Set @EvtNotes = 'Total Number of rows deleted - ' + Cast(@NumOfRowsDeleted As VarChar(20))
```

**Rationale:**
The original procedure logged an empty string in the success event notes, providing no visibility into how much work was performed. The updated version includes the total row count, consistent with the pattern already established in `lsp_DbDeleteOldPmssData`.

This enables the team to monitor deletion volumes over time, validate retention period settings, and diagnose nightly job duration changes by correlating event counts with run times.

---

## Change 4: Deletion Count Logged on Failure (Line 136)

**Before:**
```sql
Set @EvtNotes = Left(@ErrMsg, 255)
```

**After:**
```sql
Set @EvtNotes = Left('Rows deleted before failure - ' + Cast(@NumOfRowsDeleted As VarChar(20)) + '; ' + @ErrMsg, 255)
```

**Rationale:**
When the procedure fails mid-run, it's important to know how far it got before the error. The original code only logged the error message. The updated version prepends the deletion count achieved before the failure, followed by the error message. The `Left(..., 255)` truncation is preserved to respect the `@EvtNotes VarChar(255)` column width.

This is useful for troubleshooting scenarios where a failure occurs after many successful iterations — knowing the proc deleted 2.4M rows before failing tells a very different story than a failure on the first block.

---

## What Was NOT Changed

- **Loop structure:** The `WHILE EXISTS ... AND @NumOfRowsDeleted < @MaxToDelete` pattern is preserved.
- **Block deletion approach:** The CTE-based `Delete From Cte` pattern is preserved. No change to transaction scope.
- **Parameter defaults:** All defaults remain unchanged (36500 days, 100K block size, 25M max).
- **Error handling:** The Try/Catch structure, error logging calls, and return code pattern are all preserved.
- **Logging calls:** The same `lsp_DbLogSqlEvent` and `lsp_DbLogSqlError` procedures are called with the same event codes.
- **Print statements:** All diagnostic print output is preserved as-is.

---

## Testing Recommendations

1. **Functional test:** Run against a test database with a known number of qualifying rows. Verify the logged total matches the actual count of deleted rows.
2. **Final block accuracy:** Seed a test case where qualifying rows are not an even multiple of the block size (e.g., 250,003 rows with a 100K block). Confirm the logged total is exact, not rounded up to 300,000.
3. **Failure scenario:** Force a failure mid-run (e.g., temporarily revoke delete permission after the first block) and verify the failure event notes include the partial deletion count.
4. **Performance comparison:** On a representative dataset, compare execution time of v1 vs v2. The `Select Id` change should show reduced I/O in the query plan (fewer logical reads per iteration).

---
---

# lsp_DbDeleteOldEvents — v2 to v3

**Procedure:** dbo.lsp_DbDeleteOldEvents
**Affected Table:** EvtEvent

---

## Summary

Performance, observability, and code hygiene improvements to the audit event purge procedure. The overall structure and behavior of the procedure are preserved. These changes reduce unnecessary I/O during block deletions, improve the accuracy of row count tracking, add deletion totals to the event log, and clean up dead code left over from earlier versions.

---

## Change 1: Narrowed CTE Select (Line 111)

**Before:**
```sql
Select Top (@NumOfRowsBlockSize) * From EvtEvent Where EventDtTm < @CutoffDtTm Order By TrxId
```

**After:**
```sql
Select Top (@NumOfRowsBlockSize) TrxId From EvtEvent Where EventDtTm < @CutoffDtTm Order By TrxId
```

**Rationale:**
The CTE is used solely to identify rows for deletion. SQL Server only needs the row locator (the `TrxId` key) to perform the delete. Selecting `*` forced the engine to read all columns for every row in each block. Narrowing to `TrxId` allows the query to be satisfied from the index alone, reducing I/O per iteration.

Note: This table uses `TrxId` (BigInt) as its key — not `Id` (Int) — following the schema 17.03.07 migration from `EvtEvent_IntId`.

---

## Change 2: Accurate Row Count Tracking (Line 118)

**Before:**
```sql
Set @NumOfRowsDeleted = @NumOfRowsDeleted + @NumOfRowsBlockSize
```

**After:**
```sql
Set @NumOfRowsDeleted = @NumOfRowsDeleted + @@ROWCOUNT
```

**Rationale:**
The original code added the configured block size regardless of how many rows were actually deleted, making the count approximate. `@@ROWCOUNT` captures the true count from the immediately preceding `DELETE`. This makes the `@MaxToDelete` exit condition more precise and provides accurate totals for logging.

The corresponding comment in the `@MaxToDelete` parameter description ("If this value is not a multiple of @NumOfRowsBlockSize, the effect will be approximate.") was removed since the tracking is now exact.

---

## Change 3: Deletion Count Logged on Success (Line 126)

**Before:**
```sql
Set @EvtNotes = ''
```

**After:**
```sql
Set @EvtNotes = 'Total Number of rows deleted - ' + Cast(@NumOfRowsDeleted As VarChar(20))
```

**Rationale:**
The original logged an empty string on success, providing no visibility into work performed. The updated version includes the total row count, consistent with the pattern in `lsp_DbDeleteOldPmssData` and the updated `lsp_DbDeleteOldInvAuditEvents`.

---

## Change 4: Deletion Count Logged on Failure (Line 143)

**Before:**
```sql
Set @EvtNotes = Left(@ErrMsg, 255)
```

**After:**
```sql
Set @EvtNotes = Left('Rows deleted before failure - ' + Cast(@NumOfRowsDeleted As VarChar(20)) + '; ' + @ErrMsg, 255)
```

**Rationale:**
When the procedure fails mid-run, knowing how far it progressed before the error is essential for troubleshooting. The partial deletion count is prepended to the error message, with the `Left(..., 255)` truncation preserved to respect the column width.

---

## Change 5: Removed Unused Variable Declarations

**Removed:**
```sql
Declare @Rc Int                        -- SQL Command Return Code.  (Saved Value of @@Error.)
Declare @BlkIds Table(Id Int)          -- Block of IDs to be deleted.
Declare @ContentCodes Table(Code Int)  -- List of @ImgsOfContentCodes as a table.
```

**Rationale:**
- `@Rc` was part of the older `@@Error`-based error handling pattern. This procedure was migrated to Try/Catch at some point, but `@Rc` was never removed. It is not referenced anywhere in the procedure body.
- `@BlkIds` and `@ContentCodes` are table variables that match declarations in `lsp_DbDeleteOldWorkflowImages`. They appear to have been carried over when this procedure was scaffolded from that template and are not referenced anywhere.

Table variables incur a small allocation cost at declaration time even when unused. More importantly, their presence is misleading to maintainers who may assume they serve a purpose.

---

## Change 6: Removed Duplicate Print Statement

**Removed (was between the parameter validation and the WHILE loop):**
```sql
Print 'Deleting (EvtEvent) events older than ' + Convert(VarChar(30), @CutoffDtTm, 121) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
```

**Rationale:**
This message was printed identically on line 60 (before `Begin Try`) and again inside the Try block. Unlike `lsp_DbDeleteOldInvAuditEvents` — which uses the outer print for a generic message and the inner print for a table-specific message — this procedure only targets one table, making the second print a pure duplicate. Removing it reduces noise in the job history output.

---

## Change 7: Centralized Procedure Name into @Routine Variable

**Before:**
Procedure name hardcoded as `'lsp_DbDeleteOldEvents'` in four logging calls (lines 66, 114, 124, 131).

**After:**
Single `@Routine` variable set once and used in all logging calls.

**Rationale:**
Consistent with the pattern established in `lsp_DbDeleteOldInvAuditEvents` and `lsp_DbDeleteOldPmssData`. Centralizing the name means only one line needs to change if the procedure is ever renamed, rather than four.

---

## Change 8: Corrected Return Value Documentation

**Before:**
```
Return Value:
   @Rc - 0 = Successful; Non-0 = Failed
```

**After:**
```
Return Value:
   Return Code:
          0 = Successful
      Non-0 = Failed
```

**Rationale:**
The header referenced `@Rc` as the return mechanism, but the actual return statements use `Return 0` and `Return @ErrNum` directly — `@Rc` was never part of the return path. Updated to match the actual behavior and to be consistent with the format used in `lsp_DbDeleteOldInvAuditEvents`.

---

## Noted — NOT Changed: EvtEvent_IntId Purge Gap

The inline comment on line 71-72 references the legacy `EvtEvent_IntId` table (pre-schema 17.03.07 events), but this procedure only deletes from `EvtEvent`. If `EvtEvent_IntId` still contains data in any environment, those rows are not being purged by any procedure in this set.

A NOTE was added to the Description section of the header to flag this for the team. A separate investigation should determine whether `EvtEvent_IntId` still holds data in production and, if so, whether a purge strategy is needed.

---

## What Was NOT Changed

- **Loop structure:** The `WHILE EXISTS ... AND @NumOfRowsDeleted < @MaxToDelete` pattern is preserved.
- **Block deletion approach:** The CTE-based `Delete From Cte` pattern is preserved.
- **Parameter defaults:** All defaults remain unchanged (36500 days, 100K block size, 25M max).
- **Error handling:** The Try/Catch structure, error logging calls, and return code pattern are all preserved.
- **Print statements:** All diagnostic print output is preserved (minus the removed duplicate).
- **EvtEvent_IntId comment:** The inline schema migration comment is preserved as-is.

---

## Testing Recommendations

1. **Functional test:** Run against a test database with a known number of qualifying EvtEvent rows. Verify the logged total matches the actual count of deleted rows.
2. **Final block accuracy:** Seed a test case where qualifying rows are not an even multiple of the block size. Confirm the logged total is exact.
3. **Failure scenario:** Force a failure mid-run and verify the failure event notes include the partial deletion count followed by the error message.
4. **Performance comparison:** Compare execution plans of v2 vs v3. The `Select TrxId` change should show reduced logical reads per iteration.
5. **EvtEvent_IntId check:** Run `Select Count(*) From EvtEvent_IntId` in each environment to determine if the legacy table holds data that needs purging.

---
---

# lsp_DbDeleteOldPmssData — v1 to v2

**Procedure:** dbo.lsp_DbDeleteOldPmssData
**Affected Tables:** PmssCapturedOrderTrxs, PmssCapturedXmlTrxs, CfCollectedCentralFillPmssTrxs, CfCapturedPmssTrxs

---

## Summary

Code hygiene, observability, and minor correctness improvements to the PMSS transaction log purge procedure. This procedure was already ahead of its siblings in some respects (it already used `@@ROWCOUNT` for row count tracking), but had its own set of issues: unnecessary I/O in CTEs, a variable naming typo, incomplete failure logging, and dead code. The overall structure and behavior of the procedure are preserved.

---

## Change 1: Narrowed CTE Select in All Four Deletion Loops (Lines 103, 143, 181, 221)

**Before (all four loops):**
```sql
Select Top (@NumOfRowsBlockSize) * From [TableName] Where TrxDtTm < @CutoffDtTm Order By Id
```

**After (all four loops):**
```sql
Select Top (@NumOfRowsBlockSize) Id From [TableName] Where TrxDtTm < @CutoffDtTm Order By Id
```

**Rationale:**
Same as the CTE narrowing applied to `lsp_DbDeleteOldInvAuditEvents` and `lsp_DbDeleteOldEvents`. The CTE is used solely to identify rows for deletion. SQL Server only needs the row locator (`Id`) to perform the delete. Selecting `*` forced the engine to read all columns for every row in each block, which is wasteful when none of that data is used.

This procedure has four separate deletion loops (one per table), so the fix was applied in four places. The compounding benefit is significant given the default `@MaxToDelete` of 25M rows across all four tables.

---

## Change 2: Fixed Variable Naming Typo (Line 161)

**Before:**
```sql
Declare @NumOfRowsDeleted_PmssCapturedOrderTrxsint Int = 0
```

**After:**
```sql
Declare @NumOfRowsDeleted_PmssCapturedOrderTrxs Int = 0
```

**Rationale:**
The variable name had a stray `int` suffix appended — likely a copy-paste artifact where the type keyword `Int` ran into the variable name. Despite the typo, the procedure compiled and ran correctly because the misspelled name was used consistently throughout the PmssCapturedOrderTrxs loop. However, the inconsistency with the other three counter variables (`@NumOfRowsDeleted_CfCapturedPmssTrxs`, `@NumOfRowsDeleted_CfCollectedCentralFillPmssTrxs`, `@NumOfRowsDeleted_PmssCapturedXmlTrxs`) was confusing and could lead to bugs during future maintenance.

---

## Change 3: Added Error Message to Failure Event Notes (Line 276)

**Before:**
```sql
Set @EvtNotes = Left('Rows deleted before failure - ' + Cast(@NumOfRowsDeleted As VarChar(20)), 255)
```

**After:**
```sql
Set @EvtNotes = Left('Rows deleted before failure - ' + Cast(@NumOfRowsDeleted As VarChar(20)) + '; ' + @ErrMsg, 255)
```

**Rationale:**
The original failure event included the partial row count (which was good — ahead of the other procs), but omitted the actual error message. The updated version appends the error message after the count, consistent with the pattern now established across all four deletion procedures. The `Left(..., 255)` truncation is preserved to respect the column width.

---

## Change 4: Removed Unused @Rc Variable and Simplified Return

**Before:**
```sql
Declare ...
      , @Rc Int                           -- Return code
...
Set @RC = 0
Return @RC
```

**After:**
```sql
Return 0  -- Return Success
```

**Rationale:**
`@Rc` was declared and used only in two lines at the end of the procedure: `Set @RC = 0` followed by `Return @RC`. This is functionally identical to `Return 0` but adds an unnecessary variable declaration and assignment. The variable was not used anywhere else in the procedure body — unlike `lsp_DbDeleteOldWorkflowImages` where `@Rc` is actively used to capture `@@Error` after each operation.

---

## Change 5: Corrected Return Value Documentation

**Before:**
```
Return:
   Return Code:
      @Rc
          0 = Successful
      Non-0 = Failed
```

**After:**
```
Return:
   Return Code:
          0 = Successful
      Non-0 = Failed
```

**Rationale:**
The header referenced `@Rc` as the return mechanism, but with `@Rc` removed the return statements are now `Return 0` and `Return @ErrNum` directly. Updated to match actual behavior and to be consistent with the format used across the other procedures.

---

## Noted — NOT Changed: @configuredRetentionPeriod NULL Edge Case

If the `DaysToKeepEncryptedLogs` parameter definition exists in `CfgSystemParamDef` but has no corresponding value row in `vCfgSystemParamVal`, the `Select @configuredRetentionPeriod = IsNull([Value], 0)` query returns no rows and `@configuredRetentionPeriod` remains `NULL` (its implicit default). The subsequent `If @configuredRetentionPeriod > 0` evaluates `NULL > 0` as UNKNOWN, which causes the IF branch to be skipped entirely — falling through to the ELSE branch that logs "parameter is not configured" and returns success.

This is technically correct behavior (the parameter genuinely isn't configured in that scenario), but it's worth noting that it depends on SQL Server's three-valued logic for NULL comparisons. A NOTE was added to the Comments section of the header to document this for the team.

---

## What Was NOT Changed

- **Loop structure:** All four `WHILE EXISTS ... AND @NumOfRowsDeleted_[Table] < @MaxToDelete` patterns are preserved.
- **Block deletion approach:** The CTE-based `Delete From Cte` pattern is preserved in all four loops.
- **Parameter defaults:** All defaults remain unchanged (100K block size, 25M max).
- **Config-driven retention:** The procedure still reads `DaysToKeepEncryptedLogs` from `vCfgSystemParamVal` — no `@OlderThanXDays` parameter.
- **Row count tracking:** The procedure already used `@@ROWCOUNT` — no change needed here.
- **Error handling:** The Try/Catch structure, error logging calls, and return code pattern are all preserved.
- **Success event logging:** Already included total row count — no change needed.
- **Print statements:** All diagnostic print output is preserved as-is.

---

## Testing Recommendations

1. **Functional test:** Run against a test database with known qualifying rows in each of the four target tables. Verify the logged total matches the sum of actual deletions across all tables.
2. **Variable naming:** Confirm the renamed `@NumOfRowsDeleted_PmssCapturedOrderTrxs` variable compiles and the PmssCapturedOrderTrxs loop count is included in the total.
3. **Failure scenario:** Force a failure mid-run and verify the failure event notes now include both the partial count and the error message.
4. **NULL config edge case:** Test with a `CfgSystemParamDef` row present but no corresponding `vCfgSystemParamVal` row. Confirm the procedure logs "parameter is not configured" and exits successfully.
5. **Performance comparison:** Compare execution plans of v1 vs v2 for each of the four CTE queries. The `Select Id` change should show reduced logical reads per iteration.

---
---

# lsp_DbDeleteOldWorkflowImages — v3 to v4

**Procedure:** dbo.lsp_DbDeleteOldWorkflowImages
**Affected Tables:** ImgImage, ImgImage_IntId, ImgCanImgAssoc, ImgRxImgAssoc

---

## Summary

Bug fix, observability, and code hygiene improvements to the workflow image purge procedure. This procedure is architecturally different from the other deletion procedures — it uses a temp table pattern (#OlderImageIds, #BlkIds) rather than CTEs, and handles parent-child relationships across four tables. The changes fix a data type mismatch that could cause overflow failures, improve row count accuracy, add deletion totals to event logging, and centralize the procedure name. The overall structure and behavior of the procedure are preserved.

---

## Change 1: Fixed #BlkIds Temp Table Column Type (Line 85)

**Before:**
```sql
Create Table #BlkIds (Id Int)
```

**After:**
```sql
Create Table #BlkIds (Id BigInt)
```

**Rationale:**
The `#OlderImageIds` temp table was already defined with `Id BigInt` (line 82), matching the BigInt `Id` columns in `ImgImage` and `ImgImage_IntId`. However, `#BlkIds` — which receives rows copied from `#OlderImageIds` — was defined with `Id Int`. This creates a data type mismatch: if any image `Id` value exceeds 2,147,483,647 (the Int maximum), the `INSERT INTO #BlkIds SELECT TOP (@NumOfRowsBlockSize) Id FROM #OlderImageIds` would fail with an arithmetic overflow error.

This is a latent bug — it only manifests once the `Id` identity column in either `ImgImage` or `ImgImage_IntId` exceeds the Int range. Depending on image volume and history, this could be imminent or far off, but the fix is straightforward and eliminates the risk entirely.

---

## Change 2: Accurate Block Row Count Tracking (Line 191)

**Before:**
```sql
Set @Rc = @@Error
If @Rc <> 0
   Return @Rc
```

**After:**
```sql
Select @BlockRowCount = @@ROWCOUNT, @Rc = @@Error
If @Rc <> 0
   Return @Rc
```

**Rationale:**
The original code captured `@@Error` after the `#BlkIds` insert but discarded the row count. A new `@BlockRowCount` variable (declared at the top) now captures `@@ROWCOUNT` in the same `SELECT` statement as `@@Error`. This is necessary because both `@@ROWCOUNT` and `@@Error` are volatile — they reset after any statement, so they must be captured together in a single `SELECT`.

The `@BlockRowCount` value is then used at the bottom of the loop (line 278) to track the actual number of rows deleted per iteration, replacing the approximate `@NumOfRowsBlockSize` addition.

---

## Change 3: Replaced Approximate Row Count with Actual Block Count (Line 278)

**Before:**
```sql
Set @NumOfRowsDeleted = @NumOfRowsDeleted + @NumOfRowsBlockSize
```

**After:**
```sql
Set @NumOfRowsDeleted = @NumOfRowsDeleted + @BlockRowCount
```

**Rationale:**
Same principle as the `@@ROWCOUNT` fix applied to the other procedures, but implemented differently due to the temp table architecture. In the CTE-based procedures, `@@ROWCOUNT` is captured immediately after the `DELETE`. Here, the block size is determined by how many IDs were inserted into `#BlkIds` from `#OlderImageIds`, so `@@ROWCOUNT` is captured at that point (Change 2) and stored in `@BlockRowCount` for use at the end of the loop.

This is particularly important for the final iteration, where the remaining qualifying images may be fewer than `@NumOfRowsBlockSize`.

---

## Change 4: Added Missing @@Error Check After ImgImage Delete (Lines 238–240)

**Before (between ImgImage delete and ImgImage_IntId delete):**
```sql
-- (no @@Error check)
```

**After:**
```sql
Set @Rc = @@Error
If @Rc <> 0
   Return @Rc
```

**Rationale:**
Every other delete operation in the loop (ImgCanImgAssoc, ImgRxImgAssoc, ImgImage_IntId) had an `@@Error` check immediately following it. The ImgImage delete was the only one missing this check. If the ImgImage delete failed with a low-severity error (which doesn't trigger the Catch block), the procedure would continue to the ImgImage_IntId delete without detecting the failure.

This check was added to maintain consistency with the existing error handling pattern throughout the loop.

---

## Change 5: Deletion Count Logged on Success (Line 285)

**Before:**
```sql
Set @EvtNotes = ''
```

**After:**
```sql
Set @EvtNotes = 'Total Number of rows deleted - ' + Cast(@NumOfRowsDeleted As VarChar(20))
```

**Rationale:**
The original logged an empty string on success, providing no visibility into how much work was performed. The updated version includes the total row count, consistent with the pattern now established across all four deletion procedures.

---

## Change 6: Deletion Count Logged on Failure (Line 305)

**Before:**
```sql
Set @EvtNotes = Left(@ErrMsg, 255)
```

**After:**
```sql
Set @EvtNotes = Left('Rows deleted before failure - ' + Cast(@NumOfRowsDeleted As VarChar(20)) + '; ' + @ErrMsg, 255)
```

**Rationale:**
The original failure event only logged the error message. The updated version prepends the partial deletion count, consistent with all other procedures. This is especially valuable for this procedure given the smaller block sizes (1,000 vs 100,000) and max deletions (50,000 vs 25,000,000), where a failure mid-run may represent a significant portion of the intended work.

---

## Change 7: Centralized Procedure Name into @Routine Variable

**Before:**
Procedure name hardcoded as `'lsp_DbDeleteOldWorkflowImages'` in four logging calls.

**After:**
Single `@Routine VarChar(50)` variable set to `'lsp_DbDeleteOldWorkflowImages'` once and used in all logging calls.

**Rationale:**
Consistent with the pattern established across all other procedures in this set. Centralizing the name means only one line needs to change if the procedure is ever renamed.

---

## Noted — NOT Changed: @@Error/Return Pattern Inside Try Block

The `@@Error`/`Return` checks within the Try block (after individual operations like the `#BlkIds` insert and each delete) can bypass the Catch block and temp table cleanup. If `@@Error` returns a non-zero value from a low-severity error, the `Return @Rc` statement exits the procedure immediately — skipping the `Drop Table` statements at lines 288–289 and any Catch block cleanup.

In practice, the temp tables (#OlderImageIds, #BlkIds) are session-scoped and will be cleaned up when the connection is returned to the pool or the session ends. The procedure also checks for their existence at the top (lines 75–79) and drops them before recreating, so a subsequent run would not fail due to leftover temp tables.

A NOTE was added to the Comments section of the header to document this pattern for the team. A future refactor could consolidate error handling to ensure cleanup on all exit paths, but this was left unchanged to preserve backward compatibility and the existing error handling structure.

---

## What Was NOT Changed

- **Temp table architecture:** The #OlderImageIds / #BlkIds pattern is preserved (not converted to CTEs).
- **Multi-table deletion order:** ImgCanImgAssoc → ImgRxImgAssoc → ImgImage → ImgImage_IntId sequence preserved.
- **Legacy table handling:** Both ImgImage and ImgImage_IntId are purged (unlike lsp_DbDeleteOldEvents which skips EvtEvent_IntId).
- **Parameter defaults:** All defaults remain unchanged (36500 days, 1K block size, 50K max — smaller than other procs due to image BLOB data).
- **@@Error-based error handling:** The @@Error/Return pattern inside the Try block is preserved.
- **Content code filtering:** The @ImgsOfContentCodes parameter and @ContentCodes table variable logic are unchanged.
- **Archive exclusion:** The `ArchivedDtTm Is Null` filter is preserved, ensuring archived images are not deleted.
- **Print statements:** All diagnostic print output is preserved as-is.
- **NoCount toggling:** The Set NoCount On/Off pattern is preserved.

---

## Testing Recommendations

1. **Functional test:** Run against a test database with known qualifying images. Verify the logged total matches the actual count of deleted rows.
2. **BigInt overflow test:** If feasible, seed test data with image `Id` values exceeding 2,147,483,647. Confirm the procedure handles these without arithmetic overflow errors (validates the #BlkIds type fix).
3. **Final block accuracy:** Seed a test case where qualifying images are not an even multiple of the block size (e.g., 2,500 images with a 1K block). Confirm the logged total is 2,500, not 3,000.
4. **Missing @@Error check validation:** Force a low-severity error on the ImgImage delete (e.g., constraint violation) and confirm the new @@Error check catches it and returns a non-zero code.
5. **Failure scenario:** Force a failure mid-run and verify the failure event notes include the partial deletion count followed by the error message.
6. **Content code filtering:** Test with a subset of content codes (e.g., '1,2,3') and verify only images of those types are deleted.
7. **Archive exclusion:** Confirm that images with a non-null `ArchivedDtTm` are excluded from deletion regardless of their `CapturedDtTm`.

---
---

# lsp_SrtGetShipToteIfExists — v26 to v27

**Procedure:** dbo.lsp_SrtGetShipToteIfExists
**Affected Tables (read-only):** SrtShipTote, SrtShipToteShipmentAssoc, OeOrderShipmentAssoc, OeOrder, OeOrderHistory, ShpManifest, ShpCarrier, SrtSorterLoc

---

## Summary

Performance refactoring of the shipping tote lookup procedure used by the Package Sorter Status display. This procedure is called in real time as operators scan totes, so redundant queries have a direct impact on UI responsiveness. The changes eliminate a dead three-table join query, reduce a double-seek on the barcode lookup to a single seek, replace a UNION with short-circuiting OR EXISTS logic, eliminate a correlated subquery in the ShpManifest fallback path, and add a missing NoLock hint for consistency. The overall structure, result set shape, and client contract are preserved.

---

## Change 1: Collapsed Double-Query Barcode Lookup (Lines 39–44)

**Before:**
```sql
If Exists
    (
        Select Id From SrtShipTote With (NoLock)
        Where StaticToteBarcode = @ShipToteId
    )
    Set @ShipToteIdentifier = (
        Select Id From SrtShipTote With (NoLock)
        Where StaticToteBarcode = @ShipToteId)
Else
    Set @ShipToteIdentifier = @ShipToteId
```

**After:**
```sql
Select @ShipToteIdentifier = Id From SrtShipTote With (NoLock)
Where StaticToteBarcode = @ShipToteId

If @ShipToteIdentifier Is Null
    Set @ShipToteIdentifier = @ShipToteId
```

**Rationale:**
The original code performed two identical index seeks against `SrtShipTote.StaticToteBarcode` — one for the EXISTS check and one to retrieve the `Id`. The refactored version performs a single seek and uses the NULL-ness of the result to determine whether the barcode was found. If no row matches, the SELECT returns no rows and `@ShipToteIdentifier` remains NULL (its implicit default), which triggers the fallback to `@ShipToteId`.

This halves the I/O for the barcode resolution step.

---

## Change 2: Replaced UNION with OR EXISTS for Exception-Package Check (Lines 49–83)

**Before:**
```sql
If Exists
    (
        Select *
        From ... OeOrder ... Where O.OrderStatus = 'Problem'

        Union

        Select *
        From ... OeOrderHistory ... Where O.OrderStatus = 'Canceled'
    )
```

**After:**
```sql
If Exists
    (
        Select 1
        From ... OeOrder ... Where O.OrderStatus = 'Problem'
    )
    Or Exists
    (
        Select 1
        From ... OeOrderHistory ... Where O.OrderStatus = 'Canceled'
    )
```

**Rationale:**
Two improvements here:

1. **UNION → OR EXISTS:** The original UNION required SQL Server to evaluate both halves and then deduplicate the combined result set before EXISTS could test for rows. With OR EXISTS, the engine short-circuits: if the first EXISTS finds a 'Problem' Rx, it never evaluates the second query against OeOrderHistory. In the common case (totes without exceptions), both checks will be fast. But in the worst case, the UNION approach paid the cost of both full query executions plus a sort/distinct, while OR EXISTS stops at the first match.

2. **Select \* → Select 1:** Inside EXISTS, SQL Server ignores the select list entirely — it only cares whether at least one row qualifies. However, `Select *` across a three-table join is misleading to maintainers who may read it as materializing all columns. `Select 1` makes the intent explicit.

---

## Change 3: Removed Unused @ServiceCode Variable and Dead Query (formerly Lines 85–102)

**Removed:**
```sql
Declare @ServiceCode VarChar(50)
```
and:
```sql
-------------------------------------------------------------------
-- Retrieve the Service Code associated with packages in this tote.
-------------------------------------------------------------------
Select
    Top 1
        @ServiceCode = S.ServiceCode
From
    SrtShipTote T With (NoLock)
        Join
    SrtShipToteShipmentAssoc TS With (NoLock)
        On TS.ShipToteId = T.Id
        Join
    ShpShipment S With (NoLock)
            On S.Id = TS.ShipmentId
Where
    T.Id = @ShipToteIdentifier
    And
    T.CarrierCode Is Not Null
```

**Rationale:**
The `@ServiceCode` variable was populated by a three-table join (SrtShipTote → SrtShipToteShipmentAssoc → ShpShipment) but was never referenced in either output path. The SrtShipTote result set returns `IsNull(T.ServiceCode, '')` (the table column, not the variable), and the ShpManifest fallback returns `IsNull(SM.ServiceCode, '')`. The variable and its query were dead code — every call paid for a three-table join whose result was discarded.

This also eliminates a secondary issue: the `TOP 1` without an `ORDER BY` was nondeterministic, but since the result was never used, the nondeterminism was harmless. With the query removed, the issue is moot.

---

## Change 4: Replaced Correlated Subquery in ShpManifest Fallback (Line 135)

**Before:**
```sql
(Select Count(ShipToteId) From ShpManifest With (NoLock) Where ShipToteId = @ShipToteIdentifier) As NumOfPkgs,
```

**After:**
```sql
Count(*) Over() As NumOfPkgs,
```

**Rationale:**
The original correlated subquery re-scanned `ShpManifest` with the same `ShipToteId` predicate that the outer query was already filtering on. This meant the engine performed two separate seeks/scans against ShpManifest for the same tote — one for the outer query's rows and one for the count.

`Count(*) Over()` computes the count from the rows already in the outer query's result set. Since the outer query already filters `WHERE SM.ShipToteId = @ShipToteIdentifier`, the window function produces the same count without an additional table access. The join to ShpCarrier is a LEFT JOIN, so it cannot inflate the row count — `Count(*) Over()` equals the number of ShpManifest rows for the tote.

---

## Change 5: Added Missing NoLock Hint on Final Query (Line 153)

**Before:**
```sql
From
    SrtShipToteShipmentAssoc
Where
```

**After:**
```sql
From
    SrtShipToteShipmentAssoc With (NoLock)
Where
```

**Rationale:**
Every other table access in the procedure uses `With (NoLock)`, consistent with the procedure's role as a real-time UI display that tolerates dirty reads. The final query against `SrtShipToteShipmentAssoc` was the only one missing the hint — likely an oversight. Without the hint, this query acquires shared locks, which could cause blocking if the tote's shipment associations are being modified concurrently (e.g., during active sorting).

---

## What Was NOT Changed

- **Result set shape:** Both output paths (SrtShipTote and ShpManifest) return the same column set in the same order. The second result set (ShipmentId list) is also unchanged.
- **EXISTS + main query pattern for tote details (lines 91–127):** The EXISTS check against SrtShipTote is technically a redundant PK seek (the main SELECT immediately re-seeks the same PK). However, restructuring this to eliminate the double-touch would change the result set contract — the client would receive an empty result set in the "not found" path before receiving the ShpManifest fallback. This was left unchanged to preserve the client contract.
- **Commented-out join block (lines 96–101):** The commented-out Left Joins to ShpCarrier and SrtSorterLoc inside the EXISTS check were preserved. These appear to be development notes showing the EXISTS was simplified from the full query shape.
- **NoCount On/Off toggling:** `Set NoCount On` at the top suppresses rowcounts for intermediate queries; `Set NoCount Off` before the final output ensures the client receives rowcount metadata. This pattern is preserved.
- **`--/` and `/` batch delimiters:** These appear to be deployment tool markers. Preserved as-is.
- **All inline comments:** Preserved, including the original barcode comment with its truncated "fiel" typo.

---

## Testing Recommendations

1. **ABC barcode path:** Scan a tote using a StaticToteBarcode value. Confirm the same tote data is returned as v26.
2. **Direct ID path:** Scan a tote using its direct `Id`. Confirm the same tote data is returned as v26.
3. **Exception detection — Problem status:** Create a tote with a 'Problem' status active Rx. Confirm `HasExceptionPackages = 1`.
4. **Exception detection — Canceled status:** Create a tote with a 'Canceled' status history Rx. Confirm `HasExceptionPackages = 1`.
5. **No exceptions:** Confirm a tote with no Problem/Canceled Rxs returns `HasExceptionPackages = 0`.
6. **ShpManifest fallback:** Query a tote that exists in ShpManifest but not SrtShipTote. Confirm NumOfPkgs matches the actual count of manifest records for that tote. Compare v26 vs v27 output row-for-row.
7. **ServiceCode output:** Verify the ServiceCode column still returns the correct value from `T.ServiceCode` (SrtShipTote path) and `SM.ServiceCode` (ShpManifest path). The removed `@ServiceCode` variable was dead code and should not affect output.
8. **Performance comparison:** Compare execution plans of v26 vs v27 for the same tote ID. The v27 plan should show fewer index seeks (barcode lookup reduced from 2 to 1, service code 3-table join eliminated, ShpManifest correlated subquery eliminated).

---
---

# lsp_RxfGetListOfManualFillGroups — v48 to v49

**Procedure:** dbo.lsp_RxfGetListOfManualFillGroups
**Affected Tables (read-only):** OeOrder, OeGroup, OePriority, Pharmacy, SysEndPharmacy, CfStoreDeliveryCourierCutOff, GovRx, BfBatchFillRxAssoc, OrderToteAssoc, OeWorkflowStepInProgress, OeGroupCvyFillData, CvyToteAtManFillStation, InvStationNdcAssoc, vInvItemDistWithUserOverrides, vInvMasterWithUserOverrides, InvFlaggedProducts, ShpCarrier

---

## Summary

Performance refactoring of the manual fill group queue procedure used by the workstation UI. This procedure is called by operators queuing groups for manual filling, so query latency directly impacts workflow throughput. The headline change replaces nine correlated subqueries in the delivery day WHERE clause with a single CTE that pre-computes the next delivery day per pharmacy in one scan. Additional changes fix a wrap-around bug in delivery day logic, convert a table variable to a temp table for optimizer cardinality visibility, mitigate parameter sniffing, and clean up minor issues. The overall structure, result set shape, and client contract are preserved.

---

## Change 1: Replaced Correlated Subqueries with NextDeliveryDay CTE (Lines 238–260, formerly 298–305)

**Before (lines 298–305):**
```sql
((
  (Select Count(Id) From CfStoreDeliveryCourierCutOff With (NoLock) where SysEndPharmacyId = (Select Id from SysEndPharmacy Where StoreNum = O.StoreNum And IsNull(PharmacyId,0) = IsNull(O.PharmacyId,0))) > 0
  And
  CF.WeekDayCode =
     Case When (Select Count(Id) From CfStoreDeliveryCourierCutOff With (NoLock) Where SysEndPharmacyId = (Select Id from SysEndPharmacy With (NoLock) Where StoreNum = O.StoreNum And IsNull(PharmacyId,0) = IsNull(O.PharmacyId,0)) and WeekDayCode >= DATEPART(WEEKDAY, GETDATE())) >= 1
          Then (Select Top 1 WeekDayCode From CfStoreDeliveryCourierCutOff With (NoLock) Where SysEndPharmacyId = (Select Id from SysEndPharmacy With (NoLock) Where StoreNum = O.StoreNum And IsNull(PharmacyId,0) = IsNull(O.PharmacyId,0)) and WeekDayCode >= DATEPART(WEEKDAY, GETDATE()) Order By WeekDayCode Asc)
          Else (Select Top 1 WeekDayCode From CfStoreDeliveryCourierCutOff With (NoLock) Order By WeekDayCode Asc) End)
 Or (Select Count(Id) From CfStoreDeliveryCourierCutOff With (NoLock) where SysEndPharmacyId = (Select Id from SysEndPharmacy With (NoLock) Where StoreNum = O.StoreNum And IsNull(PharmacyId,0) = IsNull(O.PharmacyId,0))) <= 0)
And ...
```
Plus the unfiltered LEFT JOIN:
```sql
Left Join CfStoreDeliveryCourierCutOff CF With (NoLock)
   On EP.Id = CF.SysEndPharmacyId
```

**After:**
```sql
;With NextDeliveryDay As
(
    Select
        SysEndPharmacyId,
        Min(Case When WeekDayCode >= DatePart(Weekday, GetDate()) Then WeekDayCode End) As ThisWeekDay,
        Min(WeekDayCode) As FirstWeekDay
    From
        CfStoreDeliveryCourierCutOff With (NoLock)
    Group By
        SysEndPharmacyId
)
...
   Left Join NextDeliveryDay NDD
      On NDD.SysEndPharmacyId = EP.Id
   Left Join CfStoreDeliveryCourierCutOff CF With (NoLock)
      On CF.SysEndPharmacyId = EP.Id
         And
      CF.WeekDayCode = Coalesce(NDD.ThisWeekDay, NDD.FirstWeekDay)
```
The entire lines 298–305 WHERE block is removed.

**Rationale:**
The original WHERE clause contained nine correlated subqueries evaluated per row:

- `(Select Id From SysEndPharmacy Where StoreNum = O.StoreNum And IsNull(PharmacyId,0) = IsNull(O.PharmacyId,0))` — four times, despite the `SysEndPharmacy` lookup already being available as the `EP` alias from the LEFT JOIN on lines 278–281.
- `(Select Count(Id) From CfStoreDeliveryCourierCutOff Where SysEndPharmacyId = ...)` — three times for the same existence check.
- `(Select Top 1 WeekDayCode From CfStoreDeliveryCourierCutOff Where ... Order By WeekDayCode Asc)` — twice for the delivery day resolution.

The business logic is: "Only show groups for the next delivery courier cutoff day; if the store has no cutoff configuration, show everything." This is a set-based problem (one answer per pharmacy) being solved in a row-by-row fashion.

The `NextDeliveryDay` CTE computes the answer once per `SysEndPharmacyId` in a single scan of `CfStoreDeliveryCourierCutOff`: `ThisWeekDay` is the earliest cutoff day on or after today (NULL if past the last configured day of the week), and `FirstWeekDay` is the earliest day overall (the wrap-around fallback). `Coalesce(ThisWeekDay, FirstWeekDay)` gives the correct next delivery day.

The CF LEFT JOIN now includes the `WeekDayCode` predicate, so it only joins the correct day's rows — eliminating the fan-out across all weekdays that the old WHERE clause had to filter away. For pharmacies with no cutoff configuration, `NDD` is NULL, `Coalesce` evaluates to NULL, and the CF join finds no match — the row is included with NULL delivery columns, preserving the original "show everything" behavior.

This also eliminates the unfiltered CF fan-out. In v48, a pharmacy with 5 configured delivery days would produce 5 rows per order in the join, which the WHERE clause filtered back to 1. In v49, the join produces at most 1 row per order per pharmacy (or multiple only if there are distinct DeliveryHub/DeliveryRoute combinations on the same day, which is intentional).

---

## Change 2: Fixed Wrap-Around Bug in Delivery Day Fallback (implicit in Change 1)

**Before (line 304):**
```sql
Else (Select Top 1 WeekDayCode From CfStoreDeliveryCourierCutOff With (NoLock) Order By WeekDayCode Asc)
```

**After (implicit in CTE):**
```sql
Min(WeekDayCode) As FirstWeekDay
-- grouped by SysEndPharmacyId
```

**Rationale:**
The wrap-around fallback in v48 picked the globally minimum `WeekDayCode` across **all** pharmacies — it had no `SysEndPharmacyId` filter. If pharmacy A delivers Monday/Wednesday and pharmacy B delivers Tuesday/Thursday, and today is Saturday (past all days), the fallback picked the global minimum (Monday) for both pharmacies. Pharmacy B should wrap to Tuesday, not Monday.

The CTE groups by `SysEndPharmacyId`, so each pharmacy's wrap-around is scoped to its own configured delivery days.

---

## Change 3: Converted @ToteGroups Table Variable to #ToteGroups Temp Table

**Before:**
```sql
Declare @ToteGroups Table (ToteId varchar(24), GroupNum varchar(10), ToteGroupNum tinyint)
```

**After:**
```sql
Create Table #ToteGroups (ToteId varchar(24), GroupNum varchar(10), ToteGroupNum tinyint)
...
Create Index IX_ToteGroups_GroupNum On #ToteGroups(GroupNum)
```

**Rationale:**
Table variables in SQL Server do not have statistics. The optimizer always estimates 1 row regardless of actual cardinality. This affects the join to `#ToteGroups` (via `TG.GroupNum`) and the `EXISTS (Select 1 From #ToteGroups)` checks in the WHERE clause — both are planned as if the table has a single row.

Converting to a temp table gives the optimizer real cardinality estimates via auto-created statistics, allowing it to choose appropriate join strategies (hash vs nested loop) based on actual data volume. The index on `GroupNum` supports the LEFT JOIN predicate.

The temp table is added to both `DROP TABLE IF EXISTS` blocks (top and bottom) for defensive cleanup.

---

## Change 4: Parameter Sniffing Mitigation via Local Variable Assignment

**Added after normalization block:**
```sql
Declare @LocalStationId VarChar(8)
Declare @LocalFillType varchar(1)
Declare @LocalToteId VarChar(24)
Declare @LocalBatchFillId Int

...

Set @LocalStationId = @StationId
Set @LocalFillType = @FillType
Set @LocalToteId = @ToteId
Set @LocalBatchFillId = @BatchFillId
```

**Rationale:**
SQL Server sniffs parameter values at compile time and builds a plan optimized for those specific values. If data distribution is skewed across different parameter values, the plan compiled for one value can perform poorly for another — and the plan is cached and reused until eviction.

The primary concern is `@FillType` filtering `OeOrder` (the main data source) and `@StationId` in the NDC exclusion/inclusion queries. By assigning the normalized parameter values to local variables and using those in all queries, the optimizer uses density-based estimates (average rows per distinct value) instead of sniffed values. This produces plans that are consistent across all callers rather than optimized for the first caller after a recompile.

The assignment is performed **after** the normalization block so the local variables capture the final effective values (e.g., `@FillType` defaulted to 'M' if NULL).

All references to `@StationId`, `@FillType`, `@ToteId`, and `@BatchFillId` in subsequent queries have been updated to use the `@Local` variants.

---

## Change 5: Added Missing NoLock Hint on InvStationNdcAssoc (Inclusion List Query)

**Before:**
```sql
From
   InvStationNdcAssoc
Where
```

**After:**
```sql
From
   InvStationNdcAssoc With (NoLock)
Where
```

**Rationale:**
The inclusion list query (when `@FilteredLocationsOnly = 1`) was the only base table access in the procedure without a `NoLock` hint. The same table on line 128 (in the exclusion list query) uses `With (NoLock)`. This was likely an oversight — the procedure is a UI-driven read-only query where dirty reads are acceptable.

---

## Change 6: Removed Duplicate PriCodeSys in GROUP BY

**Before:**
```sql
Group By
   O.PriCodeSys, O.PriFillByDtTm, O.PriDesc, O.GroupingCriteriaVal, O.NumOfRxs,
   CF.DeliveryHub, CF.DeliveryRoute, O.GroupNum, GR.ToteGroupNum, O.PriCodeSys,
   OG.StartingTotePopWorkflowStepCode
```

**After:**
```sql
Group By
   O.PriCodeSys, O.PriFillByDtTm, O.PriDesc, O.GroupingCriteriaVal, O.NumOfRxs,
   CF.DeliveryHub, CF.DeliveryRoute, O.GroupNum, GR.ToteGroupNum,
   OG.StartingTotePopWorkflowStepCode
```

**Rationale:**
`O.PriCodeSys` appeared at both the beginning and end of the GROUP BY list — a copy-paste artifact. SQL Server handles the duplicate harmlessly, but it's misleading to maintainers.

---

## Noted — NOT Changed: IsNull Join Predicate on SysEndPharmacy

The join `IsNull(O.PharmacyId,0) = IsNull(EP.PharmacyId,0)` wraps both columns in `IsNull()`, which prevents index usage (non-SARGable). However, this pattern treats `NULL = NULL` as a match via the intermediate value `0 = 0`, and also matches `NULL = 0`. Changing to `(O.PharmacyId = EP.PharmacyId Or (O.PharmacyId Is Null And EP.PharmacyId Is Null))` would be SARGable but has different semantics — it would not match `NULL` to `0`. Since the original behavior is likely intentional (pharmacies with no ID matching the default), this was left unchanged to preserve backward compatibility.

---

## Noted — NOT Changed: ORDER BY on INSERT INTO #ResultTable

The `ORDER BY MinPriInternal` on the SELECT INTO #ResultTable was initially flagged as unnecessary (since SQL Server doesn't guarantee storage order in temp tables). However, it is **required** because it interacts with `TOP (@RetrieveTopXGroups)` to determine **which** groups are selected. Without it, the TOP would select arbitrary groups. The ORDER BY defines the selection criteria (lowest MinPriInternal first), even though it doesn't govern storage order. This was left unchanged.

---

## Change 7: Inlined Scalar UDF fOrdGroupingCritValForDisplay into Set-Based CTE

**Before:**
```sql
, GroupingCriteria As (
    Select G.GroupNum, X.FmtdGroupingCriteriaVal
    From DistinctGroups G
    Outer Apply (
        Select dbo.fOrdGroupingCritValForDisplay(G.GroupNum) As FmtdGroupingCriteriaVal
    ) As X)
```

**After:**
```sql
, GroupingCriteria As (
    Select
          DG.GroupNum
        , G.GroupingCriteriaVal
              + IsNull('   (' + S.SplitSuffix + ')', '')
              As FmtdGroupingCriteriaVal
    From DistinctGroups DG
    Join OeGroup G With (NoLock) On G.GroupNum = DG.GroupNum
    Left Join (
        Select
            GSA.GroupNum,
            String_Agg(SC.Descrip, '; ') As SplitSuffix
        From OeGroupSplitCodeAssoc GSA With (NoLock)
        Join OeGroupSplitCode SC With (NoLock) On SC.Code = GSA.SplitCode
        Group By GSA.GroupNum
    ) S On S.GroupNum = DG.GroupNum)
```

**Rationale:**
The scalar UDF `dbo.fOrdGroupingCritValForDisplay` had three compounding performance problems:

1. **Row-by-row execution.** Each call entered the function context, executed two internal queries (one seek into `OeGroup`, one EXISTS + join into `OeGroupSplitCodeAssoc` / `OeGroupSplitCode`), and returned. No batching, no set-based optimization across calls.

2. **Parallelism barrier.** Any query referencing a scalar UDF in SQL Server (pre-2019 without fgci inlining) is forced into a serial plan. The entire final enrichment query — including the InventoryFlags and ProductFlags CTEs — ran single-threaded because of this one function call.

3. **Hidden I/O.** The internal reads from `OeGroup`, `OeGroupSplitCodeAssoc`, and `OeGroupSplitCode` did not appear in the calling query's execution plan. Logical reads from the scalar UDF were invisible to plan analysis, making the proc's true I/O cost difficult to diagnose.

The inlined version replaces the function with a set-based LEFT JOIN subquery. `OeGroup` is joined once to `DistinctGroups` (one seek per group, but set-based — the optimizer can use hash or merge joins). The split code aggregation uses `STRING_AGG` (SQL Server 2017+), which replaces the `@Suffix = Coalesce(@Suffix + '; ', '') + S.Descrip` cursor-style concatenation pattern from the scalar UDF. The `IsNull('   (' + S.SplitSuffix + ')', '')` preserves the original formatting: groups with no split codes get just the `GroupingCriteriaVal`, while split groups get the value appended with `   (reason1; reason2)`.

The OUTER APPLY in v48 already mitigated the row-by-row cost by computing once per distinct `GroupNum` rather than per result row. But it still invoked the scalar UDF once per group, and the parallelism barrier applied to the entire query regardless of how few times the function was called. The inlined version removes both the per-call overhead and the parallelism barrier.

**Note:** The scalar UDF `dbo.fOrdGroupingCritValForDisplay` is not dropped — it may be referenced by other callers. Those callers can be migrated independently if needed.

---

## Noted — NOT Changed: fStringToTableByDelimiter

The `dbo.fStringToTableByDelimiter` function splits the comma-delimited `@HubNames` into the `#Hubs` temp table. If this is a multi-statement table-valued function (common in older codebases), it has the same 1-row cardinality estimate problem as table variables. SQL Server 2016+ provides `STRING_SPLIT` as a built-in with proper cardinality estimates. This was left unchanged pending confirmation of SQL Server version compatibility and the function's implementation type.

---

## What Was NOT Changed

- **Result set shape:** Both the group selection output and the enrichment output return the same columns in the same order.
- **NDC exclusion/inclusion logic:** The EXCEPT-based exclusion and JOIN-based inclusion patterns are preserved.
- **ENABLE_PARALLEL_PLAN_PREFERENCE hint:** Preserved on the exclusion list query.
- **OeData temp table pipeline:** The #OeData → #OeDataUserLocs flow is preserved, including the `Select *` / `Select O.*` inserts and the ByOrderId index creation.
- **Final enrichment query:** The CTE-based enrichment with InventoryFlags and ProductFlags is preserved. GroupingCriteria CTE is restructured (Change 7) but produces the same output.
- **Sort logic:** The multi-column CASE-based ORDER BY in the final SELECT is preserved.
- **`--/` and `/` batch delimiters:** Preserved as-is.
- **All inline comments:** Preserved.

---

## Testing Recommendations

1. **Functional equivalence:** Run v48 and v49 side-by-side with identical parameters. Compare result sets row-for-row. The output should be identical except for the wrap-around bug fix (Change 2), which only manifests when today's weekday is past the last configured delivery day for a pharmacy AND different pharmacies have different first-of-week delivery days.
2. **Delivery day filtering — same week:** Test with a pharmacy that has cutoff days including today's weekday. Confirm groups are filtered to the correct delivery day.
3. **Delivery day filtering — wrap-around:** Test on a day past the pharmacy's last configured delivery day. Confirm groups are filtered to that pharmacy's earliest day (not the global earliest).
4. **No cutoff configuration:** Test with a store/pharmacy that has no rows in CfStoreDeliveryCourierCutOff. Confirm all groups for that store are returned (no delivery day filtering).
5. **Hub group filtering:** Test with `@UseHubGroupFilter = 1`. Confirm only groups matching the specified hubs are returned.
6. **Tote assignment — with tote:** Scan a tote that has assigned groups. Confirm only those groups are returned.
7. **Tote assignment — without tote:** Test with no tote at the station. Confirm unassigned groups are returned.
8. **Parameter sniffing:** Execute the proc with different `@FillType` values in sequence without clearing the plan cache. Compare execution plans — v49 should show consistent plans regardless of first-caller value.
9. **#ToteGroups cardinality:** Check the execution plan for the main query's join to #ToteGroups. The estimated rows should reflect actual data, not the fixed 1-row estimate that @ToteGroups produced in v48.
10. **Performance comparison:** Compare execution plans of v48 vs v49 for the same parameters. The v49 plan should show elimination of nested loop subquery operators from the delivery day logic, replaced by a single hash or merge join to the NDD CTE result.
11. **GroupingCriteria output:** Compare `FmtdGroupingCriteriaVal` column values between v48 and v49 for groups with and without split codes. Format should be identical: plain `GroupingCriteriaVal` for unsplit groups, `GroupingCriteriaVal   (reason1; reason2)` for split groups.
12. **Parallelism check:** Examine the v49 execution plan for the final enrichment query. With the scalar UDF removed, the plan should now be eligible for parallelism (look for Parallelism/Gather Streams operators that were absent in v48).
