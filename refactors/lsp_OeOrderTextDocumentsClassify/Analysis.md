# lsp_OeOrderTextDocumentsClassify: Refactor Analysis (v1 to v2)

**Date:** 2026-05-08
**Tracking sheet row:** 38 (Priority P2, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v2 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_OeOrderTextDocumentsClassify`

**Purpose in one line:** Classify OeOrderTextDocument rows (from the secondary node via linked server) into the local OeOrderTextDocumentClassification table as Leaflet or Non-Leaflet based on `DataLength(DocData) > 153,600` bytes, in batches of `@BatchSize` rows up to `@MaxRecords` total per call.

**Tables touched:**

- `[PWDAZNSYMPH02].PHARMASSIST.dbo.OeOrderTextDocument` (linked-server source for the classification).
- `PharmAssist.dbo.OeOrderTextDocumentClassification` (local destination for the classification).

**Indexes used (predicted from the v2 body):**

| Table | Index | Where used |
|---|---|---|
| `[remote].OeOrderTextDocument` | NC on `HistoryDtTm, Id` (assumed) | linked-server batch scan |
| `OeOrderTextDocumentClassification` | clustered on `Id` (assumed) | local Max(Id) initial read |

**Callers:** the nightly classification job. Called as part of the maintenance window.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on two of four expense lists (Duration #17, Expense #25) with "algorithmic / plan rewrite candidate" as the dominant heuristic.

The dominant cost drivers in v1 are four.

The first is the WHILE EXISTS check against the linked-server table on every iteration. Each WHILE EXISTS check is a remote-server round-trip executed before the batch SELECT (which is also remote). The same answer is available from the previous iteration's `@@ROWCOUNT` without the separate query.

The second is the Max(Id) re-read against the local classification table on every iteration (line 75 in v1). The procedure does an initial read at the top (line 28), then re-reads the max on every iteration. The iteration-local update could track @LastProcessedId in-memory.

The third is the two stray debug result sets: `SELECT @LastProcessedId` on line 30 and `SELECT @TotalProcessed` on line 73. Both return single-value result sets to the caller per iteration. The maintenance orchestrator does not consume these; they are debug artifacts.

The fourth is the awkward loop control. The @MaxRecords exit is checked inside the loop body and produces a `BREAK`, while the WHILE EXISTS check sits at the top. Restructuring to put @MaxRecords in the loop condition makes the flow cleaner and removes the BREAK.

---

## 3. Evidence of Original (v1)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v1)

```
(paste STATS IO / STATS TIME for v1 against the same data state used for v2 here)
```

The expected v1 signature is N+1 linked-server round-trips (one WHILE check per iteration plus one batch SELECT per iteration), where N is the iteration count.

---

## 4. Issue Identification

**Issue 1: Linked-server WHILE EXISTS check (lines 36-41 in v1).** Eliminated via `@@ROWCOUNT`-based control.

**Issue 2: Local Max(Id) re-read on every iteration (line 75 in v1).** Track @LastProcessedId in-memory.

**Issue 3: Stray debug result sets (`SELECT @LastProcessedId` line 30, `SELECT @TotalProcessed` line 73).** Remove.

**Issue 4: Awkward loop control with BREAK inside the body.** Restructure into the loop condition.

**Issue 5: No local-variable indirection.** Program convention.

---

## 5. First Principles

**`@@ROWCOUNT`-based loop control replaces WHILE EXISTS.** The same principle that applies to local-table delete loops applies to linked-server INSERT loops. The previous iteration's @@ROWCOUNT carries the "is there more work" signal without a separate query.

**Track derived state in-memory rather than re-reading it.** The Max(Id) of the classification table at the end of each batch is exactly the Max(Id) the current iteration's INSERT just wrote. The `Output Inserted.Id Into #BatchMax` clause captures this without a re-read of the destination table.

**Clean loop condition over flag-and-break.** The loop condition `@ContinueLoop = 1 And @TotalProcessed < @LocalMaxRecords` expresses the exit criteria directly. The BREAK pattern in v1 obscures the control flow.

---

## 6. Refactor (commented)

The v2 body is reproduced in `Refactored.sql`. Salient blocks below.

**Local variables, initial @LastProcessedId, log begin.**

```sql
Declare @LocalBatchSize  Int = @BatchSize
Declare @LocalMaxRecords Int = @MaxRecords
Declare @LastProcessedId Int

Select @LastProcessedId = IsNull(Max(Id), 0)
From PharmAssist.dbo.OeOrderTextDocumentClassification;
```

**Loop driven by @@ROWCOUNT, with in-memory @LastProcessedId tracking.**

```sql
Drop Table If Exists #BatchMax
Create Table #BatchMax (BatchMaxId Int)

While @ContinueLoop = 1 And @TotalProcessed < @LocalMaxRecords
Begin
    ;With Batch As
    (
        Select Top (@LocalBatchSize) Id, OrderId, HistoryDtTm, DataLength(DocData) As DocDataLength
        From [PWDAZNSYMPH02].PHARMASSIST.dbo.OeOrderTextDocument With (NoLock)
        Where HistoryDtTm < @CutoffDate And Id > @LastProcessedId
        Order By Id Asc
    )
    Insert Into PharmAssist.dbo.OeOrderTextDocumentClassification (...)
    Output Inserted.Id Into #BatchMax (BatchMaxId)
    Select ... From Batch;

    Set @InsertedThisPass = @@ROWCOUNT
    Set @TotalProcessed   = @TotalProcessed + @InsertedThisPass

    If @InsertedThisPass = 0
        Set @ContinueLoop = 0
    Else
    Begin
        Select @LastProcessedId = IsNull(Max(BatchMaxId), @LastProcessedId) From #BatchMax;
        Truncate Table #BatchMax;
        If @InsertedThisPass < @LocalBatchSize
            Set @ContinueLoop = 0
    End
End
```

The `Output Inserted.Id Into #BatchMax` clause captures the Ids written by the current batch INSERT. The Max(BatchMaxId) lookup on the small #BatchMax temp table is much cheaper than re-reading Max(Id) from the destination table.

---

## 7. Risk & Rollback

**What could go wrong.** The `@@ROWCOUNT`-based exit is semantically equivalent to the WHILE EXISTS check as long as the same predicate (`HistoryDtTm < @CutoffDate And Id > @LastProcessedId`) drives both. The v2 form uses the same predicate inside the Batch CTE.

The @LastProcessedId tracking via the OUTPUT clause is exactly the value the v1 re-read would have produced after each INSERT, by construction.

The linked-server batch SELECT is preserved unchanged. Section 11 carries the same cross-reference to Row 35's linked-server question.

**What to look for in the first 24 hours.** Total elapsed time per call (expected: substantially lower; N linked-server round-trips eliminated), classification row count parity with v1 on the same data state, absence of debug result sets in the maintenance log.

**Rollback path.** The v1 body is in `Original.sql`. Redeploying v1 reverts cleanly.

---

## 8. Evidence of Refactor (v2)

```
(paste STATS IO / STATS TIME for v2 against the same data state used for v1 here)
```

---

## 9. Comparison & Improvement

| Metric | v1 (paste) | v2 (paste) | Delta |
|---|---|---|---|
| Total elapsed time per call | | (expected: substantially lower) | |
| Linked-server round-trips per call | N+1 (WHILE + batch) | N (batch only) | |
| Local classification table reads per call | N+1 (init + per-iteration) | 1 (init only) | |
| Rows classified per call | | (expected: identical, given the same data state) | |
| Stray result sets returned | 2N (per-iteration debug) | 0 | |

**Verdict (to be entered once the captures land):** the headline expected delta is the elimination of N linked-server round-trips for the WHILE check and N local re-reads for Max(Id) tracking.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Identical classification: same rows inserted with the same IsLeaflet values. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)
- No stray result sets returned to the maintenance orchestrator. (?)

Net call: the refactor is sound on first principles.

---

## 11. Open Items / Future Improvements

### 11.1 Cross-reference to Row 35's linked-server question

Row 35 (`lsp_DbDeleteNonLeafletOeOrderTextDocument`) and Row 38 (this procedure) both reference `[PWDAZNSYMPH02].PHARMASSIST.dbo.OeOrderTextDocument`. Row 35 references it in the WHILE check but operates on local tables in the DELETE; Row 38 references it consistently for the batch read with a local destination. The linked-server design is intentional for Row 38 (the source data lives on the secondary node and the classification is consumed locally), which suggests the linked-server reference in Row 35's WHILE check is also intentional rather than a leftover. Confirming with the iA team is still the right move; both procedures share the same upstream data assumption.

### 11.2 Consider whether the classification work belongs at the source

The classification logic (`DocDataLength > 153,600 → Non-Leaflet`) is simple and could be computed at the source on insert. A trigger or a persisted computed column on `[remote].OeOrderTextDocument` would let the local classification table be a direct projection rather than a separately-maintained derivation. This is an architectural conversation across the local and remote sides, documented for the longer-arc backlog.

### 11.3 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution. Nightly classification row count parity is the key validation metric.
