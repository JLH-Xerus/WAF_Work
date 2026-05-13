# lsp_SrtGenerateAndReturnSorterToteLocationStats: Refactor Analysis

**Date:** 2026-05-08
**Tracking sheet row:** 32 (Priority P1, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed refactor designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_SrtGenerateAndReturnSorterToteLocationStats`

**Purpose in one line:** Computes five stats about the sorter and tote state (number of assigned totes, unassigned totes, full totes, exception packages in sorter totes, packages inducted but not sorted within one minute) and returns them via OUTPUT parameters.

**Tables touched:**

- `SrtSorterLoc` (the sorter location table).
- `SrtShipToteShipmentAssoc` (the tote-to-shipment association).
- `SrtShipTote` (the ship-tote table; queried for full-tote and assigned-tote counts).
- `OeOrderShipmentAssoc` (joined for shipment associations).
- `OeOrder` (active orders; joined for the Problem-status exception filter).
- `OeOrderHistory` (history orders; joined for the Canceled-status exception filter).
- `ShpShipment` (filter join).
- `CvyRxRoute` (provides CurrLoc, NextLoc, LastLocChgDtTm for the inducted-but-not-sorted aggregate).
- `CfgSystemParamVal` (config read for the full-tote threshold).

**Callers:** the sorter monitoring dashboard. Called on a polling cadence.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on two of four expense lists (Expense #23, Plan #15) with "plan instability" and "algorithmic / plan rewrite candidate" as the dominant heuristics.

The dominant cost drivers in the original are three.

The first is the UNION-with-placeholder-columns aggregation pattern at the end of the procedure. The original procedure builds a 5-row, 5-column intermediate where each row contributes one count value and four empty-string placeholders, then sums the intermediate at the outer level. The optimizer is forced to evaluate every branch even though each branch only contributes to one OUTPUT variable. The five branches each carry their own filter and aggregation; pulling them into five direct OUTPUT assignments is clean.

The second is the eight scattered `If Object_Id ... Drop Table` blocks at the top of the procedure. Consistency cleanup.

The third is the absence of indexes on the intermediate temp tables. `#SorterTotePackageRxs`, `#AllShipmentIds`, and `#NumOfPackagesInducted` all participate in downstream joins; the missing indexes force hash joins.

A fourth observation worth flagging is the `Option (MaxDop 1)` hint on the `#SorterTotePackageRxs` scan. The hint has no surrounding comment explaining the rationale. The conservative choice for the refactor is to preserve the hint and flag it for re-evaluation in Section 11.

---

## 3. Evidence of Original

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

### 3.2 STATISTICS IO and STATISTICS TIME

```
(paste STATS IO / STATS TIME for original against the same data state used for refactor here)
```

---

## 4. Issue Identification

**Issue 1: UNION-with-placeholder-columns aggregation at the end of the procedure (lines 239-318).** Five branches each contribute one count value and four empty-string placeholders; the outer SUM extracts the contributions. The intermediate row count is small but the optimizer evaluates every branch on every call.

**Issue 2: Eight scattered `If Object_Id ... Drop Table` blocks.** Consolidate.

**Issue 3: No indexes on intermediate temp tables.** Several participate in downstream joins.

**Issue 4: `Where ShipmentId Not In (Select ShipmentId From SrtShipToteShipmentAssoc)` (lines 214-219).** NOT IN with a subquery is fine when the subquery column is NOT NULL, but NOT EXISTS is safer.

**Issue 5: Unexplained `Option (MaxDop 1)` hint on the first SELECT INTO.** No comment in the body explaining why. Preserve for now.

**Issue 6: Two-step "build + delete-non-matching" pattern on `#HistoryExceptionRxPackageShipmentIds` (lines 99-123).** The procedure builds the temp table with `Where SR.HistoryDtTm <> '12/31/9999'` and then deletes rows where `OrderStatus <> 'Canceled'`. A single SELECT with both conditions in the WHERE is cleaner.

---

## 5. First Principles

**Direct OUTPUT assignment replaces UNION-with-placeholders.** The UNION-with-placeholders pattern is a common but inefficient way to compute multiple aggregates. Each branch is independent; expressing them as separate SELECT statements that assign directly to the OUTPUT variables produces cleaner code and (typically) better plans because each branch can be planned in isolation.

**[[NOT IN vs NOT EXISTS]] for the inducted-but-not-sorted filter.** NOT EXISTS handles NULL ShipmentId values safely; NOT IN does not. Even if the column is currently NOT NULL, NOT EXISTS is the safer idiom and matches the rest of the procedure library's convention.

**[[Index Key Columns vs Included Columns]] applied to temp tables.** Indexes on intermediate temp tables support downstream joins.

**Consolidate build + filter into one SELECT.** The two-step "build then delete" pattern on `#HistoryExceptionRxPackageShipmentIds` was likely a defensive accommodation; the v_next form combines the conditions in a single WHERE clause.

---

## 6. Refactor (commented)

The proposed body is reproduced in `Refactored.sql`. Salient blocks below.

**Consolidated cleanup.**

```sql
Drop Table If Exists
      #SorterTotePackageRxs
    , #ActiveExceptionRxPackageShipmentIds
    , #HistoryExceptionRxPackageShipmentIds
    , #AllShipmentIds
    , #NumOfPackagesInducted
```

**History exception query consolidated to one SELECT.**

```sql
Select Distinct SR.ShipmentId
Into #HistoryExceptionRxPackageShipmentIds
From #SorterTotePackageRxs SR
Inner Join OeOrderHistory O With (NoLock)
    On O.OrderId = SR.OrderId And O.HistoryDtTm = SR.HistoryDtTm
Where SR.HistoryDtTm <> '12/31/9999'
  And O.OrderStatus = 'Canceled'
Option (Recompile);
```

The build-then-delete two-step is collapsed into one statement.

**Five direct OUTPUT assignments replace the UNION aggregation.**

```sql
Select @NumFullTotes = Count(Id)
From SrtShipTote With (NoLock)
Where NumOfPkgs >= @ToteFullPackageCount
  And Len(Id) = 22
  And Status <> 'Ready For Manifest'

Select @NumAssignedTotes = Count(SL.ShipToteId)
From SrtSorterLoc SL With (NoLock)
Inner Join SrtShipTote ST With (NoLock) On SL.ShipToteId = ST.Id
Where ST.Status In ('In Sorter Loc') And Len(ST.Id) = 22

-- ...three more focused SELECTs for the remaining OUTPUT variables
```

Each branch is a focused query that the optimizer can plan independently.

**`Not Exists` replaces `Not In`.**

```sql
Select @NumOfPackagesInductedAndUnsorted = Count(*)
From #NumOfPackagesInducted PI
Where Not Exists
(
    Select 1
    From SrtShipToteShipmentAssoc TSA With (NoLock)
    Where TSA.ShipmentId = PI.ShipmentId
)
  And DateDiff(SECOND, PI.InductedDtTm, GetDate()) > 60
```

---

## 7. Risk & Rollback

**What could go wrong.** The five direct OUTPUT assignments produce the same five values as the v_old UNION pattern. The Distinct removed from #HistoryExceptionRxPackageShipmentIds (now applied directly in the SELECT) preserves the same row identities.

The NOT EXISTS substitution is functionally identical when the subquery column is NOT NULL and preserves correctness when it is NULL.

The Option (MaxDop 1) hint is preserved on the first SELECT INTO. The new Option (Recompile) hints on the parameter-sniffing-sensitive aggregates are additive.

**What to look for in the first 24 hours.** Per-OUTPUT-variable value parity with v_old, elapsed time per call (expected: lower because the optimizer plans each branch independently), plan operator count (expected: substantially lower).

**Rollback path.** The v_old body is in `Original.sql`. Redeploying v_old reverts cleanly.

---

## 8. Evidence of Refactor

```
(paste STATS IO / STATS TIME for refactor against the same data state used for original here)
```

---

## 9. Comparison & Improvement

| Metric | v_old (paste) | v_new (paste) | Delta |
|---|---|---|---|
| Logical reads (total) | | | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan operator count | | (expected: substantially lower) | |
| OUTPUT values | | (expected: identical) | |

**Verdict (to be entered once the captures land):** the headline expected delta is the elimination of the UNION-with-placeholders aggregation pattern. Plan stability should improve because each OUTPUT branch now has its own cached plan.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Identical OUTPUT values: same @NumAssignedTotes, @NumUnassignedTotes, @NumFullTotes, @NumOfExceptPkgs, @NumOfPackagesInductedAndUnsorted. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles.

---

## 11. Open Items / Future Improvements

### 11.1 Re-evaluate the `Option (MaxDop 1)` hint on the SorterTotePackageRxs scan

The hint is preserved from the original procedure but carries no explanation. Operational testing should determine whether the hint is still necessary or whether the optimizer's natural plan choice produces acceptable behavior at scale.

### 11.2 Index recommendation on `CvyRxRoute(OrderId, HistoryDtTm)`

The `#AllShipmentIds` build joins `CvyRxRoute` on `(OrderId, HistoryDtTm)` to project `CurrLoc, NextLoc, LastLocChgDtTm`. Confirm the supporting index exists at Tolleson; if not, the recommended DDL is a nonclustered index keyed on `(OrderId, HistoryDtTm)` with `CurrLoc, NextLoc, LastLocChgDtTm` included.

### 11.3 Consider whether the SortLeft/NextLoc filters can be made SARGable

`Left(SI.CurrLoc, 4) = 'SORT'` and `Left(SI.NextLoc, 2) = 'DR'` are non-SARGable in their current form. A persisted computed column on the prefix (or a domain-level decomposition of CurrLoc and NextLoc into structured columns) would let the filters seek directly. This is a schema change and belongs in the longer-arc backlog.

### 11.4 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution.
