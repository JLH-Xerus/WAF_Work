# lsp_GovGetSingleVialSorterCounts: Refactor Analysis (v10 to v11)

**Date:** 2026-05-08
**Tracking sheet row:** 34 (Priority P2, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v11 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_GovGetSingleVialSorterCounts`

**Purpose in one line:** Returns one row per sorter ID (from 1 to N where N is the configured sorter count), with the count of queued single-vial Rxs and the count of on-route single-vial Rxs destined for that sorter.

**Tables touched:**

- `SrtSorter` (read once for the sorter count).
- `TallyNumbers` (used as a row source for the SorterList CTE).
- `GovRx` (joined to `OeOrder`; the source of the sorter assignment and OrderTypeCode filter).
- `OeOrder` (joined to `GovRx`; provides OrderStatus, StatusReason, PendingOrderStatus, OrderId).

**Indexes used (predicted from the v11 body):**

| Table | Index | Where used |
|---|---|---|
| `SrtSorter` | clustered (assumed) | sorter-count scan |
| `TallyNumbers` | clustered (assumed) | row source |
| `GovRx` | NC on `OrderTypeCode, SorterId` (assumed) | the consolidated scan |
| `OeOrder` | `PK_OeOrder` (clustered) | join |

**Callers:** the order governor monitoring dashboard. Called on a polling cadence.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on two of four expense lists (Duration #22, Expense #17) with "algorithmic / plan rewrite candidate" as the dominant heuristic. The procedure carries no input parameters, so parameter sniffing is not the issue. The procedure is structurally simple but reads the same `GovRx + OeOrder` join twice.

The dominant cost driver in v10 is the duplicated scan. The QueuedCounts CTE and the OnRouteCounts CTE both join `GovRx + OeOrder` and both filter to `G.OrderTypeCode = 'SV'`. The WHERE clauses differ between the two CTEs (one matches Pending+Awaiting-Governor-Allocation; the other matches the rest), but the two filters are disjoint, which means the same physical scan can satisfy both via conditional aggregation. One scan with two conditional Sum() expressions replaces the two CTE scans.

A secondary observation is that the `TallyNumbers` table is used purely as a row source for generating SorterId values from 1 to N. The use is correct; the comment in v10 notes "sufficient rows for up to ~2048 sorters." No change needed.

---

## 3. Evidence of Original (v10)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v10)

```
(paste STATS IO / STATS TIME for v10 against the same data state used for v11 here)
```

The expected v10 signature is two scans of `GovRx + OeOrder` filtered to `OrderTypeCode = 'SV'`.

---

## 4. Issue Identification

**Issue 1: Duplicated scan of `GovRx + OeOrder` across the two CTEs.** Both filter to `OrderTypeCode = 'SV'`; the WHERE clauses are otherwise disjoint. One scan with conditional aggregation produces both counts.

**Issue 2: No `Option (Recompile)` on the final SELECT.** Low impact because there are no input parameters, but the sorter count varies across sites and the optimizer's cardinality estimate for the TallyNumbers Top can affect the join shape. Recompile is a small safeguard.

---

## 5. First Principles

**[[Conditional Aggregation Consolidation]] applied to two-CTEs-over-same-source.** The two CTEs aggregate the same join with different filters. The consolidated form computes both Sum() expressions in one Group By, with the filter logic moved inside the Case expressions of the Sum().

---

## 6. Refactor (commented)

The v11 body is reproduced in `Refactored.sql`. Salient block below.

```sql
;With SorterList As
(
    Select Top (@NumOfSorters) Row_Number() Over (Order By (Select Null)) As SorterId
    From TallyNumbers
)
, RxCounts As
(
    Select
          G.SorterId
        , [QueuedRxCount]  = Sum(Case
                                    When O.OrderStatus = 'Pending'
                                     And O.StatusReason = 'Awaiting Governor Allocation'
                                    Then 1 Else 0 End)
        , [OnRouteRxCount] = Sum(Case
                                    When Not (O.OrderStatus = 'Pending' Or O.PendingOrderStatus = 'Pending')
                                     And O.OrderStatus Not Like 'Split/%'
                                     And O.OrderId    Not Like '%[(<][1-8][)>]'
                                     And G.RxCvyStateCode <> 'C'
                                    Then 1 Else 0 End)
    From GovRx G With (NoLock)
    Inner Join OeOrder O With (NoLock) On G.OrderId = O.OrderId
    Where G.OrderTypeCode = 'SV'
    Group By G.SorterId
)
Select SL.SorterId, IsNull(RC.QueuedRxCount, 0), IsNull(RC.OnRouteRxCount, 0)
From SorterList SL
Left Join RxCounts RC On RC.SorterId = SL.SorterId
Order By SL.SorterId
Option (Recompile);
```

One scan, two conditional sums, identical result shape.

---

## 7. Risk & Rollback

**What could go wrong.** The conditional-aggregation form returns the same per-sorter counts as the two CTEs by construction. The Case expressions in the consolidated Sums reproduce the WHERE clauses of the original CTEs exactly. The result-set semantics are identical: zero counts when no Rx matches, integer counts otherwise.

**What to look for in the first 24 hours.** Result-count parity with v10, plan operator count drop (one scan instead of two), elapsed time per call drop.

**Rollback path.** The v10 body is in `Original.sql`. Redeploying v10 reverts cleanly.

---

## 8. Evidence of Refactor (v11)

```
(paste STATS IO / STATS TIME for v11 against the same data state used for v10 here)
```

---

## 9. Comparison & Improvement

| Metric | v10 (paste) | v11 (paste) | Delta |
|---|---|---|---|
| Logical reads (total) | | (expected: ~50% of v10 on GovRx+OeOrder) | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan operator count | | (expected: lower) | |
| Result row count and per-sorter counts | | (expected: identical) | |

**Verdict (to be entered once the captures land):** the headline expected delta is the consolidated single-scan aggregation. The procedure is small in absolute terms; the win is structural rather than dramatic.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Non-zero result set. (?)
- Identical result set: same number of sorters, same QueuedRxCount and OnRouteRxCount per sorter. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles.

---

## 11. Open Items / Future Improvements

### 11.1 Index recommendation on `GovRx(OrderTypeCode, SorterId)`

The consolidated scan filters on `OrderTypeCode = 'SV'` and groups by `SorterId`. A nonclustered index on `(OrderTypeCode, SorterId)` with `OrderId, RxCvyStateCode` included would let the scan seek directly into the SV subset. Confirm against Tolleson.

### 11.2 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution.
