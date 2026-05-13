# lsp_OrdGetReadyForDispenseRxsInBank: Refactor Analysis (v20 to v21)

**Date:** 2026-05-08
**Tracking sheet row:** 29 (Priority P1, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v21 designed by the MFC DBA team and me, awaiting iA review.

**Important cross-reference:** the analysis for Row 21 (`lsp_RbtDetermineNextVialToProcess`) identified this procedure as the dominant cost driver for Row 21. Refactoring this procedure delivers compounding benefit to both rows.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_OrdGetReadyForDispenseRxsInBank`

**Purpose in one line:** Returns the list of Counted auto-fill Rxs ready for dispense in the specified cabinet bank, ordered by a "derived priority" that accounts for the highest-priority same-product upstream queued Rx (which may force the Counted Rx to fill first to make way).

**Tables touched:**

- `OeOrder` (the workhorse; read once for the Counted candidate set, once for the combined upstream candidate set, and optionally once more in the @ReturnFillingRelatedData = 1 branch).
- `vCfgSystemParamVal` (read once to retrieve the BoostInternalPriOfRxsInGroup configuration).
- `vInvMasterWithUserOverrides` (joined in the @ReturnFillingRelatedData = 0 branch for QtyPer100Cc).
- `OeFlaggedRxs` (left-joined in the @ReturnFillingRelatedData = 0 branch for the overflow-label flag).

**Indexes used (predicted from the v21 body):**

| Table | Index | Where used |
|---|---|---|
| `OeOrder` | `ByOrderStatusFillTypeAddrBank` (NC, key `OrderStatus, FillType, AddrBank`) | Counted candidate scan, upstream candidate scan |
| `OeOrder` | `PK_OeOrder` (clustered) | join in the optional second SELECT |
| `vCfgSystemParamVal` | base-table indexes | one-time config read |
| `vInvMasterWithUserOverrides` | base-table indexes on `ProductId` | join |
| `OeFlaggedRxs` | NC on `OrderId` (assumed) | left-join |

**Callers:** Multiple callers including `lsp_RbtDetermineNextVialToProcess` (Row 21 cohort). The Row 21 analysis identifies this procedure's `INSERT INTO #temp_table2 EXEC lsp_OrdGetReadyForDispenseRxsInBank` line as the only statement from Row 21 that appears in the cross-MFC top 50.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on three of four expense lists (Volume #19, Duration #13, Expense #11). The cross-list summary names "moderate read volume" with "review indexing & join strategy" as the dominant heuristic. This is consistent with the v20 body's pattern: clean structural design but with several incremental optimizations that compound at scale.

The dominant cost drivers in v20 are four.

The first is the two near-identical reads of `OeOrder` for the upstream candidate set (lines 104-126 and 131-151). The two reads differ only in the `AddrBank` value (`@AddrBank` versus `'-'`) and the predicate shape is identical. Combining them into one read with `AddrBank In (@AddrBank, '-')` halves the OeOrder access on this section.

The second is the two-step "find row with min value per group" pattern across `#TopUnassignedOrScheduledBehindCountedRxPriInternalPerProduct` and `#TopUnassignedOrScheduledBehindCountedRxPerProduct`. The first temp table computes the min PriInternal per product; the second re-joins to find the OrderId at that min. The same result is one ROW_NUMBER() window function over the combined upstream set, partitioned by ProductId and ordered by PriInternal.

The third is the two intermediate temp tables (`#CountedRxsWithTopUpstreamRxData01` and `#CountedRxsWithTopUpstreamRxData02`) that v20 uses to derive the adjusted upstream priority and the "Act On As" priority. Both are computed from the same base join and can be expressed in a single SELECT.

The fourth is the seven scattered `If Object_Id ... Drop Table` blocks. Consistency cleanup.

A fifth observation that v21 retains: the scalar UDF `dbo.fSqlFormatDecimalValueForDisplay` in the @ReturnFillingRelatedData = 1 branch. The UDF is called once per output row in that branch. Inlining is queued for a follow-up.

---

## 3. Evidence of Original (v20)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

**Roll-up across reporting sites:** to be filled in from the May 7 capture. Volume #19, Duration #13, Expense #11.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v20)

```
(paste STATS IO / STATS TIME for v20 against the same data state used for v21 here)
```

---

## 4. Issue Identification

**Issue 1: Two reads of `OeOrder` for the upstream candidate set (lines 104-126 and 131-151).** Identical predicate structure aside from the `AddrBank` value. Combine.

**Issue 2: Two-step min-then-join pattern for the top upstream Rx per product.** The pattern is a classic candidate for a ROW_NUMBER() window function.

**Issue 3: Two intermediate temp tables (`#CountedRxsWithTopUpstreamRxData01` and `#CountedRxsWithTopUpstreamRxData02`) that derive related expressions over the same base join.** Collapse into one SELECT.

**Issue 4: Seven scattered `If Object_Id ... Drop Table` blocks.** Consolidate.

**Issue 5: No local-variable indirection for the input parameters.** Program convention.

**Issue 6: No indexes on the intermediate temp tables.** Add explicit indexes after population.

---

## 5. First Principles

**[[LEFT JOIN OR Anti-Pattern]] applied to the AddrBank IN-list combination.** The two reads of `OeOrder` differ only in the `AddrBank` predicate; combining them via `In (@LocalAddrBank, '-')` is the natural consolidation. The combined predicate is SARGable against an index keyed by `AddrBank`.

**Window function replaces find-row-with-min-value-per-group.** The pattern of "build a min-per-group temp table, then re-join to find the row at that min" is everywhere in the procedure library. The single-pass equivalent is `Row_Number() Over (Partition By GroupKey Order By ValueColumn) = 1`. v21 applies this in the `Ranked` CTE.

**[[Correlated Subqueries to CTEs]] applied to derived-expression staging.** v20 stages two temp tables (`#CountedRxsWithTopUpstreamRxData01` and `#CountedRxsWithTopUpstreamRxData02`) where each computes a related expression. v21 uses `Cross Apply` to produce the intermediate `AdjustedUpstream` value once and references it in both the column projection and the derived `ActOnAsPriInternal` and `FillToMakeWayForUpstreamRxOrderId` expressions.

**[[Index Key Columns vs Included Columns]] applied to temp tables.** Each temp table that participates in a downstream join gets an explicit index sized for the join.

---

## 6. Refactor (commented)

The v21 body is reproduced in `Refactored.sql`. Salient blocks below.

**Combined upstream-candidate scan.**

```sql
Select
      O.ProductId
    , O.PriInternal
    , O.OrderId
Into #UpstreamCandidates
From OeOrder O With (NoLock)
Where O.AddrBank In (@LocalAddrBank, '-')
  And O.FillType = 'A'
  And O.OrderId Not Like '%<[1-2]>'
  And
  (
      O.OrderStatus = 'Scheduled'
      Or (O.OrderStatus = 'Split-DD' And O.StatusReason = 'Scheduled')
  )
  And Exists
  (
      Select 1
      From #CountedRxs C
      Where C.ProductId = O.ProductId
  )
Option (Recompile);
```

One OeOrder scan instead of two. The Exists clause filters down to the candidate products from `#CountedRxs`, which restricts the scan footprint.

**Single-pass top-upstream-per-product via ROW_NUMBER().**

```sql
;With Ranked As
(
    Select
          ProductId
        , PriInternal
        , OrderId
        , [Rk] = Row_Number() Over (Partition By ProductId Order By PriInternal Asc, OrderId Asc)
    From #UpstreamCandidates
)
Select ProductId, PriInternal, OrderId
Into #TopUpstreamPerProduct
From Ranked
Where Rk = 1
```

One pass over `#UpstreamCandidates` produces both the min PriInternal per product and the OrderId at that min. The tie-breaker on OrderId matches the v20 `Min(OrderId)` semantics.

**Consolidated derived-expression SELECT.**

```sql
Select
      C.OrderId, C.ProductId, C.QtyOrdered, C.PriInternal
    , [TopPriUpstreamQueuedRxOrderId]             = IsNull(B.OrderId, 'None')
    , [TopPriUpstreamQueuedRxAdjustedPriInternal] = AdjustedUpstream
    , [ActOnAsPriInternal]                        = Case When AdjustedUpstream < C.PriInternal Then AdjustedUpstream Else C.PriInternal End
    , [FillToMakeWayForUpstreamRxOrderId]         = Case When AdjustedUpstream < C.PriInternal Then B.OrderId Else Null End
Into #CountedRxsWithTopUpstream
From #CountedRxs C
Left Join #TopUpstreamPerProduct B
    On B.ProductId = C.ProductId
Cross Apply
(
    Select
        [AdjustedUpstream] = IsNull
        (
            Left(B.PriInternal, 1)
            + Case SubString(B.PriInternal, 2, 1)
                  When '9' Then @UpstreamQueuedRxGroupNotYetCountedAdjustedBoostVal
                  Else SubString(B.PriInternal, 2, 1)
              End
            + SubString(B.PriInternal, 3, 22)
          , '999999999999999999'
        )
) X
```

The `Cross Apply` produces the adjusted-upstream string once per row and makes it referenceable by name (`AdjustedUpstream`) in the column projection and in the two derived expressions. The v20 form had to repeat the long expression three times.

---

## 7. Risk & Rollback

**What could go wrong.** The combined AddrBank scan changes the OeOrder access pattern. The `AddrBank In (@LocalAddrBank, '-')` predicate is SARGable against an index keyed by `AddrBank`; the optimizer should produce a seek-or-scan of the index for each of the two values. If the candidate set at one of the values is empty, the optimizer should short-circuit the corresponding range.

The ROW_NUMBER() window function changes the cardinality estimate the optimizer sees for the upstream-top-per-product set. The set size is bounded by the number of distinct products in `#CountedRxs`, which is typically small.

The Cross Apply form for the derived-expression staging is equivalent to the v20 two-temp-table form by construction. The expressions are computed against the same input columns (`B.PriInternal`); the only difference is that v21 names the intermediate result once.

**What to look for in the first 24 hours.** OeOrder reads per call (expected to drop on the upstream-candidate scan), temp-table count (expected to drop from 5 to 4), result set parity with v20, plan shape changes on the joins.

**Rollback path.** The v20 body is in `Original.sql`. Redeploying v20 reverts cleanly.

---

## 8. Evidence of Refactor (v21)

```
(paste STATS IO / STATS TIME for v21 against the same data state used for v20 here)
```

---

## 9. Comparison & Improvement

| Metric | v20 (paste) | v21 (paste) | Delta |
|---|---|---|---|
| Logical reads (total) | | | |
| Logical reads on `OeOrder` | | (expected: ~50-75% of v20) | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan operator count | | | |
| Result row count | | | |

**Verdict (to be entered once the captures land):** the headline expected delta is the OeOrder read reduction from the consolidated upstream scan and the elimination of one temp table. Knock-on effect: Row 21 (`lsp_RbtDetermineNextVialToProcess`) reads should also drop because this procedure is its dominant cost driver.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Non-zero result set. (?)
- Identical result set: same Rxs returned in same priority order in both branches of the @LocalReturnFillingRelatedData switch. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles. The compounding effect on Row 21 makes per-MFC monitoring especially valuable on this procedure.

---

## 11. Open Items / Future Improvements

### 11.1 Inline `dbo.fSqlFormatDecimalValueForDisplay` in the @ReturnFillingRelatedData = 1 branch

The scalar UDF carries the [[Scalar UDF Parallelism Barrier]] cost. The UDF body is a decimal formatter; the inline form is a `Format()` call or a Cast/Convert with explicit precision. Confirm the UDF body before deployment and inline in a v22.

### 11.2 Cross-reference to Row 21

This procedure is identified in `refactors/lsp_RbtDetermineNextVialToProcess/Analysis.md` Section 11.2 as the dominant cost driver for Row 21. The v21 refactor here is the structural improvement that Row 21's analysis was waiting on. Once v21 deploys, Row 21's reads should drop substantially even without a v11 deployment of Row 21 itself.

### 11.3 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution. The capture should look at both Row 29 (this procedure) and Row 21 (the caller) to measure the compounding benefit.

### 11.4 Consider whether Counted Rxs need a more selective filter

The Counted candidate scan filters on `OrderStatus In ('Counted', 'Partial Counted') Or (OrderStatus = 'Split-DD' And StatusReason = 'Counted')` with `AddrBank = @LocalAddrBank` and `FillType = 'A'`. A filtered index on this combination might be useful if the post-deployment plan shows the scan as the dominant cost. The filtered index would be Tolleson-evaluated first; documented for the architectural backlog.
