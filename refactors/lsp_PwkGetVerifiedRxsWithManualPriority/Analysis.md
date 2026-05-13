# lsp_PwkGetVerifiedRxsWithManualPriority: Refactor Analysis (v2 to v3)

**Date:** 2026-05-08
**Tracking sheet row:** 23 (Priority P0, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v3 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_PwkGetVerifiedRxsWithManualPriority`

**Purpose in one line:** Returns the top 10 verified Rxs (single-vial parents and Split/MV parents whose children are all in totes) ordered by DateFilled, with the top single-vial Manual Rx prioritized first, filtered by a modulo-10 mask on PatCustId.

**Tables touched:**

- `OeOrder` (the workhorse; queried once for the candidate-set build).
- `OeOrderPoNumAssoc` (inner-joined for the PoNum lookup).
- `OrderToteAssoc` (queried twice as correlated subqueries in v2: once for the in-tote ToteId lookup per row, once for the child-vial completeness count per Split/MV parent row).

**Indexes used (predicted from the v3 body, to be confirmed against the captured plan):**

| Table | Index | Where used |
|---|---|---|
| `OeOrder` | `ByOrderStatus` (NC, key `OrderStatus`) | the candidate-set build filtered on `OrderStatus In ('Verified', 'Split/MV')` |
| `OeOrder` | `PK_OeOrder` (clustered, key `OrderId, HistoryDtTm`) | join into `OeOrderPoNumAssoc` |
| `OeOrderPoNumAssoc` | `PK_OeOrderPoNumAssoc` (clustered, key `OrderId`) | inner join for PoNum |
| `OrderToteAssoc` | clustered or NC on `OrderId` (assumed) | the single pre-materialization scan; v3 reads this once instead of per-row |

**Index DDL gap.** The pilot index extract covers the `OeOrder` family. `OeOrderPoNumAssoc` and `OrderToteAssoc` are partially covered. Confirm the indexes against Tolleson before final sign-off.

**Callers:** the paperwork poller. Called regularly to drive paperwork generation for verified Rxs. The 1,409x worst-case-to-average ratio in the cross-list capture reflects the high call frequency interacting with the mask-filter parameter sniffing.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on all four expense lists (Volume rank 21, Duration rank 11, Expense rank 12, Plan rank 18) with a 1,409x worst-case-to-average ratio. The volatility signature is consistent with two things: the modulo-10 mask predicate on `@PatCustIdMask`, which is non-SARGable and whose selectivity varies meaningfully across callers; and the two correlated subqueries against `OrderToteAssoc`, which run once per candidate row in v2 and whose cost scales with the number of candidate rows that survive the outer WHERE clause.

The dominant cost driver in v2 is the two correlated subqueries against `OrderToteAssoc`. The first one looks up the in-tote `ToteId` for each candidate parent (Top 1, OrderToteStateCode = 'I', joined by a `LIKE` pattern that matches the parent itself or any child with the form `parent-NN`). The second one counts the in-tote children for each Split/MV parent, used in the multi-tote completeness check. Both subqueries scan `OrderToteAssoc` filtered by the same `OrderToteStateCode = 'I'` predicate. Doing the work once instead of per-row reduces the per-call cost by a factor that scales with the candidate-set size.

The second cost driver is parameter sniffing on `@PatCustIdMask`. The predicate `@PatCustIdMask Like '%' + Cast((PatCustId % 10) As varchar) + '%'` is fundamentally non-SARGable, so any plan the optimizer chooses is doing a scan of the candidate set. The volatility comes from the optimizer's selectivity estimate for the predicate: a mask that matches "any digit" selects all rows, while a mask that matches one specific digit selects roughly 10 percent. A plan compiled against one selectivity assumption runs poorly against the other.

The third cost driver is the legacy `Object_Id` temp-table check. Small in absolute terms but ubiquitous in the procedure library, and worth fixing on every refactor.

v3 addresses all three of these systematically.

---

## 3. Evidence of Original (v2)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Max-vs-avg ratio |
|-----|---------------|------------------|-------------|----------------|--------------------|------------------|
| (paste from capture) | | | | | | |

**Roll-up across reporting sites:** to be filled in from the May 7 capture. The cross-list capture reports the procedure on all four expense lists with the 1,409x worst-case-to-average ratio as the dominant signal.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v2)

Captured against Tolleson. Warm cache. Two captures at different `@PatCustIdMask` selectivities are needed to expose the volatility.

```
(paste STATS IO / STATS TIME for v2 against the same data state used for v3 here)
```

The expected v2 signature is one `OeOrder` scan plus N `OrderToteAssoc` lookups, where N is the candidate-set size before the mask filter. For a typical busy site this is in the low thousands of OrderToteAssoc lookups per call.

---

## 4. Issue Identification

The v2 body has five issues.

**Issue 1: Correlated scalar subquery for the in-tote ToteId (line 37 in v2).** `IsNull((Select Top 1 ToteId From OrderToteAssoc With (NoLock) Where ((OrderId like O.OrderId + '-[0-9][0-9]') Or (OrderId = O.OrderId)) And OrderToteStateCode = 'I'), '')`. One scalar subquery per outer row, non-SARGable LIKE predicate, scans the in-tote portion of the table for each candidate parent.

**Issue 2: Correlated scalar Count subquery for child-vial completeness (line 46 in v2).** `O.NumOfVials = (Select Count(OrderId) from OrderToteAssoc With (NoLock) Where OrderId like O.OrderId + '-[0-9][0-9]' And OrderToteStateCode = 'I')`. Same shape as Issue 1, also non-SARGable, executed per Split/MV parent.

**Issue 3: Non-SARGable mask filter on PatCustId (lines 55-56 and 66 in v2).** `@PatCustIdMask Like '%' + CAST((RFP.PatCustId % 10) as varchar) + '%'`. The modulus-then-LIKE pattern is fundamentally non-SARGable. The predicate is repeated identically in both branches of the UNION.

**Issue 4: Parameter sniffing on `@PatCustIdMask` and `@MultiToteRequired`.** No local-variable indirection. Plan caching is therefore sensitive to the first call's parameter values.

**Issue 5: Legacy `Object_Id` temp table check (lines 32-33 in v2).** `If Object_Id('TempDb..#ReadyForPaperworkManPriority') Is Not Null Drop Table #ReadyForPaperworkManPriority`. The modern equivalent is `Drop Table If Exists #ReadyForPaperworkManPriority`.

---

## 5. First Principles

The refactor draws on three masterclass entries.

**[[Correlated Subqueries to CTEs]] with the pre-materialize-and-left-join fix.** The two correlated subqueries against `OrderToteAssoc` share the same filter (`OrderToteStateCode = 'I'`) and the same parent-child relationship (`parent OR parent-NN`). Pre-materializing the in-tote state once with a derived `ParentOrderId` lets both lookups become a single left-join. The aggregate (`ChildCount`, `FirstToteId`) is produced in one pass, and the outer query references the materialized columns directly.

**[[Parameter Sniffing]] with the local-variable form.** `@LocalPatCustIdMask` and `@LocalMultiToteRequired` break the parameter sniffing on the modulo-10 mask predicate. The candidate-set build and the final SELECT reference the locals.

**[[Non-SARGable Predicates]] applied at the procedure level rather than the query level.** The modulo-10 mask predicate is fundamentally non-SARGable; no index can satisfy it directly. The fix is not to rewrite the predicate but to reduce the row count it is evaluated against. v3 pulls the mask filter into the Filtered CTE so it is applied once over the candidate set, which is already small by virtue of the upstream OrderStatus filter. The two SELECTs that follow reference the filtered set rather than re-evaluating the mask predicate.

The "constructed-parent-key self-join" pattern (deriving `ParentOrderId` via `Left(OrderId, Len(OrderId) - 3)` when the OrderId matches `'%-[0-9][0-9]'`, otherwise using OrderId directly) is the same pattern that surfaced in Row 22. Section 11 reiterates the masterclass-entry recommendation.

---

## 6. Refactor (commented)

The v3 body is reproduced in `Refactored.sql`. The salient blocks below.

**Local-variable copies and modern temp-table cleanup.**

```sql
Declare @LocalPatCustIdMask    VarChar(20) = @PatCustIdMask
Declare @LocalMultiToteRequired Bit         = @MultiToteRequired

Drop Table If Exists #ToteState, #ReadyForPaperworkManPriority
```

**Pre-materialized OrderToteAssoc state with a derived ParentOrderId.**

```sql
Select
    [ParentOrderId] = Case
                         When OTA.OrderId Like '%-[0-9][0-9]' Then Left(OTA.OrderId, Len(OTA.OrderId) - 3)
                         Else OTA.OrderId
                      End
  , OTA.OrderId
  , OTA.ToteId
Into #ToteState
From OrderToteAssoc OTA With (NoLock)
Where OTA.OrderToteStateCode = 'I'

Create Index IX_ToteState_Parent On #ToteState(ParentOrderId)
```

The derivation runs once over the in-tote portion of `OrderToteAssoc`. The index on `ParentOrderId` supports the subsequent join.

**Aggregated in-tote view and the candidate-set build.**

```sql
;With InToteAgg As
(
    Select
          ParentOrderId
        , [ChildCount]  = Count(*)
        , [FirstToteId] = Min(ToteId)
    From #ToteState
    Group By ParentOrderId
)
Select ...
     , [ToteId] = IsNull(ITA.FirstToteId, '')
Into #ReadyForPaperworkManPriority
From OeOrder As O With (NoLock)
Inner Join OeOrderPoNumAssoc As Po With (NoLock)
    On O.OrderId = Po.OrderId
Left Join InToteAgg ITA
    On ITA.ParentOrderId = O.OrderId
Where
    (O.OrderStatus = 'Verified' And O.OrderId Not Like '%-[0-9][0-9]')
    Or
    (O.OrderStatus = 'Split/MV' And O.StatusReason = 'Verified'
        And (@LocalMultiToteRequired = 0 Or O.NumOfVials = IsNull(ITA.ChildCount, 0)))
```

The two correlated subqueries are gone. `FirstToteId` and `ChildCount` come from the aggregated CTE.

**Filtered candidate set with the mask predicate applied once.**

```sql
;With Filtered As
(
    Select *
    From #ReadyForPaperworkManPriority RFP
    Where @LocalPatCustIdMask Like '%' + Cast((RFP.PatCustId % 10) As VarChar) + '%'
)
Select ... From (Select Top 1 * From Filtered Where FillType = 'M' Order By DateFilled Asc) ManualTop
Union
Select ... From (Select Top 10 * From Filtered Order By DateFilled Asc) AnyTop
Order By DateFilled Asc
Option (Recompile)
```

The mask predicate is in the Filtered CTE. The two branches reference the filtered set and apply their own Top. UNION (not UNION ALL) preserves the intentional dedup. The Recompile hint takes the per-call compile cost in exchange for accurate selectivity estimates on the local-variable mask.

---

## 7. Risk & Rollback

**What could go wrong.** The pre-materialization of `OrderToteAssoc` filtered by `OrderToteStateCode = 'I'` produces a temp table whose size scales with the number of in-tote rows fleet-wide. At the busiest MFCs this is in the tens of thousands of rows. The materialization is one-pass and bounded by an index on the source table; the temp table itself has an index on the derived ParentOrderId. The risk is small but should be validated under load.

The `ChildCount` aggregate produced from `InToteAgg` includes the parent's own row in the case where the parent itself is also in a tote (the parent OrderId itself matches `ParentOrderId` derivation as `OrderId` when there is no `-NN` suffix). For the multi-tote completeness check this needs to count children only. The check in v3 is `O.NumOfVials = IsNull(ITA.ChildCount, 0)`, which matches the v2 semantics where `OrderId like O.OrderId + '-[0-9][0-9]'` excluded the parent itself. The pre-materialization in v3 includes the parent row in the count whenever the parent has a row in OrderToteAssoc, which can differ. The fix is to filter the `InToteAgg` to children only, or to compute two aggregates (one including the parent, one children-only). The safer of the two is the children-only count for completeness, which is what v2 implicitly did. Section 11 carries the recommendation to confirm this semantic match with the iA team before deployment.

The mask-filter selectivity is what drives the plan-stability ratio. With Recompile the per-call plan is compiled against the actual selectivity, so the plan should be tight for the observed mask. Without Recompile the local-variable form alone would produce a single plan optimized for "10 percent selectivity" (the optimizer's default density estimate when it cannot sniff the value). The trade-off is the compile cost; for this procedure, called at moderate frequency, Recompile is the right default.

**What to look for in the first 24 hours.** Plan variant count per site, max-vs-avg ratio per site, and the count of `OrderToteAssoc` reads per call. The headline expected delta is the OrderToteAssoc read count dropping from N (one per candidate row) to 1 (one pre-materialization scan).

**Rollback path.** The v2 body is in `Original.sql` in this folder. Redeploying v2 reverts cleanly. No schema change.

---

## 8. Evidence of Refactor (v3)

Captured against Tolleson. Warm cache. Same data state as Section 3.2. Two captures at different `@PatCustIdMask` selectivities.

```
(paste STATS IO / STATS TIME for v3 against the same data state used for v2 here)
```

The expected post-refactor signature is one `OrderToteAssoc` scan (the pre-materialization), one #ToteState lookup per candidate row (very cheap given the index on ParentOrderId), one mask-filter evaluation per row in the Filtered CTE, and the same plan across the two captures.

---

## 9. Comparison & Improvement

| Metric | v2 (paste) | v3 (paste) | Delta |
|---|---|---|---|
| Logical reads (total, median run) | | | |
| Logical reads on `OrderToteAssoc` | | | |
| Logical reads on `OeOrder` | | | |
| Reads on `#ToteState` (new) | n/a | | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan variant count across two captures | (expected: 2+) | (expected: 1) | |
| Max-vs-avg ratio (cross-MFC, 30d) | 1,409x | (expected: <50x) | |
| Result row count | | | |

**Verdict (to be entered once the captures land):** the headline expected delta is the elimination of the per-row correlated subqueries against `OrderToteAssoc`, which on a busy site dominate the per-call read cost. The mask-filter parameter sniffing is the second expected delta, addressed via the local variable plus Recompile.

---

## 10. Validation Checklist

Marked once the captures and the plans are in hand.

- Same data state. Captures taken back to back, ideally with a data freeze. (?)
- Warm cache only. Two runs, cold-cache numbers discarded. (?)
- Non-zero result set. Both runs returned at least one row. (?)
- Identical result set. Same row count and same row identities; this is the gating check for the ChildCount semantic match noted in Section 7. (?)
- Plan shape matches prediction. One `OrderToteAssoc` scan, indexed `#ToteState` lookups, stable plan across two captures. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles. The single semantic check that gates sign-off is whether the `ChildCount` aggregate in v3 matches the v2 child-only count. If it does not (Issue noted in Section 7), v3 needs a children-only filter on the InToteAgg CTE.

---

## 11. Open Items / Future Improvements

### 11.1 Confirm the ChildCount semantic match

The v2 child-count predicate `OrderId Like O.OrderId + '-[0-9][0-9]'` matches children only; the parent OrderId itself does not match. The v3 InToteAgg aggregates by ParentOrderId, which includes the parent row when the parent itself has an in-tote assignment. For the multi-tote completeness check (`@LocalMultiToteRequired = 1`) this can differ by one row in edge cases.

The safer form is to compute the children-only count explicitly in InToteAgg:

```sql
;With InToteAgg As
(
    Select
          ParentOrderId
        , [ChildCount]  = Sum(Case When OrderId <> ParentOrderId Then 1 Else 0 End)
        , [FirstToteId] = Min(ToteId)
    From #ToteState
    Group By ParentOrderId
)
```

This is a one-line change and should be applied if the validation check in Section 10 flags a row-count divergence.

### 11.2 Index recommendation on `OrderToteAssoc(OrderToteStateCode, OrderId)`

The pre-materialization scan filters on `OrderToteStateCode = 'I'`. If the table is keyed on `(OrderId, OrderToteStateCode)` the scan has to read the full table and filter. A nonclustered index on `(OrderToteStateCode, OrderId)` with `ToteId` included would let the scan seek directly into the in-tote subset. Recommended DDL:

```sql
Create NonClustered Index IX_OrderToteAssoc_InToteParent
   On OrderToteAssoc(OrderToteStateCode, OrderId)
   Include (ToteId)
   Where OrderToteStateCode = 'I';
```

The filter predicate is reproduced verbatim. This is a Tolleson-first recommendation; evaluate before fleet deployment.

### 11.3 Reconsider the mask-filter as application-side logic

The modulo-10 mask predicate is a way to shard the candidate set across multiple paperwork pollers (one mask digit per poller, presumably). The non-SARGable form makes it expensive at the database. If the application can either pre-compute the modulo-10 digit (via a persisted computed column on PatCustId, indexed) or push the sharding logic into the application layer entirely, the database can take the predicate out of the WHERE clause. This is an application-team conversation, not a same-PR fix, and is documented for the longer arc.

### 11.4 Write the "Constructed-Parent-Key Self-Joins" masterclass entry

The pattern of deriving a parent key from a child OrderId via `Left(OrderId, Len(OrderId) - 3)` and then joining recurs across the procedure library (Row 22 had the same shape with sibling-OrderId; Row 23 has it with ParentOrderId). The masterclass entry should cover the cost mechanism (non-SARGable derivation forces a scan in the absence of a persisted computed column), the recommended fix (a persisted computed column with an index, where the application contract allows it), and the alternative when the application contract precludes the schema change (the pre-materialize-into-temp-table approach v3 uses). The next refactor that surfaces this pattern should produce the entry.
