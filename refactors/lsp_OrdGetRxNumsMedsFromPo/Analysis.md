# lsp_OrdGetRxNumsMedsFromPo: Refactor Analysis (v22 to v23)

**Date:** 2026-05-08
**Tracking sheet row:** 33 (Priority P1, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v23 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_OrdGetRxNumsMedsFromPo`

**Purpose in one line:** For a PO number, returns the list of Rx numbers, medications, and supporting label-data columns for every active or non-canceled-history Rx on the PO, joined to secondary data, patient language, flag, and user-def overrides.

**Tables touched:**

- `OeOrderPoNumAssoc` (the PO-to-OrderId map).
- `OeOrder` (active Rxs).
- `OeOrderHistory` (history Rxs, non-canceled, non-Split/MV).
- `OeOrderSecondaryData` (joined for CustomerPrice).
- `OeOrderCurrHistoryDtTm` (joined to confirm the current HistoryDtTm).
- `OePatientCustSecondaryData` (joined for LanguageCode).
- `OeOrderExtUserDef` (left-joined for the UserDef24/31/32 columns).
- `OeFlaggedRxs` (left-joined for the IsDiffDrugFromLastFill flag).
- `OeGroupUserDef` (left-joined for the UserDef01-06 columns at the group level).

**Indexes used (predicted from the v23 body):**

| Table | Index | Where used |
|---|---|---|
| `OeOrderPoNumAssoc` | NC on `PoNum, OrderId` (assumed) | initial OrderId scan |
| `OeOrder` | `PK_OeOrder` (clustered) | active Rx join |
| `OeOrderHistory` | `PK_OeOrderHistory` (clustered) | history Rx join |
| `OeOrderSecondaryData` | clustered on `OrderId, HistoryDtTm` (assumed) | secondary-data join |
| `OeOrderCurrHistoryDtTm` | clustered on `OrderId` (assumed) | current HistoryDtTm confirmation |
| `OePatientCustSecondaryData` | clustered on `PatCustId` (assumed) | language code |
| `OeOrderExtUserDef` | clustered on `OrderId, HistoryDtTm` (assumed) | left-join |
| `OeFlaggedRxs` | NC on `OrderId, HistoryDtTm` with `IsFlaggedFor` included (assumed) | left-join with flag filter |
| `OeGroupUserDef` | clustered on `GroupNum` (assumed) | left-join |

**Callers:** the label-data generator (mentioned in the v22 parameter comment: "passed in from OrdrGetLabelData").

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on two of four expense lists (Duration #24, Plan #21) with a 1,720x worst-case-to-average ratio and "algorithmic / plan rewrite candidate" as the dominant heuristic. The 1,720x ratio is consistent with parameter sniffing on the PoNum filter: some PO numbers map to a single Rx, others map to a multi-Rx order or a complex multi-vial parent set, and a single cached plan cannot serve both selectivities well.

The dominant cost drivers in v22 are four.

The first is parameter sniffing on `@PoNum`. The selectivity of the PoNum filter varies materially across calls. v23 addresses this via the local-variable form plus `Option (Recompile)` on the three parameter-sensitive INSERTs.

The second is the constructed parent-OrderId derivation in the final SELECT's secondary-data join. The original v22 expresses the parent-OrderId logic as an Or-clause inside the join condition: `((O.OrderId Like '%-[0-9][0-9]' And O2.OrderId = Left(O.OrderId, Len(O.OrderId) - 3)) Or (O.OrderId Not Like '%-[0-9][0-9]' And O2.OrderId = O.OrderId))`. The Or-with-function-on-column shape defeats clean index seeks on `OeOrderSecondaryData.OrderId`. v23 computes the parent-OrderId once per row into a `ParentOrderId` column on `#tmpOrders`, and the secondary-data join becomes a clean equality.

The third is the `OrderId In (Select OrderId From #tmpPoOrdrId)` subquery, used three times. The temp table is small (typically a handful of OrderIds per PO) but the IN subquery defeats clean join behavior. v23 replaces these with direct inner joins to `#tmpPoOrdrId`, which now carries an index on OrderId.

The fourth is the mixed convention on temp-table cleanup. v22 uses three legacy `If Object_Id ... Drop Table` blocks at entry and three modern `Drop Table If Exists` at exit. v23 consolidates both.

---

## 3. Evidence of Original (v22)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Max-vs-avg ratio |
|-----|---------------|------------------|-------------|----------------|--------------------|------------------|
| (paste from capture) | | | | | | |

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v22)

```
(paste STATS IO / STATS TIME for v22 against the same data state used for v23 here)
```

Two captures at different `@PoNum` selectivities are needed to expose the plan-volatility behavior.

---

## 4. Issue Identification

**Issue 1: Parameter sniffing on `@PoNum`.** 1,720x worst-case-to-average ratio.

**Issue 2: Constructed parent-OrderId in the final SELECT's secondary-data join (lines 108-114 in v22).** Or-clause shape defeats clean seek. Compute the derivation once per row into `#tmpOrders.ParentOrderId`.

**Issue 3: Repeated `OrderId In (Select OrderId From #tmpPoOrdrId)` subqueries.** Replace with direct joins.

**Issue 4: Mixed-convention temp-table cleanup.** Consolidate.

**Issue 5: No indexes on the three temp tables.**

---

## 5. First Principles

**[[Parameter Sniffing]] with the local-variable form** plus `Option (Recompile)` on the parameter-sensitive INSERTs.

**Constructed-key once per row.** The parent-OrderId derivation (the same pattern as the constructed-sibling-OrderId in Row 22 and the constructed-ParentOrderId in Row 23) is non-SARGable against the source-table index. The fix is to compute the derivation once per row into a typed column on the temp table and join on the typed column. This is the same pattern documented in [[Correlated Subqueries to CTEs]] and recommended for promotion to a new "Constructed-Key Self-Joins" masterclass entry in the Row 22 and Row 23 analyses.

**Direct joins beat IN-subqueries on small candidate sets.** The three `OrderId In (Select OrderId From #tmpPoOrdrId)` subqueries are functionally equivalent to inner joins on the indexed temp table; the join form gives the optimizer more flexibility and produces cleaner plans.

**[[Index Key Columns vs Included Columns]] applied to temp tables.**

---

## 6. Refactor (commented)

The v23 body is reproduced in `Refactored.sql`. Salient blocks below.

**Local variable and consolidated cleanup.**

```sql
Declare @LocalPoNum VarChar(30) = @PoNum
Drop Table If Exists #tmpPoOrdrId, #tmpOrdSecondaryData, #tmpOrders
```

**Indexed temp table for the PO-to-OrderId map.**

```sql
Select OrderId
Into #tmpPoOrdrId
From OeOrderPoNumAssoc With (NoLock)
Where PoNum = @LocalPoNum
Option (Recompile);

Create Index IX_PoOrdrId_OrderId On #tmpPoOrdrId(OrderId)
```

**Parent-OrderId derivation in #tmpOrders.**

```sql
Select
      O.OrderId, O.RxNum, O.Medication, O.HistoryDtTm, O.PatCustId, O.GroupNum
    , [ParentOrderId] = Case
                          When O.OrderId Like '%-[0-9][0-9]' Then Left(O.OrderId, Len(O.OrderId) - 3)
                          Else O.OrderId
                       End
Into #tmpOrders
From OeOrder O With (NoLock)
Inner Join #tmpPoOrdrId P On P.OrderId = O.OrderId
Where O.OrderStatus Not Like 'Split/%'
Option (Recompile);

Insert Into #tmpOrders (...)
Select ..., [ParentOrderId] = Case ... End
From OeOrderHistory O With (NoLock)
Inner Join #tmpPoOrdrId P On P.OrderId = O.OrderId
Where O.OrderStatus <> 'Canceled' And O.OrderStatus Not Like 'Split/%'
Option (Recompile);

Create Index IX_TmpOrders_Lookup On #tmpOrders(OrderId, HistoryDtTm)
   Include (RxNum, Medication, PatCustId, GroupNum, ParentOrderId)
```

**Final SELECT with a clean parent-OrderId equality.**

```sql
Select Distinct
      O.RxNum, O.Medication, O2.CustomerPrice, ...
From #tmpOrders O
Inner Join OeOrderCurrHistoryDtTm CHDT With (NoLock)
    On CHDT.OrderId = O.OrderId And CHDT.HistoryDtTm = O.HistoryDtTm
Inner Join #tmpOrdSecondaryData O2
    On O2.OrderId = O.ParentOrderId
   And O2.HistoryDtTm = O.HistoryDtTm
Inner Join OePatientCustSecondaryData P2 With (NoLock)
    On P2.PatCustId = O.PatCustId
Left Join OeOrderExtUserDef E With (NoLock) On ...
Left Join OeFlaggedRxs F With (NoLock) On ... And F.IsFlaggedFor = 'Diff Drug From Last Fill'
Left Join OeGroupUserDef G2 With (NoLock) On G2.GroupNum = O.GroupNum
Option (Recompile);
```

The `O2.OrderId = O.ParentOrderId` equality replaces the Or-clause join condition.

---

## 7. Risk & Rollback

**What could go wrong.** The parent-OrderId derivation in v23 is logically equivalent to the v22 Or-clause: a multi-vial portion Rx has OrderId of the form `parent-NN`, and the parent OrderId is the first `Len - 3` characters; a single-vial Rx has OrderId without that suffix and uses the OrderId directly. The Case expression in v23 produces the same value the v22 Or-clause expected on each side of the disjunction.

The Recompile hint on the three INSERTs takes a per-call compile cost. The PoNum filter selectivity is what drives the plan volatility; Recompile pays the compile cost once per call and produces a plan matched to the actual candidate-set size.

The Distinct in the final SELECT is preserved as a safety net for duplicate rows arising from the Left Joins. v23 does not attempt to remove the Distinct because the duplicate-row risk depends on the OeGroupUserDef cardinality per GroupNum, which is application-side state.

**What to look for in the first 24 hours.** Plan variant count per site (expected: drop from many to one), max-vs-avg ratio per site (expected to fall substantially from 1,720x), Distinct row count parity with v22, OeOrderSecondaryData read shape (expected: indexed seek on ParentOrderId rather than a scan-or-fallback driven by the Or-clause join condition).

**Rollback path.** The v22 body is in `Original.sql`. Redeploying v22 reverts cleanly.

---

## 8. Evidence of Refactor (v23)

```
(paste STATS IO / STATS TIME for v23 against the same data state used for v22 here)
```

---

## 9. Comparison & Improvement

| Metric | v22 (paste) | v23 (paste) | Delta |
|---|---|---|---|
| Logical reads (total, median run) | | | |
| Logical reads on `OeOrderSecondaryData` | | (expected: substantially lower) | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan variant count across two captures | (expected: 2+) | (expected: 1) | |
| Max-vs-avg ratio (cross-MFC, 30d) | 1,720x | (expected: <50x) | |
| Result row count | | (expected: identical) | |

**Verdict (to be entered once the captures land):** the headline expected delta is the plan-stability improvement plus the clean-equality parent-OrderId join into `OeOrderSecondaryData`.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Non-zero result set. (?)
- Identical result set: same RxNum/Medication/CustomerPrice/flag tuples returned. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles.

---

## 11. Open Items / Future Improvements

### 11.1 Cross-reference to Row 22 and Row 23 on the constructed-key pattern

The constructed parent-OrderId derivation here is the third occurrence of the "constructed-key self-join" pattern in the deep-dive cohort (after Row 22 sibling-OrderId and Row 23 parent-OrderId). The pattern is recurrent enough to warrant its own masterclass entry. The recommendation in Row 22's Section 11 and Row 23's Section 11 is unchanged: write the "Constructed-Key Self-Joins" entry the next time the pattern surfaces in a deep-dive.

### 11.2 Index recommendation on `OeOrderPoNumAssoc(PoNum)`

If the table is keyed on `(OrderId, PoNum)` instead of `(PoNum, OrderId)`, the initial scan has to read the table and filter rather than seek. A nonclustered index on `(PoNum)` with `OrderId` included would let the scan seek directly. Confirm against Tolleson.

### 11.3 Investigate the Distinct in the final SELECT

The `Select Distinct` is a defensive accommodation. If the OeGroupUserDef table has multiple rows per GroupNum (multiple user-def overrides per group), the Distinct collapses them. Confirm whether OeGroupUserDef is unique per GroupNum; if so, the Distinct can be dropped, and the optimizer can avoid the sort.

### 11.4 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution. Two captures at different `@PoNum` selectivities are required to confirm plan stability.
