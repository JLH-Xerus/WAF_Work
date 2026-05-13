# lsp_GovGetTopPriorityToteGroupsToAllocate: Refactor Analysis (v2 to v3)

**Date:** 2026-05-08
**Tracking sheet row:** 31 (Priority P1, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v3 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_GovGetTopPriorityToteGroupsToAllocate`

**Purpose in one line:** Returns the top-priority tote groups eligible to be allocated from the queued Rx group pool, broken down by order type (single tote, group tote, group vial, single vial), with optional exclusion filters per order-type bucket and per-sorter.

**Tables touched:**

- `OeOrder` (read once to build the awaiting-allocation candidate set, then twice more for the blocking check and the allocated-Rx check).
- `GovRx` (the tote-group association table; joined throughout).
- `vCfgSystemParamVal` and `vCfgComputerParamVal` (config reads).

**Indexes used (predicted from the v3 body):**

| Table | Index | Where used |
|---|---|---|
| `OeOrder` | NC on `OrderStatus, StatusReason` (assumed) | awaiting-allocation candidate scan |
| `OeOrder` | NC on `OrderStatus, StatusReason` (assumed) plus filter on Problem path | blocking-pair scan |
| `GovRx` | clustered on `OrderId, ToteGroupNum` (assumed) | every join |

**Callers:** the Order Governor allocation service. Called on the allocation pass cadence.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on two of four expense lists (Expense #16, Plan #7) with "plan instability" and "algorithmic / plan rewrite candidate" as the dominant heuristics. The procedure carries five exclusion-bit parameters plus a table-valued parameter, and the WHERE-clause shape changes across branches depending on which parameters are set.

The dominant cost drivers in v2 are three.

The first is the duplicated "no blocking Rx" check. The Not Exists subquery on lines 94-106 is reproduced verbatim or nearly verbatim five times across the five Insert blocks. The subquery is approximately 10 lines and does a `GovRx + OeOrder` join with a non-trivial disjunction on order status and pending order status. The work is performed five times in the worst-case path where every exclusion bit is 0 and the procedure runs all five Inserts.

The second is the @GroupsToAllocate table variable. Table variables do not carry meaningful statistics. The final SELECT joins back to @GroupsToAllocate and adds a correlated Exists subquery for HasAllocatedRx, and the optimizer's cardinality estimate for the table variable is 1 row regardless of the actual content. The downstream join order and join method choices are systematically worse than they would be against an equivalent indexed temp table.

The third is the parameter-sniffed plan caching. Five exclusion bits plus a TVP plus two config values produce a large enough state space that a single cached plan is unlikely to be optimal for all branches.

v3 addresses all three drivers.

---

## 3. Evidence of Original (v2)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v2)

```
(paste STATS IO / STATS TIME for v2 against the same data state used for v3 here)
```

The expected v2 signature is up to five executions of the blocking-Rx subquery (one per Insert), plus an additional Exists subquery per row in the final SELECT for HasAllocatedRx.

---

## 4. Issue Identification

**Issue 1: Duplicated Not Exists "blocking Rx" subquery across five Insert blocks.** Identical structure, ~10 lines each.

**Issue 2: `@GroupsToAllocate` table variable.** No statistics; downstream join cardinality estimates are 1.

**Issue 3: Correlated Exists subquery for HasAllocatedRx in the final SELECT.** Per-row execution.

**Issue 4: No local-variable indirection for the bit parameters.**

**Issue 5: No indexes on the temp tables (n/a in v2; v3 introduces #AwaitingAllocationRxs, #BlockedPairs, #GroupsToAllocate, #AllocatedPairs and indexes each).**

---

## 5. First Principles

**[[Correlated Subqueries to CTEs]] with the pre-materialize-once fix for the blocking check.** The Not Exists subquery is structurally identical across all five Insert blocks. Pre-materializing the (GroupNum, ToteGroupNum) pairs that are blocked into `#BlockedPairs` and joining via Left Join + IS NULL probe converts five subquery executions into one set-based scan plus five indexed lookups.

**[[Table Variables vs Temp Tables]] applied to `@GroupsToAllocate`.** The table variable does not carry statistics. The temp-table equivalent `#GroupsToAllocate` with a `(GroupNum, ToteGroupNum)` index supports the final-SELECT join and gives the optimizer realistic cardinality estimates.

**[[Correlated Subqueries to CTEs]] applied to the HasAllocatedRx check.** The Exists subquery in the final SELECT becomes a Left Join to `#AllocatedPairs` with a Case-When-Not-Null projection.

**[[Parameter Sniffing]] with the local-variable form** for the bit parameters.

---

## 6. Refactor (commented)

The v3 body is reproduced in `Refactored.sql`. Salient blocks below.

**Pre-materialized blocking-pair set.**

```sql
Select Distinct
      O.GroupNum
    , G.ToteGroupNum
Into #BlockedPairs
From GovRx G With (NoLock)
Inner Join OeOrder O With (NoLock)
    On O.OrderId = G.OrderId
Where
(
    (O.OrderStatus = 'Pending' And O.StatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping'))
    Or
    (O.OrderStatus = 'Problem' And O.PendingOrderStatus = 'Pending'
        And O.PendingStatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping'))
)
Option (Recompile);

Create Index IX_BP_Pair On #BlockedPairs(GroupNum, ToteGroupNum)
```

One scan over `GovRx + OeOrder` produces every blocking pair. The five Insert blocks below reference `#BlockedPairs` via Left Join + IS NULL.

**Five Inserts use the consolidated blocking set.**

```sql
Insert Into #GroupsToAllocate ...
Select Top (@CfgAllocateXGroupsEachPass) ...
From #AwaitingAllocationRxs A
Inner Join GovRx G With (NoLock) On G.OrderId = A.OrderId
Left Join #BlockedPairs BP On BP.GroupNum = A.GroupNum And BP.ToteGroupNum = G.ToteGroupNum
Where G.OrderTypeCode = 'ST'
  And BP.GroupNum Is Null
...
Option (Recompile);
```

The Left Join + IS NULL probe is cheap because `#BlockedPairs` has the supporting index.

**Pre-materialized allocated-pair set replaces the correlated Exists.**

```sql
Select Distinct
      O.GroupNum, G.ToteGroupNum
Into #AllocatedPairs
From OeOrder O With (NoLock)
Inner Join GovRx G With (NoLock) On G.OrderId = O.OrderId
Inner Join #GroupsToAllocate A On A.GroupNum = O.GroupNum And A.ToteGroupNum = G.ToteGroupNum
Where G.RxCvyStateCode = '0'
Option (Recompile);
```

The final SELECT then Left-Joins `#AllocatedPairs` and projects `Case When AP.GroupNum Is Not Null Then 1 Else 0 End` for `HasAllocatedRx`.

---

## 7. Risk & Rollback

**What could go wrong.** The `#BlockedPairs` set is the central semantic replacement. The set is computed by scanning all rows that match the blocking criteria; the v2 Not Exists subquery checked per outer pair whether any blocking Rx exists for that pair. The two forms are logically equivalent: a pair is blocked iff at least one blocking Rx exists for it. The Distinct in the v3 SELECT removes duplicate pair entries that appear when multiple Rxs in the same pair satisfy the blocking criteria.

The `#AllocatedPairs` set similarly replaces the Exists subquery with a Left Join. The `RxCvyStateCode = '0'` filter is preserved.

The temp-table conversion from `@GroupsToAllocate` to `#GroupsToAllocate` is semantically equivalent; the only behavioral change is that the temp table carries statistics, which lets the optimizer plan the downstream join more accurately.

**What to look for in the first 24 hours.** Plan variant count per site, executions of the blocking subquery (expected: 1 per call instead of 5), final-SELECT operator count (expected: simpler with the Left Join replacing the correlated Exists).

**Rollback path.** The v2 body is in `Original.sql`. Redeploying v2 reverts cleanly.

---

## 8. Evidence of Refactor (v3)

```
(paste STATS IO / STATS TIME for v3 against the same data state used for v2 here)
```

---

## 9. Comparison & Improvement

| Metric | v2 (paste) | v3 (paste) | Delta |
|---|---|---|---|
| Logical reads (total) | | | |
| Logical reads on `OeOrder` | | (expected: substantially lower) | |
| Logical reads on `GovRx` | | (expected: lower) | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan operator count on final SELECT | | (expected: simpler) | |
| Plan variant count across captures | (expected: 2+) | (expected: 1) | |
| Result row count and HasAllocatedRx values | | (expected: identical) | |

**Verdict (to be entered once the captures land):** the headline expected delta is the consolidated blocking-pair check and the table-variable to temp-table conversion. The procedure does meaningful work; the v3 changes are structural rather than dramatic, but the cross-list "plan instability" classification should improve materially.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Non-zero result set. (?)
- Identical result set: same (GroupNum, ToteGroupNum, OrderTypeCode, SorterId) tuples returned with identical NumOfItems, PriInternal, and HasAllocatedRx values. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles.

---

## 11. Open Items / Future Improvements

### 11.1 Index recommendation on `OeOrder(OrderStatus, StatusReason)` filtered

The blocking-pair scan filters by `(OrderStatus = 'Pending' And StatusReason In (...)) Or (OrderStatus = 'Problem' And ...)`. A filtered index that covers the most common state would let the scan seek directly. Recommended DDL is Tolleson-evaluated first.

### 11.2 Consider whether the five Insert blocks can share more structure

The five Inserts differ in OrderTypeCode filter, NumOfItems calculation, SorterId projection, and Group By shape. v3 keeps them separate because consolidating them via CASE-driven shapes would defeat the optimizer's ability to plan each branch independently. A future evaluation could prototype a single-Insert form and compare; v3 takes the conservative choice.

### 11.3 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution.
