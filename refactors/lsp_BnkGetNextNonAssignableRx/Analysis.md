# lsp_BnkGetNextNonAssignableRx: Refactor Analysis (v11 to v12)

**Date:** 2026-05-08
**Tracking sheet row:** 36 (Priority P2, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v12 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_BnkGetNextNonAssignableRx`

**Purpose in one line:** Returns the top-priority Bank-Unassigned auto-fill Rx that cannot be assigned because either its product is not in any qualifying dispenser or its product is only in excluded banks.

**Tables touched:**

- `OeOrder` (the Bank-Unassigned Rx candidate set).
- `OeOrderSecondaryData` (joined for RequestedFillMethodCode).
- `TcdStatus` (the product-to-bank-dispenser mapping; queried twice in v11, consolidated to once in v12).
- `BnkConfiguredBanks` (joined for the IsExcluded flag).
- `BnkAllBanks` (joined for AutoFillTypeCode, which maps to SupportedFillMethod).

**Indexes used (predicted from the v12 body):**

| Table | Index | Where used |
|---|---|---|
| `OeOrder` | NC on `OrderStatus, FillType, AddrBank` | candidate scan |
| `OeOrderSecondaryData` | clustered on `OrderId, HistoryDtTm` | secondary-data join |
| `TcdStatus` | NC on `ProductId` (assumed) | product-to-bank map |
| `BnkConfiguredBanks` | clustered on `AddrBank` (assumed) | exclusion lookup |
| `BnkAllBanks` | clustered on `AddrBank` (assumed) | fill-method lookup |

**Callers:** the bank-assignment service. Called when a Bank-Unassigned Rx is detected as un-assignable, to surface it on the exception screen.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on two of four expense lists (Duration #14, Expense #21) with "algorithmic / plan rewrite candidate" as the dominant heuristic. The procedure carries no input parameters, so parameter sniffing is not the issue.

The dominant cost driver in v11 is the duplicated scan of `TcdStatus + BnkConfiguredBanks + BnkAllBanks` for `#IncludedBankProducts` and `#ExcludedBankProducts`. The two SELECTs are identical aside from the `B.IsExcluded` filter. A single scan with the IsExcluded column projected lets both downstream consumers reference the same temp table, filtered as needed.

A secondary observation is the UNION (with implicit DISTINCT) on the final SELECT. The two source sets (`#RxsNotInAnyBank` and `#RxsOnlyInExclBank`) are disjoint by construction. UNION ALL is semantically equivalent and avoids the DISTINCT overhead.

---

## 3. Evidence of Original (v11)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v11)

```
(paste STATS IO / STATS TIME for v11 against the same data state used for v12 here)
```

---

## 4. Issue Identification

**Issue 1: Duplicated scan of `TcdStatus + BnkConfiguredBanks + BnkAllBanks`.** Two near-identical SELECTs differ only in `B.IsExcluded`. Consolidate.

**Issue 2: No indexes on intermediate temp tables.** Several participate in downstream joins.

**Issue 3: UNION (with DISTINCT) on disjoint source sets.** UNION ALL avoids the implicit DISTINCT and is semantically equivalent.

**Issue 4: No Recompile hints.** Plan quality benefits from compile-time visibility of current temp-table cardinalities.

---

## 5. First Principles

**[[Conditional Aggregation Consolidation]] applied to the product-bank scans.** The two near-identical SELECTs are consolidated into one scan that projects `IsExcluded` as a column. Both downstream consumers reference the consolidated temp table with the appropriate `IsExcluded` filter on the join.

**UNION ALL on disjoint source sets.** When two row sets are disjoint by construction, UNION ALL is the right choice. UNION-with-DISTINCT only matters when duplicates can occur.

**[[Index Key Columns vs Included Columns]] applied to temp tables.**

---

## 6. Refactor (commented)

The v12 body is reproduced in `Refactored.sql`. Salient blocks below.

**Consolidated #BankProducts.**

```sql
Select
      T.ProductId
    , [SupportedFillMethod] = Case BA.AutoFillTypeCode When 'O' Then 'A' When 'H' Then 'S' Else Null End
    , T.AddrBank
    , [IsExcluded] = B.IsExcluded
Into #BankProducts
From TcdStatus T With (NoLock)
Inner Join BnkConfiguredBanks B With (NoLock) On T.AddrBank = B.AddrBank
Inner Join BnkAllBanks BA With (NoLock) On BA.AddrBank = T.AddrBank
Where T.ProductId Is Not Null
Group By T.ProductId, T.AddrBank, BA.AutoFillTypeCode, B.IsExcluded
Option (Recompile);

Create Index IX_BP_Product On #BankProducts(ProductId) Include (SupportedFillMethod, AddrBank, IsExcluded)
```

The #RxsOnlyInExclBank step then references `#BankProducts` twice: once filtered to `IsExcluded = 1` and once filtered to `IsExcluded = 0`, in two separate Left Joins.

**UNION ALL replaces UNION on the final SELECT.**

```sql
Select Top 1 OrderId, NonAssignableReason, PriInternal
From
(
    Select OrderId, NonAssignableReason, PriInternal From #RxsNotInAnyBank
    Union All
    Select OrderId, NonAssignableReason, PriInternal From #RxsOnlyInExclBank
) AS NonAssignable
Order By PriInternal, OrderId
Option (Recompile);
```

---

## 7. Risk & Rollback

**What could go wrong.** The consolidated `#BankProducts` produces one row per (ProductId, AddrBank, AutoFillTypeCode, IsExcluded) combination. The original v11 had separate rows in #IncludedBankProducts (IsExcluded = 0) and #ExcludedBankProducts (IsExcluded = 1); v12's consolidated table carries both via the IsExcluded column. The two downstream Left Joins to `#BankProducts` filtered by IsExcluded produce the same row sets v11 produced from the separate temp tables.

The UNION ALL substitution is safe because the two source sets are disjoint: an Rx is in #RxsNotInAnyBank iff its product has no row in #RxsInAnyBank (the TcdSn IS NULL probe), and in #RxsOnlyInExclBank iff its product has at least one matching excluded-bank entry and zero matching included-bank entries with the right fill method. The two conditions are mutually exclusive, so UNION ALL produces the same result as UNION.

**What to look for in the first 24 hours.** Plan operator count drop (one TcdStatus+BnkConfiguredBanks+BnkAllBanks scan instead of two), result identity parity with v11, elapsed time drop.

**Rollback path.** The v11 body is in `Original.sql`. Redeploying v11 reverts cleanly.

---

## 8. Evidence of Refactor (v12)

```
(paste STATS IO / STATS TIME for v12 against the same data state used for v11 here)
```

---

## 9. Comparison & Improvement

| Metric | v11 (paste) | v12 (paste) | Delta |
|---|---|---|---|
| Logical reads on `TcdStatus` | | (expected: ~50% of v11) | |
| Logical reads on `BnkConfiguredBanks` | | (expected: ~50% of v11) | |
| Logical reads on `BnkAllBanks` | | (expected: ~50% of v11) | |
| Plan operator count | | (expected: lower) | |
| Result row count | | (expected: identical) | |

**Verdict (to be entered once the captures land):** the headline expected delta is the consolidation of the duplicated three-table scan and the elimination of the UNION DISTINCT overhead.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Non-zero result set (when a non-assignable Rx exists). (?)
- Identical result set: same OrderId/NonAssignableReason/PriInternal as v11. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles.

---

## 11. Open Items / Future Improvements

### 11.1 Filtered index on `BnkConfiguredBanks(IsExcluded)`

The `#BankProducts` scan filters on `B.IsExcluded` implicitly (via the row's value). A filtered index on `BnkConfiguredBanks(AddrBank) Where IsExcluded = 0` would be useful in other Bnk-family procedures; consider as part of an architectural index review for the Bnk family.

### 11.2 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution.
