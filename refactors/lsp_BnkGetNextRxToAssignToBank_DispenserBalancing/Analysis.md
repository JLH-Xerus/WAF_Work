# lsp_BnkGetNextRxToAssignToBank_DispenserBalancing: Refactor Analysis (v9 to v10)

**Date:** 2026-05-08
**Tracking sheet row:** 28 (Priority P1, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v10 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_BnkGetNextRxToAssignToBank_DispenserBalancing`

**Purpose in one line:** In Dispenser Balancing assignment mode, returns the next unassigned auto-fill Rx and the best-fit cabinet bank for it, choosing the bank by available room per online dispenser for the Rx's product.

**Tables touched:**

- `TcdStatus` (queried for the count of Online dispensers per bank and product).
- `BnkConfiguredBanks` (joined for the non-excluded-bank filter).
- `OeOrder` (queried twice: once for the assigned-Rx counts per bank, once for the unassigned Rx candidate set).
- `BnkAllBanks` (joined for the bank-level fill-method code).
- `OeOrderSecondaryData` (joined in the final SELECT for the requested fill method).

**Indexes used (predicted from the v10 body):**

| Table | Index | Where used |
|---|---|---|
| `TcdStatus` | NC on `Status, ProductId` (assumed) | online-dispenser count per bank/product |
| `BnkConfiguredBanks` | clustered on `AddrBank` (assumed) | configured-bank filter |
| `OeOrder` | NC on `AddrBank, ProductId` (assumed) | assigned-Rx counts |
| `OeOrder` | NC on `OrderStatus, FillType, AddrBank` (`ByOrderStatusFillTypeAddrBank`) | unassigned-Rx candidates |
| `BnkAllBanks` | clustered on `AddrBank` (assumed) | bank-level fill method |
| `OeOrderSecondaryData` | clustered on `OrderId, HistoryDtTm` (assumed) | requested-fill-method join |

**Index DDL gap.** The Bnk-family tables and `OeOrderSecondaryData` are not in the pilot index extract. Confirm against Tolleson before deployment.

**Callers:** the bank-assignment service when running in Dispenser Balancing mode. Called once per "what should I assign next" question.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on three of four expense lists (Volume #23, Duration #9, Expense #10). The cross-list summary identifies "heavy logical reads" as the dominant heuristic, with the recommended fixes being index review, predicate pushdown, and statistics refresh. This is a different signature from the plan-instability procedures that dominated the P0 cohort.

The dominant cost drivers in v9 are three.

The first is that the four intermediate temp tables (`#OnlineTcdCountPerBankProd`, `#QueuedAssignedRxCountPerBank`, `#QueuedUnassignedRxs`, `#BankProdHavingRoomForQueuedInBankRx`) carry no indexes. Each one participates in a downstream join, and the optimizer has to hash-join into the joined relation because the temp tables are unstructured heaps. The cost is small in absolute terms for any single call because the candidate sets are bounded, but it is paid on every call.

The second is the mixed-convention temp-table cleanup (two `If Object_Id` legacy blocks and two `Drop Table If Exists` modern statements). The cost is cosmetic; the consistency cleanup is a low-effort improvement.

The third is the absence of `Option (Recompile)` on the final SELECT, which uses the parameter `@NumOfQueuedInBankRxsPerTcd` in the arithmetic that filters `#BankProdHavingRoomForQueuedInBankRx`. The parameter value affects the optimizer's row-count estimate for the temp table; a cached plan against a stale estimate can pick a suboptimal join order.

v10 addresses all three drivers and adds local variables for consistency with the program convention.

---

## 3. Evidence of Original (v9)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

**Roll-up across reporting sites:** to be filled in from the May 7 capture. Volume #23, Duration #9, Expense #10.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v9)

```
(paste STATS IO / STATS TIME for v9 against the same data state used for v10 here)
```

The expected v9 signature is one read of `TcdStatus`, two reads of `OeOrder` (one for the assigned-count aggregate, one for the unassigned-candidate scan), one read of `BnkAllBanks`, and a hash-join shape across the four temp tables in the final SELECT.

---

## 4. Issue Identification

**Issue 1: No indexes on the four intermediate temp tables.** All four participate in downstream joins. The optimizer cannot seek and is forced into hash joins even on bounded sets.

**Issue 2: Mixed-convention temp-table cleanup.** Two `If Object_Id` legacy blocks and two `Drop Table If Exists` modern statements. Consolidate.

**Issue 3: No local-variable indirection for `@NumOfQueuedInBankRxsPerTcd`.** Low impact; parameter sniffing is not the dominant cost driver, but the local-variable form is the program convention.

**Issue 4: No `Option (Recompile)` on the final SELECT.** The parameter value drives the row-count estimate for `#BankProdHavingRoomForQueuedInBankRx`; the optimizer benefits from accurate estimates at compile time.

**Issue 5: The `BnkAllBanks` join is written in non-aliased positional form (`join BnkAllBanks BA on D.AddrBank = BA.AddrBank`).** The procedure-wide convention uses `Inner Join ... On` with consistent capitalization; the positional shape is a style inconsistency.

---

## 5. First Principles

**[[Index Key Columns vs Included Columns]] applied to temp tables.** Indexing intermediate temp tables that participate in downstream joins is one of the cheapest wins available in T-SQL. v10 adds an explicit index after each temp-table population, sized to support the specific join the temp table will be subjected to.

**[[Parameter Sniffing]] with the local-variable form.** Low-impact on this procedure but consistent with the program convention.

**[[TOP with ORDER BY Semantics]] applied via Recompile.** The final SELECT uses `Top 1 ... Order By ...`. Recompile gives the optimizer the actual selectivity at compile time, which helps rowgoal apply to the underlying scan.

---

## 6. Refactor (commented)

The v10 body is reproduced in `Refactored.sql`. Salient blocks below.

**Local variable and consolidated cleanup.**

```sql
Declare @LocalNumOfQueuedInBankRxsPerTcd Decimal(2,1) = @NumOfQueuedInBankRxsPerTcd

Drop Table If Exists
      #OnlineTcdCountPerBankProd
    , #QueuedAssignedRxCountPerBank
    , #QueuedUnassignedRxs
    , #BankProdHavingRoomForQueuedInBankRx
```

**Indexes on every temp table.**

```sql
Create Index IX_OTC_BankProd On #OnlineTcdCountPerBankProd(AddrBank, ProductId)
Create Index IX_QAR_BankProd On #QueuedAssignedRxCountPerBank(AddrBank, ProductId)
Create Index IX_QUR_Product On #QueuedUnassignedRxs(ProductId, NumDispenses)
   Include (OrderId, HistoryDtTm, PriInternal)
Create Index IX_BPR_Product On #BankProdHavingRoomForQueuedInBankRx(ProductId, OnlineTcdCount)
   Include (AddrBank, RoomForMoreRxCount, SupportedFillMethod)
```

Each index is keyed by the columns the downstream join uses for equality, with Include columns added where the join projects additional values.

**Cleaned-up final SELECT with Recompile.**

```sql
Select Top 1
      O.OrderId As TopPriBankUnassignedRxOrderIdWithRoomInBank
    , P.AddrBank As MostRoomBank
From #BankProdHavingRoomForQueuedInBankRx P
Inner Join #QueuedUnassignedRxs O
    On O.ProductId = P.ProductId
   And P.OnlineTcdCount >= O.NumDispenses
Inner Join OeOrderSecondaryData O2 With (NoLock)
    On O2.OrderId = O.OrderId
   And O2.HistoryDtTm = O.HistoryDtTm
Where (O2.RequestedFillMethodCode = P.SupportedFillMethod Or O2.RequestedFillMethodCode Is Null)
Order By O.PriInternal, O.OrderId, P.RoomForMoreRxCount Desc
Option (Recompile);
```

---

## 7. Risk & Rollback

**What could go wrong.** The temp-table indexes change the optimizer's join order options. The expected change is from hash joins to nested-loop joins driven by the indexed temp tables, which is favorable for the small candidate sets typical of this procedure. If the candidate set is unexpectedly large at some MFC (for example, after a long Dispenser Balancing outage where many Rxs accumulate as unassigned), the nested-loop choice can be slower than hash; in that case the optimizer should still choose hash because the temp-table statistics will reflect the actual size. The Recompile hint ensures the optimizer sees accurate statistics on each call.

**What to look for in the first 24 hours.** Plan shape for the final SELECT (expected: nested-loop or merge join into the indexed temp tables), elapsed time per call (expected: small reduction), result-set identity parity with v9.

**Rollback path.** The v9 body is in `Original.sql`. Redeploying v9 reverts cleanly.

---

## 8. Evidence of Refactor (v10)

```
(paste STATS IO / STATS TIME for v10 against the same data state used for v9 here)
```

---

## 9. Comparison & Improvement

| Metric | v9 (paste) | v10 (paste) | Delta |
|---|---|---|---|
| Logical reads (total) | | | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan shape on final SELECT | hash join | (expected: nested-loop or merge) | |
| Result row count | | | |

**Verdict (to be entered once the captures land):** the headline expected delta is the join-shape change driven by the indexed temp tables. The improvement is structural and bounded by the candidate-set size.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Non-zero result set. (?)
- Identical result set: same TopPriBankUnassignedRxOrderIdWithRoomInBank and MostRoomBank for the same input. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles. Low absolute-cost improvement per call, multiplied by the call volume on this procedure.

---

## 11. Open Items / Future Improvements

### 11.1 Confirm `OeOrder` index coverage for the two scans

The two `OeOrder` scans (assigned-count aggregate and unassigned-candidate scan) filter by different combinations of `AddrBank`, `FillType`, and `OrderStatus`. The pilot index extract confirms that `ByOrderStatusFillTypeAddrBank` exists; that index should serve both scans. If the post-deployment plan shows table scans rather than seeks, the index inventory should be re-confirmed.

### 11.2 Statistics refresh on `OeOrder.AddrBank`

The cross-list capture's "heavy logical reads" heuristic recommends statistics refresh. The candidate-set sizes here depend on the distribution of Rxs across banks. If statistics on `AddrBank` are stale, the optimizer's cardinality estimates will be off and the join order may be suboptimal. A statistics refresh on `OeOrder` (or on the relevant columns) is a low-cost operational step that should be confirmed as current before the v10 plan-shape evaluation.

### 11.3 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution.
