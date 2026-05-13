# lsp_TcdGetTcdsThatCanCountProduct: Refactor Analysis (v17 to v18)

**Date:** 2026-05-08
**Tracking sheet row:** 30 (Priority P1, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v18 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_TcdGetTcdsThatCanCountProduct`

**Purpose in one line:** Returns the list of online, initialized, non-dry, non-flagged dispensers in a specified bank that can count a specified product, ordered so that dispensers with sufficient hopper quantity come first and within each category dispensers with the least quantity come first.

**Tables touched:**

- `TcdStatus` (the workhorse; main filter for the candidate dispenser set).
- `TcdSecondaryData` (left-joined for OldestLotInitReplenDtTm).
- `TcdDryDispensers` (left-joined for the run-dry exclusion).
- `TcdFlaggedTcds` (left-joined for the count-dry-flag exclusion).
- `CanCanister` (queried in the two correlated Top 1 fallback subqueries in v17).
- `CanLotCode` (joined to `CanCanister` in the fallback subqueries).

**Indexes used (predicted from the v18 body):**

| Table | Index | Where used |
|---|---|---|
| `TcdStatus` | NC on `ProductId, AddrBank` (assumed) | candidate Tcd scan |
| `TcdSecondaryData` | clustered on `TcdSn` (assumed) | left-join |
| `TcdDryDispensers` | clustered on `TcdSn` (assumed) | left-join + NULL probe |
| `TcdFlaggedTcds` | clustered on `TcdSn` (assumed) | left-join + flag-list exclusion |
| `CanCanister` | NC on `TcdSn` (assumed) | lot-aggregate scan |
| `CanLotCode` | NC on `CanisterSn` (assumed) | lot-aggregate join |

**Callers:** the TCD Controller Count Launcher. Called at high frequency (cross-list flags "high-volume / sub-ms") during the count-launch decision loop.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on three of four expense lists (Volume #8, Duration #15, Expense #14) with "high-volume / sub-ms: caching, batching, or memory-optimized table likely worthwhile" plus "algorithmic / plan rewrite candidate" as the dominant heuristics. The procedure is short and structurally simple, but it is called often enough that the per-call cost compounds at scale.

The dominant cost drivers in v17 are three.

The first is the two correlated `Select Top 1` scalar subqueries in the SELECT list. Each one executes per row of the outer query: one for the EarliestExpDate fallback against `CanLotCode.ExpireDate`, one for the OldestLotInitReplenDtTm fallback against `CanLotCode.LastAddDtTm`. Both join `CanCanister` to `CanLotCode` on the same `TcdSn`. Pre-materializing the lot aggregates once per call replaces N pairs of per-row reads with one indexed scan.

The second is the two catch-all parameter predicates. `(T.InvPool = @InvPool Or @InvPool = 0)` and `IsNull(T.LastNdcReplenished, '') = Case When Len(@MatchingNdc) > 0 Then @MatchingNdc Else IsNull(T.LastNdcReplenished, '') End` are the canonical [[Catch-All Query Anti-Pattern]]. Both effectively express "if the parameter is the special sentinel, no filter; otherwise filter by the parameter value." The Case form especially is hard for the optimizer to simplify because the right-hand side is a function of the column being filtered.

The third is the absence of local-variable indirection and `Option (Recompile)`. The procedure carries five parameters and is called at high frequency; a stale cached plan against an unfortunate parameter combination is a real risk.

v18 addresses all three drivers. The Recompile decision is flagged in Section 11 as something to validate against the cost-per-call after deployment.

---

## 3. Evidence of Original (v17)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v17)

```
(paste STATS IO / STATS TIME for v17 against the same data state used for v18 here)
```

The expected v17 signature is one read of `TcdStatus` plus N pairs of `CanCanister + CanLotCode` reads, where N is the candidate-dispenser count.

---

## 4. Issue Identification

**Issue 1: Two correlated Top 1 scalar subqueries in the SELECT list (lines 56-58 and 59-61 in v17).** Per-row execution. Pre-materialize.

**Issue 2: Catch-all parameter predicate `(T.InvPool = @InvPool Or @InvPool = 0)`.** Defeats clean plan caching.

**Issue 3: Catch-all parameter predicate `IsNull(T.LastNdcReplenished, '') = Case When Len(@MatchingNdc) > 0 Then @MatchingNdc Else IsNull(T.LastNdcReplenished, '') End`.** Same anti-pattern, more obscure form.

**Issue 4: No local-variable indirection for any of the five parameters.**

**Issue 5: No indexes on the temp tables (n/a in v17 since there are no temp tables; v18 introduces them and indexes each).**

---

## 5. First Principles

**[[Correlated Subqueries to CTEs]] with the pre-materialize-once fix.** The two Top 1 fallback subqueries are aggregations against the same source pair (`CanCanister + CanLotCode`). One scan with `Min(ExpireDate)` and `Min(LastAddDtTm)` per TcdSn produces both fallback columns in a single pass. The Min() aggregations match the v17 semantics of Top 1 with ascending Order By.

**[[Catch-All Query Anti-Pattern]] with the conditional-predicate fix.** Both catch-all predicates are rewritten as `@sentinel = special_value Or column = @sentinel`. Under `Option (Recompile)`, the optimizer can simplify the disjunction at compile time given the actual parameter values, producing a plan that either includes or excludes the filter rather than evaluating the disjunction per row.

**[[Parameter Sniffing]] with the local-variable form** for all five parameters.

---

## 6. Refactor (commented)

The v18 body is reproduced in `Refactored.sql`. Salient blocks below.

**Candidate-Tcd identification with the conditional catch-all predicates.**

```sql
Select
      T.TcdSn
    , T.AddrCabinet, T.AddrRow, T.AddrSlot, T.TcdModel
    , T.QtyInHopper, T.QtyInBuffer, T.EpromVersion
    , T.EarliestExpDate
    , T.InUse, T.NdcParamChg, T.LocCanModel
    , [TS_OldestLotInitReplenDtTm] = TS.OldestLotInitReplenDtTm
Into #CandidateTcds
From TcdStatus T With (NoLock)
Left Join TcdSecondaryData TS With (NoLock) On TS.TcdSn = T.TcdSn
Left Join TcdDryDispensers TDD With (NoLock) On TDD.TcdSn = T.TcdSn
Left Join TcdFlaggedTcds TFT With (NoLock) On TFT.TcdSn = T.TcdSn
Where T.ProductId    = @LocalProductId
  And T.AddrBank     = @LocalAddrBank
  And T.Status       = 'Online'
  And T.TcdState     = 'Tcd Initialized.'
  And T.LocationState = 'Initialized Location.'
  And TDD.TcdSn Is Null
  And (TFT.IsFlaggedFor Is Null Or TFT.IsFlaggedFor Not In ('Count Dry Unassign', 'Count Dry Inv Count'))
  And (@LocalInvPool = 0 Or T.InvPool = @LocalInvPool)
  And (@LocalMatchingNdc = '' Or IsNull(T.LastNdcReplenished, '') = @LocalMatchingNdc)
Option (Recompile);
```

The two catch-all predicates are rewritten as `sentinel-or-column-equals-param` form, which Recompile can simplify at compile time.

**Pre-materialized lot-aggregate fallbacks.**

```sql
Select
      C.TcdSn
    , [FallbackEarliestExpDate]         = Min(L.ExpireDate)
    , [FallbackOldestLotInitReplenDtTm] = Min(L.LastAddDtTm)
Into #TcdLotFallbacks
From #CandidateTcds CT
Inner Join CanCanister C With (NoLock)
    On C.TcdSn = CT.TcdSn
Inner Join CanLotCode L With (NoLock)
    On L.CanisterSn = C.CanisterSn
Group By C.TcdSn
```

One scan over `CanCanister + CanLotCode` filtered to the candidate Tcds. Both fallback columns produced in one pass.

---

## 7. Risk & Rollback

**What could go wrong.** The Recompile decision is the main consideration. The procedure is called at sub-ms cadence; a 1-2 ms compile overhead can dominate the per-call cost. The local-variable form alone (without Recompile) is the fallback if compile cost is too high.

The `Min(ExpireDate)` and `Min(LastAddDtTm)` aggregates match the v17 `Top 1 ... Order By ExpireDate / LastAddDtTm` semantics. If the v17 subqueries returned a different specific row for tied minimum dates, the v18 form would still return the same minimum value (the Top 1 OrderId would differ between calls but the fallback column is what's projected).

The two catch-all predicate rewrites preserve the same WHERE semantics. The `@LocalInvPool = 0 Or T.InvPool = @LocalInvPool` form is logically equivalent to the v17 `(T.InvPool = @InvPool Or @InvPool = 0)`. The `@LocalMatchingNdc = '' Or IsNull(T.LastNdcReplenished, '') = @LocalMatchingNdc` form is logically equivalent to the v17 Case-form. Both rewrites are safe.

**What to look for in the first 24 hours.** Per-call compile time (expected to be a few hundred microseconds), per-call elapsed time (expected to be the same or lower than v17), plan shape for the candidate-Tcd scan (expected to be a seek on `(ProductId, AddrBank)`), CanCanister read count (expected to drop from N pairs to 1 scan).

**Rollback path.** The v17 body is in `Original.sql`. Redeploying v17 reverts cleanly.

---

## 8. Evidence of Refactor (v18)

```
(paste STATS IO / STATS TIME for v18 against the same data state used for v17 here)
```

---

## 9. Comparison & Improvement

| Metric | v17 (paste) | v18 (paste) | Delta |
|---|---|---|---|
| Logical reads (total) | | | |
| Logical reads on `CanCanister` | | (expected: substantially lower) | |
| Logical reads on `CanLotCode` | | (expected: substantially lower) | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Result row count | | (expected: identical) | |

**Verdict (to be entered once the captures land):** the headline expected delta is the per-row to per-set transformation on the CanCanister/CanLotCode reads. The catch-all predicate rewrite is the secondary win, plan-quality dependent.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Non-zero result set. (?)
- Identical result set: same dispensers returned in same order; same fallback EarliestExpDate and OldestLotInitReplenDtTm values. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles. The Recompile evaluation in post-deployment monitoring is the gating check for the high-volume call pattern.

---

## 11. Open Items / Future Improvements

### 11.1 Validate the Recompile hint against per-call cost

For a sub-millisecond procedure called at high frequency, the compile cost can exceed the plan-quality benefit. The post-deployment monitoring should capture per-call CPU and elapsed time with Recompile enabled and compare against the same metrics without Recompile. The local-variable form alone is the fallback.

### 11.2 Consider memory-optimized table caching for `TcdStatus`

The cross-list capture explicitly mentions "memory-optimized table likely worthwhile." `TcdStatus` is a candidate because it is read at high frequency, has bounded row count (one row per Tcd, in the thousands), and changes at moderate frequency. Moving `TcdStatus` to In-Memory OLTP would eliminate the disk I/O on every call. This is a substantial architectural change requiring DBA evaluation and is documented for the longer-arc backlog.

### 11.3 Filtered index on `TcdStatus(ProductId, AddrBank)` with `Status, TcdState, LocationState` filtered

The candidate-Tcd scan has fixed-value filters on Status, TcdState, and LocationState. A filtered index that pre-filters these would let the scan seek directly into the much smaller "online and ready" subset. This is a Tolleson-first recommendation; evaluate before fleet deployment.

### 11.4 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution.
