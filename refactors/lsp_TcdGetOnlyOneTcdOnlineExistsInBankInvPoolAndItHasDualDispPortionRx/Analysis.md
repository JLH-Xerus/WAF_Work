# lsp_TcdGetOnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx: Refactor Analysis

**Date:** 2026-05-08
**Tracking sheet row:** 39 (Priority P2, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed refactor designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_TcdGetOnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx`

**Purpose in one line:** Sets an OUTPUT bit to 1 when exactly one online in-cabinet dispenser exists for a given product, bank, and inventory pool, and that single dispenser currently holds a dual-dispenser portion Rx (OrderId matching `%<[1-2]>`).

**Tables touched:**

- `TcdStatus` (the only table; queried once per call).

**Indexes used (predicted):**

| Table | Index | Where used |
|---|---|---|
| `TcdStatus` | NC on `AddrBank, ProductId, Status` (assumed) | the candidate-dispenser scan |

**Callers:** the bank-assignment service. Called once per "is this the only-one-with-dual-portion edge case" question, in a high-frequency loop alongside Row 30 (`lsp_TcdGetTcdsThatCanCountProduct`) and other Tcd-family procedures.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on two of four expense lists (Volume #5, Duration #19) with "high-volume / sub-ms: caching, batching, or memory-optimized table likely worthwhile" plus "algorithmic / plan rewrite candidate" as the dominant heuristics. The Volume #5 ranking is notable: this is the fifth-most-frequently-called procedure in the cohort.

The dominant cost drivers in v_old are three. None of them is dramatic in isolation; together they compound on the high call frequency.

The first is the temp-table-and-recheck pattern. The procedure does a `SELECT INTO #OrderIdsInOnlineTcdsInBankInvPool` to populate a temp table, then reads `@@RowCount` to decide if exactly one row was returned, then does a separate `Select OrderId From #OrderIdsInOnlineTcdsInBankInvPool` to read the single value. Three operations where one suffices: a single `SELECT Count(*), Max(OrderId) Into @local_vars` returns both values in one pass and keeps the work in-memory.

The second is the catch-all `(InvPool = @InvPool Or @InvPool = 0)` predicate. The same pattern surfaced in Row 30. Rewriting as `(@LocalInvPool = 0 Or InvPool = @LocalInvPool)` under `Option (Recompile)` lets the optimizer simplify the disjunction at compile time given the actual @InvPool value.

The third is the legacy `If Object_Id('TempDb..#X') Is Not Null Drop Table #X` block. v_next removes the temp table entirely, so the cleanup block goes with it.

---

## 3. Evidence of Original

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run

```
(paste STATS IO / STATS TIME for v_old against the same data state used for v_next here)
```

The expected v_old signature is two `TcdStatus` reads on every call (the SELECT INTO scan plus the temp-table re-read for the single OrderId), with the temp-table operations adding tempdb allocation overhead.

---

## 4. Issue Identification

**Issue 1: Temp-table-and-recheck pattern.** The procedure could express the entire check in-memory: one SELECT into two local variables (count and Max(OrderId)) plus an IF on the locals.

**Issue 2: Catch-all `(InvPool = @InvPool Or @InvPool = 0)` predicate.** Same as Row 30; rewrite as conditional shape under Recompile.

**Issue 3: Legacy `Object_Id` temp-table check.** Removed entirely in v_next because no temp table is needed.

**Issue 4: No local-variable indirection for the three input parameters.** Program convention.

---

## 5. First Principles

**In-memory scalar evaluation when the temp table holds at most one row.** A temp table that is only checked for "did it have one row" and "what was the value in that one row" is over-engineered for a scalar evaluation. The condensed form (one SELECT into two local variables, one IF on the locals) is structurally simpler and avoids the tempdb allocation entirely.

**[[Catch-All Query Anti-Pattern]] with the conditional-predicate fix.** Same pattern as Row 30. Recompile lets the optimizer simplify the disjunction at compile time.

**[[Parameter Sniffing]] with the local-variable form** for the three input parameters.

---

## 6. Refactor (commented)

The v_next body is reproduced in `Refactored.sql`. Salient block below.

```sql
Declare @MatchCount    Int
Declare @SingleOrderId VarChar(30)

Select
      @MatchCount    = Count(*)
    , @SingleOrderId = Max(OrderId)
From TcdStatus With (NoLock)
Where AddrBank    = @LocalAddrBank
  And ProductId   = @LocalProductId
  And AddrCabinet > 0
  And TcdSn       > 0
  And Status      = 'Online'
  And (@LocalInvPool = 0 Or InvPool = @LocalInvPool)
Option (Recompile);

If @MatchCount = 1 And @SingleOrderId Like '%<[1-2]>'
    Set @OnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx = 1
Else
    Set @OnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx = 0
```

One scan, two local variables, one IF. No temp table.

---

## 7. Risk & Rollback

**What could go wrong.** The Max(OrderId) projection is semantically meaningful only when MatchCount = 1 (the case where exactly one matching row exists). When MatchCount = 0, Max(OrderId) is NULL and the IF condition's `@SingleOrderId Like '%<[1-2]>'` evaluates to UNKNOWN, which is treated as false in the IF, so the OUTPUT is correctly set to 0. When MatchCount > 1, Max(OrderId) returns some OrderId, but the @MatchCount = 1 check fails first and the OUTPUT is correctly set to 0. The semantics are preserved across all three cases.

The catch-all predicate rewrite preserves the @InvPool = 0 sentinel behavior under Recompile: the optimizer sees the actual @LocalInvPool value at compile time and can simplify either branch of the disjunction.

**What to look for in the first 24 hours.** OUTPUT value parity with v_old, elapsed time per call (expected: lower because tempdb allocation is eliminated), plan operator count.

**Rollback path.** The v_old body is in `Original.sql`. Redeploying v_old reverts cleanly.

---

## 8. Evidence of Refactor

```
(paste STATS IO / STATS TIME for v_next against the same data state used for v_old here)
```

---

## 9. Comparison & Improvement

| Metric | v_old (paste) | v_next (paste) | Delta |
|---|---|---|---|
| Logical reads on `TcdStatus` | | (expected: ~50% of v_old; one scan instead of two) | |
| tempdb allocations per call | 1 | 0 | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| OUTPUT value parity | | (expected: identical) | |

**Verdict (to be entered once the captures land):** the headline expected delta is the elimination of the temp table and the second TcdStatus read. The procedure is short and frequent; the per-call improvement compounds at scale.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Identical OUTPUT bit across all input combinations. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles. Per-MFC monitoring is appropriate given the Volume #5 ranking.

---

## 11. Open Items / Future Improvements

### 11.1 Memory-optimized table for `TcdStatus`

The cross-list capture flags this procedure (and Row 30) with "memory-optimized table likely worthwhile." `TcdStatus` is a small, high-read, moderate-write table. Moving it to In-Memory OLTP would eliminate disk I/O on every call for both Row 30 and Row 39 and several other Tcd-family procedures. This is a substantial architectural change documented in the longer-arc backlog.

### 11.2 Filtered index on `TcdStatus` for the online-in-cabinet subset

The candidate-dispenser scan filters on `AddrCabinet > 0 And TcdSn > 0 And Status = 'Online'`. A filtered index over the "online and in-cabinet" subset, keyed by `(AddrBank, ProductId)` with `InvPool, OrderId` included, would let the scan seek directly. Confirm against Tolleson.

### 11.3 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution.
