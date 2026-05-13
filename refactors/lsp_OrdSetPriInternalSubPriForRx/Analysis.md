# lsp_OrdSetPriInternalSubPriForRx: Refactor Analysis (v1 to v2)

**Date:** 2026-05-08
**Tracking sheet row:** 37 (Priority P2, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v2 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_OrdSetPriInternalSubPriForRx`

**Purpose in one line:** Appends a six-digit decimal sub-priority to the end of an Rx's `PriInternal` so that Rxs in different groups with otherwise identical priorities can be ordered deterministically.

**Tables touched:**

- `OeOrder` (read three times in v1, written once; consolidated to one read and one write in v2).
- `vCfgSystemParamVal` (config read for `MaxNumEnabledPriorityPreferences`).
- `EvtRxPrioritizationEvents` (insert target for the event log).

**Indexes used (predicted from the v2 body):**

| Table | Index | Where used |
|---|---|---|
| `OeOrder` | `PK_OeOrder` (clustered) | initial single-row read, the UPDATE |
| `OeOrder` | NC on `GroupNum` (assumed) | the same-group Rx scan |
| `OeOrder` | NC on `DateEntered` (assumed) | the count-based fallback scan |
| `vCfgSystemParamVal` | base-table indexes | config read |

**Callers:** the order-prioritization service, called when a new Rx is created and needs its sub-priority assigned.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on two of four expense lists (Duration #20, Expense #22) with "algorithmic / plan rewrite candidate" as the dominant heuristic.

The dominant cost drivers in v1 are three.

The first is the three reads of `OeOrder` for the same OrderId: one for the length check (line 52), one for FIFO/LIFO sequence and GroupNum and DateEntered (lines 58-65), and one in the event-log INSERT after the UPDATE (lines 112-114). The three reads return the same row in different states (pre-update for the first two, post-update for the third). v2 consolidates the first two into a single read at the top and computes the post-update PriInternal in-memory by concatenating `@PriInternal + @SubPriority`, eliminating the third read.

The second is the `PriInternal Like '%' + @FifoLifoSeq + '%'` pattern, which appears twice (line 74 and line 85). The leading wildcard defeats any clean index seek on PriInternal. This is unavoidable without a schema change (a persisted computed column on the FIFO/LIFO substring would let the LIKE become an equality on the typed column). v2 preserves the pattern and adds `Option (Recompile)` so the optimizer compiles against the actual selectivity for the current FIFO/LIFO substring; Section 11 carries the schema-change recommendation.

The third is the absence of local-variable indirection. Low impact because there is one parameter, but consistent with the program convention.

---

## 3. Evidence of Original (v1)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v1)

```
(paste STATS IO / STATS TIME for v1 against the same data state used for v2 here)
```

---

## 4. Issue Identification

**Issue 1: Three reads of `OeOrder` for the same OrderId.** Consolidate to one initial read; compute the post-update PriInternal in-memory.

**Issue 2: `PriInternal Like '%' + @FifoLifoSeq + '%'`** (two occurrences). Non-SARGable. Preserve and recompile.

**Issue 3: No local-variable indirection.** Program convention.

---

## 5. First Principles

**Single-read consolidation.** When a procedure reads the same row multiple times in different parts of its body, the consolidation is straightforward: read once into local variables and reference the locals. v2 also computes the post-update PriInternal in-memory rather than re-reading it after the UPDATE.

**[[Non-SARGable Predicates]] with the recompile-for-current-selectivity fallback.** The `LIKE '%' + variable + '%'` pattern is fundamentally non-SARGable. Recompile gives the optimizer the actual variable value at compile time so it can produce a plan based on the actual selectivity rather than a default density estimate. The deeper fix (a persisted computed column on the FIFO/LIFO substring) is a schema change documented in Section 11.

**[[Parameter Sniffing]] with the local-variable form.** Low-impact safeguard.

---

## 6. Refactor (commented)

The v2 body is reproduced in `Refactored.sql`. Salient blocks below.

**Consolidated initial read.**

```sql
Declare @PriInternal     VarChar(50)
Declare @FifoLifoSeq     VarChar(14)
Declare @GroupNum        VarChar(10)
Declare @DateEntered     DateTime
Declare @PriInternalLen  Int

Select
      @PriInternal    = PriInternal
    , @PriInternalLen = Len(PriInternal)
    , @FifoLifoSeq    = SubString(PriInternal, 5, 14)
    , @GroupNum       = GroupNum
    , @DateEntered    = DateEntered
From OeOrder With (NoLock)
Where OrderId = @LocalOrderId

If @PriInternalLen > @PriInternalLengthWithoutSubPri
    Return 0
```

The early-return short-circuit comes immediately after the single read.

**In-memory post-update PriInternal.**

```sql
Declare @NewPriInternal VarChar(56) = @PriInternal + @SubPriority

Update OeOrder Set PriInternal = @NewPriInternal Where OrderId = @LocalOrderId;

Insert Into EvtRxPrioritizationEvents (..., OrderId, PriInternal)
Values (..., @LocalOrderId, @NewPriInternal);
```

The event-log INSERT uses the in-memory @NewPriInternal rather than re-reading from `OeOrder`.

---

## 7. Risk & Rollback

**What could go wrong.** The consolidated read is semantically equivalent to the three v1 reads as long as no other process modifies the same OeOrder row between the v1 reads. The v1 reads with NoLock would be subject to the same race, so v2 does not introduce new race exposure.

The in-memory @NewPriInternal computation is the same value the v1 third read would have returned (the UPDATE wrote exactly `PriInternal + @SubPriority`, and the post-update read would return that value).

The Recompile hints on the two non-SARGable scans add a per-call compile cost. The procedure is called per new Rx (a moderate frequency) and the compile cost is bounded.

**What to look for in the first 24 hours.** Per-call elapsed time, post-update PriInternal value parity with v1, event-log row count parity (one EvtRxPrioritizationEvents row per call).

**Rollback path.** The v1 body is in `Original.sql`. Redeploying v1 reverts cleanly.

---

## 8. Evidence of Refactor (v2)

```
(paste STATS IO / STATS TIME for v2 against the same data state used for v1 here)
```

---

## 9. Comparison & Improvement

| Metric | v1 (paste) | v2 (paste) | Delta |
|---|---|---|---|
| Logical reads on `OeOrder` | | (expected: ~67% of v1; one initial read eliminated, one post-update read eliminated) | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan operator count | | | |
| PriInternal value written | | (expected: identical) | |

**Verdict (to be entered once the captures land):** the headline expected delta is the elimination of two of the three OeOrder reads.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Identical UPDATE result: same PriInternal value written. (?)
- Identical event log: one EvtRxPrioritizationEvents row per call with the same PriInternal. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles.

---

## 11. Open Items / Future Improvements

### 11.1 Persisted computed column for the FIFO/LIFO substring on `OeOrder`

The `PriInternal Like '%' + @FifoLifoSeq + '%'` pattern is non-SARGable. A persisted computed column `PriInternalFifoLifo As Cast(SubString(PriInternal, 5, 14) As Char(14)) Persisted`, with a nonclustered index, would let both occurrences become equality predicates on the typed column. The two affected SELECTs would change from `Like '%' + @FifoLifoSeq + '%'` to `PriInternalFifoLifo = @FifoLifoSeq`.

This is a schema change. It needs DBA review. It benefits not only this procedure but every procedure that reads the FIFO/LIFO portion of PriInternal; the change should be evaluated holistically across the OeOrder consumers.

### 11.2 Decompose `PriInternal` into structured columns

Same observation as Row 27 (`lsp_OrdSetGroupPriBoost`): the layout of `PriInternal` is structured but stored as an undelimited string. Decomposing it into discrete columns would benefit every procedure that reads or writes parts of `PriInternal`. Documented for the longer-arc architectural backlog.

### 11.3 Per-MFC post-deployment monitoring

Two-week capture per site, three-bucket outcome distribution.
