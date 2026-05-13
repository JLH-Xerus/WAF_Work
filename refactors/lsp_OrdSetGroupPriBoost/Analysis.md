# lsp_OrdSetGroupPriBoost: Refactor Analysis (v11 to v12)

**Date:** 2026-05-08
**Tracking sheet row:** 27 (Priority P1, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v12 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_OrdSetGroupPriBoost`

**Purpose in one line:** Updates the priority-boost digit (character position 4 of `PriInternal`) on every Rx in a group or split-package super-group whose current boost is lower than the target boost, then logs both a group-level and a per-Rx prioritization event for audit.

**Tables touched:**

- `OeOrder` (the workhorse; read once to build the candidate set, updated to apply the new boost, then read again to log the post-update PriInternal per Rx).
- `EvtRxPrioritizationEvents` (insert target for the per-Rx event log).

**Called procedures:**

- `lsp_EvtInsertEvtEventRecord` (group-level event log).
- `lsp_DbLogError` (error-handling path).

**Indexes used (predicted from the v12 body):**

| Table | Index | Where used |
|---|---|---|
| `OeOrder` | NC on `GroupNum` (assumed) | the two #GroupRxs INSERT branches |
| `OeOrder` | `PK_OeOrder` (clustered, key `OrderId, HistoryDtTm`) | the UPDATE join and the per-Rx event INSERT join |

**Callers:** the order-prioritization logic in the application, called whenever a group needs its priority boosted (typically because a member Rx was reprioritized).

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on two of four expense lists (Duration #12, Expense #8). The cross-list summary names "algorithmic / plan rewrite candidate" as the dominant heuristic, not parameter sniffing.

The dominant cost drivers in v11 are three. The first is the OR-combined predicate `(GroupNum = @GroupNum Or GroupNum Like @SuperGroupNum + '[A-Z]')` on the candidate-set build. The optimizer cannot satisfy both branches with a single seek into the GroupNum index; it has to choose either a scan or a join-style fallback that scans the index for the LIKE predicate.

The second is that the procedure reads `OeOrder` three times for the same row set: once to build `#GroupRxs` (which only stores `OrderId`), once to UPDATE the PriInternal, and once to read the post-update PriInternal for the per-Rx event INSERT. The intermediate temp table carries less data than it needs to.

The third is the non-SARGable `SubString(PriInternal, 4, 1) > Cast(@PriBoost As Char(1))` predicate. The Substring function on the column defeats any index seek on PriInternal. This is unavoidable with the current schema; a persisted computed column on the boost digit (indexed) would solve it but is a schema change. The mitigation is to narrow the row set as much as possible before evaluating the Substring, which is what splitting the OR into two focused INSERTs accomplishes.

v12 addresses all three drivers and is a clean, deployment-ready refactor.

---

## 3. Evidence of Original (v11)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

**Roll-up across reporting sites:** to be filled in from the May 7 capture. The cross-list capture flags Duration #12 and Expense #8.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v11)

```
(paste STATS IO / STATS TIME for v11 against the same data state used for v12 here)
```

The expected v11 signature is three `OeOrder` reads (build, update, event-insert) and an index scan for the OR predicate on the candidate-set build.

---

## 4. Issue Identification

**Issue 1: OR predicate combining equality and LIKE on the same column (line 69 in v11).** Defeats clean index seek.

**Issue 2: Non-SARGable `SubString(PriInternal, 4, 1)` predicate (line 71 in v11).** Unavoidable with the current schema; mitigated by reducing the row count it evaluates against.

**Issue 3: Three reads of `OeOrder` for the same row set.** `#GroupRxs` carries only OrderId; PriInternal is re-read from OeOrder in both downstream statements.

**Issue 4: `with (nolock)` hint on the UPDATE target (line 93 in v11).** Misleading; NoLock has no effect on the update target, and the surrounding hint convention is more confusing than it is helpful.

**Issue 5: No local-variable indirection for the input parameters.** Low impact on this procedure because parameter sniffing is not the dominant cost, but the local-variable form is the program convention.

---

## 5. First Principles

**[[LEFT JOIN OR Anti-Pattern]] applied to a single-table OR.** The textbook fix for an `OR` across different join paths is UNION ALL. The same fix applies here even though the OR is on a single table: two focused INSERTs into `#GroupRxs` (one for the exact GroupNum equality, one for the super-group LIKE pattern) let the optimizer seek into the GroupNum index for each branch and avoid the scan-or-fallback that the combined predicate forces.

**[[Non-SARGable Predicates]] with the narrow-then-evaluate fix.** The `SubString(PriInternal, 4, 1)` predicate is non-SARGable but its cost is bounded by the row count it evaluates against. v12 narrows the row count via the GroupNum predicates first, then evaluates the Substring against the small intermediate set.

**Carry forward needed columns through the temp table.** The principle is general: a temp table that carries only a join key forces every downstream consumer to re-read the source for any additional column. Carrying PriInternal forward through `#GroupRxs` eliminates one full read of `OeOrder` for the UPDATE's read of PriInternal (the UPDATE still has to write to OeOrder, but the read of the existing value is now in cache from the temp-table population).

---

## 6. Refactor (commented)

The v12 body is reproduced in `Refactored.sql`. Salient blocks below.

**Local variables and the precomputed super-group pattern.**

```sql
Declare @LocalGroupNum            VarChar(10) = @GroupNum
-- (three more locals)
Declare @SuperGroupLikePattern    VarChar(12)
Declare @PriBoostChar             Char(1) = Cast(@LocalPriBoost As Char(1))

If @LocalGroupNum Like '%[A-Z]'
   Set @SuperGroupNum = Left(@LocalGroupNum, Len(@LocalGroupNum) - 1)
Else
   Set @SuperGroupNum = @LocalGroupNum

Set @SuperGroupLikePattern = @SuperGroupNum + '[A-Z]'
```

The LIKE pattern is computed once, used once.

**Two-branch UNION ALL replaces the OR predicate.**

```sql
Create Table #GroupRxs
(
    OrderId     VarChar(30) Primary Key,
    PriInternal VarChar(50)
)

Insert Into #GroupRxs (OrderId, PriInternal)
Select OrderId, PriInternal
From OeOrder With (NoLock)
Where GroupNum = @LocalGroupNum
  And SubString(PriInternal, 4, 1) > @PriBoostChar

Insert Into #GroupRxs (OrderId, PriInternal)
Select O.OrderId, O.PriInternal
From OeOrder O With (NoLock)
Left Join #GroupRxs G
    On G.OrderId = O.OrderId
Where O.GroupNum Like @SuperGroupLikePattern
  And SubString(O.PriInternal, 4, 1) > @PriBoostChar
  And G.OrderId Is Null
```

The two INSERTs use the same WHERE shape; only the GroupNum predicate differs. The second INSERT excludes rows already in `#GroupRxs` via the anti-join, which preserves uniqueness even if a row qualifies through both branches.

The `Primary Key` on `#GroupRxs.OrderId` indexes the temp table for the downstream joins.

**UPDATE without the misleading NoLock hint.**

```sql
Update O
Set O.PriInternal = SubString(O.PriInternal, 1, 1) + SubString(O.PriInternal, 2, 2)
                  + @PriBoostChar
                  + SubString(O.PriInternal, 5, 14) + SubString(O.PriInternal, 19, 40)
From OeOrder O
Inner Join #GroupRxs G
    On G.OrderId = O.OrderId;
```

**Per-Rx event INSERT reads the post-update PriInternal once.**

```sql
Insert Into EvtRxPrioritizationEvents (...)
Select ..., G.OrderId, O.PriInternal
From #GroupRxs G
Inner Join OeOrder O With (NoLock)
    On O.OrderId = G.OrderId;
```

The OeOrder read here returns the post-update PriInternal because the UPDATE has already committed in the same transaction.

---

## 7. Risk & Rollback

**What could go wrong.** The two-branch UNION ALL preserves row identity through the Primary Key on `#GroupRxs.OrderId`. The anti-join in the second branch prevents duplicates in the edge case where a GroupNum somehow matches both the equality and the LIKE pattern (which should not occur given the trailing-letter convention, but the anti-join is defensive).

The Substring predicate is preserved in both branches. The non-SARGable predicate still runs per row of the GroupNum-filtered intermediate; the row count is bounded by the group size (typically dozens).

The UPDATE NoLock removal is mechanical. The engine ignores NoLock on update targets; the hint was misleading documentation rather than effective behavior.

**What to look for in the first 24 hours.** Plan shape for the two `#GroupRxs` INSERTs (expected: two index seeks into GroupNum), elapsed time per call (expected: similar to v11 because the procedure is fast on a small candidate set), and row count parity between v11 and v12 on the same data state.

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
| Logical reads on `OeOrder` | | (expected: ~67% of v11) | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan shape for #GroupRxs build | OR predicate; mixed scan/seek | two clean seeks into GroupNum | |
| Result: row count updated | | | |

**Verdict (to be entered once the captures land):** the headline expected delta is the clean two-branch plan on `#GroupRxs` build and the elimination of the PriInternal re-read in the UPDATE. The procedure is fast in absolute terms; the improvement is structural rather than dramatic.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Non-zero result set. (?)
- Identical result set: same Rxs updated, same PriInternal values written, same events logged. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles and the structural improvements are small in absolute terms but consistent with the program's pattern library.

---

## 11. Open Items / Future Improvements

### 11.1 Persisted computed column for the boost digit on `OeOrder`

The `SubString(PriInternal, 4, 1)` predicate is fundamentally non-SARGable. A persisted computed column `PriInternalBoostDigit As Cast(SubString(PriInternal, 4, 1) As Char(1)) Persisted`, with a filtered nonclustered index on the column, would let the candidate-set build seek directly. This is a schema change and belongs in the architectural backlog.

### 11.2 Consider PriInternal as a structured column

The procedure body documents the layout of `PriInternal` as `[precedence][system priority][boost][FIFO/LIFO sequence][priority preference + subpriority]`. The layout suggests the column should be decomposed into discrete columns. The decomposition would benefit not only this procedure but every procedure that reads or writes parts of `PriInternal`. This is a substantial schema change that requires the application contract to accept the new shape. Out of scope for v12; documented for the longer-arc architectural backlog.

### 11.3 Per-MFC post-deployment monitoring

The cross-list capture does not flag this procedure for plan instability, but per-MFC monitoring is still required by the program methodology for any deployed refactor. Two-week capture per site, three-bucket outcome distribution, per-site root cause analysis for any site recorded as Regressed.
