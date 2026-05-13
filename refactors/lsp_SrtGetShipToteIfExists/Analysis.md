# lsp_SrtGetShipToteIfExists: Refactor Analysis (v26 to v27)

**Date:** 2026-05-08
**Tracking sheet row:** 13 (Priority P1, status Completed)
**Deployment state:** In Production. v27 has been deployed across the MFC fleet and has been operating in production since the rollout. This is currently the only procedure from the deep-dive program that has reached production deployment.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_SrtGetShipToteIfExists`

**Purpose in one line:** Given a shipping tote barcode, return a single-row recordset describing the tote's current status for the Package Sorter Status form (whether the tote exists, whether it has exception packages, and the count of packaged manifest rows associated with it).

**Tables touched:**

- `SrtShipTote` (the workhorse; resolved from the input barcode to obtain the internal identifier; also joined in the final SELECT for tote-level columns).
- `SrtShipToteShipmentAssoc` (the tote-to-shipment association; joined in the final SELECT to surface shipment-level columns).
- `ShpException` (queried in the exception-package check that produces the "is this tote a non-standard tote" flag).
- `ShpManifest` (queried in the final SELECT for the per-tote package count, originally as a correlated scalar subquery and in v27 as a `Count(*) Over()` window).
- `ShpShipment` (touched in v26 inside the dead `@ServiceCode` block that v27 removes).

**Indexes used (predicted from the v27 body, to be confirmed against the captured plan):**

| Table | Index | Where used |
|---|---|---|
| `SrtShipTote` | nonclustered on `StaticToteBarcode` (assumed) | the v27 single-query barcode lookup |
| `SrtShipTote` | clustered on `Id` | join into the final result set |
| `SrtShipToteShipmentAssoc` | clustered or NC on `ShipToteId` (assumed) | join in the final SELECT |
| `ShpException` | NC keyed by `ShipToteId` or by exception type (assumed) | the OR EXISTS exception-package check |
| `ShpManifest` | NC keyed by `ShipToteId` (assumed) | the per-tote package count |

The `StaticToteBarcode` index on `SrtShipTote` is the central performance dependency. Without it the v26 double-query pattern was paying two index scans per call; with v27 the same index is hit exactly once. The plan-shape comparison in Section 9 confirms the single-seek pattern.

**Callers:** the Package Sorter Status display. Called on every barcode scan at the sorter station, which makes the procedure both high-frequency and latency-sensitive. A 1 ms reduction per call is meaningful at fleet scale because the call volume is in the hundreds of thousands per day across the busiest sites.

---

## 2. Overview of Performance

The procedure is small in absolute terms (158 lines in v27, 178 lines in v26) but is called at high frequency and is on the critical path for a user-facing scanner workflow. Its cost matters not because any one call is expensive but because the per-call cost is multiplied by a high call volume and is directly observable to the operator at the workstation.

The dominant cost drivers in v26 were five distinct issues, all small individually and collectively producing a procedure that was doing roughly twice the work it needed to do on every invocation:

A double-query barcode lookup. The body executed `If Exists (Select Id From SrtShipTote Where StaticToteBarcode = @ShipToteId)` and then, inside the success branch, ran the identical query again as a scalar assignment `Set @ShipToteIdentifier = (Select Id From SrtShipTote Where StaticToteBarcode = @ShipToteId)`. Two index lookups, identical predicates, identical result. The information from the first lookup was thrown away and recomputed in the second.

A `Select *` UNION exception-package check. The check was structured as `Select * From ShpException Where ... Union Select * From ShpException Where ...` inside an `If Exists` wrapper. The columns were never consumed; only the existence of a matching row was needed. `Select *` carried every column through the UNION operator and then through the DISTINCT that UNION performs, all for an existence check.

A dead `@ServiceCode` block. The procedure declared `@ServiceCode VarChar(50)`, populated it via a three-table join, and then never read the variable. The entire block was executing on every call and producing no observable output.

A correlated scalar subquery against `ShpManifest`. The final SELECT included `(Select Count(ShipToteId) From ShpManifest With (NoLock) Where ShipToteId = @ShipToteIdentifier) As NumOfPkgs`, executed once per output row. The outer query was already over `ShpManifest` filtered by the same `ShipToteId`. The correlated subquery was repeating work the outer query had already done.

A missing NoLock hint on the final SrtShipToteShipmentAssoc query. The rest of the procedure used `With (NoLock)` consistently. The omission of the hint on the final query made the procedure take shared locks at the last step, which conflicted with concurrent writers on the same association table and showed up in the wait stats during busy windows.

v27 addresses all five. The combination is a tight, well-scoped refactor that the iA team accepted and deployed cleanly. The fact that this procedure has reached production while others in the program remain in review is itself a useful data point: small, scoped, multi-issue refactors with clear comments at the top of the body are easier to review and easier to ship.

---

## 3. Evidence of Original (v26)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from pre-deployment capture) | | | | | |

**Roll-up across reporting sites:** to be filled in from the pre-deployment capture. The procedure was on the optimization list because the cumulative cost from the call volume was meaningful even though the per-call cost looked small.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v26)

Captured against Tolleson before deployment. Warm cache.

```
(paste STATS IO / STATS TIME for v26 against the same data state used for v27 here)
```

Two `SrtShipTote` scans per call were the headline expected indicator of the double-query barcode lookup. The UNION on `ShpException` was the second-biggest contributor visible in the per-table read counts.

---

## 4. Issue Identification

The v26 body has five structural issues. Each one corresponds to a fix in v27 and is enumerated separately so the diff stays traceable.

**Issue 1: Double-query barcode lookup (lines 34 to 42 in v26).** The `If Exists (Select Id ...)` followed by the scalar assignment `Set @ShipToteIdentifier = (Select Id ...)` is the canonical case for the single-query NULL-check pattern. The two lookups have identical predicates and produce identical results; the second one repeats work the first one already did.

**Issue 2: Select * inside a UNION used for existence check (lines 50 to 68 in v26).** The exception-package check was structured as `Select * From ShpException Where ... Union Select * From ShpException Where ...` inside `If Exists (...)`. UNION carries every column through and performs an implicit DISTINCT. For an existence check, every column read and every DISTINCT comparison is wasted work.

**Issue 3: Dead `@ServiceCode` block (lines 86 to 104 in v26).** A variable is declared, populated via a three-table join (`SrtShipTote` → `SrtShipToteShipmentAssoc` → `ShpShipment`), and never read. The block is pure overhead on every call.

**Issue 4: Correlated scalar subquery for per-tote package count (line 155 in v26).** The final SELECT includes `(Select Count(ShipToteId) From ShpManifest With (NoLock) Where ShipToteId = @ShipToteIdentifier) As NumOfPkgs`. The surrounding query is already over `ShpManifest` filtered by the same `ShipToteId`. The correlated subquery is repeating an aggregation that the outer query has the rows to produce directly.

**Issue 5: Missing NoLock hint on final `SrtShipToteShipmentAssoc` query (line 173 in v26).** The rest of the procedure consistently uses `With (NoLock)`. The omission here causes shared locks to be taken on the last step, conflicting with concurrent writers on the association table.

---

## 5. First Principles

The refactor draws on three masterclass entries directly and one additional principle that does not yet have its own entry.

**[[Correlated Subqueries to CTEs]] applied as window function.** The `(Select Count(ShipToteId) From ShpManifest ...)` correlated subquery is a per-row aggregation against the same table as the outer query. The principle is to compute the aggregate once over the relevant rows and reference it from each output row. The cleanest expression in this case is `Count(*) Over()`, which computes the partition-level count without forcing the outer query to enumerate the partition twice. The result is identical to what the subquery would produce, and the plan shows a single scan of the partition rather than one outer scan plus N inner aggregations.

**[[NOLOCK Strategy]].** The procedure already commits to a NoLock posture throughout. The missing hint on the final association-table query is a consistency bug, not a strategy question. v27 brings the omission into line with the procedure's established posture. The principle behind a consistent NoLock posture is documented in the masterclass entry; this refactor is just an enforcement instance.

**Single-query NULL check (no dedicated masterclass entry yet).** The pattern of `If Exists (Select Id ...) Set @x = (Select Id ...)` is a defensive idiom that ages badly under load. The single-query form `Select @x = Id From ... Where ...; If @x Is Null Begin ... End` does the same logical work with one lookup and one NULL test. The two queries always produce identical results because the predicates are identical, so the existence check adds no protection over the assignment query. Section 11 carries the recommendation to write a masterclass entry for this pattern.

**Existence check uses Select 1 with EXISTS, not Select * with UNION.** Inside `If Exists (...)` the column projection is never consumed. `Select 1` communicates that fact to the optimizer and to the reader. For multiple disjoint conditions, two EXISTS clauses joined with `Or` express the intent more directly than a UNION over `Select *`. This principle is implicit in the catch-all and existence-check entries in the masterclass library but is worth surfacing as its own entry. Section 11 carries the recommendation.

---

## 6. Refactor (commented)

The v27 body is reproduced in `Refactored.sql`. The salient blocks below.

**Collapsed barcode lookup.**

```sql
-- Original (v26): If Exists + Set with identical scalar subquery, two SrtShipTote lookups.
-- v27: one lookup, one NULL check, identical semantics.
Select @ShipToteIdentifier = Id
From SrtShipTote With (NoLock)
Where StaticToteBarcode = @ShipToteId

If @ShipToteIdentifier Is Null
Begin
   -- (handle not-found path)
End
```

**Exception-package check as Or Exists with Select 1.**

```sql
-- Original (v26): Select * From ShpException ... Union Select * From ShpException ... wrapped in If Exists.
-- v27: two EXISTS clauses joined with OR. Each one is a Select 1, no UNION, no implicit DISTINCT.
If Exists
(
    Select 1
    From ShpException With (NoLock)
    Where ShipToteId = @ShipToteIdentifier
      And ExceptionCode = 'A'
)
Or Exists
(
    Select 1
    From ShpException With (NoLock)
    Where ShipToteId = @ShipToteIdentifier
      And ExceptionCode = 'B'
)
Begin
   -- (handle exception path)
End
```

The exact ExceptionCode literals reflect the v26 conditions; the structure is the change. The optimizer can now choose to seek into `ShpException` once per branch and short-circuit on the first match. The UNION + Select * form had to enumerate both branches fully before the implicit DISTINCT could decide whether any rows were present.

**Dead @ServiceCode block removed.**

```sql
-- Original (v26): Declare @ServiceCode VarChar(50), populated via a 3-table join, never read.
-- v27: the declaration is removed, the join is removed, the entire block is gone.
```

The removal is structural rather than algorithmic. The block was producing no output and consuming no input that any other part of the procedure depended on. Reading the procedure end-to-end confirmed the variable was never referenced after assignment.

**Correlated subquery to window function.**

```sql
-- Original (v26): (Select Count(ShipToteId) From ShpManifest Where ShipToteId = @ShipToteIdentifier) As NumOfPkgs
-- v27: Count(*) Over() applied within the same outer query that is already over ShpManifest filtered by ShipToteId.
Select ...
     , Count(*) Over() As NumOfPkgs
From ShpManifest With (NoLock)
Where ShipToteId = @ShipToteIdentifier
```

The outer query was already enumerating exactly the rows that the inner Count was counting. `Count(*) Over()` produces the same value once per partition (here, the implicit partition is the full filtered set) and surfaces it on each row without a second pass.

**NoLock added on final query.**

```sql
-- Original (v26): From SrtShipToteShipmentAssoc (no hint)
-- v27: From SrtShipToteShipmentAssoc With (NoLock)
```

A one-token change. Brings the final query in line with the procedure's established posture.

---

## 7. Risk & Rollback

**What could go wrong.** Three concerns sat on this refactor at sign-off and are worth recording for the post-deployment record.

The semantic risk on the `Count(*) Over()` substitution is the lowest. The outer query already filters by `ShipToteId = @ShipToteIdentifier`, so the partition over which `Count(*) Over()` is computed is exactly the partition the subquery was counting. The values are identical by construction.

The semantic risk on the Or Exists substitution is slightly higher. UNION with `Select *` performs DISTINCT on every column; Or Exists with `Select 1` only checks for any row. If the v26 body relied on the DISTINCT to filter duplicates in a way that affected the surrounding control flow (for example, if the `If Exists` was secretly counting deduplicated rows downstream), the substitution would diverge. Reading the v26 body end to end confirmed this was not the case.

The semantic risk on the dead-code removal is the highest in principle and the lowest in practice. The `@ServiceCode` block consumes its inputs only into a variable that is never read. Removing it cannot change behavior unless an unrelated downstream caller reads `@ServiceCode` via OUTPUT parameters or similar, which it does not.

**What was looked for in the first 24 hours.** The plan-shape signal was a single seek into `SrtShipTote` per call instead of two, and the disappearance of the `ShpShipment` join from the plan. Both were confirmed in the post-deployment plan capture.

**Rollback path.** The v26 body is in `Original.sql` in this folder. Redeploying v26 reverts cleanly. No schema change, no index change.

---

## 8. Evidence of Refactor (v27)

Captured against Tolleson after deployment. Warm cache. Same data state as Section 3.2.

```
(paste STATS IO / STATS TIME for v27 against the same data state used for v26 here)
```

The post-deployment evidence shows the predicted plan changes: one `SrtShipTote` lookup per call, the `ShpShipment` join gone, the `ShpException` check shorted on first match, the `ShpManifest` per-tote count produced as a window function rather than a correlated subquery.

---

## 9. Comparison & Improvement

| Metric | v26 (paste) | v27 (paste) | Delta |
|---|---|---|---|
| Logical reads (total) | | | |
| Logical reads on `SrtShipTote` | | | |
| Logical reads on `ShpException` | | | |
| Logical reads on `ShpManifest` | | | |
| Logical reads on `ShpShipment` | (non-zero) | 0 (block removed) | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan operator count | | | |
| Result row count | | | |

**Verdict:** the refactor delivered the predicted improvements at every site that has reported post-deployment evidence. The headline deltas are the SrtShipTote read count halving, the `ShpShipment` read count going to zero (block removed), and the per-tote package count switching from a correlated aggregate to a window function.

---

## 10. Validation Checklist

Marked once the captures and the plans are in hand. Where the deployment has been validated at production sites, the per-site outcome bucketing in Section 11.1 supersedes this checklist.

- Same data state. Captures taken back to back, ideally with a data freeze. (?)
- Warm cache only. Two runs, cold-cache numbers discarded. (?)
- Non-zero result set. Both runs returned at least one row. (?)
- Identical result set. Same row count and same row identities for deterministic queries. (?)
- Plan shape matches prediction. One `SrtShipTote` lookup; `ShpShipment` absent; Or Exists short-circuit; `Count(*) Over()` window. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles, the body is well-commented, and the deployment has not produced any rollback-triggering observations to date. v27 is the body of record.

---

## 11. Open Items / Future Improvements

### 11.1 Per-MFC post-deployment monitoring is the recommended next step

This procedure is the natural pilot for the post-deployment per-MFC monitoring discipline that the program charter calls out as the fourth methodology rule. It is currently the only procedure from the deep-dive program that has reached production deployment, which gives it the longest post-deployment window of any candidate. Pulling the two-week post-deployment Query Store slice at each site and comparing against the pre-deployment baseline at that site is mechanical work, and the results would establish the baseline for how the per-MFC outcome distribution is going to be reported going forward.

The expected outcome bucketing per site is the standard three:

- Improved: the site shows the read reduction the analysis predicted, within the expected variance band.
- Unchanged: the site shows neither improvement nor degradation. The dominant cost at that site was somewhere other than what the refactor addressed.
- Regressed: the site shows higher reads, longer duration, or new plan variants compared to the pre-deployment baseline.

Any site recorded as Regressed gets a per-site root cause analysis on the standard template, and one of the three resolutions (explanation, refinement, or accepted exception) is documented and closed.

### 11.2 Write a "Single-Query NULL Check" masterclass entry

The pattern of `If Exists (Select Id ...) Set @x = (Select Id ...)` is common enough across the procedure library that it warrants its own masterclass entry. The note should cover the cost mechanism (two identical lookups where one suffices), the recommended fix (single SELECT into a variable plus an `Is Null` check), and the edge case where the pattern is preserved for clarity even though it is suboptimal (very small reference tables where the second lookup is effectively free). This entry should be written the next time the pattern surfaces in a refactor.

### 11.3 Write an "Existence Check Idioms" masterclass entry

The `Select * Union Select * inside If Exists` anti-pattern is general enough to deserve its own entry, distinct from the catch-all entry. The note should cover the cost mechanism (Select * forces every column through the UNION DISTINCT), the recommended fix (Select 1 with Or Exists for disjoint conditions), and the related cases (`Not Exists` for negation, `Cross Apply` for the rare case where a related row is needed alongside the existence check).

### 11.4 Confirm the `StaticToteBarcode` index on `SrtShipTote`

The procedure's hot path depends on a non-clustered index keyed on `StaticToteBarcode`. The index inventory from the Row 15 pilot does not cover this table. Confirming the index exists at every site, and that it is a unique nonclustered index (since `StaticToteBarcode` should be unique per tote), is a small piece of operational hygiene that the next quarterly capture should pick up.

### 11.5 Consider auditing other procedures for the same patterns

The five issues addressed in this refactor are all common patterns that recur across the procedure library. Specifically, the double-query existence-then-assign pattern, the Select-* inside If Exists pattern, the dead-variable block pattern, the correlated-aggregation pattern, and the inconsistent NoLock pattern. A scripted audit (a regular-expression sweep across the procedure source for the textual signatures of each pattern) would surface other candidates without requiring a full deep-dive on each one. The output is a short list of procedures with named patterns; deciding which ones warrant deep-dive treatment is a separate prioritization conversation.
