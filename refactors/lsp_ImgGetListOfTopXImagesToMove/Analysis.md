# lsp_ImgGetListOfTopXImagesToMove: Refactor Analysis (v6 to v7)

**Date:** 2026-05-07; Tolleson capture added 2026-05-13.
**Tracking sheet row:** earlier cohort (pre-Row 15).
**Deployment state:** Cataloged; ready for iA review handoff. `Refactored.sql` matches the committed v7 body.

**Note on the 2026-05-13 Tolleson capture.** The captures measure two bodies:

- v6 (original).
- An exploratory body that is not v7: temp-table + EXISTS shape, `ImageData Is Not Null` commented out, no FORCESEEK hints.

The committed v7 has not yet been measured at Tolleson. Sections 8 and 9 label numbers accordingly.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_ImgGetListOfTopXImagesToMove`

**Purpose:** Return up to N image rows eligible to be moved from database to file system, where eligibility is "associated with a Shipped Rx" or "associated with a Verified Canister." N comes from `vCfgSystemParamVal`.

**Tables touched:**

- `vImgImage`: UNION ALL view over `ImgImage` and `ImgImage_IntId`. Filtered by `ImageData Is Not Null`, `IsMovedToFile`, `ArchivedDtTm`.
- `ImgRxImgAssoc`: Rx-to-image, joined on `ImgId`.
- `OeOrderHistory`: joined via `ImgRxImgAssoc.OrderId`; filter `OrderStatus = 'Shipped'`.
- `ImgCanImgAssoc`: canister-to-image, joined on `ImgId`.
- `CanCanister`: joined via `ImgCanImgAssoc.CanisterSn`; filter `Status = 'Verified'`.
- `vCfgSystemParamVal`: parameter source for `MaxNumberOfImagesToPullInASingleRunFromBgWk`.

**Indexes used (predicted from the v7 body; not all confirmed against a v7 plan):**

| Table | Index | Where used |
|---|---|---|
| `vImgImage` / `ImgImage` | clustered on `Id` (assumed) | seed of both UNION branches |
| `ImgRxImgAssoc` | `ByImgId` (FORCESEEK retained) | Branch 1 association seek |
| `OeOrderHistory` | `ByOrderStatus` | Branch 1 status filter |
| `ImgCanImgAssoc` | `ByImgId` (FORCESEEK retained) | Branch 2 association seek |
| `CanCanister` | clustered on `CanisterSn` (assumed) | Branch 2 status filter |

**Callers:** background image-mover job. Regular cadence.

**Index DDL gap.** The Row 15 index extract covers the `OeOrder` family only. Image and canister table indexes need a fresh extract from Tolleson before §11 recommendations are finalized.

---

## 2. Overview of Performance

The dominant cost in v6 is the LEFT JOIN + OR pattern across two join paths (Rx and Canister), which forces the optimizer into a single plan that cannot seek efficiently on either path. `OeOrderHistory` binds to `PK_OeOrderHistory`; `OrderStatus = 'Shipped'` ends up as a residual rather than a seek-key predicate.

The v7 refactor splits the two paths into independent INNER JOIN branches and combines them with `UNION` (not `UNION ALL`, to deduplicate). Each branch plans in isolation. The Rx branch can seek on `OeOrderHistory.ByOrderStatus`. A secondary change moves the `TOP` operand from a scalar subquery to a local variable, intended to enable rowgoal optimization (not confirmed by the 2026-05-13 capture; see §11.5).

The LOB predicate `i.ImageData Is Not Null` is the largest unrealized win. It costs about 66,000 LOB page reads per execution at Tolleson and is not addressed by v7. The fix is a non-LOB indicator column (§11.1).

---

## 3. Evidence of Original (v6)

### 3.1 Query Store, cross-MFC view

Procedure lands in the Query Store top-50 at every reporting site. Largest LOB-read consumer in the Img family.

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | LOB reads / exec (approx) |
|-----|---------------|------------------|-------------|----------------|--------------------|---------------------------|
| (paste from capture) | | | | | | |

**Roll-up across reporting sites:** to be filled in once the cross-MFC aggregation against the May 7 capture is run on this procedure.

### 3.2 STATISTICS IO and STATISTICS TIME

Tolleson, 2026-05-13. Warm cache.

```
(0 rows affected)
Table 'CanCanister'.       Scan count 0,     logical reads 155,     LOB logical reads 0
Table 'OeOrderHistory'.    Scan count 66035, logical reads 286341,  LOB logical reads 0
Table 'ImgCanImgAssoc'.    Scan count 66109, logical reads 211135,  LOB logical reads 0
Table 'ImgRxImgAssoc'.     Scan count 66109, logical reads 281793,  LOB logical reads 0
Table 'ImgImage_IntId'.    Scan count 1,     logical reads 0,       LOB logical reads 0
Table 'ImgImage'.          Scan count 1,     logical reads 114577,  LOB logical reads 66109
Table 'CfgSystemParamVal'. Scan count 0,     logical reads 2,       LOB logical reads 0
Table 'CfgSystemParamDef'. Scan count 0,     logical reads 2,       LOB logical reads 0

(1 row affected)

 SQL Server Execution Times:
   CPU time = 2047 ms,  elapsed time = 2142 ms.

Completion time: 2026-05-13T23:29:10
```

Findings:

- 66,109 LOB logical reads on `ImgImage`. One per outer row, driven by `i.ImageData Is Not Null`.
- 66,035 scans / 286,341 logical reads on `OeOrderHistory`. Plan binds `PK_OeOrderHistory`; `OrderStatus = 'Shipped'` is a residual.
- CPU 2,047 ms; elapsed 2,142 ms.

Plan: `Tolleson/original_executionPlan.sqlplan`. Source: `Tolleson/procresults.txt`.

---

## 4. Issue Identification

**Issue 1: TOP consumes a scalar subquery (line 33).** `Top (Select Convert(Int, [Value]) From vCfgSystemParamVal Where ...)` puts the parameter inside the TOP. The optimizer cannot apply rowgoal to a TOP it cannot evaluate at compile time, so the plan does worst-case work and trims at the top.

**Issue 2: LEFT JOIN with OR across different join paths (lines 42-52).** `Oh.OrderStatus = 'Shipped' Or C.Status = 'Verified'` spans two unrelated join chains. The optimizer cannot seek into `OeOrderHistory.ByOrderStatus` because the predicate lives in a disjunction across tables. Canonical LEFT JOIN + OR anti-pattern.

**Issue 3: Redundant join predicate (line 50).** `(i.Id = IR.ImgId Or i.Id = IC.ImgId)` repeats the JOIN ON clauses. Structurally redundant, and adjacent to the LOB predicate, which complicates plan reasoning.

**Issue 4: LOB predicate on `ImageData` (line 46).** `i.ImageData Is Not Null` inspects the LOB pointer per candidate row. Source of the roughly 66,000 LOB page reads. Fix is a non-LOB indicator column.

**Issue 5: FORCESEEK hints on association tables (lines 42, 44).** Hints indicate the original author was fighting the v6 plan shape. Retained in v7 pending post-deployment validation; should be removable per §11.2.

---

## 5. First Principles

**[[LEFT JOIN OR Anti-Pattern]].** Textbook fix for LEFT JOIN + OR across different join paths is to split into a UNION of INNER JOIN branches, one per path. UNION (not UNION ALL) deduplicates rows that qualify through both paths.

**[[TOP with ORDER BY Semantics]].** Rowgoal optimization applies to TOP only when the operand is evaluable at compile time. A scalar subquery defeats this. A local variable lets the optimizer sniff the value. §11.5 notes that this expected behavior is not confirmed by the 2026-05-13 capture.

**[[Index Key Columns vs Included Columns]].** The optimizer switches from `PK_OeOrderHistory` to `ByOrderStatus` only when `OrderStatus = 'Shipped'` sits in a position the optimizer can seek into. The v6 OR shape blocks the switch; the v7 INNER JOIN unlocks it. Query shape constrains plan choice independently of which indexes exist.

---

## 6. Refactor (commented)

The v7 body lives in `Refactored.sql`. Key blocks:

```sql
-- Pull the configured batch size into a local variable so the optimizer
-- can sniff the value and apply rowgoal optimization to the TOP operator.
Declare @MaxImages Int

Select @MaxImages = Convert(Int, [Value])
From vCfgSystemParamVal With (NoLock)
Where Section = 'Operation'
         And
      Parameter = 'MaxNumberOfImagesToPullInASingleRunFromBgWk'
```

```sql
Select Top (@MaxImages)
      Id
    , ContentCode
From (
    -- Branch 1: Images associated with a Shipped Rx order
    Select i.Id, i.ContentCode
    From vImgImage As i With (NoLock)
    Inner Join ImgRxImgAssoc As IR With (NoLock, FORCESEEK) On i.Id = IR.ImgId
    Inner Join OeOrderHistory As OH With (NoLock) On IR.OrderId = OH.OrderId
    Where i.ImageData Is Not Null
             And (i.IsMovedToFile Is Null Or i.IsMovedToFile <> 1)
             And i.ArchivedDtTm Is Null
             And OH.OrderStatus = 'Shipped'

    Union

    -- Branch 2: Images associated with a Verified Canister
    Select i.Id, i.ContentCode
    From vImgImage As i With (NoLock)
    Inner Join ImgCanImgAssoc As IC With (NoLock, FORCESEEK) On i.Id = IC.ImgId
    Inner Join CanCanister As C With (NoLock) On IC.CanisterSn = C.CanisterSn
    Where i.ImageData Is Not Null
             And (i.IsMovedToFile Is Null Or i.IsMovedToFile <> 1)
             And i.ArchivedDtTm Is Null
             And C.Status = 'Verified'
) Combined
Order By Id Asc
```

Each branch is a clean INNER JOIN chain. UNION (not UNION ALL) deduplicates images that qualify through both paths. The outer TOP applies after the UNION; rowgoal is expected to push down to each branch but is unverified (§11.5). FORCESEEK retained pending observation (§11.2).

---

## 7. Risk & Rollback

**Concerns:**

- UNION implicit DISTINCT cost. Result is two columns (`Id`, `ContentCode`), `Id` is the clustering key, so distinct cost is bounded. Worth confirming in monitoring.
- Rowgoal sensitivity to data shape. A site with few Shipped Rx images and many Verified Canister images may require both branches to fill the TOP. Plan stability across MFCs should be confirmed.
- FORCESEEK retention. If the optimizer's natural choice differs from what FORCESEEK forces, the hint blocks a better plan. The cleaner end state is removing the hints (§11.2).

**First 24 hours:** watch the plan choice on `OeOrderHistory`. `ByOrderStatus` confirms the predicted win. `PK_OeOrderHistory` means something is preventing the switch and the analysis needs revisiting.

**Rollback:** `Original.sql` in this folder is the v6 body. Redeploy reverts cleanly; no schema dependency to unwind.

---

## 8. Evidence of Refactor

### 8.1 v7 (committed): not yet captured at Tolleson

Holding for v7 STATS IO / STATS TIME and plan capture under the same data state used for §3.2.

```
(holding for v7 capture)
```

### 8.2 Exploratory body (temp table + EXISTS, LOB predicate commented out): 2026-05-13 Tolleson capture

The captured body is not v7. It:

- Reads `vCfgSystemParamVal` into `@MaxImages` (same as v7).
- Pre-filters into `#qualifyingImages (Id, ContentCode)` from `vImgImage` using `IsMovedToFile` and `ArchivedDtTm` only. `i.ImageData Is Not Null` is commented out.
- Adds clustered index `byQIId (Id)` to the temp table.
- Selects `Top (@MaxImages)` from the temp table with two EXISTS branches (Rx + `OrderStatus = 'Shipped'`, or Canister + `Status = 'Verified'`).
- No FORCESEEK hints.

Not a deploy candidate as written. Removing `i.ImageData Is Not Null` changes semantics (rows with NULL `ImageData` would be returned). The correct end state is the indicator column in §11.1.

```
-- Statement 2: SELECT INTO #qualifyingImages (DOP=4)
Table 'CfgSystemParamVal'. Scan count 0, logical reads 2,      LOB logical reads 0
Table 'CfgSystemParamDef'. Scan count 0, logical reads 2,      LOB logical reads 0
Table 'ImgImage_IntId'.    Scan count 1, logical reads 0,      LOB logical reads 0
Table 'ImgImage'.          Scan count 5, logical reads 114580, LOB logical reads 0
Table 'Worktable'.         Scan count 0, logical reads 0,      LOB logical reads 0
CPU time = 625 ms,  elapsed time = 185 ms.

-- Statement 3: parallel-insert auto-statement (DOP=4, MemGrant 6656 KB)
Table 'Worktable'.            Scan count 0, logical reads 0,   LOB logical reads 0
Table '#qualifyingImages___'. Scan count 1, logical reads 158, LOB logical reads 0
CPU time = 47 ms,  elapsed time = 45 ms.

-- Statement 4: SELECT TOP (@MaxImages) ... WHERE EXISTS (...) OR EXISTS (...)
Table 'OeOrderHistory'.       Scan count 66032, logical reads 264647, LOB logical reads 0
Table 'ImgRxImgAssoc'.        Scan count 66106, logical reads 265221, LOB logical reads 0
Table 'CanCanister'.          Scan count 0,     logical reads 148,    LOB logical reads 0
Table 'ImgCanImgAssoc'.       Scan count 66106, logical reads 198706, LOB logical reads 0
Table '#qualifyingImages___'. Scan count 1,     logical reads 157,    LOB logical reads 0
CPU time = 594 ms,  elapsed time = 610 ms.

Total: CPU time = 1266 ms, elapsed time = 840 ms.
Completion time: 2026-05-13T23:29:05
```

Headline numbers:

- LOB logical reads on `ImgImage`: 0 (down from 66,109).
- CPU 1,266 ms; elapsed 840 ms.
- `OeOrderHistory` logical reads: 264,647 (down from 286,341).
- `SELECT INTO` ran parallel (DOP=4): 185 ms elapsed for 625 ms of CPU.

Plan-shape observations (`Tolleson/refactor_executionPlan.sqlplan`):

- Four `StmtSimple` elements: scalar assign, parallel `SELECT INTO`, auto-generated parallel-insert, final `SELECT TOP`.
- `OeOrderHistory` bound to `ByOrderStatus` with `'Shipped'` in the seek key prefix.
- `ImgRxImgAssoc` and `ImgCanImgAssoc` seeked on `ByImgId` naturally (`ForcedIndex="false"`); no FORCESEEK applied.
- Final statement uses `Concatenation` over two `Nested Loops` semi-joins; outer driver is a clustered scan of `#qualifyingImages` on `byQIId`.
- No rowgoal: `EstimateRowsForRowGoal` not populated on any RelOp (§11.5).
- Missing-index hint, Impact 66.45 (§11.6): `ImgImage`, `EQUALITY:[ArchivedDtTm], INEQUALITY:[IsMovedToFile], INCLUDE:[ContentCode]`.

---

## 9. Comparison & Improvement

Comparison is v6 (original) vs the **exploratory body** captured 2026-05-13. The committed v7 has not been captured at Tolleson; LOB-reads and CPU/elapsed deltas attributable to v7 specifically cannot be inferred from this table.

| Metric | v6 (original) | Exploratory (2026-05-13) | Delta |
|---|---|---|---|
| CPU time (ms) | 2,047 | 1,266 (625 + 47 + 594) | −38% |
| Elapsed time (ms) | 2,142 | 840 (185 + 45 + 610) | −61% |
| LOB page reads on `ImgImage` | 66,109 | 0 | −100% |
| Logical reads on `ImgImage` | 114,577 | 114,580 | flat |
| Logical reads on `OeOrderHistory` | 286,341 | 264,647 | −7.6% |
| Logical reads on `ImgRxImgAssoc` | 281,793 | 265,221 | −5.9% |
| Logical reads on `ImgCanImgAssoc` | 211,135 | 198,706 | −5.9% |
| Logical reads on `CanCanister` | 155 | 148 | −4.5% |
| Plan: `OeOrderHistory` | `PK_OeOrderHistory` (residual) | `ByOrderStatus` (seek key) | switched |
| Statement count | 1 | 4 | structural |
| FORCESEEK exercised | n/a | none | n/a |
| Rowgoal applied on `TOP` | no | no | unchanged |

Findings:

- LOB reads at −100% are from commenting out `i.ImageData Is Not Null`, not from the structural rewrite. v7 retains the predicate; v7 will not see this drop on deploy.
- `OeOrderHistory` index switch fired (`PK_OeOrderHistory` → `ByOrderStatus`) but only saved 7.6% on this dataset. Per-image probe was already 1-row under v6; the residual-to-seek-key promotion is cosmetic at Tolleson's current data shape.
- CPU/elapsed gains (38% / 61%) split between LOB elimination and the parallel `SELECT INTO` (185 ms elapsed at DOP=4 for 625 ms of CPU, about 3.4x parallel efficiency).
- Rowgoal did not fire under either body. The local-variable claim in §6 is unverified (§11.5).
- The committed v7 (UNION of INNER JOINs, LOB predicate retained, FORCESEEK retained) has not been measured. The structural rewrite alone is unlikely to deliver an order-of-magnitude headline given the 7.6% `OeOrderHistory` delta seen here. v7 capture pending (§11.7).

---

## 10. Validation Checklist

Status reflects the 2026-05-13 Tolleson capture. Items dependent on the v7 body are pending.

- `[PASS]` Same data state. Captures completed 23:29:05 and 23:29:10, same instance, same day.
- `[PASS]` Warm cache only. Assumed warm from back-to-back execution.
- `[WARN]` Non-zero result set. Original recorded `(0 rows affected)`; exploratory row count not captured. Re-run for a non-empty population.
- `[?]` Identical result set. Not verified; exploratory body's commented-out LOB predicate changes semantics.
- `[PASS]` Plan shape matches prediction. `OeOrderHistory` bound to `ByOrderStatus`.
- `[PASS]` No new error or warning messages.
- `[PASS]` Warm-cache elapsed time at or below original. 840 ms vs 2,142 ms.
- `[?]` v7 body captured at Tolleson. Pending (§11.7).
- `[?]` Rowgoal application on `TOP` confirmed. Pending (§11.5).

Net call: structural refactor delivers about 7.6% on `OeOrderHistory` logical reads on this dataset; the bulk of the captured win belongs to the LOB predicate change and the parallel `SELECT INTO`, neither of which describes the committed v7. v7 capture required before review sign-off.

---

## 11. Open Items / Future Improvements

### 11.1 Remove the LOB predicate on `ImageData`

Largest unrealized win on this procedure. 2026-05-13 capture confirms the prediction: exploratory body with the predicate commented out ran at 0 LOB logical reads against `ImgImage`, down from 66,109.

Fix is a non-LOB indicator column (`HasImageData Bit`, `ImageDataLen Int`, or similar) on `ImgImage` and `ImgImage_IntId`, populated by trigger or by the application. Schema change, not a same-PR refactor.

Both UNION branches then change from:

```sql
Where i.ImageData Is Not Null
   And (i.IsMovedToFile Is Null Or i.IsMovedToFile <> 1)
   And i.ArchivedDtTm Is Null
   And ...
```

to:

```sql
Where i.HasImageData = 1
   And (i.IsMovedToFile Is Null Or i.IsMovedToFile <> 1)
   And i.ArchivedDtTm Is Null
   And ...
```

### 11.2 Remove FORCESEEK hints

Sequence: deploy v7 with hints retained, observe at multiple MFCs, ship v8 without hints if the natural plan binds the same seeks.

Indirect evidence in favor: the exploratory body carries no FORCESEEK hints and the optimizer still bound `ByImgId` seeks naturally on both association tables (`ForcedIndex="false"`). Not direct evidence for v7 (different shape) but raises the prior probability the hints are safe to drop.

### 11.3 Write a "LOB Column Predicates" masterclass entry

Pattern (LOB pointer inspection per row), fix (non-LOB indicator column), trade-off (schema change, not same-PR). The next refactor that surfaces the pattern should produce the note.

### 11.4 Confirm `vImgImage` is the right read source

`ImgImage_IntId` showed `Scan count 1, logical reads 0` in both 2026-05-13 captures, consistent with the table being empty. If confirmed empty across MFCs, replace the view reference with `ImgImage` directly. Row-count check at each MFC before fleet-wide change.

### 11.5 Verify rowgoal application on the `TOP` operator

The §6 justification for the local-variable rewrite (rowgoal optimization applied to TOP) is not exercised by the 2026-05-13 capture. Neither plan populates `EstimateRowsForRowGoal`.

Two hypotheses worth distinguishing on the next capture: (1) the rewrite is correct in principle but the parameter-sniffing path is suppressed by `OPTION (RECOMPILE)` semantics or by the value of `@MaxImages` at compile time; (2) the heuristic fires but the captured XML does not surface it under the attribute we inspect. The v7 capture (same local-variable pattern) is the test. Open until then.

### 11.6 Add the missing-index hint surfaced by the exploratory plan

```
ON ImgImage
EQUALITY:   [ArchivedDtTm]
INEQUALITY: [IsMovedToFile]
INCLUDE:    [ContentCode]
```

Impact 66.45. Applies to the same candidate-image filter v7 carries in both UNION branches. Evaluate cross-procedure impact in the Img family before adding; add to the index inventory follow-up rather than the v7 deploy PR.

### 11.7 Capture v7 at Tolleson before the deploy decision

Run the v7 body at Tolleson under the same data state and warm-cache conditions used 2026-05-13. Capture STATS IO/TIME and the plan.

Three questions for the capture:

1. Does the `OeOrderHistory` index switch hold under v7? Expect yes; predicate sits in seek position in both branches.
2. Does rowgoal fire under v7? Open per §11.5.
3. What is the CPU/elapsed delta v7 vs original, with the LOB predicate held constant? This is the number that defends the deploy.
