# lsp_ImgGetListOfTopXImagesToMove: Refactor Analysis (v6 to v7)

**Date:** 2026-05-07
**Tracking sheet row:** earlier cohort (pre-Row 15), worked through individually before the deep-dive cohort began
**Deployment state:** Cataloged; ready for iA review handoff. The body in this Refactored.sql matches the latest committed refactor body.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_ImgGetListOfTopXImagesToMove`

**Purpose in one line:** Returns up to N image rows that are eligible to be moved from the database to the file system, where N is read from a system parameter, and where eligibility is "either this image is associated with a Shipped Rx, or this image is associated with a Verified Canister."

**Tables touched:**

- `vImgImage` (a UNION ALL view over `ImgImage` and `ImgImage_IntId`; the workhorse, queried with `(NoLock)` and filtered by `ImageData Is Not Null`, `IsMovedToFile`, and `ArchivedDtTm`).
- `ImgRxImgAssoc` (the Rx-to-image association table, joined on `ImgId`).
- `OeOrderHistory` (joined via `ImgRxImgAssoc.OrderId` to filter on `OrderStatus = 'Shipped'`).
- `ImgCanImgAssoc` (the canister-to-image association table, joined on `ImgId`).
- `CanCanister` (joined via `ImgCanImgAssoc.CanisterSn` to filter on `Status = 'Verified'`).
- `vCfgSystemParamVal` (the system-parameter view; read once at the top of the procedure to retrieve `MaxNumberOfImagesToPullInASingleRunFromBgWk`).

**Indexes used (predicted from the v7 body, to be confirmed against the captured plan):**

| Table | Index | Where used |
|---|---|---|
| `vImgImage` / base `ImgImage` | clustered on `Id` (assumed) | the seed of both UNION branches |
| `ImgRxImgAssoc` | `ByImgId` or similar (FORCESEEK hint retained) | Branch 1 association seek |
| `OeOrderHistory` | `ByOrderStatus` (NC, key `OrderStatus`) | Branch 1 status filter |
| `ImgCanImgAssoc` | `ByImgId` or similar (FORCESEEK hint retained) | Branch 2 association seek |
| `CanCanister` | clustered on `CanisterSn` (assumed) | Branch 2 status filter |

The pivotal index for the refactor's win is `OeOrderHistory.ByOrderStatus`. Under v6 the optimizer was binding `OeOrderHistory` via `PK_OeOrderHistory` because the outer LEFT JOIN + OR pattern constrained the plan shape. Under v7 the INNER JOIN inside Branch 1 lets the optimizer choose `ByOrderStatus`, which is the index whose key matches the dominant filter. This index choice is the single biggest lever in the refactor, and it would not have been available without the structural rewrite.

**Index DDL gap.** The shared index extract from the Row 15 pilot covers the `OeOrder` family. `ImgImage`, `ImgImage_IntId`, `ImgRxImgAssoc`, `OeOrderHistory`, `ImgCanImgAssoc`, and `CanCanister` are not all in that extract. The index inventory for the image and canister tables should be confirmed against Tolleson before deployment so the index recommendations in Section 11 are written against ground truth rather than inference.

**Callers:** the background image-mover job. The procedure is called on a regular cadence by the worker that drains images from the database to the file system.

---

## 2. Overview of Performance

The procedure is one of the most visible offenders on the optimization list because it is the largest single source of LOB page reads in the Img family, and because its cost scales with the population of "eligible-to-move" images, which grows steadily with order volume. The single dominant cost in v6 was that the LEFT JOIN with OR pattern across two distinct join paths (the Rx path and the Canister path) prevented the optimizer from seeking efficiently on either path. The plan was forced to materialize a wide cartesian-ish intermediate that included rows from both association tables and then post-filter on `OrderStatus = 'Shipped'` or `C.Status = 'Verified'`. Index choice on `OeOrderHistory` defaulted to the primary key rather than the much more selective `ByOrderStatus` nonclustered index.

The refactor splits the two join paths into independent INNER JOIN branches and combines them with `UNION`. The optimizer now sees two clean shapes it can plan independently. The Shipped-Rx branch can seek on `OeOrderHistory.ByOrderStatus` directly, and the Verified-Canister branch can seek on the canister status path directly. `UNION` (not `UNION ALL`) handles the natural deduplication for images that qualify through both paths.

A second cost driver in v6 was that `Top` consumed a scalar subquery against `vCfgSystemParamVal`. The optimizer cannot estimate the cardinality of a scalar subquery at compile time, and the `Top` rowgoal optimization cannot be applied to a value the optimizer cannot inspect. The v6 plan therefore did the maximum work for the underlying SELECT and applied the limit at the very top of the operator tree. Extracting the parameter value into a local variable before the SELECT lets the optimizer sniff the actual value and apply rowgoal to each UNION branch.

A third observation worth flagging here, even though v7 does not yet address it, is that `i.ImageData Is Not Null` is a LOB predicate that forces the engine to inspect the LOB pointer for every candidate row. Under apples-to-apples conditions during the refactor exploration this single predicate was costing roughly 68,000 LOB page reads per execution at Tolleson. Removing it (replacing it with a non-LOB indicator column) eliminated 100 percent of the LOB reads on the procedure during the exploratory branch where the change was tested. The committed v7 still carries the predicate. Section 11 carries this as the next follow-up.

---

## 3. Evidence of Original (v6)

### 3.1 Query Store, cross-MFC view

The procedure is read-heavy with a regular call cadence rather than a tight polling cadence, so it lands in the Query Store top-50 at every reporting site. The cross-MFC view from the May 7, 2026 capture shows the procedure as one of the largest LOB-read consumers in the fleet. Per-site totals are paste-ready in the cross-MFC capture once Justin updates this section with the matched aggregation.

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | LOB reads / exec (approx) |
|-----|---------------|------------------|-------------|----------------|--------------------|---------------------------|
| (paste from capture) | | | | | | |

**Roll-up across reporting sites:** to be filled in once the cross-MFC aggregation against the May 7 capture is run on this procedure.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run

Captured against Tolleson. Warm cache. Same data state as Section 8.

```
(paste STATS IO / STATS TIME for v6 against the same data state used for v7 here)
```

The Tolleson exploratory runs that are recorded in the lessons log showed that under v6 the per-execution LOB page reads ran at roughly 68,000 and the logical reads on `OeOrderHistory` ran in the millions per execution. Those numbers represent the conditions the refactor was designed against, and they are the conditions under which the v7 plan should be measured.

---

## 4. Issue Identification

The v6 body is short, which makes the issues easy to enumerate. Each one is structural rather than cosmetic.

**Issue 1: TOP consumes a scalar subquery (line 33).** `Top (Select Convert(Int, [Value]) From vCfgSystemParamVal Where ...)` puts the parameter inside the TOP itself. The optimizer cannot apply rowgoal to a TOP whose cardinality it cannot evaluate at compile time. This forces the plan to do worst-case work for the underlying SELECT and then trim at the top.

**Issue 2: LEFT JOIN with OR across different join paths (lines 42 to 52).** The query joins both `ImgRxImgAssoc` (and onward to `OeOrderHistory`) and `ImgCanImgAssoc` (and onward to `CanCanister`) as LEFT JOINs, then filters with an OR on `Oh.OrderStatus = 'Shipped' or C.Status = 'Verified'`. This is the canonical LEFT JOIN + OR anti-pattern across different join paths. The optimizer has to consider both paths simultaneously, cannot prune via either selectivity, and cannot seek into `OeOrderHistory.ByOrderStatus` because the predicate is in a disjunction that spans two unrelated tables.

**Issue 3: Redundant join predicate in the WHERE clause (line 50).** `(i.Id = IR.ImgId Or i.Id = IC.ImgId)` repeats the JOIN ON clauses already declared in the FROM list. It is structurally redundant. It is also adjacent to a LOB predicate on the same table, which makes plan reasoning harder than it needs to be.

**Issue 4: LOB predicate on `ImageData` (line 46).** `i.ImageData Is Not Null` forces the engine to inspect the LOB pointer for every candidate row to evaluate the predicate. This is the source of the 68,000 LOB page reads per execution recorded during the exploratory work. A non-LOB indicator column would express the same intent without the LOB traversal.

**Issue 5: FORCESEEK hints on association tables (lines 42 and 44).** FORCESEEK hints on `ImgRxImgAssoc` and `ImgCanImgAssoc` indicate the original author was already fighting the optimizer for the right plan shape under the v6 structure. Hints of this kind are a tell that the surrounding query is constraining the optimizer's choices. With the structural refactor in v7 the hints should no longer be necessary, but they were retained in the committed body pending post-deployment validation.

---

## 5. First Principles

The refactor draws on three masterclass entries.

**[[LEFT JOIN OR Anti-Pattern]].** The textbook fix for LEFT JOIN + OR across different join paths is to split the query into a UNION of INNER JOIN branches, one per path. Each branch contains only the join chain it needs, and the optimizer can plan each branch independently. The UNION combines the two result sets and deduplicates by row, which is what we want when an image can qualify through both paths.

**[[TOP with ORDER BY Semantics]].** The optimizer applies rowgoal optimization to TOP only when it can evaluate the row count at compile time. A scalar subquery inside TOP defeats this. Extracting the value into a local variable lets the optimizer sniff it and apply rowgoal correctly. The local-variable approach also has the secondary benefit of making the TOP value visible to operator-level cardinality estimates throughout the plan.

**[[Index Key Columns vs Included Columns]].** The biggest win in the v7 plan comes from the optimizer switching from `PK_OeOrderHistory` to `ByOrderStatus`. That switch is possible because the INNER JOIN inside Branch 1 makes `OrderStatus = 'Shipped'` a direct seek predicate against an index keyed on `OrderStatus`. Under v6 the predicate was in an OR that spanned two unrelated tables, so the optimizer could not bind to a status-keyed index. The principle is that the plan choice the optimizer is willing to make is constrained by the shape of the query, not only by the existence of the index. A reshape can unlock an index that has been sitting unused.

A net-new observation worth tracking here is the LOB predicate point. It is not yet a masterclass entry of its own, but it has come up enough on the Img family of procedures that it warrants one. Section 11 carries the recommendation to write a masterclass entry titled "LOB Column Predicates" the next time this pattern surfaces on another procedure.

---

## 6. Refactor (commented)

The v7 body is reproduced in `Refactored.sql` with inline commentary. The salient blocks:

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

The local variable replaces the scalar subquery inside TOP. Rowgoal optimization can now be applied to the outer TOP and propagated to both UNION branches.

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

The two branches are the structural change. Each one is a clean INNER JOIN chain. The UNION (without ALL) deduplicates images that qualify through both paths. The outer TOP applies after the UNION, and the optimizer can push rowgoal down into each branch because the value is a sniffable local variable.

The FORCESEEK hints on the association tables were retained in v7. They should no longer be necessary now that the join shape is unambiguous, but the call to remove them is being held back until the post-deployment behavior at multiple MFCs is observed. Section 11 carries the follow-up.

---

## 7. Risk & Rollback

**What could go wrong.** Three concerns sit on this refactor.

The first is that `UNION` carries an implicit DISTINCT that can be expensive on a wide result set. The result set here is two columns (`Id` and `ContentCode`), and `Id` is the clustering key, so the distinct cost is bounded. The risk is small but worth confirming in the post-deployment monitoring.

The second is that the rowgoal optimization, once enabled, can be sensitive to data shape variance across MFCs. A site with a very small population of qualifying images can satisfy TOP from the first branch alone, while a site with very few Shipped Rx images and many Verified Canister images can require both branches to fill the TOP. Plan stability across this variance should be confirmed at each site.

The third is the FORCESEEK retention. If the optimizer's natural choice under v7 differs from what FORCESEEK forces, the hint can prevent the optimizer from choosing a better plan. Removing the hints is the cleaner end state but requires evidence that the natural choice is correct.

**What to look for in the first 24 hours.** The plan choice on `OeOrderHistory` is the headline indicator. If the deployed plan binds `OeOrderHistory.ByOrderStatus` we have the predicted win. If it binds `PK_OeOrderHistory` something in the refactor or the statistics is preventing the switch and the analysis has to be revisited.

**Rollback path.** The v6 body is in `Original.sql` in this folder. Redeploying v6 reverts cleanly. There is no schema dependency that v7 introduces that v6 cannot accept back.

---

## 8. Evidence of Refactor (v7)

Captured against Tolleson. Warm cache. Same data state as Section 3.2.

```
(paste STATS IO / STATS TIME for v7 against the same data state used for v6 here)
```

The exploratory runs recorded during the refactor work showed the LOB reads dropping from roughly 68,000 per execution to near zero under the exploratory branch that also removed the LOB predicate, and the logical reads on `OeOrderHistory` dropping by roughly an order of magnitude under the index switch from `PK_OeOrderHistory` to `ByOrderStatus`. The committed v7 retains the LOB predicate, so the LOB reads will not collapse to zero under v7 as deployed. They will, however, be exercised over a smaller candidate set because the per-branch INNER JOINs prune earlier.

---

## 9. Comparison & Improvement

| Metric | v6 (paste) | v7 (paste) | Delta |
|---|---|---|---|
| Logical reads (total) | | | |
| Logical reads on `OeOrderHistory` | | | |
| LOB page reads | | | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan operator count | | | |
| Plan: index used on `OeOrderHistory` | | | |
| Result row count | | | |

**Verdict (to be entered once the captures land):** the headline expected delta is the index switch on `OeOrderHistory` from `PK_OeOrderHistory` to `ByOrderStatus`, driven by the structural refactor. Logical reads on `OeOrderHistory` should drop by roughly an order of magnitude. LOB reads should be present at roughly the same per-row cost but applied to fewer candidate rows, because each UNION branch prunes earlier. Total CPU and elapsed should fall accordingly.

---

## 10. Validation Checklist

Marked once the captures and the plans are in hand.

- Same data state. Captures taken back to back, ideally with a data freeze. (?)
- Warm cache only. Two runs, cold-cache numbers discarded. (?)
- Non-zero result set. Both runs returned at least one row. (?)
- Identical result set. Same row count and same row identities for deterministic queries. (?)
- Plan shape matches prediction. `OeOrderHistory` bound to `ByOrderStatus`. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call (to be entered once the checklist is completed): the refactor is sound on first principles and the exploratory evidence supports the expected delta. The committed v7 is the right body to send through review.

---

## 11. Open Items / Future Improvements

### 11.1 Remove the LOB predicate on `ImageData` (next follow-up)

The single largest unrealized win on this procedure is removing `i.ImageData Is Not Null` from both UNION branches. The exploratory work recorded that this change eliminates 100 percent of the LOB reads on the procedure. The committed v7 carries the predicate because the predicate was preserved during the v6-to-v7 transition to keep the semantic surface area unchanged.

The follow-up is to introduce a non-LOB indicator (a `HasImageData Bit` column, a `ImageDataLen` int, or similar) on the `ImgImage` and `ImgImage_IntId` tables, populated by trigger or by the application code that writes the image. The procedure can then filter on the non-LOB column and skip the LOB inspection entirely. This is a schema change and is therefore a recommendation rather than a same-PR refactor.

Once the non-LOB indicator is in place, both UNION branches change from:

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

with no other body change required.

### 11.2 Remove FORCESEEK hints once the natural plan is observed

The FORCESEEK hints on `ImgRxImgAssoc` and `ImgCanImgAssoc` are vestiges of the v6 fight against the optimizer. With the v7 structural rewrite they should no longer be necessary, but removing them is gated on a post-deployment observation that the natural plan binds the same seeks. Sequence: deploy v7 with hints retained, observe, then ship v8 that removes the hints if the observed plan is stable.

### 11.3 Write a "LOB Column Predicates" masterclass entry

The LOB predicate observation has come up enough on the Img family of procedures that it warrants its own pattern note in the masterclass library. The note should cover the cost mechanism (the LOB pointer inspection on every candidate row), the recommended fix (a non-LOB indicator column), and the schema-vs-query trade-off (the indicator column is a schema change, not a same-PR fix). The next refactor that surfaces the pattern should produce the note.

### 11.4 Confirm `vImgImage` is the right read source

`vImgImage` is a UNION ALL view over `ImgImage` and `ImgImage_IntId`. The retention in v7 is forward-compatible and is correct as long as both base tables are populated. If `ImgImage_IntId` is empty in production, the view adds an empty UNION ALL branch to every query plan, which carries a small per-execution overhead. Confirming the population of `ImgImage_IntId` and, if it is empty, replacing the view reference with the base table directly is a one-line win.
