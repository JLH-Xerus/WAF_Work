# Refactor Recommendation: lsp_ImgGetListOfTopXImagesToMove

**Date:** 2026-05-07
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. `Refactored.sql` matches the committed v7 body; ready for iA review handoff. v7 has not yet been captured at Tolleson; an exploratory body (not v7) was measured on 2026-05-13 and is documented in `Analysis.md` Section 8.2.

---

## Problem

v6 of `dbo.lsp_ImgGetListOfTopXImagesToMove` lands in the Query Store top-50 at every reporting site and is the largest LOB-read consumer in the Img family. The dominant cost in v6 is a `LEFT JOIN` plus `OR` across two different join paths (Rx and Canister), which forces the optimizer into a single plan that cannot seek efficiently on either path. `OeOrderHistory` binds to `PK_OeOrderHistory` rather than `ByOrderStatus`; `OrderStatus = 'Shipped'` ends up as a residual rather than a seek key. The Tolleson 2026-05-13 baseline measured 66,109 LOB logical reads on `ImgImage`, 286,341 logical reads on `OeOrderHistory`, and 2,142 ms elapsed.

The structural issues in v6:

- LEFT JOIN with OR across two join paths (lines 42-52). `Oh.OrderStatus = 'Shipped' Or C.Status = 'Verified'` spans two unrelated join chains. The optimizer cannot seek into `OeOrderHistory.ByOrderStatus` because the predicate lives in a disjunction across tables.
- TOP consumes a scalar subquery (line 33). `Top (Select Convert(Int, [Value]) From vCfgSystemParamVal Where ...)` defeats rowgoal optimization because the optimizer cannot evaluate the operand at compile time.
- Redundant join predicate `(i.Id = IR.ImgId Or i.Id = IC.ImgId)` (line 50). Repeats the JOIN ON clauses and complicates plan reasoning.
- LOB predicate `i.ImageData Is Not Null` (line 46) drives the 66,109 LOB page reads. Fix is a non-LOB indicator column; schema change, deferred.
- FORCESEEK hints on the association tables (lines 42, 44). Indicate the original author was fighting the v6 plan shape. Retained in v7 pending post-deployment validation.

## Recommendation

Apply the v7 package, which is the textbook fix for the LEFT JOIN + OR anti-pattern: split the two join paths into independent INNER JOIN branches and combine with `UNION` (not `UNION ALL`, because images can qualify through both paths and the result must deduplicate). The LOB predicate stays in v7 and is the largest open item; the schema change behind it is tracked in Section 11.1 of `Analysis.md`.

1. Pull the configured batch size into a local variable (`@MaxImages`) so the optimizer can sniff the value and apply rowgoal to the outer TOP. The rowgoal claim is unverified by the 2026-05-13 capture; the v7 capture is the test.
2. Split the LEFT JOIN + OR into two INNER JOIN branches in a derived table: Branch 1 joins `vImgImage` to `ImgRxImgAssoc` and `OeOrderHistory` with `OH.OrderStatus = 'Shipped'`. Branch 2 joins `vImgImage` to `ImgCanImgAssoc` and `CanCanister` with `C.Status = 'Verified'`.
3. Combine the branches with `UNION` and apply the outer `Top (@MaxImages)` plus `Order By Id Asc` over the combined set.
4. Retain the existing `WITH (NoLock, FORCESEEK)` hints on the association tables for the v7 ship, pending post-deployment observation. Section 11.2 of `Analysis.md` flags the removal as the cleaner end state.

The full v7 body is in `Refactored.sql`. The v6 body is in `Original.sql` for diff and rollback.

## First Principles

**LEFT JOIN + OR splits into UNION of INNER JOIN branches.** From `masterclass/LEFT JOIN OR Anti-Pattern.md`:

> 1. Each branch uses INNER JOINs, so the optimizer can push predicates into the join tree and use index seeks.
> 2. Each branch gets its own sub-plan with accurate cardinality estimates.
> 3. The optimizer never has to reason about an OR that spans different join trees.
> 4. UNION (not UNION ALL) handles deduplication automatically. Use UNION ALL only when you can guarantee a row cannot match both paths.

The exploratory 2026-05-13 capture confirmed the index-switch prediction: `OeOrderHistory` bound to `ByOrderStatus` with `'Shipped'` in the seek key prefix, rather than the v6 residual binding to `PK_OeOrderHistory`. The reads delta on this dataset was modest (-7.6%) because the per-image probe was already 1-row under v6 at Tolleson's current data shape; the structural win is the plan-shape correctness, not the headline number on this particular sample.

**TOP with a local variable enables rowgoal optimization.** From `masterclass/TOP with ORDER BY Semantics.md`:

> When the TOP value comes from a subquery (as in our proc), the optimizer doesn't know N at compile time and can't optimize the rowgoal as aggressively. Pulling the value into a local variable first helps

v7 declares `@MaxImages`, assigns from `vCfgSystemParamVal` in a separate statement, and passes the local to `Top (@MaxImages)`. The 2026-05-13 capture did not populate `EstimateRowsForRowGoal` on any RelOp under the exploratory body; the v7 capture is the test for whether the rewrite actually fires.

**Index choice follows query shape.** From `masterclass/Index Key Columns vs Included Columns.md`:

> A long residual on a Seek is a common cause of high reads. The seek looks like it's doing the right thing on paper, but each leaf row is being checked against more predicates than it should.

In v6 the `OrderStatus = 'Shipped'` predicate is the residual on `PK_OeOrderHistory` seeks. The v7 INNER JOIN form lifts the predicate into a position the optimizer can seek on directly. The existence of `ByOrderStatus` matters: without the index, no query rewrite produces the same win.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- UNION of branches. [Set Operators - UNION (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-union-transact-sql) documents the UNION-versus-UNION-ALL semantics that justify keeping the deduplicating UNION in v7 (an image can qualify through both Rx and Canister paths).
- TOP and rowgoal. [TOP (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/top-transact-sql) and the [ORDER BY clause](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql) describe the TOP-with-ORDER-BY semantics. The local-variable pattern is the mechanism that lets the optimizer apply rowgoal; the [Predicates](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) page anchors the SARGability prerequisite.
- Index selection by predicate. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) and [Create Indexes with Included Columns](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-indexes-with-included-columns) describe the seek-key versus included-column structure that `ByOrderStatus` exploits once the predicate sits in seek position.
- FORCESEEK semantics. [Table Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table) documents the FORCESEEK hint retained on the association tables pending observation.

## Risk Note

UNION over a two-column projection where `Id` is the clustering key has bounded dedup cost; worth confirming in monitoring that the plan choice on `OeOrderHistory` is `ByOrderStatus` and not back to `PK_OeOrderHistory`. Rowgoal sensitivity is real: a site with few Shipped Rx images and many Verified Canister images may need both branches to fill the TOP. FORCESEEK retention is conservative; if the optimizer's natural choice differs, the hint blocks a better plan, and the cleaner end state is v8 without hints. Watch the plan shape on `OeOrderHistory` in the first 24 hours after deploy; `ByOrderStatus` is the predicted win. Capture v7 at Tolleson under the same data state used 2026-05-13 before fleet-wide rollout, per Section 11.7 of `Analysis.md`. Rollback path: redeploy v6 from `Original.sql`. No schema dependency.
