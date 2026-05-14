# Refactor Recommendation: lsp_RxvGetRxsQueuedForVerification

**Date:** 2026-05-07
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** v7 deployed across the MFC fleet; v8 cataloged as the proposed deployment, captured 2026-05-07.

---

## Problem

v7 of `dbo.lsp_RxvGetRxsQueuedForVerification` is the verification-queue loader for the Verification UI. The cross-MFC capture from 2026-05-07 puts it at roughly 772 billion logical reads per month across 19.2 million executions, distributed across 7 of 14 reporting sites, and Indy, Liberty, and Memphis each carry three plan variants on the same statement (avg-duration spreads of 2.3x to 3.8x between variants). The cost concentrates in the duplicate `#QueuedRxs` build and the 4-level nested OR/AND in the `#RxList` WHERE clause.

The five structural issues:

- LEFT JOIN OR pattern. The first INSERT into `#QueuedRxs` is `LEFT JOIN ImgImage ... Where ContentCode In (2,3) Or @CfgRxDigitalVerificationMode = 'Local'`. The OR path lets the row survive without a join match, so the optimizer cannot push the predicate into the join and ends up with a scan-with-residual-filter shape on #RxList.
- Duplicate INSERT pair into `#QueuedRxs`. Two near-identical INSERTs that differ only in image table (`ImgImage` vs `ImgImage_IntId`) and a trailing OR. Each carries its own `Select Distinct` dedup.
- 4-level nested OR/AND in the #RxList WHERE clause. The autofill exclusion logic mixes configuration constants and row-level predicates inside the nest, defeating clean index usage on `ByOrderStatusFillTypeAddrBank`.
- Duplicate config reads. `vCfgSystemParamVal` is scanned twice per call, once per parameter.
- Unindexed temp tables. `#RxList`, `#AuditRxs`, and `#SemiAutoFillBanks` are all joined downstream as heaps. `#AuditRxs` also row-multiplies when an OrderId has multiple audit rules.

## Recommendation

Apply the v8 package as a single deployment unit. No schema change. No index change against base tables. All five fixes are independent and additive.

1. Consolidate the two INSERTs into one `INSERT ... SELECT DISTINCT` over a `UNION ALL` of three INNER JOIN branches (ImgImage path, ImgImage_IntId path, Local-mode pass-through).
2. Flatten the nested OR/AND. Compute `@ExcludeAutoFill = @CfgRxInheritsCanisterVerification` once and rewrite the WHERE as a flat OR list with one `Exists` probe against `#SemiAutoFillBanks`.
3. Fold the two `vCfgSystemParamVal` reads into one `Max(Case ...)` pivot scan.
4. Add clustered indexes to `#RxList(OrderId, HistoryDtTm)`, `#AuditRxs(OrderId)`, and `#SemiAutoFillBanks(Bank)` after population.
5. Aggregate `#AuditRxs` to one row per OrderId via `Max(Cast(IsVisualOnly As Int))` to eliminate the multi-rule row multiplication.

The full v8 body is in `Refactored.sql`. The v7 body is in `Original.sql` for diff and rollback.

## First Principles

**LEFT JOIN OR is two access paths the optimizer cannot plan together.** From `masterclass/LEFT JOIN OR Anti-Pattern.md`:

> The optimizer needs to satisfy `Condition_A OR Condition_B` where A and B involve different tables. It can't seek on table X's index AND table M's index simultaneously for an OR.

The first v7 INSERT is the disguised form: the LEFT JOIN to `ImgImage` is path 1 and the `@CfgRxDigitalVerificationMode = 'Local'` survival is path 2. The v8 split into three explicit INNER JOIN branches inside a single UNION ALL gives each path its own sub-plan with its own cardinality estimate, and the outer `Select Distinct` collapses any cross-branch overlap in a single sort instead of two per-INSERT sorts. In non-Local mode the optimizer constant-folds the Local-mode branch to false and prunes it from execution.

**Statistics on intermediate temp tables drive join strategy.** From `masterclass/Table Variables vs Temp Tables.md`:

> The "insert then index" pattern is a best practice. Building the index on the complete dataset is a single efficient sort, versus maintaining the b-tree during every insert.

`#RxList`, `#AuditRxs`, and `#SemiAutoFillBanks` are all joined downstream against base tables. v7 leaves them as heaps, so each join is a scan of the temp table. v8 indexes each after population at the natural lookup column. The clustered index on `#RxList(OrderId, HistoryDtTm)` is the highest-leverage one because the consolidated INSERT into `#QueuedRxs` joins it to `ImgRxImgAssoc` on those exact columns.

**Pre-computed configuration flags fold at runtime.** The 4-level nested OR/AND mixes `@CfgRxInheritsCanisterVerification` (a configuration constant for the call) with row-level FillType conditions. The optimizer plans for both branches because it cannot prove the configuration value is stable for the duration of the call. Lifting the flag into `@ExcludeAutoFill` at the top and reading it in a flat predicate gives the optimizer a constant to fold and gives the reader a clearer split between configuration cases and row-level filters.

**Same-table repeated reads consolidate to one scan.** Two SELECTs against `vCfgSystemParamVal` (one per parameter) collapse to one scan with a `Max(Case When Parameter = ... Then ...)` pivot. Single scan, single compile, two scalars assigned. The per-call cost is small in absolute terms but at 19 million monthly executions the doubled work is meaningful.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- UNION ALL consolidation. [UNION (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-union-transact-sql) documents the set-operator semantics and the difference between `UNION` and `UNION ALL`. The outer `Select Distinct` after the UNION ALL is what handles the cross-branch dedup.
- Existence checks. [EXISTS (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql) specifies the short-circuit behavior the `Exists` probe against `#SemiAutoFillBanks` depends on.
- Temp-table indexing. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) and the [Index Architecture and Design Guide](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-index-design-guide) cover the "insert then index" pattern and the statistics that the optimizer reads from temp tables once they are populated.
- Window-free aggregation. [Aggregate Functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) covers `Max(Case ...)` pivot semantics for the consolidated config read.

## Risk Note

Semantic risk concentrates on two fixes. The LEFT JOIN OR split must produce row-count parity with v7 across Local and non-Local modes; the outer `Select Distinct` handles any cross-branch overlap by construction. The `#AuditRxs` aggregation must preserve the "any visual-only rule wins" semantics, which `Max(Cast(IsVisualOnly As Int))` does provided the application's intent matches that interpretation. Confirm with the iA team before deployment. First 24 hours: watch Query Store plan count for the Indy, Liberty, and Memphis statements (should converge to 1 to 2), watch the Verification UI for empty result sets or duplicate rows, and watch tempdb extent churn at high-volume sites. Rollback is `Alter Procedure` from `Original.sql`. No schema or index change to revert.
