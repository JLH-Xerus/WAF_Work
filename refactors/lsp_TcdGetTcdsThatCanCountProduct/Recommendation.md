# Refactor Recommendation: lsp_TcdGetTcdsThatCanCountProduct

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. Refactored.sql is a proposed v18 awaiting iA review.

---

## Problem

v17 returns the online, initialized, non-dry, non-flagged dispensers in a bank that can count a given product, ordered by hopper sufficiency. The procedure is short but the cross-list capture flags it on three of four expense lists (Volume #8, Duration #15, Expense #14) with "high-volume / sub-ms" and "algorithmic / plan rewrite candidate" as the dominant heuristics. The structure carries a per-row cost that compounds at scale.

- Two correlated `Select Top 1` scalar subqueries in the SELECT list. Each one executes per row of the outer query: one for the `EarliestExpDate` fallback against `CanLotCode.ExpireDate`, one for the `OldestLotInitReplenDtTm` fallback against `CanLotCode.LastAddDtTm`. Both join `CanCanister` to `CanLotCode` on the same `TcdSn`.
- Catch-all `(T.InvPool = @InvPool Or @InvPool = 0)` predicate. The canonical compile-once-scan-forever shape.
- Catch-all `IsNull(T.LastNdcReplenished, '') = Case When Len(@MatchingNdc) > 0 Then @MatchingNdc Else IsNull(T.LastNdcReplenished, '') End`. Same anti-pattern, more obscure form. The Case on the right-hand side is a function of the column being filtered, which the optimizer cannot simplify at compile time.
- No local-variable indirection for any of the five parameters.

## Recommendation

Apply the v18 package. Pre-materialize the lot aggregates once per call, rewrite both catch-all predicates as `@sentinel = special_value Or column = @sentinel`, declare locals for all five input parameters, and let `Option (Recompile)` simplify the disjunctions at compile time.

1. Declare locals for all five input parameters at the top of the procedure.
2. Build `#CandidateTcds` from `TcdStatus` with the conditional catch-all predicates and the existing exclusions on `TcdDryDispensers` and `TcdFlaggedTcds`.
3. Build `#TcdLotFallbacks` keyed on `TcdSn` with `Min(ExpireDate)` and `Min(LastAddDtTm)` from `CanCanister` joined to `CanLotCode`, filtered to the candidate Tcds.
4. Final SELECT left-joins `#TcdLotFallbacks` for the two fallback columns, applies the order-by, and ends with `Option (Recompile)`.
5. Validate the Recompile cost against the sub-millisecond call cadence post-deployment; the local-variable form alone is the fallback if the per-call compile overhead is dominant.

The full v18 body is in `Refactored.sql`. The v17 body is in `Original.sql` for diff and rollback.

## First Principles

**Per-row subqueries to single-pass aggregations.** From `masterclass/Correlated Subqueries to CTEs.md`:

> A correlated subquery references columns from the outer query, which means the database engine must evaluate the subquery once per row of the outer result set. The subquery is logically correlated to each outer row.

The two `Select Top 1 ... Order By` subqueries are correlated on `TcdSn` and run once per candidate dispenser. Pre-materializing both fallback columns into `#TcdLotFallbacks` with `Group By TcdSn` and `Min(ExpireDate)`, `Min(LastAddDtTm)` aggregates produces both values in a single indexed scan over `CanCanister + CanLotCode`. The `Min` aggregation matches the v17 `Top 1 ... Order By ... Asc` semantics for the projected value (the OrderId of the row holding the min would differ on ties but the projected min itself is identical).

**Catch-all rewrite under Recompile.** From `masterclass/Catch-All Query Anti-Pattern.md`:

> `Option (Recompile)` tells the engine to skip plan caching and recompile the query on every call. At runtime the parameter values are known, so the engine can substitute them into the predicates and apply parameter embedding. With `@Barcode = 'ABC'` and `@SortationCode = Null`, the predicate effectively becomes `(False Or Barcode = 'ABC') And (True Or ...)`, which the optimizer simplifies to `Barcode = 'ABC'`. That predicate is seekable.

Both catch-alls become `@sentinel = special_value Or column = @sentinel`. Under Recompile the optimizer evaluates the left disjunct at compile time and produces the simplified plan for the actual call: either "no filter on the column" or "equality filter on the column." The Case-form catch-all collapses the same way once the right-hand side is no longer a function of the column.

**Plan stability via local variables.** From `masterclass/Parameter Sniffing.md`:

> The classic fix is to assign input parameters to local variables and use only the locals in your queries.

The five input parameters move into locals at the top of the procedure. Every reference in the body resolves to the local. The Recompile evaluation in Section 11 of the Analysis is the gating check: at sub-millisecond cadence a 1-2ms compile overhead can dominate. If post-deployment monitoring shows compile time as the dominant cost, drop Recompile and keep the local-variable form.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Aggregation in place of per-row Top 1. [Aggregate Functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) covers the `Min` semantics; the [FROM clause plus JOIN, APPLY, PIVOT](https://learn.microsoft.com/en-us/sql/t-sql/queries/from-transact-sql) page documents the `Group By` shape that produces one row per candidate Tcd.
- Catch-all rewrite. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) documents `Option (Recompile)`. [Predicates (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) covers the SARGability behavior the rewrite restores.
- Parameter sensitivity. [Parameter Sensitive Plan Optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization) and [Recompile a Stored Procedure](https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/recompile-a-stored-procedure) describe the compile-time substitution that the catch-all rewrite depends on.

## Risk Note

The Recompile decision carries the main risk. At sub-ms cadence, a 1-2ms compile cost can exceed the plan-quality benefit. The fallback is the local-variable form without Recompile. The `Min` aggregates preserve the v17 `Top 1 ... Order By` projected-value semantics; tie-breaking on `OrderId` may differ but neither projected column is the OrderId of the tied row. Both catch-all rewrites are logically equivalent to v17. In the first 24 hours watch per-call compile time, per-call elapsed time (expected to be the same or lower than v17), `CanCanister` and `CanLotCode` read counts (expected to drop from N pairs to one indexed scan), and the candidate-Tcd seek plan. Rollback path: redeploy v17 from `Original.sql`. No schema change, no index change.
