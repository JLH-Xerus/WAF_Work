# Refactor Recommendation: lsp_PwkGetVerifiedRxsWithManualPriority

**Date:** 2026-05-08
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** Cataloged. v3 proposed by the MFC DBA team, awaiting iA review.

---

## Problem

v2 of `dbo.lsp_PwkGetVerifiedRxsWithManualPriority` runs the paperwork poller. The cross-list capture puts it on all four expense lists (Volume rank 21, Duration 11, Expense 12, Plan 18) with a 1,409x worst-case-to-average ratio. Cost concentrates on two correlated subqueries against `OrderToteAssoc` that run once per candidate parent and on a non-SARGable modulo-10 mask predicate that drives parameter-sniffing volatility.

The five structural issues:

- Correlated scalar subquery for the in-tote ToteId. `IsNull((Select Top 1 ToteId From OrderToteAssoc ... Where (OrderId Like O.OrderId + '-[0-9][0-9]' Or OrderId = O.OrderId) And OrderToteStateCode = 'I'), '')` runs once per outer row with a non-SARGable LIKE.
- Correlated scalar Count subquery for child-vial completeness. Same shape, executed per Split/MV parent, also non-SARGable.
- Non-SARGable mask filter on PatCustId. `@PatCustIdMask Like '%' + Cast((PatCustId % 10) As varchar) + '%'` is fundamentally non-seekable and is repeated identically in both UNION branches.
- Parameter sniffing on `@PatCustIdMask` and `@MultiToteRequired`. No local-variable indirection.
- Legacy `Object_Id` temp-table check. Modern equivalent is `Drop Table If Exists`.

## Recommendation

Apply v3 as proposed. No schema change. No index change. The package handles correlation, sniffing, and the legacy idiom in a single deployment unit.

1. Pre-materialize `OrderToteAssoc` filtered to `OrderToteStateCode = 'I'` into `#ToteState`, deriving `ParentOrderId` once per row via `Left(OrderId, Len(OrderId) - 3)` when the OrderId matches `'%-[0-9][0-9]'`, else OrderId.
2. Index `#ToteState(ParentOrderId)` after population.
3. Aggregate the in-tote state in a single `InToteAgg` CTE that returns `ChildCount` and `FirstToteId` per parent, joined once into the candidate build.
4. Assign `@LocalPatCustIdMask` and `@LocalMultiToteRequired` after parameter normalization and reference the locals downstream.
5. Apply the mask predicate inside a `Filtered` CTE so it is evaluated once over the candidate set, and drive the two UNION branches off that filtered set.
6. Add `Option (Recompile)` to the final SELECT so the per-call plan sees the actual mask selectivity.
7. Replace the `Object_Id` check with `Drop Table If Exists`.

The full v3 body is in `Refactored.sql`. The v2 body is in `Original.sql` for diff and rollback.

## First Principles

**A correlated subquery is per-row evaluation.** From `masterclass/Correlated Subqueries to CTEs.md`:

> A correlated subquery references columns from the outer query, which means the database engine must evaluate the subquery once per row of the outer result set. The subquery is logically correlated to each outer row.

Both v2 subqueries share the same filter (`OrderToteStateCode = 'I'`) and the same parent-child relationship. v3 scans `OrderToteAssoc` once into `#ToteState`, derives `ParentOrderId` at materialization time, and joins back via an indexed lookup. The aggregate (`ChildCount`, `FirstToteId`) is produced in one pass; the outer query reads the materialized columns directly. Reads on `OrderToteAssoc` drop from N (one per candidate) to 1 (one filtered scan).

**Local variables defeat sniffing by removing the value the optimizer would otherwise sniff.** From `masterclass/Parameter Sniffing.md`:

> Why it works: the optimizer knows it can't see the runtime value of a local variable at compile time, so it falls back to density-based estimates. It uses the [[Density Vector]] statistics (average rows per distinct value) instead of the histogram for a specific value. This produces a middle-of-the-road plan that is never optimal for any single value but is acceptably good for all values.

`@LocalPatCustIdMask` and `@LocalMultiToteRequired` give the optimizer a density-based plan rather than a histogram-based plan tied to whatever the first caller passed. The mask predicate is non-SARGable regardless, so the goal is not to enable a seek; the goal is to take the value out of the optimizer's compile-time view so that successive callers do not share a plan tuned to a different selectivity. `Option (Recompile)` on the final SELECT pays the per-call compile cost in exchange for accurate selectivity estimates on the local-variable form.

**Apply non-SARGable filters once, over the smallest possible set.** The modulo-10 mask cannot be made seekable without a persisted computed column on PatCustId. v3 does not rewrite the predicate; it reduces the row count the predicate is evaluated against by pulling the filter into the `Filtered` CTE so the candidate set is already small (bounded by the upstream `OrderStatus` filter) before the mask runs.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- CTE materialization and consumption. [WITH common_table_expression (CTE)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) documents the syntax and semantics for both the `InToteAgg` aggregation and the `Filtered` candidate set.
- Recompile semantics. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) and [Recompile a Stored Procedure](https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/recompile-a-stored-procedure) cover the `Option (Recompile)` behavior and its trade-off against plan-cache reuse.
- Predicate SARGability. [Predicates](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) describes the seek-versus-scan distinction that the modulo-10 mask predicate fails on.

## Risk Note

The principal semantic risk is the `ChildCount` aggregate. v2 counted children only via `OrderId Like O.OrderId + '-[0-9][0-9]'`, which excludes the parent. v3's `InToteAgg` aggregates by the derived `ParentOrderId`, which can include the parent row when the parent itself is in a tote. For the `@MultiToteRequired = 1` completeness check, this can differ by one row. Section 11.1 of the analysis carries the children-only `Sum(Case When OrderId <> ParentOrderId Then 1 Else 0 End)` form as the safer alternative; apply it if the validation check flags a row-count divergence. First 24 hours: watch the `OrderToteAssoc` read count per call (expected to drop from N to 1), the plan variant count per site, and the max-vs-avg ratio. Rollback is `Alter Procedure` from `Original.sql`.
