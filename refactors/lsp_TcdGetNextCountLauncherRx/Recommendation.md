# Refactor Recommendation: lsp_TcdGetNextCountLauncherRx

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. Refactored.sql is a proposed v10 awaiting iA review.

---

## Problem

v9 returns the next Scheduled Auto-Fill Rx for the TCD Controller Count Launcher to attempt to count within a cabinet bank. The cross-list capture flags it on all four expense lists (Volume #3, Duration #1, Expense #4, Plan #24) with a 6,024x worst-case-to-average ratio. A 6,024x spread on the same statement is a plan-volatility problem, not a query-shape problem; the optimizer is producing wildly different plans for the same statement depending on what parameter values it sniffs at compile time, and on a tight-loop caller the worst-case plan is what the operator sees during the bad windows.

- Non-SARGable `O.PriCodeSys <> Case @InclFillByNotReachedPriRxs When 1 Then -1 Else 60 End` predicate. The Case inside the WHERE clause defeats index seek on `PriCodeSys` and forces one cached plan across two distinct operational modes.
- Parameter sniffing on `@AddrBank` and `@InclFillByNotReachedPriRxs`. The cabinet banks vary significantly in candidate population; the bimodal `@InclFillByNotReachedPriRxs` is sniffed once and cached. The 6,024x ratio reflects both pressures.
- Forced `Index(ByOrderStatusFillTypeAddrBank)` hint on the main candidate scan. Freezes the plan choice and prevents the optimizer from adapting to statistics changes.
- Legacy `If Object_Id` temp-table cleanup in three places.
- `If Exists (Select * From #temp)` after the INSERT that just populated `#temp`. `@@ROWCOUNT` carries the same information without the second read.
- Duplicated sibling-OrderId inline derived table in two paths. Structurally awkward and harder to reason about than a CTE.
- Inner subquery in the group-together path returns one row per qualifying Rx, not one row per group. Joining on `GroupNum` produces a cardinality explosion when a group has many counted Rxs.

## Recommendation

Apply the v10 package. The dominant lever is plan stability via the local-variable form combined with the explicit-branch rewrite of the catch-all Case predicate.

1. Declare `@LocalAddrBank` and `@LocalInclFillByNotReachedPriRxs` and use the locals throughout.
2. Split the main path into two explicit branches selected by an `If` on `@LocalInclFillByNotReachedPriRxs`. The "include all priorities" branch has no `PriCodeSys` filter; the other branch carries `O.PriCodeSys <> 60`.
3. Drop the `Index(...)` hint. The optimizer will choose `ByOrderStatusFillTypeAddrBank` naturally once the predicate is seekable.
4. Replace the `If Exists (Select * From #temp)` after the INSERT with `If @@ROWCOUNT > 0`.
5. Express the sibling-OrderId derivation once per branch as a `CountedSiblings` CTE referenced from a Left Join on `O.OrderId = CS.SiblingOrderId`.
6. Pre-aggregate the group-together inner subquery to one row per `GroupNum` via `Max(DateFilled)` and `Min(LastStatusChgDtTm)`.
7. Convert the three legacy `Object_Id` cleanup blocks to `Drop Table If Exists`.
8. Add `Option (Recompile)` to each branch's final SELECT. Validate the compile cost against the tight-loop cadence post-deployment.

The full v10 body is in `Refactored.sql`. The v9 body is in `Original.sql` for diff and rollback.

## First Principles

**Plan stability via local variables.** From `masterclass/Parameter Sniffing.md`:

> The classic fix is to assign input parameters to local variables and use only the locals in your queries.

> Local variables give you plan stability at the cost of plan optimality. You will never get the best possible plan for a specific value, but you will also never get a catastrophically bad plan. For high-frequency OLTP procedures, stability almost always wins.

`@LocalAddrBank` and `@LocalInclFillByNotReachedPriRxs` move parameter binding to local-variable assignment, which the optimizer plans against the density-based estimate rather than a sniffed value. The 6,024x worst-case ratio is exactly the failure mode this pattern prevents.

**Catch-all rewrite via explicit branching.** From `masterclass/Catch-All Query Anti-Pattern.md`:

> The optimizer sees four separate queries. Each one compiles a plan suited to its actual filter, with no NULL guard interfering. Each branch caches its own plan and reuses it across calls. Index seeks become possible.

The procedure has two distinct operational modes selected by `@InclFillByNotReachedPriRxs`. Splitting them into two explicit branches lets the optimizer cache a tight plan per branch, with `PriCodeSys` either filtered or unfiltered. The Case-in-WHERE form prevented index seek on `PriCodeSys` and forced a single plan that served both modes badly.

**CTE in place of duplicated inline derivation.** From `masterclass/Correlated Subqueries to CTEs.md`:

> The key insight: the CTE scans `CfStoreDeliveryCourierCutOff` once, groups by pharmacy, and produces a small result set. The join back to the outer query is a simple lookup.

The sibling-OrderId derivation (a concatenation built from substrings of `OrderId`) appears twice in v9 as inline derived tables. Lifting it to a `CountedSiblings` CTE expresses the derivation once per branch and makes `O.OrderId = CS.SiblingOrderId` the visible join predicate rather than burying it inside an inline derived table. The CTE materializes once per call within each branch and the optimizer can plan the join cleanly.

**TOP-with-ORDER BY rowgoal and `@@ROWCOUNT`.** From `masterclass/TOP with ORDER BY Semantics.md`:

> TOP combined with ORDER BY creates a rowgoal optimization. The optimizer knows it only needs N rows in a specific order, so it may [...] use an ordered index scan. If an index on the ORDER BY column exists, it scans in order and stops after N rows. Very efficient.

The `If Exists (Select * From #temp)` after the INSERT re-reads the temp table that was just populated. `@@ROWCOUNT > 0` carries the same information without the second read. The rowgoal benefit is incidental; the larger win is one fewer scan per call on a tight-loop caller.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Parameter sensitivity. [Parameter Sensitive Plan Optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization) and [Recompile a Stored Procedure](https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/recompile-a-stored-procedure) cover the plan-stability mechanisms the local-variable form and Recompile use.
- Predicates and SARGability. [Predicates (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) documents the SARGable form that the catch-all rewrite restores.
- CTE and EXISTS. [WITH common_table_expression (CTE)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) documents the `CountedSiblings` shape. [EXISTS (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql) covers the existence semantics relevant to the `@@ROWCOUNT` substitution.
- TOP rowgoal. [TOP (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/top-transact-sql) and [ORDER BY clause](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql) describe the rowgoal optimization each branch's `Top 1` SELECT depends on.

## Risk Note

The local-variable plan-stabilization is generally safe but can produce a slightly worse plan than v9's sniffed-and-lucky case at a specific MFC. Per-MFC monitoring is non-optional given the 6,024x baseline ratio. The Recompile hint accepts a per-call compile cost; on a tight-loop caller this is observable in CPU time. If post-deployment monitoring shows compile cost as dominant, drop Recompile and rely on the local-variable form alone. The pre-aggregation in the group-together inner subquery changes the cardinality estimate the optimizer sees for the inner join; semantic output is identical because the outer query filters by `OrderStatus = 'Scheduled'` and the inner join only establishes group membership. In the first 24 hours watch plan variant count per site (expected to drop from many to one or two), max-vs-avg ratio (expected to drop from 6,024x to single or low double digits), and average duration. Rollback path: redeploy v9 from `Original.sql`. No schema change, no index change.
