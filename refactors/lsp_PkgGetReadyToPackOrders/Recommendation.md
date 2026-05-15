# Refactor Recommendation: lsp_PkgGetReadyToPackOrders

**Date:** 2026-05-08
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** Cataloged. v44 proposed by the MFC DBA team, awaiting iA review.

---

## Problem

v43 of `dbo.lsp_PkgGetReadyToPackOrders` drives the Package/Ship Orders form at every pack station. The cross-list capture reports the procedure on three of four expense lists (Duration rank 5, Expense 6, Plan 2) with "severe parameter sniffing" identified as the dominant heuristic. The proc carries seven input parameters and the WHERE clause in the final SELECT changes shape across parameter combinations, so v43 caches a single plan against the first call's values.

The structural issues:

- Parameter sniffing across all seven input parameters. No local-variable indirection.
- Three independent CTEs for the hazardous flags (HazardousToHandle, HazardousToDispose, HazardousToShip). Each scans `InvFlaggedProducts` independently and is left-joined three times into the main `#Rxs` SELECT.
- Correlated scalar Count subquery for `NumOfPendingOrProblemRxs` in the `#GrpData02` SELECT (line 464). Per-row scan of `#Rxs` with four equality predicates.
- Scalar UDF `dbo.fOrdPatNameFmtd` per row of `#Rxs` (line 313). Parallelism barrier on the per-Rx data assembly.
- Catch-all WHERE in the final SELECT (lines 547-610). Five parameter-controlled disjunctions that the optimizer plans against the worst-case shape.
- Five scattered legacy `If Object_Id ... Drop Table` blocks.

## Recommendation

Apply v44 as proposed. Three drivers (sniffing, hazmat consolidation, pending/problem aggregation) get the direct structural fix. The catch-all WHERE gets `Option (Recompile)` rather than an IF/ELSE branch tree because seven parameters and five disjunctions would multiply the proc body for marginal benefit. The scalar UDF is deferred to v45 to keep the v44 diff reviewable.

1. Assign `@LocalRouteStop` and the six other locals from the input parameters and reference the locals downstream.
2. Replace the three hazardous-flag CTEs with a single `#HazFlags` aggregate keyed on `ProductId`, producing `IsHazHandle`, `IsHazDispose`, `IsHazShip` via `Max(Case ...)`. The main `#Rxs` SELECT consumes this via one left-join.
3. Pre-materialize `#PendingProblemCounts` keyed on `(GroupNum, ShipmentId, ToteId)` via `Sum(Case When OrderStatus In ('Pending', 'Problem') And QueueStationId Is Not Null Then 1 Else 0 End)`. The `#GrpData02` SELECT left-joins to this on the three keys.
4. Add `Option (Recompile)` on the `#Rxs` build and the final SELECT.
5. Consolidate the five legacy `Object_Id` blocks into one `Drop Table If Exists`.

The full v44 body is in `Refactored.sql`. The v43 body is in `Original.sql` for diff and rollback.

## First Principles

**Several adjacent same-source flags collapse into one aggregate.** From `masterclass/Conditional Aggregation Consolidation.md`:

> One scan, one statement, three counts. The shared `Where AddrBank = @AddrBank` predicate is evaluated once. The branch-specific predicates move into `Case` expressions inside the aggregate. The optimizer reads the table the minimum number of times required, which is once.

The three hazmat flags all come from `InvFlaggedProducts` filtered on the same ProductId set. `Max(Case When IsFlaggedFor = 'HAZMAT' Then 1 Else 0 End)` and its two siblings produce all three flags in one scan with one Group By. The main `#Rxs` SELECT goes from three left-joins to one.

**Correlated scalar subqueries become set-based aggregates.** From `masterclass/Correlated Subqueries to CTEs.md`:

> A correlated subquery references columns from the outer query, which means the database engine must evaluate the subquery once per row of the outer result set. The subquery is logically correlated to each outer row.

The v43 `NumOfPendingOrProblemRxs` subquery scans `#Rxs` once per row of `#GrpData01` with four equality predicates per scan. The v44 `#PendingProblemCounts` aggregate is one pass over `#Rxs` grouped by `(GroupNum, ShipmentId, ToteId)`, with the four predicates inside the Case expression. The downstream join is a clean equality on the three keys.

**Local variables defeat sniffing volatility across many parameters.** From `masterclass/Parameter Sniffing.md`:

> Why it works: the optimizer knows it can't see the runtime value of a local variable at compile time, so it falls back to density-based estimates. It uses the [[Density Vector]] statistics (average rows per distinct value) instead of the histogram for a specific value. This produces a middle-of-the-road plan that is never optimal for any single value but is acceptably good for all values.

Seven sniffing-prone parameters compound. The local-variable form makes the optimizer plan against density rather than against whatever the first caller passed.

**Recompile handles the catch-all WHERE that branch-trees would explode.** From `masterclass/Catch-All Query Anti-Pattern.md`:

> `Option (Recompile)` tells the engine to skip plan caching and recompile the query on every call. At runtime the parameter values are known, so the engine can substitute them into the predicates and apply parameter embedding.

Five disjunctions and seven parameters would produce dozens of IF/ELSE branches; the Recompile hint gives the optimizer the actual parameter values at compile time so it can simplify the dead disjunction branches. The per-call compile cost is bounded by the catch-all's roughly seven distinct effective shapes across the parameter combinations in practice.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Aggregation. [Aggregate Functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) covers the `Max(Case ...)` and `Sum(Case ...)` pivot semantics used in `#HazFlags` and `#PendingProblemCounts`.
- Plan hints. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) and [Recompile a Stored Procedure](https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/recompile-a-stored-procedure) cover the `Option (Recompile)` semantics on the catch-all SELECT.
- Parameter-sensitive plans. [Parameter Sensitive Plan Optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization) covers the SQL Server 2022+ feature relevant to the sniffing follow-up, when the deployment fleet reaches that compat level.
- CTE syntax. [WITH common_table_expression (CTE)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) is the canonical reference for the consolidated CTEs.

## Risk Note

The hazmat consolidation produces one row per ProductId regardless of how many flag rows existed in the source. `Max()` of a boolean flag is idempotent, and the `In ('HAZMAT', 'HAZDSP', 'HAZSHP')` filter matches the v43 set. The pending/problem aggregate preserves v43's `QueueStationId Is Not Null` condition inside the Case. The Recompile hint takes the per-call compile cost; the catch-all WHERE has roughly seven distinct effective shapes across the parameter combinations, so the compile cost is bounded. v44 retains the `dbo.fOrdPatNameFmtd` UDF call; v45 will inline it once the UDF body is confirmed against the production deployment. First 24 hours: per-MFC plan variant count, scans of `InvFlaggedProducts` per call (expected to drop from 3 to 1), and absence of the per-row Count subquery against `#Rxs`. Rollback is `Alter Procedure` from `Original.sql`. No schema or index change.
