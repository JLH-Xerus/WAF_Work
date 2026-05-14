# Refactor Recommendation: lsp_CanGetPriorityCanistersForReplenishment

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. Refactored.sql is a proposed v74 awaiting iA review. v74 is the first of an expected multi-version refactor sequence; v75 through v77 are scoped in the Analysis Section 11.

---

## Problem

v73 returns a prioritized list of M4C canisters that need replenishment. The cross-list capture reports it on all four expense lists (Volume rank 1, Duration rank 18, Expense rank 24, Plan rank 1) with a 249,457x worst-case-to-average ratio. That ratio is the largest plan-volatility signature anywhere in the rows 22 to 39 cohort and by a substantial margin the strongest parameter-sniffing signal in the program's evidence base. The cross-list summary names "severe parameter sniffing" as the dominant heuristic.

The procedure is also 1,347 lines with six cursor loops, nine intermediate temp tables, the scalar UDF `dbo.fFormatNdcForDisplay` called per output row, and a multi-branch UNION over `TcdStatus` that builds `#PriorityTcds` from roughly fifteen near-identical SELECT branches. The structural complexity is the second-largest cost driver after parameter sniffing.

v74 is a phased refactor. It addresses the parameter-sniffing dominant cost driver and the cheapest cleanup item; the cursor elimination, scalar UDF inlining, correlated-subquery consolidation, and priority-bucket UNION restructuring are deferred to v75 through v77. The 249,457x ratio is what v74 attacks directly; the structural changes in v75 and beyond compound the win incrementally.

- Parameter sniffing on all six input parameters. The 249,457x worst-case ratio is consistent with one cached plan reused across calls whose parameter selectivities differ by many orders of magnitude. `@StationId` gates the stock-location filtering in the cursor branches; `@SortColumn` and `@SortDirection` drive the long Case ladder in the final ORDER BY.
- Scalar UDF `dbo.fFormatNdcForDisplay` called once per output row in the final SELECT and the two `#CanisterList` INSERT blocks. v75.
- Six cursor loops. Two iterate `#PriorityTcds` and `#PriorityCans` for canister-to-tcd assignment; four iterate forward-stock and back-stock location regex patterns. v76.
- Multi-branch UNION over `TcdStatus` that builds `#PriorityTcds`, roughly fifteen near-identical branches sharing the same join chain. v77.
- Correlated subqueries against `InvReplenProductOverrides` (six per `#CanisterList` INSERT) and `InvNdcLocAssoc`. v75.
- Ten scattered `Drop Table If Exists` statements at the procedure tail. Consolidated in v74.

## Recommendation

Apply the v74 package. The local-variable substitution and the Recompile hint on the final ORDER BY SELECT address the dominant cost driver. Everything else stays in place pending v75 through v77.

1. Declare locals `@LocalStationId` through `@LocalSortDirection` for all six input parameters. Every reference in the body resolves to a local.
2. Add `Option (Recompile)` to the final ORDER BY SELECT so the optimizer can simplify the 20-branch Case ladder to the single applicable branch at compile time.
3. Consolidate the ten scattered tail `Drop Table If Exists` statements into a single block.
4. Defer the scalar UDF inlining, the correlated-subquery pre-materialization, the cursor elimination, and the priority-bucket UNION consolidation. Each one is scoped to a follow-on version with its own validation surface.

The full v74 body is in `Refactored.sql`. The v73 body is in `Original.sql` for diff and rollback.

## First Principles

**Plan stability via local variables.** From `masterclass/Parameter Sniffing.md`:

> The classic fix is to assign input parameters to local variables and use only the locals in your queries.

> Local variables give you plan stability at the cost of plan optimality. You will never get the best possible plan for a specific value, but you will also never get a catastrophically bad plan. For high-frequency OLTP procedures, stability almost always wins.

The six locals are assigned from the parameters at the top of the procedure. Every reference in the body below that point resolves to the local. The substitution was applied programmatically and verified by count: `@LocalStationId` appears 3 times in the body, the bit parameters 2 each, `@LocalSortColumn` and `@LocalSortDirection` 41 each. The procedure carries the largest plan-volatility ratio in the entire cohort; the local-variable form is the canonical fix and what v74 ships.

**Recompile on the final ORDER BY.** From `masterclass/TOP with ORDER BY Semantics.md`:

> TOP combined with ORDER BY creates a rowgoal optimization. The optimizer knows it only needs N rows in a specific order, so it may [...] use an ordered index scan. If an index on the ORDER BY column exists, it scans in order and stops after N rows. Very efficient.

The final ORDER BY is a Case ladder over `@LocalSortColumn` and `@LocalSortDirection` with twenty branches. Under `Option (Recompile)` the optimizer can simplify the Case ladder at compile time to the single branch that applies for the current call, avoiding the cost of evaluating every branch of the Case at run time. The Recompile is justified by the volatility ratio; on a procedure called by the replenishment screen at moderate frequency, the per-call compile cost is bounded.

**Phased refactor to keep validation surfaces narrow.** A 1,347-line procedure rewritten in one PR is harder to review than a sequence of focused changes. v74 ships the change that addresses the 249,457x ratio directly. v75 inlines `dbo.fFormatNdcForDisplay` (the scalar-UDF parallelism barrier covered in `masterclass/Scalar UDF Parallelism Barrier.md`) and pre-materializes the correlated subqueries against `InvReplenProductOverrides` and `InvNdcLocAssoc` (the pattern in `masterclass/Correlated Subqueries to CTEs.md`). v76 rewrites the six cursor loops as set-based equivalents using `Row_Number()` over the `#PriorityTcds * #PriorityCans` join. v77 consolidates the multi-branch UNION on `TcdStatus` using the discipline in `masterclass/UNION ALL Views.md`: one base join plus explicit Case-driven priority projection.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Parameter sensitivity. [Parameter Sensitive Plan Optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization) covers the optimizer behavior the 249,457x ratio diagnoses. [Recompile a Stored Procedure](https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/recompile-a-stored-procedure) describes the per-call recompile mechanism the final SELECT uses.
- Query hints. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) documents `Option (Recompile)` and the Case-ladder simplification it enables.
- ORDER BY semantics. [ORDER BY clause](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql) covers the Case-driven ORDER BY shape relevant to the 20-branch ladder.

## Risk Note

The local-variable substitution is mechanical and the risk surface is narrow; the substitution removes the optimizer's ability to sniff a high-selectivity parameter and produce a tight plan for that specific call, in exchange for a stable plan compiled against the density-based estimate. For the worst-case-to-average ratio this procedure exhibits, the stable plan is the better deal even when it is slightly slower than v73's best case. The Recompile on the final ORDER BY takes a per-call compile cost; the Case ladder is long but the compile cost should be small in absolute terms and the simplification benefit is large. In the first 24 hours watch plan variant count per site (expected to drop from many to one), max-vs-avg ratio per site (expected to drop from 249,457x to single or double digits), and average duration per site (expected to fall for previously-worst-case calls and rise slightly for previously-best-case calls). Per-MFC post-deployment monitoring is non-optional given the baseline ratio. Rollback path: redeploy v73 from `Original.sql`. No schema change, no index change.
