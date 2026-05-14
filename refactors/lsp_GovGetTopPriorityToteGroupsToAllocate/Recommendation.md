# Refactor Recommendation: lsp_GovGetTopPriorityToteGroupsToAllocate

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** v2 in production. v3 proposed by the MFC DBA team, awaiting iA review.

---

## Problem

v2 of `dbo.lsp_GovGetTopPriorityToteGroupsToAllocate` returns the top-priority tote groups eligible to allocate from the queued Rx group pool, broken down by order type (single tote, group tote, group vial, single vial). The procedure appears on two of four expense lists (Expense #16, Plan #7) with "plan instability" and "algorithmic / plan rewrite candidate" as the dominant heuristics. The cost concentrates in three places, all addressable without schema change.

The five issues:

- Duplicated `Not Exists` "blocking Rx" subquery across five Insert blocks. Identical structure, roughly 10 lines each, joins `GovRx` to `OeOrder` with a non-trivial status disjunction. Worst-case path runs all five blocks and pays for the same scan five times.
- `@GroupsToAllocate` table variable. Joined back to in the final SELECT alongside a correlated `Exists` for `HasAllocatedRx`. The optimizer's one-row estimate drives nested loops where hash or merge would be better.
- Correlated `Exists` subquery for `HasAllocatedRx` in the final SELECT. Per-row execution against `GovRx + OeOrder`.
- No local-variable indirection for the bit parameters.
- No supporting indexes on the new temp tables (introduced by v3).

## Recommendation

Ship v3 as a single deployment unit. The package converts a duplicated subquery into a pre-materialized indexed temp table, swaps a table variable for a temp table, and replaces the correlated `Exists` with a Left Join probe. No schema change.

1. Pre-materialize the blocking pairs into `#BlockedPairs (GroupNum, ToteGroupNum)` with a supporting index. Each of the five Insert blocks then probes `#BlockedPairs` via `Left Join ... Where BP.GroupNum Is Null`.
2. Convert `@GroupsToAllocate` to `#GroupsToAllocate` with an index on `(GroupNum, ToteGroupNum)`.
3. Pre-materialize the allocated pairs into `#AllocatedPairs` and replace the per-row `Exists` in the final SELECT with a Left Join plus `Case When AP.GroupNum Is Not Null Then 1 Else 0 End`.
4. Add local-variable indirection for the bit parameters and the TVP.
5. Apply `Option (Recompile)` to the statements where parameter shape drives the selected plan.

The full v3 body is in `Refactored.sql`. The v2 body is in `Original.sql` for diff and rollback.

## First Principles

**Pre-materialize a correlated subquery when it appears in multiple call sites.** From `masterclass/Correlated Subqueries to CTEs.md`:

> The key insight: the CTE scans `CfStoreDeliveryCourierCutOff` once, groups by pharmacy, and produces a small result set. The join back to the outer query is a simple lookup. Total reads on the delivery table: 6 logical reads instead of thousands.

The `Not Exists` subquery in v2 appears verbatim across five Insert blocks. The materialize-once form scans `GovRx + OeOrder` once into `#BlockedPairs`, indexes it on `(GroupNum, ToteGroupNum)`, and replaces five subquery executions with five clustered-index probes. The `HasAllocatedRx` correlated `Exists` in the final SELECT follows the same pattern: one scan into `#AllocatedPairs`, one Left Join in the final SELECT.

**Table variables do not carry statistics.** From `masterclass/Table Variables vs Temp Tables.md`:

> Table variables (`Declare @T Table (...)`) have no statistics. The SQL Server optimizer always estimates that a table variable contains 1 row, regardless of how many rows you actually insert.

`@GroupsToAllocate` participates in the final SELECT's join and in the `#AllocatedPairs` build. The one-row estimate cascades to every downstream operator, so the optimizer chooses nested loops when hash or merge would be appropriate. `#GroupsToAllocate` with an index on `(GroupNum, ToteGroupNum)` gives the optimizer real cardinality and supports the join as a clean indexed access.

**Parameter sniffing volatility against bit-parameter combinations.** From `masterclass/Parameter Sniffing.md`:

> The classic fix is to assign input parameters to local variables and use only the locals in your queries:

Five exclusion bits plus a TVP plus two config values is enough state space that a single cached plan is unlikely to be optimal across all branches. The cross-list capture flagged this procedure for plan instability specifically. Local variables paired with `Option (Recompile)` on the parameter-sensitive statements gives the optimizer the actual values without committing to a sniffed plan.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Existence checks. [EXISTS (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql) documents the semantics behind the `Left Join + IS NULL` probe pattern in the five Insert blocks.
- Index support. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) covers the supporting index on `#BlockedPairs` that turns the probe into a seek.
- Plan control. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) documents `OPTION (RECOMPILE)` for the parameter-sensitive statements.
- Plan sensitivity. [Parameter Sensitive Plan Optimization](https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization) describes the engine-side facility that PSPO-aware deployments can fall back on if recompile per call is too expensive.

## Risk Note

The semantic equivalence relies on three claims, each independently verifiable. First, a (GroupNum, ToteGroupNum) pair is blocked iff at least one blocking Rx exists for it; the v3 `Distinct` on `#BlockedPairs` removes duplicates that arise when multiple Rxs in the same pair satisfy the blocking criteria. Second, `#AllocatedPairs` with `RxCvyStateCode = '0'` reproduces the v2 `Exists` semantics exactly. Third, the table-variable to temp-table conversion is semantically equivalent; the only behavior change is the statistics the optimizer now sees. Watch the first 24 hours for plan-variant count per site (expected: 1, down from 2+), executions of the blocking subquery (expected: 1 per call instead of 5), and result-row parity with v2 on the same data state. Rollback is a pure DDL revert; redeploy v2 from `Original.sql`.
