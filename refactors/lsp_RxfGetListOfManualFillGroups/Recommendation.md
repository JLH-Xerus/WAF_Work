# Refactor Recommendation: lsp_RxfGetListOfManualFillGroups

**Date:** 2026-05-07
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** In iA Review. v49 Merged_v1 body submitted; v48 is the committed baseline.

---

## Problem

v48 of `dbo.lsp_RxfGetListOfManualFillGroups` drives the Manual Fill station UI. The body is 459 lines, the call frequency scales with active manual-fill workstations, and the cost is spread across five structural drivers rather than concentrated in one. The cross-list capture flags the procedure on three of four expense lists (Duration 5, Expense 6, Plan 2) with "severe parameter sniffing" as the dominant heuristic. The proc has been associated with operational cancellations at multiple MFCs.

The structural issues:

- Scalar UDF (`dbo.fOrdGroupingCritValForDisplay`) on every output row. The UDF serializes the entire surrounding plan and carries its own per-row `OeGroupSplitCodeAssoc` join.
- Three correlated subqueries against `CfStoreDeliveryCourierCutOff` for the per-pharmacy next-delivery-day computation. Same target table, related but not identical predicates, all evaluated per outer row.
- `@ToteGroups` table variable joined to large base tables. One-row cardinality estimate forces nested loops everywhere downstream.
- Parameter sniffing across the seven input parameters. The proc normalizes inputs at entry but then references the raw parameter symbols in downstream queries.
- Three independent CTEs for hazardous flags. Each scans `InvFlaggedProducts` and is left-joined separately into the main result build.
- Heuristic per-group flags computed in the SELECT list rather than once per group.
- `Select *` and implicit column lists across `Insert Into #OeDataUserLocs`. Fragile against schema change.

## Recommendation

Apply v49 Merged_v1 as a single deployment unit. The five fixes are each independent, but the combination is the right size for one review pass. Per-MFC post-deployment monitoring is non-optional given the breadth of the change.

1. Assign `@LocalStationId`, `@LocalFillType`, `@LocalToteId`, `@LocalBatchFillId` (and the other locals) after the parameter normalization block, and reference the locals exclusively downstream.
2. Convert `@ToteGroups` to `#ToteGroups` and add `IX_ToteGroups_GroupNum` after population.
3. Replace the scalar UDF with the `GroupingCriteria` CTE driven by `String_Agg(SC.Descrip, '; ')` over `OeGroupSplitCodeAssoc` joined to `OeGroupSplitCode`.
4. Consolidate the three correlated `CfStoreDeliveryCourierCutOff` subqueries into the `NextDeliveryDay` CTE that computes `ThisWeekDay` and `FirstWeekDay` per pharmacy in one pass.
5. Produce all per-group hazmat, controlled-substance, cold-storage, and pen/sulfa flags in the `InventoryFlags` and `ProductFlags` CTEs via `Max(Case ...)`. The final SELECT consumes them with `Coalesce`.
6. Add explicit indexes to `#Hubs(Hub)` and `#OeDataUserLocs(OrderId)` to match the join shapes in the final result build.
7. Make all `Insert Into #OeDataUserLocs` column lists explicit.

The full v49 body is in `Refactored.sql`. The v48 body is in `Original.sql` for diff and rollback.

## First Principles

**Scalar UDFs serialize the surrounding plan.** From `masterclass/Scalar UDF Parallelism Barrier.md`:

> In SQL Server (pre-2019, or 2019+ without scalar UDF inlining enabled), a scalar user-defined function (UDF) in a query forces the entire query to run in a serial plan. The optimizer will not consider parallel execution for any part of the query if a scalar UDF is present anywhere in the SELECT list, WHERE clause, or JOIN conditions.

`dbo.fOrdGroupingCritValForDisplay` is invoked per row of the final SELECT. Removing it via the `GroupingCriteria` CTE restores parallelism to the entire enrichment query. The split-suffix portion that the UDF computed per row becomes a one-time set-based aggregation via `String_Agg`, which produces an identical output shape with a single pass over `OeGroupSplitCodeAssoc`.

**Correlated subqueries become CTEs that compute once and join back.** From `masterclass/Correlated Subqueries to CTEs.md`:

> The key insight: the CTE scans `CfStoreDeliveryCourierCutOff` once, groups by pharmacy, and produces a small result set. The join back to the outer query is a simple lookup. Total reads on the delivery table: 6 logical reads instead of thousands.

The `NextDeliveryDay` CTE is the exact pattern. Three correlated subqueries become one `Group By SysEndPharmacyId` aggregation with `Min(Case ...)` for the "this week" branch and `Min(WeekDayCode)` for the wrap-around. The main result build joins on `SysEndPharmacyId` and reads `Coalesce(NDD.ThisWeekDay, NDD.FirstWeekDay)`.

**Table variables get a 1-row cardinality estimate regardless of actual content.** From `masterclass/Table Variables vs Temp Tables.md`:

> When the optimizer thinks a table has 1 row, it makes decisions that are catastrophic for larger datasets. It chooses nested loop joins (perfect for 1 row, disastrous for 10,000), skips parallelism, and underestimates memory grants (which causes spills to tempdb).

`@ToteGroups` is the textbook case: populated conditionally, joined to multiple base tables, never indexed. Converting to `#ToteGroups` with `IX_ToteGroups_GroupNum` lets the optimizer see the real row count and pick hash or merge joins where the data shape supports them.

**Local variables stabilize plan choice across a sniffing-prone parameter set.** From `masterclass/Parameter Sniffing.md`:

> Local variables give you plan stability at the cost of plan optimality. You will never get the best possible plan for a specific value, but you will also never get a catastrophically bad plan. For high-frequency OLTP procedures, stability almost always wins.

Seven parameters means many sniff-prone shapes. The local-variable form gives a density-based plan that is acceptably good for all parameter shapes the proc receives in practice.

**Same-source flags consolidate into one aggregate pass.** The hazmat, controlled, cold-storage, and pen/sulfa flags all come from `vInvMasterWithUserOverrides` or `InvFlaggedProducts` on a per-product basis. v49 produces all flags from one source in one `Group By GroupNum` aggregation with `Max(Case ...)` per flag. Three independent left-joins collapse into one.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Scalar UDF removal. [Scalar UDF Inlining](https://learn.microsoft.com/en-us/sql/relational-databases/user-defined-functions/scalar-udf-inlining) documents the automatic-inlining feature available under compat level 150, and explains why manual inlining remains the reliable approach when the compat level or function eligibility is not guaranteed.
- String aggregation. [STRING_AGG](https://learn.microsoft.com/en-us/sql/t-sql/functions/string-agg-transact-sql) covers the set-based replacement for cursor-style concatenation inside the original UDF.
- CTE consolidation. [WITH common_table_expression (CTE)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) covers the syntax for `NextDeliveryDay`, `GroupingCriteria`, `InventoryFlags`, and `ProductFlags`.
- Window-free aggregation. [Aggregate Functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) covers `Max(Case ...)` pivot semantics used in the flag consolidation.
- Temp-table indexing. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) and the [Index Architecture and Design Guide](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-index-design-guide) cover the temp-table index additions.

## Risk Note

The breadth of v49 is the principal risk. Five distinct refactor patterns ship together; one or two MFCs may see a plan shape that diverges from the median. The `NextDeliveryDay` CTE must match v48's fallback semantics; `Coalesce(ThisWeekDay, FirstWeekDay)` reproduces the v48 wrap-around when the pharmacy has no remaining delivery days this week. The scalar UDF inlining preserves `String_Agg` output, but Section 11.3 of the analysis flags the `Within Group (Order By ...)` clause as the deterministic-ordering safeguard if the v48 UDF produced a stable order operationally. First 24 hours: per-MFC plan stability, result set row count parity, and absence of `NonParallelPlanReason: NoParallelScalarUDFs` in the v49 plan. Rollback is `Alter Procedure` from `Original.sql`. No schema change.
