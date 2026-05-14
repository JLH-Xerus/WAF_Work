# Refactor Recommendation: lsp_GovGetSingleVialSorterCounts

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** v10 in production. v11 proposed by the MFC DBA team, awaiting iA review.

---

## Problem

v10 of `dbo.lsp_GovGetSingleVialSorterCounts` returns one row per sorter ID with two counts: queued single-vial Rxs and on-route single-vial Rxs. The procedure is on the optimization list because it appears on two of four expense lists (Duration #22, Expense #17) with "algorithmic / plan rewrite candidate" as the dominant heuristic. There are no input parameters, so parameter sniffing is not the issue. The procedure is structurally simple but reads the same `GovRx + OeOrder` join twice.

The two issues:

- Duplicated scan of `GovRx + OeOrder`. The `QueuedCounts` CTE and the `OnRouteCounts` CTE both join the same two tables, both filter to `G.OrderTypeCode = 'SV'`, and differ only in their per-CTE WHERE clauses. The two filters are disjoint, which is the exact shape conditional aggregation handles in one scan.
- No `Option (Recompile)` on the final SELECT. Low impact, but the sorter count varies across sites and a recompile gives the optimizer the actual cardinality of the TallyNumbers TOP for each call.

## Recommendation

Ship v11 as proposed. Two changes, both low-risk, no schema or index dependency.

1. Replace the two CTEs (`QueuedCounts`, `OnRouteCounts`) with a single `RxCounts` CTE that scans `GovRx + OeOrder` once and produces both totals via two `Sum(Case When ... Then 1 Else 0 End)` expressions, grouped by `G.SorterId`.
2. Add `Option (Recompile)` to the final SELECT.

The full v11 body is in `Refactored.sql`. The v10 body is in `Original.sql` for diff and rollback.

## First Principles

**Conditional aggregation collapses same-source counts into one scan.** From `masterclass/Conditional Aggregation Consolidation.md`:

> When all the counts read the same table, conditional aggregation collapses them into a single scan:

> One scan, one statement, three counts. The shared `Where AddrBank = @AddrBank` predicate is evaluated once. The branch-specific predicates move into `Case` expressions inside the aggregate. The optimizer reads the table the minimum number of times required, which is once.

The v10 CTEs are the canonical Flavor A shape from the note. Same join (`GovRx + OeOrder`), same shared filter (`G.OrderTypeCode = 'SV'`), different per-bucket predicates. The v11 form moves the per-bucket logic into the Case expressions inside `Sum()`, so the engine reads the joined row set once and produces both totals from the same Group By. The result shape is identical: zero counts when no Rx matches the bucket, integer counts otherwise. The `SorterList` CTE and the final LEFT JOIN preserve the per-sorter row count for sorters with no matching Rxs.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Aggregation. [Aggregate Functions (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) and [COUNT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/functions/count-transact-sql) describe the per-group aggregation semantics that conditional Sum exploits.
- CTE structure. [WITH common_table_expression (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) covers the CTE shape used by `SorterList` and `RxCounts`.
- Plan control. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) documents `OPTION (RECOMPILE)`.

## Risk Note

The semantic risk is minimal. The Case expressions inside the Sum() reproduce the two CTE WHERE clauses verbatim, so the per-sorter counts are identical to v10 by construction. Watch result-row parity with v10 in the first 24 hours of deployment, plus the expected drop in plan operator count (one scan instead of two). The procedure is small in absolute terms; the win is structural rather than dramatic. Rollback is a pure DDL revert; redeploy v10 from `Original.sql`.
