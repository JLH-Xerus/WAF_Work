# Refactor Recommendation: lsp_DbDeleteOldRxData

**Date:** 2026-05-12
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** Cataloged. v2 is the proposed body in `Refactored.sql`, pending evidence capture before deploy. Scope of this revision is one CTE inside a 1550-line procedure; the rest of the proc is unchanged.

---

## Problem

v1 of `dbo.lsp_DbDeleteOldRxData` is a 1550-line nightly purge that cascades through the OeOrder family. The single CTE on line 1513, which targets `OeOrderAcceptReject`, projects `*`. The table has 60 columns including four wide `varchar` fields (`Directions varchar(1033)`, `Address1 varchar(1024)`, `Address2 varchar(1024)`, `DurData varchar(1023)`). v1 materializes roughly 4 KB of varchar bytes per row in the deletion block, on every iteration of the purge loop, and the delete operator throws them away. All other CTEs in this proc were already moved to the PK-only form in a prior pass and are not touched here.

The single structural issue:

- CTE delete projection on the OeOrderAcceptReject block is `Select *` against a wide-row table. v1 inflates the per-row size estimate and forces the optimizer toward a clustered access path. v2 projects `Id` and the same query becomes covered by any nonclustered index on `RejectDtTm`.

## Recommendation

Apply the one-line projection narrowing shipped as v2. Scope is intentionally narrow: one CTE, no schema change, no index change, no other modifications inside the 1550-line proc. The CTE name, where-clause, and order-by-clause are preserved exactly. The row identity is determined by the where-clause and the top-N, so v1 and v2 delete the same set of rows by construction.

1. Replace `Select Top (@NumOfRowsBlockSize) *` with `Select Top (@NumOfRowsBlockSize) Id` in the `Cte` block on line 1513 against `OeOrderAcceptReject`. `Id` is the clustered primary key and the order-by column.

The full v2 body is in `Refactored.sql`. The v1 body is in `Original.sql` for diff and rollback.

## First Principles

**CTE delete projection.** From `masterclass/CTE Delete Projection.md`:

> The CTE inside a `Delete From Cte` statement is not a materialized copy of the rows. It is an inline view definition that the optimizer folds into the delete plan. The actual delete operator runs against the base table. To find a row and delete it, the engine needs only the row locator, which is the clustered key. Every other column in the CTE's projection list is information the delete operator will not use.

On `OeOrderAcceptReject` the dominant cost dimension is the per-row size estimate driving the sort's memory grant. With four `varchar(1000+)` columns in the projection, the estimate runs to several KB per row. v2 collapses the estimate to the `bigint Id` width and frees the optimizer to choose a narrow nonclustered access path on `RejectDtTm` if one exists.

**Covering index unlock.** From `masterclass/Index Key Columns vs Included Columns.md`:

> An index "covers" a query when every column the query references (in SELECT, WHERE, JOIN, GROUP BY, and ORDER BY) is available in the index, either as a key or as an INCLUDE. The optimizer can satisfy the query entirely from the nonclustered index without touching the clustered index.

A nonclustered index on `RejectDtTm` becomes a covering candidate for the v2 projection because the PK `Id` is carried at the leaf as the row locator. Without the projection narrowing, the same index could not cover the query without including the four wide `varchar` columns, which would be a non-starter. The cross-proc parity matters here: `lsp_DbPurgeHistoryData` targets the same table with the same pattern and is being refactored in parallel.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- CTE-driven delete. The [WITH common_table_expression (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) and [DELETE (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/delete-transact-sql) pages describe how the CTE feeding the delete is folded into the delete plan and what the projection list scopes on the read side.
- Covering index access. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) documents the row-locator carry on nonclustered indexes that lets the PK-only projection cover automatically.

## Risk Note

Semantic risk is zero; the projection list does not influence which rows the CTE represents, and the scope is one CTE inside a much larger proc, so there is no risk of inadvertent change to the surrounding statements. Watch STATS IO on `OeOrderAcceptReject` during the proc's execution (logical reads for the block should drop), and the nightly maintenance log entry for this proc's wall-clock duration against prior-week runs. Cross-proc parity with `lsp_DbPurgeHistoryData` matters: the two refactors should deploy together so post-deploy signals on the OeOrderAcceptReject access path surface consistently at both procs. Rollback path: redeploy v1 from `Original.sql`.
