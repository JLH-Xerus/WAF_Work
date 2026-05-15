# Refactor Recommendation: lsp_DbPurgeHistoryData

**Date:** 2026-05-12
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** Cataloged. v2 is the proposed body in `Refactored.sql`, pending evidence capture before deploy. Scope of this revision is one CTE inside a 1618-line procedure; the rest of the proc is unchanged.

---

## Problem

v1 of `dbo.lsp_DbPurgeHistoryData` is a 1618-line nightly purge that cascades through the OeOrder family over a configurable history window. The single CTE on line 1590, which targets `OeOrderAcceptReject`, projects `*`. The table has 60 columns including four wide `varchar` fields (`Directions varchar(1033)`, `Address1 varchar(1024)`, `Address2 varchar(1024)`, `DurData varchar(1023)`). v1 materializes those fields for every row in the deletion block, on every iteration of the purge loop. The delete operator throws them away. Unlike the parallel proc `lsp_DbDeleteOldRxData`, the where-clause here is a two-sided range `RejectDtTm >= @HistoryDtTm_EffectiveFrom And RejectDtTm < @HistoryDtTm_EffectiveTo`, which is fully SARGable against any nonclustered index on `RejectDtTm`.

The single structural issue:

- CTE delete projection on the OeOrderAcceptReject block is `Select *` against a wide-row table. v1 inflates the per-row size estimate and forces the optimizer toward a clustered access path. v2 projects `Id` and the same query becomes covered by any nonclustered index on `RejectDtTm`.

## Recommendation

Apply the one-line projection narrowing shipped as v2. Scope is intentionally narrow: one CTE, no schema change, no index change, no other modifications inside the 1618-line proc. The CTE name, where-clause, and order-by-clause are preserved exactly. The row identity is determined by the where-clause and the top-N, so v1 and v2 delete the same set of rows by construction.

1. Replace `Select Top (@NumOfRowsBlockSize) *` with `Select Top (@NumOfRowsBlockSize) Id` in the `Cte` block on line 1590 against `OeOrderAcceptReject`. `Id` is the clustered primary key and the order-by column.

The full v2 body is in `Refactored.sql`. The v1 body is in `Original.sql` for diff and rollback.

## First Principles

**CTE delete projection.** From `masterclass/CTE Delete Projection.md`:

> The CTE inside a `Delete From Cte` statement is not a materialized copy of the rows. It is an inline view definition that the optimizer folds into the delete plan. The actual delete operator runs against the base table. To find a row and delete it, the engine needs only the row locator, which is the clustered key. Every other column in the CTE's projection list is information the delete operator will not use.

On `OeOrderAcceptReject` the dominant cost dimension is the per-row size estimate driving the sort's memory grant. The four `varchar(1000+)` columns push the per-row estimate into the multi-KB range. v2 collapses it to the `bigint Id` width and lets the optimizer pick a narrower access path.

**Covering index unlock.** From `masterclass/Index Key Columns vs Included Columns.md`:

> An index "covers" a query when every column the query references (in SELECT, WHERE, JOIN, GROUP BY, and ORDER BY) is available in the index, either as a key or as an INCLUDE. The optimizer can satisfy the query entirely from the nonclustered index without touching the clustered index.

The two-sided range predicate on `RejectDtTm` is fully SARGable, so a nonclustered index on `RejectDtTm` becomes a covering range seek with no residual on the predicate. The PK `Id` is carried as the row locator. Without the projection narrowing, the same index could not cover the query without including the four wide `varchar` columns, which would defeat the index's purpose.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- CTE-driven delete. The [WITH common_table_expression (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) and [DELETE (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/delete-transact-sql) pages describe how the CTE feeding the delete is folded into the delete plan and what the projection list scopes on the read side.
- SARGable range predicates. The [Predicates (SARGability reference)](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) page documents the seek-versus-residual rules that make the two-sided range against an index on `RejectDtTm` a clean range seek with no residual.
- Covering index access. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) documents the row-locator carry that lets the PK-only projection cover automatically.

## Risk Note

Semantic risk is zero; the projection list does not influence which rows the CTE represents, and the scope is one CTE inside a much larger proc. Cross-proc parity with `lsp_DbDeleteOldRxData` matters: the two procs target the same table with the same pattern, and the two refactors should deploy together so any post-deploy regression on the OeOrderAcceptReject access path surfaces at both call sites consistently. Watch STATS IO on `OeOrderAcceptReject` during both procs' executions and the nightly maintenance log entry for wall-clock duration. Rollback path: redeploy v1 from `Original.sql`.
