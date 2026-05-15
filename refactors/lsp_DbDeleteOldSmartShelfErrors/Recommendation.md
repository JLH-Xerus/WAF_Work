# Refactor Recommendation: lsp_DbDeleteOldSmartShelfErrors

**Date:** 2026-05-12
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** Cataloged. v2 is the proposed body in `Refactored.sql`, pending evidence capture before deploy.

---

## Problem

v1 of `dbo.lsp_DbDeleteOldSmartShelfErrors` drives a batched purge on `SmartShelf.EvtErrorLog`, a wide-row table whose dominant column is `Message varchar(6500)`. The CTE that selects the deletion block projects `*`, which forces the engine to materialize the 6500-byte field for every row in the block. The delete operator throws the bytes away. The per-block cost in v1 scales with the average `Message` length, not with row count.

The single structural issue:

- CTE delete projection is `Select *` against a wide-row table. v1 reads `Message` for every candidate row on every iteration of the purge loop. v2 projects `Id` (the clustered PK and the order-by column) and lets the per-row size estimate collapse from 6500-plus bytes to 8.

## Recommendation

Apply the one-line projection narrowing shipped as v2. No schema change, no index change, no other modifications to the procedure. The CTE name, where-clause, and order-by-clause are preserved exactly. The row identity is determined by the where-clause and the top-N, so v1 and v2 delete the same set of rows by construction.

1. Replace `Select Top(@NumOfRowsBlockSize) *` with `Select Top(@NumOfRowsBlockSize) Id` in the `Cte` block on line 90. `Id` is the clustered primary key and the order-by column, so the optimizer can satisfy the top-N from the clustered index in `Id` order without an explicit sort.

The full v2 body is in `Refactored.sql`. The v1 body is in `Original.sql` for diff and rollback.

## First Principles

**CTE delete projection.** From `masterclass/CTE Delete Projection.md`:

> The CTE inside a `Delete From Cte` statement is not a materialized copy of the rows. It is an inline view definition that the optimizer folds into the delete plan. The actual delete operator runs against the base table. To find a row and delete it, the engine needs only the row locator, which is the clustered key. Every other column in the CTE's projection list is information the delete operator will not use.

The relevant cost dimension on this table is the memory grant, not LOB reads. The note covers it directly: SQL Server estimates the memory grant from the per-row size implied by the projection, and a `Select *` against a row with a `varchar(6500)` produces a per-row estimate in the thousands of bytes. With `Order By` involved, the sort operator requests a grant on the order of tens of megabytes per pass. v2 collapses the per-row estimate to the `bigint Id` width and the grant collapses with it.

**Covering index unlock.** From `masterclass/Index Key Columns vs Included Columns.md`:

> An index "covers" a query when every column the query references (in SELECT, WHERE, JOIN, GROUP BY, and ORDER BY) is available in the index, either as a key or as an INCLUDE. The optimizer can satisfy the query entirely from the nonclustered index without touching the clustered index.

A nonclustered index on `DtTm` (the filter column) becomes a covering candidate for the v2 projection automatically, because `Id` is carried as the row locator. The same index could not cover v1's `Select *` without including `Message`, which would defeat the purpose. The projection fix is the prerequisite for any future index decision on this table.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- CTE-driven delete projection. The [WITH common_table_expression (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) page and the [DELETE (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/delete-transact-sql) page describe how the CTE feeding the delete is folded into the delete plan; the projection list scopes the read side of that plan.
- Covering index access. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) documents the leaf carry of the clustered key in nonclustered indexes, which is what makes the PK-only projection cover automatically.

## Risk Note

Semantic risk is zero; the projection list does not influence which rows the CTE represents. The expected wins on this table are the memory grant reduction and the per-page row density during the scan. Watch STATS IO logical reads on `SmartShelf.EvtErrorLog` per block (should drop because fewer pages are scanned), the sort memory grant on the plan, and the nightly maintenance log's wall-clock duration for this step. Rollback path: redeploy v1 from `Original.sql`. No schema change, no index change.
