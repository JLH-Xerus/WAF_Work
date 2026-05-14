# Refactor Recommendation: lsp_DbDeleteOldScriptImages

**Date:** 2026-05-12
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. v2 is the proposed body in `Refactored.sql`, pending evidence capture before deploy.

---

## Problem

v1 of `dbo.lsp_DbDeleteOldScriptImages` drives a batched purge on `OeScriptImage`, the only target in the nightly maintenance sweep with off-row LOB storage. The CTE that selects the deletion block projects `*`, which forces the engine to follow the LOB pointer to the `Image` and `ImageBack` pages for every row in the block. The delete operator never consumes those bytes. At the default block size of 1000 and a `@MaxToDelete` of 50,000, a worst-case nightly run reads the LOB chains of 50,000 image rows for no purpose.

The single structural issue:

- CTE delete projection is `Select *` against an LOB-heavy table. `OeScriptImage` carries two `image` columns with `TEXTIMAGE_ON [PRIMARY]`. v1 materializes both LOB chains per row per block; v2 projects `RxNum` only and reads zero LOB pages.

## Recommendation

Apply the one-line projection narrowing shipped as v2. No schema change, no index change, no other modifications to the procedure. The CTE name, where-clause, order-by-clause, and delete clause are preserved exactly. The row identity in the CTE is determined entirely by the where-clause and the top-N, so v1 and v2 delete the same set of rows by construction.

1. Replace `Select Top (@NumOfRowsBlockSize) *` with `Select Top (@NumOfRowsBlockSize) RxNum` in the `BlockOfRows` CTE on line 80. `RxNum` is the clustered primary key on `OeScriptImage`, not `Id`.

The full v2 body is in `Refactored.sql`. The v1 body is in `Original.sql` for diff and rollback.

## First Principles

**CTE delete projection.** From `masterclass/CTE Delete Projection.md`:

> The CTE inside a `Delete From Cte` statement is not a materialized copy of the rows. It is an inline view definition that the optimizer folds into the delete plan. The actual delete operator runs against the base table. To find a row and delete it, the engine needs only the row locator, which is the clustered key. Every other column in the CTE's projection list is information the delete operator will not use.

`OeScriptImage` is the canonical case in this codebase. The note calls it out by name: two `image` columns and `TEXTIMAGE_ON [PRIMARY]` mean `Select *` reads the LOB pages for every row in the block, every iteration of the purge loop. The cost is proportional to image size, not row count. With `Select RxNum` the same query reads zero LOB pages.

**Covering index unlock.** From `masterclass/Index Key Columns vs Included Columns.md`:

> An index "covers" a query when every column the query references (in SELECT, WHERE, JOIN, GROUP BY, and ORDER BY) is available in the index, either as a key or as an INCLUDE. The optimizer can satisfy the query entirely from the nonclustered index without touching the clustered index.

The PK-only projection unlocks a covering-index path. Any nonclustered index on `ScanDateTime` automatically carries `RxNum` as the row locator, so the v2 projection is covered without an INCLUDE list. If such an index does not exist, the filtered DDL in Section 11.1 of `Analysis.md` is the recommended add. Either way, the projection fix is the prerequisite.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- CTE-driven delete projection. The [WITH common_table_expression (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) page and the [DELETE (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/delete-transact-sql) page together describe how a CTE feeding a delete is folded into the delete plan; the projection list scopes the read side of that plan.
- Covering index access. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) and [Create Filtered Indexes](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-filtered-indexes) document the index shape that becomes covering once the projection is narrowed to the primary key.

## Risk Note

Semantic risk is zero by construction; the projection list does not influence which rows the CTE represents. Plan-shape risk is on the index choice. If the optimizer was previously forced to clustered scan because of the `Select *` projection, v2 frees it to choose a narrower nonclustered access path on `ScanDateTime` if one exists. Watch the nightly maintenance log for the "Delete Old Script Images" step, the LOB logical reads on `OeScriptImage` in STATS IO (should drop to zero), and tempdb peak during the maintenance window (the sort memory grant should collapse). Rollback path: redeploy v1 from `Original.sql`.
