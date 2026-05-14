# Refactor Recommendation: lsp_DbDeleteOldSmartShelfEvents

**Date:** 2026-05-12
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. v2 is the proposed body in `Refactored.sql`, pending evidence capture before deploy.

---

## Problem

v1 of `dbo.lsp_DbDeleteOldSmartShelfEvents` drives a batched purge on `SmartShelf.EvtEventLog`, a 13-column table with no LOB and no single very wide column (the widest is `Process varchar(256)`). The CTE that selects the deletion block projects `*`, which forces the engine to materialize the full 13-column row for every candidate row. This is the lower-payoff instance of the CTE delete projection pattern in the nightly sweep, but the fix is mechanical and the principle is the same as the higher-payoff cases. Applying it uniformly keeps the pattern out of future grep sweeps.

The single structural issue:

- CTE delete projection is `Select *` on a 13-column table. v1 inflates the sort's per-row size estimate to the full-row width; v2 projects `Id` and the estimate collapses to 8 bytes.

## Recommendation

Apply the one-line projection narrowing shipped as v2. No schema change, no index change, no other modifications to the procedure. The CTE name, where-clause, and order-by-clause are preserved exactly. The row identity is determined by the where-clause and the top-N, so v1 and v2 delete the same set of rows by construction.

1. Replace `Select Top(@NumOfRowsBlockSize) *` with `Select Top(@NumOfRowsBlockSize) Id` in the `Cte` block on line 87. `Id` is the clustered primary key and the order-by column.

The full v2 body is in `Refactored.sql`. The v1 body is in `Original.sql` for diff and rollback.

## First Principles

**CTE delete projection.** From `masterclass/CTE Delete Projection.md`:

> The CTE inside a `Delete From Cte` statement is not a materialized copy of the rows. It is an inline view definition that the optimizer folds into the delete plan. The actual delete operator runs against the base table. To find a row and delete it, the engine needs only the row locator, which is the clustered key. Every other column in the CTE's projection list is information the delete operator will not use.

On this table the dominant cost dimension is the memory grant, not LOB reads or wide varchar materialization. The grant savings on a 13-column row are modest in absolute terms; the practical payoff is the operator's output column list narrowing and the unlock of a covering nonclustered index on `DtTm` if one is added later. The mechanical fix is worth taking because the cost of the fix is one line.

**Covering index unlock.** From `masterclass/Index Key Columns vs Included Columns.md`:

> An index "covers" a query when every column the query references (in SELECT, WHERE, JOIN, GROUP BY, and ORDER BY) is available in the index, either as a key or as an INCLUDE. The optimizer can satisfy the query entirely from the nonclustered index without touching the clustered index.

A nonclustered index on `DtTm` becomes a covering candidate for the v2 projection automatically. The PK `Id` is carried as the row locator, so no INCLUDE is needed. Whether the add is justified depends on the table's other access patterns and is out of scope for this revision; v2 is the prerequisite either way.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- CTE-driven delete. The [WITH common_table_expression (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) page and the [DELETE (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/delete-transact-sql) page together describe the CTE-fed delete plan and what the projection list scopes on the read side.
- Covering index access. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) documents the row-locator carry that makes the PK-only projection cover automatically.

## Risk Note

Semantic risk is zero; the projection list does not influence which rows the CTE represents. The expected wall-clock effect is modest given the table is not LOB-heavy or extremely wide; the main observable change is the operator's output column list dropping to `Id` only and the sort memory grant collapsing. Watch the nightly maintenance log for this step; it should not regress. Rollback path: redeploy v1 from `Original.sql`. No schema change, no index change.
