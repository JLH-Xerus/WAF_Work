# Refactor Recommendation: lsp_DbDeleteNonLeafletOeOrderTextDocument

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. `Refactored.sql` is a proposed v4 designed by the MFC DBA team and me, awaiting iA review. v4 carries an open semantic question on the linked-server reference that must be resolved before deployment.

---

## Problem

v3 of `dbo.lsp_DbDeleteNonLeafletOeOrderTextDocument` lands on the Duration #6 and Expense #19 expense lists with "algorithmic / plan rewrite candidate" as the dominant heuristic. The proc is one of the slower nightly maintenance items. The dominant cost driver is a `WHILE EXISTS` check at the top of the delete loop that queries a linked-server pair (`[PWDAZNSYMPH02].pharmassist.dbo.OeOrderTextDocument` joined to its classification table) on every iteration, just to ask whether there is more work to do. The same answer is already carried by the previous iteration's `@@ROWCOUNT`. Several smaller issues compound.

The structural issues in v3:

- Linked-server `WHILE EXISTS` check (lines 114-123). N remote round-trips per nightly run, where N is the iteration count. The information is freely available locally from `@@ROWCOUNT`.
- CTE projection is `Select Top (@NumOfRowsBlockSize) *` (line 134). The delete only needs the row locator; reading every column for every candidate is wasted I/O.
- Stray debug `select @NumOfRowsDeleted` (line 143). Returns a single-value result set to the caller on every iteration. Almost certainly a development leftover.
- IN-subquery shape for the candidate-Id filter inside the CTE. The optimizer evaluates the inner subquery per outer row unless it is pre-materialized. An indexed temp table converts the per-row work into a single seek per block.
- Asymmetry between the WHILE check (linked-server) and the DELETE body (local tables). Open question for the iA team; tracked in Section 11.1 of `Analysis.md`.

## Recommendation

Apply the four-fix package shipped as v4, contingent on the iA team's answer to the linked-server question. v4 assumes the local `OeOrderTextDocumentClassification` table is authoritative; if instead the linked-server table is the source of truth, v4 must be revised to query it consistently or to confirm the local table is the right source.

1. Replace the linked-server `WHILE EXISTS` check with an `@@ROWCOUNT`-driven loop. The loop exits when either the candidate-Id selection returns zero rows or the DELETE removes fewer rows than the block size.
2. Pre-materialize the candidate `Id` set into a `#BlockIds (Id BigInt Primary Key)` temp table per iteration, truncated rather than dropped to preserve the downstream plan cache. Drive the DELETE off an INNER JOIN to the temp table.
3. Narrow the candidate-Id selection projection from `*` to `dc.Id`.
4. Remove the stray `select @NumOfRowsDeleted` debug statement.

The full v4 body is in `Refactored.sql`. The v3 body is in `Original.sql` for diff and rollback. The Section 11.1 linked-server question is the gating issue for deploy.

## First Principles

**Pre-materialize into an indexed temp table.** From `masterclass/Table Variables vs Temp Tables.md`:

> Why temp tables work: SQL Server creates auto-statistics on temp tables. After the INSERT, the statistics reflect the actual row count and data distribution. The optimizer reads these statistics and makes informed decisions.

The `#BlockIds` table carries a clustered primary key on `Id` and a real row count after each `INSERT INTO`. The DELETE's join becomes a seek against the temp table's PK rather than a per-row evaluation of an IN-subquery. The "insert then index" pattern is in the note as a best practice; v4 follows it by declaring the PK inline at create time on a table whose contents fit a single block.

**CTE delete projection.** From `masterclass/CTE Delete Projection.md`:

> The actual delete operator runs against the base table. To find a row and delete it, the engine needs only the row locator, which is the clustered key. Every other column in the CTE's projection list is information the delete operator will not use.

v3's `Select Top (@NumOfRowsBlockSize) *` materializes columns the delete never reads. v4 narrows to `dc.Id`. The pattern is identical to the purge-sweep CTE projection refactors, applied to the candidate-Id selection rather than the DELETE driver itself.

**Loop control belongs to `@@ROWCOUNT`, not a separate scan.** A `WHILE EXISTS` at the top of a delete loop is a separate query that the engine must compile, parameterize, and execute on every iteration. When that query crosses a linked-server boundary, the iteration cost includes a remote round-trip and a remote join. The previous iteration's `@@ROWCOUNT` carries the same answer for free, with two equivalence conditions: zero rows deleted means no more work, and a partial block means the qualifying set is exhausted. v4 reads both signals off `@@ROWCOUNT` directly.

**Single existence test, no spurious result sets.** A debug `select @NumOfRowsDeleted` inside the loop body emits a single-row, single-column result set per iteration to whatever orchestrator called the proc. The maintenance orchestrator does not consume it. Removing the statement avoids polluting the caller's result-set stream and the SQL Server batch's network round-trip count.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- DELETE with join. The [DELETE (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/delete-transact-sql) page documents the `DELETE ... FROM ... INNER JOIN` form used to drive deletes off the pre-materialized `#BlockIds` temp table.
- Existence semantics. [EXISTS (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql) covers the short-circuit semantics the optimizer relies on once the IN-subquery is replaced by an indexed temp join.
- Recompile and OPTION hints. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) covers `OPTION (Recompile)`, applied to the candidate-Id selection to keep the per-block compile aligned with the truncated temp table's row count.
- Temp tables and indexes. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) documents the inline primary-key declaration on `#BlockIds`.

## Risk Note

The gating risk is the linked-server question in Section 11.1 of `Analysis.md`; v4 cannot deploy until the iA team confirms the local classification table is authoritative. If qualifying rows are being added to the classification table during the loop, the `@@ROWCOUNT`-driven exit may end one block earlier than v3, which is acceptable because the next nightly run catches the remainder. The `#BlockIds` truncate-then-insert pattern preserves the downstream DELETE join's plan cache across iterations. Watch the nightly maintenance log for total elapsed time (expected: substantially lower because the linked-server check is gone), deletion-count parity with v3 on the first night after deployment, and the absence of debug result sets in the orchestrator's stream. Rollback path: redeploy v3 from `Original.sql`. No schema change.
