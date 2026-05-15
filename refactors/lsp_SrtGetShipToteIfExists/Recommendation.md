# Refactor Recommendation: lsp_SrtGetShipToteIfExists

**Date:** 2026-05-08
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** v27 deployed across the MFC fleet.

---

## Problem

v26 of `dbo.lsp_SrtGetShipToteIfExists` runs five distinct pieces of wasted work on every invocation from the Package Sorter Status display. No single line is expensive in absolute terms. The procedure matters because it sits on the critical path for the scanner workflow and is called at high frequency, so the per-call overhead multiplies cleanly into operator-visible latency.

The five issues:

- A double-query barcode lookup. `If Exists (Select Id ...)` followed by `Set @ShipToteIdentifier = (Select Id ...)`. Identical predicates, identical results, two index lookups against `SrtShipTote`.
- An exception-package check structured as `Select * From ShpException ... Union Select * From ShpException ...` inside an `If Exists` wrapper. UNION carries every column through and performs an implicit DISTINCT, all for an existence test.
- A dead `@ServiceCode` block populated by a three-table join (`SrtShipTote` to `SrtShipToteShipmentAssoc` to `ShpShipment`) and then never read.
- A correlated scalar subquery for the per-tote package count. The outer query is already over `ShpManifest` filtered by the same `ShipToteId`.
- A missing `With (NoLock)` hint on the final `SrtShipToteShipmentAssoc` query, against a procedure that is otherwise consistent in its NoLock posture.

## Recommendation

Apply the five-fix package shipped as v27. Each fix is independent and additive; the combination is a tight, well-scoped refactor that requires no schema or index change.

1. Collapse the barcode lookup to a single `Select @ShipToteIdentifier = Id From SrtShipTote With (NoLock) Where StaticToteBarcode = @ShipToteId` followed by an `If @ShipToteIdentifier Is Null Begin ... End` branch.
2. Rewrite the exception-package check as two `Exists (Select 1 ...)` clauses joined with `Or`. Drop the `Union` and the `Select *`.
3. Remove the `@ServiceCode` declaration, the three-table populating join, and the now-unused variable reference. Confirm by reading the procedure end-to-end that the variable is never consumed downstream (it is not).
4. Replace the correlated `(Select Count(ShipToteId) From ShpManifest ...)` projection with `Count(*) Over()` inside the outer query that already filters `ShpManifest` by `ShipToteId`.
5. Add `With (NoLock)` to the final `SrtShipToteShipmentAssoc` query to match the procedure's established posture.

The full v27 body is in `Refactored.sql`. The v26 body is in `Original.sql` for diff and rollback.

## First Principles

**Correlated subquery to set-based aggregation.** From `masterclass/Correlated Subqueries to CTEs.md`:

> A correlated subquery references columns from the outer query, which means the database engine must evaluate the subquery once per row of the outer result set. The subquery is logically correlated to each outer row.

> The key insight: the CTE scans `CfStoreDeliveryCourierCutOff` once, groups by pharmacy, and produces a small result set. The join back to the outer query is a simple lookup. Total reads on the delivery table: 6 logical reads instead of thousands.

For the per-tote package count in this procedure, a CTE is heavier than the situation needs. The outer query is already over `ShpManifest` filtered by `ShipToteId`, so `Count(*) Over()` computes the same partition aggregate in a single scan and surfaces it on each row without a second pass. Same principle, lighter-weight expression.

**Consistent NoLock posture.** From `masterclass/NOLOCK Strategy.md`:

> When using NOLOCK in a procedure, apply it consistently to every table reference. Mixing locked and unlocked reads creates a false sense of consistency. You're getting dirty reads from some tables and locked reads from others, which is worse than committing to one strategy.

v26 commits to NoLock everywhere except the final `SrtShipToteShipmentAssoc` query. That single omission causes shared locks to be taken on the last step and conflicts with concurrent writers on the association table. The fix is the missing hint, not a strategy revisit.

**Existence checks express intent with `Select 1`.** `Select *` inside `If Exists` forces every column through the operator and performs the UNION's implicit DISTINCT on every column, even though the projection is never consumed. `Select 1` communicates to the optimizer and to the reader that only the existence of a row matters. For two disjoint conditions, two `Exists` clauses joined with `Or` express the intent more directly than a `Union` over `Select *` and let the optimizer short-circuit on the first match.

**Single-query NULL check over existence-then-assign.** `If Exists (Select Id ...) Set @x = (Select Id ...)` runs two identical lookups when one suffices. `Select @x = Id From ... Where ...` followed by `If @x Is Null` does the same logical work with one lookup and one NULL test. The two queries always produce identical results because the predicates are identical, so the existence check adds no protection over the assignment query.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Window aggregation. The `Count(*) Over()` substitution is documented under the [OVER clause (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-over-clause-transact-sql) and the [COUNT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/functions/count-transact-sql) aggregate page, which together describe partition-level aggregation evaluated in a single pass over the windowed rowset.
- Existence checks. [EXISTS (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql) specifies the subquery-to-existence semantics. The page is explicit that EXISTS returns TRUE on first qualifying row, which is the short-circuit behavior the `Or Exists` rewrite depends on.
- NoLock semantics. [Table Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table) documents `NOLOCK` and `READUNCOMMITTED` equivalence and states that both hints apply only to data locks and that schema-stability locks are still acquired during compilation and execution. Relevant when reading a procedure end-to-end for hint consistency.

## Risk Note

The semantic risk on the `Count(*) Over()` substitution is the lowest of the five fixes; the partition over which the window function is computed is exactly the partition the subquery counted, so the values are identical by construction. The semantic risk on the `Or Exists` substitution is slightly higher: if the v26 body relied on the UNION's implicit DISTINCT to filter duplicates in a way that affected downstream control flow, the substitution would diverge. Reading v26 end to end confirmed it did not. The dead-code removal is the highest semantic risk in principle and the lowest in practice. Rollback path: redeploy v26 from `Original.sql`. No schema change, no index change.
