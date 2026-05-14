# Refactor Recommendation: lsp_RbtDetermineNextVialToProcess

**Date:** 2026-05-07
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. Refactored.sql is a proposed v11 awaiting iA review.

---

## Problem

v10 decides which type of vial the robot should fill next (dispenser reset, count-dry non-Rx, or standard Rx) for a given bank, then returns a header recordset describing the decision plus a detail recordset of work items. The procedure is the highest-call-volume member of the deep-dive cohort so far: roughly 705 billion logical reads per month across 43.4 million executions, present in 14 of 14 reporting sites. The cross-MFC Query Store evidence is unambiguous that the dominant cost is one statement, `INSERT INTO #temp_table2 EXEC lsp_OrdGetReadyForDispenseRxsInBank @AddrBank, 0`, which v11 does not change. The proc-body wins in v11 are real but small per-call; the big lever lives in the called sub-proc and is the subject of the Row 29 refactor.

- Three separate `Count(*)` variable-assignment statements at the top of the procedure. Different table sources (`OeOrder`, `TcdStatus + RvoNonRxProductVial`, `RbtQueuedForResetTcds + TcdStatus`), so the leaf I/O cannot be reduced by consolidation alone; the per-statement compile, plan-cache slot, and dispatch overhead is paid three times.
- Two-pass `If Exists` check with a correlated `Top 1 / Order By` subquery against `RbtVialSupply`. The inner subquery scans for the largest vial size; the outer existence check re-scans to confirm availability. Same answer is one-pass via conditional aggregation.
- Heap `#temp_table2` populated by `Insert ... Exec lsp_OrdGetReadyForDispenseRxsInBank` and read back with `Order By ActOnAsPriInternal, PriInternal`. The read-time sort is on uncovered columns.
- `Select * From #temp_table2`. The contract between the called proc and the temp-table column shape is implicit; an explicit column list pins the contract on both ends.
- Inconsistent `WITH (NOLOCK)` capitalization throughout.

## Recommendation

Apply the v11 package. The proc-body changes are structural cleanup with small per-call savings; the dominant lever lives in `lsp_OrdGetReadyForDispenseRxsInBank` (Row 29) and is the compounding refactor.

1. Consolidate the three `Count(*)` statements into one `Union All` of projection queries with `Sum(Case When Bucket = ... Then 1 Else 0 End)` per output bucket. Coalesce each local to zero (`Sum` returns NULL on empty input, `Count(*)` returns 0).
2. Replace the two-pass `If Exists` check with a single-pass conditional aggregation: `Max(StoredVialSizeDrams)` against `Max(Case When StatusId In (0, 1) Then StoredVialSizeDrams End)` to test whether the largest size is also available.
3. Declare `#temp_table2` with a clustered primary key on `(ActOnAsPriInternal, PriInternal)` so the read-time `Order By` becomes a no-cost ordered scan.
4. Add an explicit column list to both the `Insert ... Exec` and the final `Select` against `#temp_table2`.
5. Normalize `WITH (NOLOCK)` capitalization across the procedure.

The full v11 body is in `Refactored.sql`. The v10 body is in `Original.sql` for diff and rollback.

## First Principles

**Conditional aggregation, Flavor B.** From `masterclass/Conditional Aggregation Consolidation.md`:

> When the queries hit different tables but share parameters, the engine still has to compile and dispatch each statement separately. The per-statement overhead (parse, compile, plan lookup, parameter binding, result return) is paid N times even though the work is logically one composite count.

> Flavor B is a structural and cache-pressure win. It is not a leaf-I/O win. Be honest about which flavor you are getting when you write the analysis. A claim of "fewer reads" is only true for Flavor A.

The three counts hit three different table sources, so the savings are in dispatch, compile, and plan-cache slots, not in scan counts. The three counts become one statement with three compile slots collapsed to one.

**Single-pass existence-plus-filter.** The "is the largest-X record in available status?" pattern is computable in one scan: `Max(StoredVialSizeDrams)` is the global max; `Max(Case When StatusId In (0, 1) Then StoredVialSizeDrams End)` is the max among available rows. They are equal if and only if at least one available row holds the global max. This is the boolean-flag variant of the same conditional-aggregation principle the count consolidation uses. One scan replaces the two-pass `If Exists` with correlated inner `Top 1`.

**Indexed temp tables eliminate heap sorts.** From `masterclass/Table Variables vs Temp Tables.md`:

> The "insert then index" pattern is a best practice. Building the index on the complete dataset is a single efficient sort, versus maintaining the b-tree during every insert.

A `#temp` declared without an index is a heap; reading it with `Order By` forces a Sort operator at read time. Declaring `#temp_table2` with a clustered primary key on `(ActOnAsPriInternal, PriInternal)` moves the sort to insertion time, where it is part of the clustered-index maintenance. The final read becomes a no-cost ordered scan of the clustered structure. The primary-key constraint enforces uniqueness; if the called proc ever returns duplicate `(ActOnAsPriInternal, PriInternal)` pairs the insert fails immediately, which is a deliberate fail-closed choice. Downgrade to a non-unique clustered index if validation surfaces duplicates as legitimate.

**Explicit column lists at procedure boundaries.** `Select *` against a temp table populated by `Insert ... Exec` is brittle: the contract between the called proc's `Select` list and the temp-table column shape is implicit, and a future change to the called proc propagates silently. The explicit column list on both the `Insert` and the `Select` makes the contract visible at both ends.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- UNION ALL and aggregation. [UNION (set operators)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-union-transact-sql) and [Aggregate Functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) cover the conditional-aggregation shape that consolidates the three counts.
- Single-pass max comparison. [COUNT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/functions/count-transact-sql) and [Aggregate Functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) document the `Max(Case When ...)` form that replaces the two-pass existence check.
- Clustered primary key on a temp table. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) covers the index shape; [ORDER BY clause](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql) and [Index Architecture and Design Guide](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-index-design-guide) describe how a clustered key satisfies an `Order By` without a Sort operator.
- SELECT semantics. [SELECT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-transact-sql) documents the explicit column list form pinning the contract.

## Risk Note

The `Sum` versus `Count(*)` semantic difference is the smallest risk: `Sum(Case ...)` returns NULL on empty input where `Count(*)` returns 0, so each local needs an `IsNull` coalesce. The primary-key constraint on `#temp_table2` is the largest semantic risk in principle: if `lsp_OrdGetReadyForDispenseRxsInBank` ever returns duplicate `(ActOnAsPriInternal, PriInternal)` pairs, the `Insert ... Exec` fails with a constraint violation. The validation run must confirm the called proc never returns duplicates on this composite for the parameter shapes in production use; if duplicates are legitimate, downgrade to a non-unique clustered index. In the first 24 hours watch the robot dispense controller log for primary-key violations on the `Insert`, the cross-MFC reads on the `INSERT ... EXEC` line (which v11 does not change; flat is the expected signal), and the plan cache (expected: small stable plan count). Rollback path: redeploy v10 from `Original.sql`. No schema change, no index change.
