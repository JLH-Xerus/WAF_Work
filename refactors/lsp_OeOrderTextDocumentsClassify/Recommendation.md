# Refactor Recommendation: lsp_OeOrderTextDocumentsClassify

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. v2 proposed by the MFC DBA team, awaiting iA review.

---

## Problem

v1 of `dbo.lsp_OeOrderTextDocumentsClassify` is the nightly classification job that walks `[PWDAZNSYMPH02].PHARMASSIST.dbo.OeOrderTextDocument` over a linked server, classifies rows as Leaflet or Non-Leaflet based on `DataLength(DocData) > 153,600` bytes, and writes results into the local `PharmAssist.dbo.OeOrderTextDocumentClassification` table in batches of `@BatchSize` up to `@MaxRecords`. The cross-list capture reports it on two of four expense lists (Duration #17, Expense #25) with "algorithmic / plan rewrite candidate" as the dominant heuristic.

The structural issues:

- Linked-server `While Exists` check on every iteration (lines 36-41). Each check is a remote round-trip executed before the batch SELECT, which is itself a remote round-trip. The same answer is available from the previous iteration's `@@ROWCOUNT` at zero extra cost.
- Local `Max(Id)` re-read against the classification table on every iteration (line 75). The procedure already reads the max once at the top (line 28). The iteration-local max can come from `Output Inserted.Id` instead of a re-read.
- Two stray debug result sets: `Select @LastProcessedId` on line 30 and `Select @TotalProcessed` on line 73. Single-value result sets returned per iteration that the maintenance orchestrator does not consume.
- Awkward loop control. The `@MaxRecords` exit is checked inside the loop body and produces a `Break`, while the `While Exists` check sits at the top. The flow obscures the exit criteria.
- No local-variable indirection. Program convention.

## Recommendation

Apply v2 as proposed. No schema change. No index change. The package replaces the linked-server polling pattern, tracks state in-memory, removes the debug result sets, and tightens the loop condition in a single deployment unit.

1. Assign `@LocalBatchSize` and `@LocalMaxRecords` from the input parameters.
2. Read the initial `@LastProcessedId` once via `IsNull(Max(Id), 0)` from the local classification table.
3. Drive the loop on `@ContinueLoop = 1 And @TotalProcessed < @LocalMaxRecords` instead of `While Exists` plus a `Break`.
4. Inside the loop, build the batch via a `Batch` CTE that selects `Top (@LocalBatchSize)` from the linked-server source where `HistoryDtTm < @CutoffDate And Id > @LastProcessedId`, ordered by `Id Asc`.
5. Capture the inserted Ids via `Output Inserted.Id Into #BatchMax` so the iteration-local max comes from the in-memory temp table rather than a re-read of the destination.
6. Set `@InsertedThisPass = @@ROWCOUNT`. Exit when the pass inserts zero rows or fewer than `@LocalBatchSize` rows.
7. Remove the `Select @LastProcessedId` and `Select @TotalProcessed` debug statements.

The full v2 body is in `Refactored.sql`. The v1 body is in `Original.sql` for diff and rollback.

## First Principles

**The previous iteration's `@@ROWCOUNT` carries the "is there more work" signal.** The `While Exists` form runs an extra remote round-trip per iteration to ask a question that the previous INSERT already answered. If the previous batch inserted fewer than `@LocalBatchSize` rows, there is no more work; if it inserted zero, the loop is done. Both signals come from `@@ROWCOUNT` on the INSERT without a separate query. For a remote linked-server source, this is the cheapest possible loop control: N remote round-trips instead of 2N.

**Track derived state in-memory.** The `Max(Id)` of the classification table at the end of each batch is, by construction, the maximum Id the current iteration's INSERT just wrote. `Output Inserted.Id Into #BatchMax` captures this as part of the INSERT itself, and `Max(BatchMaxId)` over the small `#BatchMax` temp table is dramatically cheaper than a re-read of the destination. The destination read drops from N+1 (init plus per-iteration) to 1 (init only).

**Express the exit criteria in the loop condition, not in the body.** `While @ContinueLoop = 1 And @TotalProcessed < @LocalMaxRecords` states the loop's contract at the top: continue while there is more work and the cap is not reached. The v1 form with `While Exists` plus a `Break` inside the body forces the reader to combine two pieces of control flow to understand when the loop ends. The clean condition removes the `Break` and makes the exit reasoning local.

**Remove stray result sets from maintenance procedures.** `Select @LastProcessedId` and `Select @TotalProcessed` return per-iteration single-value rowsets to the caller. The maintenance orchestrator does not consume these; they are debug artifacts. Returning them to the client wastes round-trip bandwidth and clutters any orchestrator that logs result-set metadata.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- OUTPUT clause for inserted-row capture. [SELECT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-transact-sql) and the [WITH common_table_expression (CTE)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) reference cover the `Batch` CTE shape and the `Insert ... Output Inserted.Id` pattern for capturing identity values without a re-read.
- TOP with ORDER BY for batch progression. [TOP (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/top-transact-sql) and [ORDER BY clause](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql) cover the row-goal interaction that produces a deterministic batch from the linked-server source.
- Aggregation. [COUNT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/functions/count-transact-sql) and [Aggregate Functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) cover the `Max(BatchMaxId)` lookup on the small temp table.

## Risk Note

The `@@ROWCOUNT`-based exit is semantically equivalent to the v1 `While Exists` check as long as both reference the same predicate. v2 uses the same `HistoryDtTm < @CutoffDate And Id > @LastProcessedId` predicate inside the `Batch` CTE that the v1 `While Exists` and the v1 batch SELECT used. The `@LastProcessedId` tracking via `Output Inserted.Id` produces the same value the v1 re-read of `Max(Id)` would have produced after each INSERT, by construction. The linked-server batch SELECT itself is preserved unchanged; Section 11.1 of the analysis flags the open question on the linked-server design as a cross-reference to Row 35, not a same-PR change. First 24 hours: total elapsed time per call (expected substantially lower with N linked-server round-trips eliminated), classification row count parity with v1 against the same data state, and absence of debug result sets in the maintenance log. Rollback is `Alter Procedure` from `Original.sql`.
