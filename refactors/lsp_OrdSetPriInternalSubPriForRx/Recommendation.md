# Refactor Recommendation: lsp_OrdSetPriInternalSubPriForRx

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** v1 in production. v2 proposed by the MFC DBA team, awaiting iA review.

---

## Problem

v1 of `dbo.lsp_OrdSetPriInternalSubPriForRx` appends a six-digit decimal sub-priority to the end of an Rx's `PriInternal` so that Rxs in different groups with otherwise identical priorities can be ordered deterministically. The procedure appears on two of four expense lists (Duration #20, Expense #22) with "algorithmic / plan rewrite candidate" as the dominant heuristic. The cost concentrates in three small structural issues, none individually dramatic but together meaningful at the procedure's call frequency.

The three issues:

- Three reads of `OeOrder` for the same OrderId. One for the length check, one for FIFO/LIFO sequence and GroupNum and DateEntered, and one in the event-log INSERT after the UPDATE. The three reads return the same row in different states (pre-update for the first two, post-update for the third).
- `PriInternal Like '%' + @FifoLifoSeq + '%'` appears twice. The leading wildcard defeats any clean index seek on `PriInternal`. Unavoidable without a schema change.
- No local-variable indirection for the input parameter.

## Recommendation

Ship v2 as a single deployment unit. The package consolidates the three `OeOrder` reads into one, computes the post-update `PriInternal` in-memory rather than re-reading after the UPDATE, and adds `Option (Recompile)` to the non-SARGable LIKE scans so the optimizer compiles against actual selectivity. Local-variable indirection on the input parameter rounds out the program convention. No schema change.

1. Consolidate the two pre-update reads of `OeOrder` into one initial `Select` that populates local variables for `PriInternal`, `PriInternalLen`, `FifoLifoSeq`, `GroupNum`, and `DateEntered`. Apply the early-return short-circuit immediately after this read.
2. Compute the new `PriInternal` in-memory as `@PriInternal + @SubPriority`. The UPDATE writes this value; the event-log INSERT uses the same in-memory value rather than re-reading from `OeOrder`.
3. Add `Option (Recompile)` to the two non-SARGable LIKE scans so the optimizer compiles against the actual `@FifoLifoSeq` selectivity each call.
4. Add local-variable indirection for the input parameter.

The full v2 body is in `Refactored.sql`. The v1 body is in `Original.sql` for diff and rollback.

## First Principles

**Single-read consolidation when the same row is referenced multiple times.** A procedure that reads the same row in multiple places typically wants one consolidated read into local variables. v1's three reads return the same OeOrder row in different states: the first two are pre-update reads that differ only in their projected columns, the third is the post-update read used to populate the event log. v2 collapses the first two into one read at the top and computes the post-update `PriInternal` in-memory by concatenating `@PriInternal + @SubPriority`. The UPDATE writes exactly this value; the post-update read returns exactly this value; there is no observable difference between reading the post-update row from `OeOrder` and using the in-memory computation.

**Non-SARGable predicates: recompile for current selectivity.** From `masterclass/Non-SARGable Predicates.md`:

> A predicate is SARGable if the query optimizer can use an index seek to evaluate it. The column must appear naked on one side of the comparison, with no functions, no computations, and no wrapping.

> Not every non-SARGable predicate needs to be fixed. If the table is small (under 10,000 rows), a scan is fine. Don't over-optimize.

The `Like '%' + @FifoLifoSeq + '%'` pattern is fundamentally non-SARGable; the leading wildcard prevents any index seek on `PriInternal`. The schema-level fix is a persisted computed column on the FIFO/LIFO substring with an indexed equality predicate, documented in Section 11.1 of the analysis. The interim mitigation is `Option (Recompile)`, which gives the optimizer the actual `@FifoLifoSeq` value at compile time so it can produce a plan based on the actual selectivity rather than a default density estimate.

**Parameter sniffing with the local-variable form.** From `masterclass/Parameter Sniffing.md`:

> The classic fix is to assign input parameters to local variables and use only the locals in your queries:

Low-impact safeguard. The procedure has one parameter and the cost is not dominated by parameter sniffing, but the local-variable form is the program convention and costs nothing to apply.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Plan control. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) documents `OPTION (RECOMPILE)`, which gives the optimizer the actual `@FifoLifoSeq` value at compile time for the two non-SARGable scans.
- Predicates. [Predicates (SARGability reference)](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) describes the SARGability rules that the LIKE-with-leading-wildcard pattern violates.
- Recompile semantics. [Recompile a Stored Procedure](https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/recompile-a-stored-procedure) covers the statement-level recompile mechanics.
- Update target. [SELECT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-transact-sql) covers the consolidated multi-variable assignment used for the single initial read.

## Risk Note

The consolidated read is semantically equivalent to the three v1 reads as long as no other process modifies the same OeOrder row between the v1 reads. The v1 reads with NoLock would be subject to the same race, so v2 does not introduce new race exposure. The in-memory `@NewPriInternal` computation produces the same value the v1 third read would have returned: the UPDATE writes exactly `PriInternal + @SubPriority`, and the post-update read returns that value. The `Option (Recompile)` hints add a per-call compile cost, which is bounded for a procedure called per new Rx. Watch the first 24 hours for per-call elapsed time, post-update `PriInternal` value parity with v1, and event-log row count parity (one EvtRxPrioritizationEvents row per call). Rollback is a pure DDL revert; redeploy v1 from `Original.sql`.
