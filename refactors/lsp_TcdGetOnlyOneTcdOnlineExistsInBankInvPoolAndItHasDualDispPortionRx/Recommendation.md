# Refactor Recommendation: lsp_TcdGetOnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. Refactored.sql is a proposed refactor awaiting iA review.

---

## Problem

The procedure sets an OUTPUT bit when exactly one Online dispenser exists for a given product, bank, and inventory pool, and that single dispenser holds a dual-dispenser portion Rx. It sits on the bank-assignment service hot path and lands at Volume rank 5 in the cross-list capture, which is the fifth-most-frequently-called procedure in the cohort. None of the issues in v_old is dramatic in isolation. They compound on the call frequency.

- Temp-table-and-recheck pattern. A `Select Into #OrderIdsInOnlineTcdsInBankInvPool` populates a temp table, `@@RowCount` is checked, then a second `Select OrderId From ...` re-reads the temp table to recover the single value. Two `TcdStatus` reads and one tempdb allocation, all to evaluate a scalar.
- Catch-all `(InvPool = @InvPool Or @InvPool = 0)` predicate. The same anti-pattern that appears in Row 30. The optimizer cannot push the disjunction into a seek without knowing whether the parameter is the sentinel.
- Legacy `If Object_Id('TempDb..#X')` cleanup block. Removed entirely once the temp table is gone.
- No local-variable indirection for the three input parameters. Program convention.

## Recommendation

Replace the temp-table workflow with a single in-memory aggregation. One scan of `TcdStatus` produces both the row count and the candidate `OrderId` in two local variables; an `If` on the locals sets the OUTPUT.

1. Declare locals for the three input parameters at the top of the procedure.
2. Collapse the temp-table flow into one `Select @MatchCount = Count(*), @SingleOrderId = Max(OrderId) From TcdStatus ...`.
3. Rewrite the catch-all as `(@LocalInvPool = 0 Or InvPool = @LocalInvPool)` and add `Option (Recompile)` so the optimizer can simplify the disjunction at compile time.
4. Remove the `If Object_Id` cleanup block; no temp table exists in v_next.
5. Set the OUTPUT bit from `@MatchCount = 1 And @SingleOrderId Like '%<[1-2]>'`.

The full v_next body is in `Refactored.sql`. The v_old body is in `Original.sql` for diff and rollback.

## First Principles

**Catch-all predicates compile to scans.** From `masterclass/Catch-All Query Anti-Pattern.md`:

> The optimizer compiles plans at compile time, not at runtime. When it sees `(@Param Is Null Or Col = @Param)`, it cannot evaluate `@Param Is Null` until the query runs. Compile time has to plan for both branches of the OR. Both branches have to be in the plan, which means the predicate cannot be pushed into an index seek that requires `Col = constant`. The result is a full scan of the table or its widest covering index, every call.

`(InvPool = @InvPool Or @InvPool = 0)` is the same shape with the sentinel inverted. Rewriting as `(@LocalInvPool = 0 Or InvPool = @LocalInvPool)` under `Option (Recompile)` lets the optimizer evaluate the left disjunct at compile time given the actual parameter and simplify the residual predicate to either everything or `InvPool = @LocalInvPool`. The optimizer can then choose an index seek on `(AddrBank, ProductId)` with `InvPool` either filtered or unfiltered as appropriate.

**Plan stability via the local-variable form.** From `masterclass/Parameter Sniffing.md`:

> The classic fix is to assign input parameters to local variables and use only the locals in your queries.

At Volume rank 5, plan stability is more valuable than a sniffed best case. The local-variable form caches a single density-based plan that serves all parameter shapes consistently. Recompile then gives the optimizer the actual sentinel value at compile time, which is the right combination for a sub-millisecond high-frequency caller where one bad cached plan against an unfortunate parameter combination is the failure mode worth preventing.

**In-memory scalar evaluation beats temp-table-and-recheck.** A temp table that is only interrogated for "did it have one row" and "what was the value in that one row" is over-engineered for a scalar evaluation. `Max(OrderId)` returns the single value when `Count(*) = 1` and returns a defined value otherwise that the `Like '%<[1-2]>'` test handles correctly. When `Count(*) = 0`, `Max(OrderId)` is NULL and the `Like` returns UNKNOWN, which the IF treats as false. When `Count(*) > 1`, the count check fails first. The semantics are preserved across all three cases with one scan and zero tempdb allocations.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Catch-all rewrite under Recompile. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) documents `Option (Recompile)`, which is the mechanism that lets the optimizer substitute the actual `@LocalInvPool` value at compile time and simplify the disjunction.
- Aggregate functions for the scalar evaluation. [COUNT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/functions/count-transact-sql) and [Aggregate Functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) cover the `Count(*)` and `Max(OrderId)` semantics that the single statement relies on.
- Table hints. [Table Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table) documents `NoLock` and READUNCOMMITTED equivalence; the procedure is read-only with side-effect-free output, which matches the documented NoLock use case.

## Risk Note

The semantic risk on the single-statement substitution is narrow because the three input states (zero, one, more-than-one matching rows) all produce the same OUTPUT as v_old by construction. The catch-all rewrite is logically equivalent under Recompile. The temp-table removal eliminates one tempdb allocation per call, which is favorable. In the first 24 hours watch OUTPUT parity against v_old, per-call elapsed time (expected to fall), and the plan shape on `TcdStatus` (expected to be a seek on `(AddrBank, ProductId)` with `InvPool` as a residual or as a seek key depending on the parameter value). Rollback path: redeploy v_old from `Original.sql`. No schema change, no index change.
