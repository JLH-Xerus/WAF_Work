# Refactor Recommendation: lsp_BnkGetNextRxToAssignToBank_DispenserBalancing

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. Refactored.sql is a proposed v10 awaiting iA review.

---

## Problem

v9 returns the next unassigned auto-fill Rx plus the best-fit cabinet bank in Dispenser Balancing mode. The cross-list capture reports the procedure on three of four expense lists (Volume #23, Duration #9, Expense #10) with "heavy logical reads" as the dominant heuristic. The cost is structural: four intermediate temp tables participate in downstream joins, none of them carry an index, and the final SELECT compiles without compile-time visibility into the parameter that gates the row-count estimate.

- No indexes on the four temp tables (`#OnlineTcdCountPerBankProd`, `#QueuedAssignedRxCountPerBank`, `#QueuedUnassignedRxs`, `#BankProdHavingRoomForQueuedInBankRx`). The optimizer is forced into hash joins against unstructured heaps even on bounded candidate sets.
- Mixed-convention temp-table cleanup. Two `If Object_Id` legacy blocks and two `Drop Table If Exists` modern statements. Consolidate.
- No local-variable indirection for `@NumOfQueuedInBankRxsPerTcd`. Low impact, but the local-variable form is the program convention.
- No `Option (Recompile)` on the final SELECT. The parameter `@NumOfQueuedInBankRxsPerTcd` drives the row-count estimate for `#BankProdHavingRoomForQueuedInBankRx`; without Recompile the optimizer estimates from a stale cached plan.
- `BnkAllBanks` join written in non-aliased positional form. Style inconsistency with the rest of the procedure.

## Recommendation

Apply the v10 package. The five fixes are independent and additive, no schema or index change required outside the temp tables themselves.

1. Declare `@LocalNumOfQueuedInBankRxsPerTcd` and consolidate the four `Drop Table If Exists` statements into a single tail block.
2. Add an explicit index after each temp-table population, keyed by the columns the downstream join uses for equality. Use `Include` for projected residuals.
3. Rewrite the `BnkAllBanks` join in aliased form (`Inner Join BnkAllBanks BA With (NoLock) On BA.AddrBank = D.AddrBank`) to match the surrounding convention.
4. Add `Option (Recompile)` to the final SELECT so the optimizer sees the actual row counts for the indexed temp tables at compile time.
5. Validate against an `OeOrder` statistics check before deployment; the cross-list capture's "heavy logical reads" heuristic recommends a statistics refresh on the candidate-set tables.

The full v10 body is in `Refactored.sql`. The v9 body is in `Original.sql` for diff and rollback.

## First Principles

**Indexed temp tables for downstream joins.** From `masterclass/Index Key Columns vs Included Columns.md`:

> The b-tree only needs to sort by the columns the optimizer will actually use to navigate. Every column you add to the key list makes the tree wider at every level, which means more pages, more memory, and slower writes. Included columns sit at the leaf only, so they cost storage but not navigation overhead.

`#OnlineTcdCountPerBankProd` is keyed on `(AddrBank, ProductId)` because the downstream join probes by both. `#QueuedAssignedRxCountPerBank` follows the same shape. `#QueuedUnassignedRxs` is keyed on `(ProductId, NumDispenses)` with `OrderId`, `HistoryDtTm`, and `PriInternal` Included so the final SELECT's join projection and ORDER BY columns stay at the leaf. `#BankProdHavingRoomForQueuedInBankRx` is keyed on `(ProductId, OnlineTcdCount)` with `AddrBank`, `RoomForMoreRxCount`, and `SupportedFillMethod` Included.

**Rowgoal applies cleanly under Recompile with a TOP+ORDER BY.** From `masterclass/TOP with ORDER BY Semantics.md`:

> TOP combined with ORDER BY creates a rowgoal optimization. The optimizer knows it only needs N rows in a specific order, so it may [...] use an ordered index scan. If an index on the ORDER BY column exists, it scans in order and stops after N rows. Very efficient.

The final `Select Top 1 ... Order By O.PriInternal, O.OrderId, P.RoomForMoreRxCount Desc` becomes a rowgoal-amenable shape once `#QueuedUnassignedRxs` has an index on the join keys with `PriInternal` Included. Recompile gives the optimizer the actual selectivity of the room-count filter at compile time so the nested-loop choice is grounded in real cardinality.

**Local-variable form for parameter consistency.** From `masterclass/Parameter Sniffing.md`:

> The classic fix is to assign input parameters to local variables and use only the locals in your queries.

`@NumOfQueuedInBankRxsPerTcd` flows into the room-count arithmetic that filters `#BankProdHavingRoomForQueuedInBankRx`. The local-variable form is low-impact on this procedure but matches the program convention applied across the cohort. Recompile is the larger lever here because the temp-table cardinalities are what move the plan.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Indexed temp tables. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) and [Create Indexes with Included Columns](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-indexes-with-included-columns) cover the key-versus-include split.
- TOP with ORDER BY. [TOP (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/top-transact-sql) and [ORDER BY clause](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql) document the rowgoal interaction the indexed temp tables enable.
- Recompile semantics. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) and [Recompile a Stored Procedure](https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/recompile-a-stored-procedure) describe the compile-time substitution the final SELECT relies on.

## Risk Note

The temp-table indexes change the optimizer's join order options. The expected shift is from hash joins to nested-loop joins driven by the indexed temp tables, which is favorable for the small candidate sets typical of this procedure. After a long Dispenser Balancing outage where many Rxs accumulate as unassigned, the candidate set can be large; the optimizer should still choose hash because the temp-table auto-statistics reflect the actual size, and Recompile guarantees it sees those statistics on each call. In the first 24 hours watch the plan shape for the final SELECT (expected: nested-loop or merge into the indexed temp tables), elapsed time per call (expected: small reduction), and result-set identity parity with v9. Rollback path: redeploy v9 from `Original.sql`. No schema change, no index change beyond the temp-table indexes.
