# Refactor Recommendation: lsp_BnkGetNextNonAssignableRx

**Date:** 2026-05-08
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** Cataloged. Refactored.sql is a proposed v12 awaiting iA review.

---

## Problem

v11 returns the top-priority Bank-Unassigned Rx that cannot be assigned because its product is not in any qualifying dispenser or only in excluded banks. The procedure has no input parameters, so parameter sniffing is not the issue. The cross-list capture flags it on two of four expense lists (Duration #14, Expense #21) with "algorithmic / plan rewrite candidate" as the dominant heuristic. The cost concentrates in duplicated work and unnecessary set operations.

- Duplicated scan of `TcdStatus + BnkConfiguredBanks + BnkAllBanks`. The v11 body builds `#IncludedBankProducts` and `#ExcludedBankProducts` from two near-identical SELECTs that differ only in the `B.IsExcluded` filter. Same three-table join chain, evaluated twice.
- No indexes on the intermediate temp tables. Each participates in a downstream join and the optimizer is left to hash-join into a heap.
- UNION (with implicit DISTINCT) on the final SELECT. The two source sets `#RxsNotInAnyBank` and `#RxsOnlyInExclBank` are disjoint by construction.
- No `Option (Recompile)` on the final SELECT. The temp-table cardinalities benefit from compile-time visibility.

## Recommendation

Apply the v12 package. Consolidate the duplicated three-table scan into a single `#BankProducts` keyed on `ProductId` that carries the `IsExcluded` flag, index every temp table that participates in a downstream join, replace UNION with UNION ALL on the disjoint source sets, and add `Option (Recompile)` where compile-time temp-table cardinality matters.

1. Build `#BankProducts` once from `TcdStatus`, `BnkConfiguredBanks`, and `BnkAllBanks`, projecting `IsExcluded` as a column. Add an index keyed on `ProductId` with `SupportedFillMethod`, `AddrBank`, and `IsExcluded` included.
2. Reference `#BankProducts` from the downstream `#RxsOnlyInExclBank` step twice, filtered on `IsExcluded = 1` and `IsExcluded = 0`, in two separate Left Joins.
3. Replace the final UNION with UNION ALL inside a derived table, with the `Top 1 ... Order By PriInternal, OrderId` and `Option (Recompile)` applied to the outer select.

The full v12 body is in `Refactored.sql`. The v11 body is in `Original.sql` for diff and rollback.

## First Principles

**Consolidate near-identical work.** From `masterclass/Conditional Aggregation Consolidation.md`:

> When the queries hit the same table with different predicates, the engine reads the table multiple times. Each statement compiles its own plan, scans or seeks the table independently, and produces one count. The shared work (the table read itself) is done N times.

v11's two SELECTs differ only in `B.IsExcluded`. The consolidated `#BankProducts` projects `IsExcluded` as a column and the two downstream consumers reference the same temp table with the appropriate filter on the join. The three-table scan happens once instead of twice. The downstream filters land on an indexed column.

**UNION ALL on disjoint sources.** From `masterclass/UNION ALL Views.md`:

> Use `Union All` (never `Union`) when the source-tag column makes the rows distinguishable.

An Rx appears in `#RxsNotInAnyBank` only when its product has no row in the in-any-bank set, and in `#RxsOnlyInExclBank` only when its product has at least one excluded-bank match and zero included-bank matches with the right fill method. The two conditions are mutually exclusive by construction. The UNION DISTINCT in v11 buys nothing semantically and pays for the implicit sort or hash that DISTINCT requires. UNION ALL is the right form.

**Indexed temp tables for downstream joins.** From `masterclass/Index Key Columns vs Included Columns.md`:

> The b-tree only needs to sort by the columns the optimizer will actually use to navigate. Every column you add to the key list makes the tree wider at every level, which means more pages, more memory, and slower writes. Included columns sit at the leaf only, so they cost storage but not navigation overhead.

`#BankProducts` is keyed on `ProductId` because the downstream `#RxsNotInAnyBank` and `#RxsOnlyInExclBank` joins probe by `ProductId`. `SupportedFillMethod`, `AddrBank`, and `IsExcluded` are Included so the temp table covers the downstream join's projection and residual without a back-probe to a heap row.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- UNION versus UNION ALL. [UNION (set operators)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-union-transact-sql) documents that UNION removes duplicates while UNION ALL preserves them. The page is explicit on the cost difference, which is relevant when the source sets are disjoint by construction.
- Indexed temp tables. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) and [Create Indexes with Included Columns](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-indexes-with-included-columns) cover the key-vs-include split that `#BankProducts` uses.
- Recompile and Aggregate Functions. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) documents `Option (Recompile)`; [Aggregate Functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) covers the `Group By ProductId, AddrBank, AutoFillTypeCode, IsExcluded` shape inside `#BankProducts`.

## Risk Note

The consolidated `#BankProducts` produces one row per `(ProductId, AddrBank, AutoFillTypeCode, IsExcluded)` combination. v11 carried both populations in separate temp tables; v12 carries both via the `IsExcluded` column with downstream filters on the join. The row identities are preserved by construction. The UNION ALL substitution is safe because the two source sets are mutually exclusive. In the first 24 hours watch plan operator count (expected: lower, one three-table scan instead of two), `TcdStatus` and `BnkConfiguredBanks` and `BnkAllBanks` read counts (expected: roughly halved), result identity parity with v11, and elapsed time. Rollback path: redeploy v11 from `Original.sql`. No schema change, no index change.
