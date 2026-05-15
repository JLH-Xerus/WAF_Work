# Refactor Recommendation: lsp_OrdGetRxStatusCounts

**Date:** 2026-05-07
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** v1 in production across the MFC fleet. v2 proposed, awaiting deployment.

---

## Problem

v1 of `dbo.lsp_OrdGetRxStatusCounts` runs roughly 845K times per month across nine reporting MFCs and burns 579 billion logical reads doing so. The procedure populates 38 OUTPUT counters for the Order/Track Rx status dashboard. The cost concentrates in two duplicated 38-CASE SELECT blocks (one per value of `@CountOnlyMostRecentRxForEachOrderId`) and in a silently broken EXISTS predicate that has been disabling the `@AddrBankFilter` parameter since the procedure was written.

The five issues:

- Ambiguous self-comparison in the EXISTS subquery. `Where AddrBank = AddrBank` resolves both sides to the inner table; the intended outer reference is dropped at parse time. The filter parameter has no observable effect in v1.
- 100-percent-duplicated SELECT block. Lines 128-192 and 194-255 differ only in whether `OeOrderHistory` joins to `OeOrderCurrHistoryDtTm`. Two cached plans, two compile costs, two maintenance surfaces.
- `@RestrictAddrBank` is a table variable. The optimizer estimates one row regardless of the actual filter size, which will matter once the EXISTS predicate is fixed and the cardinality starts driving real plan choice.
- WHILE-loop CSV parser. Procedural string slicing with a trailing-comma fix-up, predates `STRING_SPLIT` by several SQL Server versions.
- Hint inconsistency. The two history branches use different syntax for the `Index(ByHistoryDtTm)` hint (`As O` alias on one, no alias on the other).

## Recommendation

Ship v2 as a single deployment unit. The package addresses one correctness bug, one structural duplication, two parameter-handling modernizations, and one cosmetic standardization. No schema change, no index change. The EXISTS fix is a behavior change; callers passing a non-empty `@AddrBankFilter` will see strictly smaller counts after deployment than they did under v1.

1. Qualify both sides of the EXISTS predicate (`R.AddrBank = O.AddrBank`, and the same pattern for `OH`, `RR`). Apply consistently across all four branches.
2. Collapse the two 38-CASE blocks into a single CTE with four UNION ALL branches. Move the `@CountOnlyMostRecentRxForEachOrderId` selector into the OeOrderHistory branch's WHERE predicate via a LEFT JOIN to `OeOrderCurrHistoryDtTm` plus the gate `(@CountOnlyMostRecentRxForEachOrderId = 0 Or OC.OrderId Is Not Null)`.
3. Replace `@RestrictAddrBank` with `#RestrictAddrBank` plus a clustered index on `AddrBank`.
4. Replace the WHILE loop with one `STRING_SPLIT` insert, with `LTrim/RTrim` and an empty-token filter for defensive correctness on caller input.
5. Standardize the `Index(ByHistoryDtTm)` hint syntax on the aliased form. Preserve the hint itself.

The full v2 body is in `Refactored.sql`. The v1 body is in `Original.sql` for diff and rollback. Before deploying to Tolleson, unforce the existing forced plan (`forcedplan = 750632189`); v2's consolidated statement will not match the forced shape.

## First Principles

**Ambiguous self-comparison in subqueries.** From `masterclass/Ambiguous Self-Comparison Predicates.md`:

> A predicate inside a subquery references a column name that exists in both the inner and outer scope, without qualifying which one it means. SQL Server's name resolution always prefers the innermost scope, so both sides of the comparison resolve to the same column. The predicate becomes a self-comparison, which is true for every non-NULL row, and the intended cross-scope filter is silently lost.

The four EXISTS sites in v1 all match this pattern exactly. `R.AddrBank = R.AddrBank` is true for every non-NULL row in the filter table, so any non-empty `@AddrBankFilter` produces the same counts as no filter at all. The fix is mechanical: alias every table in scope and qualify every column reference. The fix is also a behavior change, which is why Section 7 of the analysis walks the caller-impact implications.

**Table variables do not carry statistics.** From `masterclass/Table Variables vs Temp Tables.md`:

> Table variables (`Declare @T Table (...)`) have no statistics. The SQL Server optimizer always estimates that a table variable contains 1 row, regardless of how many rows you actually insert.

Today the one-row estimate on `@RestrictAddrBank` is masked by the broken EXISTS predicate; the optimizer's bad estimate does not matter when the predicate is a tautology. Once v2 fixes the predicate, the optimizer's view of "is the filter narrow or broad" starts driving join order and access method choice on the outer tables. `#RestrictAddrBank` with a clustered index on `AddrBank` gives the optimizer the real cardinality and supports the four EXISTS probes as seeks.

**STRING_SPLIT replaces procedural CSV parsing.** From `masterclass/STRING_SPLIT vs WHILE Loop CSV Parsing.md`:

> `STRING_SPLIT` was added in SQL Server 2016 and has been the canonical replacement since. It returns a single-column table of tokens:

The v1 WHILE loop and its trailing-comma fix-up are procedural cruft that has nothing to do with the proc's purpose. One `Insert ... Select Distinct LTrim(RTrim(value)) From String_Split(@AddrBankFilter, ',') Where LTrim(RTrim(value)) > ''` replaces both. The Distinct keeps the temp-table cardinality estimate honest if a caller passes duplicates.

**Code duplication inside an IF/ELSE.** The two 38-CASE blocks differ in exactly one operator (the OeOrderHistory join shape). A single CTE with a LEFT JOIN to `OeOrderCurrHistoryDtTm` plus a parameter gate produces the same row set under both values of `@CountOnlyMostRecentRxForEachOrderId`. When the parameter is 1, the gate requires a join match (semantically equivalent to v1's INNER JOIN). When the parameter is 0, the gate evaluates true for every row (semantically equivalent to v1's branch with no join). Two cached plans become one. The optimizer sees a single set of cardinality estimates instead of two parallel sets.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Existence checks. [EXISTS (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql) specifies the subquery-to-existence semantics. The four corrected EXISTS sites in v2 rely on the page's documented short-circuit behavior.
- CSV parsing. [STRING_SPLIT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/functions/string-split-transact-sql) documents the set-based replacement for the WHILE-loop parser, including the compatibility-level requirement and the `value` output column.
- Plan control for parameter-driven branches. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) covers `RECOMPILE`, which is the simplest mitigation if the consolidated CTE shows parameter-sniffing volatility on `@CountOnlyMostRecentRxForEachOrderId` after deployment.
- CTE structure. [WITH common_table_expression (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) documents the CTE shape used to consolidate the two duplicated SELECT blocks into one.

## Risk Note

The dominant semantic risk is the EXISTS fix, which changes the procedure's contract with any caller that passes `@AddrBankFilter`. Validation must run v1 and v2 side by side on the all-banks case (counts must match) and on a narrow-filter case (v2 strictly smaller, expected). The structural changes (table variable to temp table, WHILE loop to `STRING_SPLIT`, two SELECTs to one CTE) are semantically equivalent by construction. Watch Query Store for the first 72 hours: plan count should stabilize at one per site (down from two), avg reads per execution should drop substantially at sites where narrow filters are commonly passed, and the Tolleson forced plan must be unforced before deployment to that site or it will fail to apply. Rollback is a pure DDL revert; redeploy v1 from `Original.sql`. No data state is touched.
