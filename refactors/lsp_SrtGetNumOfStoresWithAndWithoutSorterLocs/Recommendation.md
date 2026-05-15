# Refactor Recommendation: lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs

**Date:** 2026-05-07
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** v6 in production across all 14 reporting MFCs. v7 proposed, awaiting deployment.

---

## Problem

v6 of `dbo.lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs` is a top-tier I/O contributor present in all 14 reporting sites. The cross-MFC Query Store capture shows 1.20 trillion logical reads per month across 6.1 million executions, averaging 196K reads per execution fleet-wide. NorthLake spends 102 hours of CPU per month on this single procedure; the fleet total is roughly 27 days of CPU per month. Avg duration runs up to 1,201 ms at Orlando for what is structurally a four-priority counting query. The cost stacks in three layers and reaches into every priority branch.

The six issues:

- Three-table sorter-loc join chain repeated four times. `SrtSorterLoc -> SysEndPharmacyShpCarrierAssoc -> SysEndPharmacy` appears in priorities 1, 2, 3, and 4 independently, plus inside the two `NOT IN` subqueries. Each repetition independently scans or seeks the three base tables.
- NOT IN with NULL-safety risk in priorities 1 and 4. The subquery uses a `Left Join SysEndPharmacy` which makes NULL `StoreNum` possible; the `Where SEP.StoreNum Is Not Null` filter at the bottom papers over the bug today but the pattern is fragile against future edits.
- TRIM on column in WHERE. Five separate `TRIM(O.StoreNum) = TRIM(@StoreNum)` (or `SEP.StoreNum`) sites wrap the column in a function call. Non-SARGable. Forces a scan on whichever index covers StoreNum.
- UNION (not UNION ALL) at the seams between the four priority branches. The Priority column carries 1, 2, 3, 4 in distinct branches, so no row from branch A could match branch B. The dedup is wasted work.
- Legacy `If Object_Id ... Drop Table` temp-table existence check.
- Inconsistent capitalization on `With (NoLock)` across multiple lines.

## Recommendation

Ship v7 as a single deployment unit. The package materializes the repeated three-table join chain once into an indexed temp table, replaces `NOT IN` with `NOT EXISTS`, moves the TRIM off the column via a pre-trimmed local variable, swaps UNION for UNION ALL, and modernizes the cleanup blocks. No schema change. The expected drop is from roughly 196K reads per execution to under 50K fleet-wide, with the biggest absolute wins at the high-cost sites (NorthLake, Bolingbrook, Orlando, Tolleson, Mechanicsville, Mansfield).

1. Pre-trim `@StoreNum` into `@StoreNumTrimmed` at the top of the procedure. Every downstream WHERE clause compares a bare column to the local variable.
2. Materialize the sorter-loc assignment chain once into `#SorterLocStores (StoreNum, HasTote, IsFull)` with a clustered index on `StoreNum`. The HasTote and IsFull columns are carried forward so priorities 2 and 3 do not need to re-join `SrtSorterLoc`.
3. Convert the `Left Join`s in the materialized chain to `Inner Join`s. v6's `Where SEP.StoreNum Is Not Null` filter effectively converted LEFT to INNER by post-filtering; the explicit form gives the optimizer better cardinality upfront.
4. Replace `NOT IN` against the multi-table subquery with `NOT EXISTS` against `#SorterLocStores` in priorities 1 and 4.
5. Restructure priorities 2 and 3 to read from `#SorterLocStores` directly with the `HasTote = 0` and `IsFull = 1` predicates respectively.
6. Replace `UNION` with `UNION ALL` between the four priority branches. The Priority column makes overlap impossible.
7. Replace the legacy temp-table existence check with `Drop Table If Exists` at top and bottom of the procedure.
8. Standardize `With (NoLock)` casing.

The full v7 body is in `Refactored.sql`. The v6 body is in `Original.sql` for diff and rollback.

## First Principles

**Materialize-once for repeated multi-table join chains.** From `masterclass/Correlated Subqueries to CTEs.md`:

> The key insight: the CTE scans `CfStoreDeliveryCourierCutOff` once, groups by pharmacy, and produces a small result set. The join back to the outer query is a simple lookup. Total reads on the delivery table: 6 logical reads instead of thousands.

The same principle applies to the four-times-repeated `SrtSorterLoc -> SPSC -> SEP` chain. One materialization into `#SorterLocStores` with a clustered index on `StoreNum` collapses four independent join chains into one scan plus four indexed probes. The `HasTote` and `IsFull` columns are carried forward into the materialized set so the differentiators between priorities 2 and 3 do not require re-joining the base tables.

**NOT IN is unsafe against NULL-able subquery columns.** From `masterclass/NOT IN vs NOT EXISTS.md`:

> `NOT IN` against a subquery is unsafe in the presence of NULL. If any single row in the subquery returns NULL for the compared column, the entire `NOT IN` predicate evaluates to UNKNOWN, which excludes every outer row from the result. The query silently returns zero rows, or fewer rows than expected, with no error and no warning. `NOT EXISTS` does not have this problem and produces the result the author almost always intended.

v6's `Left Join SysEndPharmacy` makes NULL `StoreNum` possible in the subquery's result set. The inner `Where SEP.StoreNum Is Not Null` filter mitigates the immediate bug, but the predicate is fragile against future edits. v7's `NOT EXISTS` against `#SorterLocStores` is NULL-safe by construction and produces a Left Anti-Semi Join plan with cleaner cardinality.

**Non-SARGable predicates: function on column.** From `masterclass/Non-SARGable Predicates.md`:

> A predicate is SARGable if the query optimizer can use an index seek to evaluate it. The column must appear naked on one side of the comparison, with no functions, no computations, and no wrapping.

`TRIM(O.StoreNum) = TRIM(@StoreNum)` wraps the column in a function call, defeating any index seek on `StoreNum`. Pre-trimming the parameter into `@StoreNumTrimmed` once at the top of the procedure restores SARGability. The bare-column comparison lets the optimizer choose an index seek when one is available. The schema-level fix (a persisted computed column on the trimmed value, or a one-time data cleanup plus a constraint) is documented in Section 11.3 of the analysis but out of scope for v7.

**UNION versus UNION ALL with mutually exclusive label columns.** From `masterclass/LEFT JOIN OR Anti-Pattern.md`:

> UNION (not UNION ALL) handles deduplication automatically. Use UNION ALL only when you can guarantee a row cannot match both paths.

The Priority column carries one of four distinct integers per branch, so no row from one branch can match any row from another. The `UNION` dedup sort in v6 is wasted work. `UNION ALL` concatenates the branches directly.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Existence checks. [EXISTS (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql) documents the NULL-safe semantics that make `Not Exists` the correct replacement for `Not In` in priorities 1 and 4.
- Set operators. [UNION (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-union-transact-sql) covers the distinction between UNION and UNION ALL and the implicit DISTINCT that UNION imposes.
- Predicates. [Predicates (SARGability reference)](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) describes the predicate forms the optimizer can push into an index seek.
- Index support. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) and [Create Filtered Indexes](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-filtered-indexes) cover the clustered index on `#SorterLocStores` and the filtered-index opportunity on `SrtSorterLoc.CarrierCode` documented in Section 11 of the analysis.

## Risk Note

Three semantic changes compound, each independently safe. First, `NOT EXISTS` is identical to `NOT IN` for the non-NULL inner case v6 was already producing (the inner filter ensured this) and correct for the NULL case v6 papered over. Second, `Inner Join` replacing `Left Join` in the materialized chain is equivalent to v6 because the post-filter on `Is Not Null` converted LEFT to INNER. Third, `UNION ALL` replacing `UNION` is equivalent because the Priority column makes overlap impossible. The result set should be byte-identical for any data state v6 was producing correct results on. Watch the first 24 hours for the four per-priority counts (must match), avg reads per execution (expected: drop from 196K to under 50K), and plan-variant count at sites that previously showed parameter-sniffing volatility (Orlando, BrooklynPark, Indy, Kent, Mansfield, West Jordan). Rollback is a pure DDL revert; redeploy v6 from `Original.sql`. No data state is touched.
