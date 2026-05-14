# Refactor Recommendation: lsp_SrtGenerateAndReturnSorterToteLocationStats

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Original in production. Proposed refactor designed by the MFC DBA team, awaiting iA review.

---

## Problem

The original `dbo.lsp_SrtGenerateAndReturnSorterToteLocationStats` returns five OUTPUT counters about the sorter and tote state for the sorter monitoring dashboard. The procedure appears on two of four expense lists (Expense #23, Plan #15) with "plan instability" and "algorithmic / plan rewrite candidate" as the dominant heuristics. The cost concentrates in a UNION-with-placeholder-columns aggregation pattern at the end of the procedure plus several smaller structural issues.

The six issues:

- UNION-with-placeholder-columns aggregation at the end of the procedure (lines 239-318). Five branches each contribute one count value and four empty-string placeholders; the outer SUM extracts the contributions. The optimizer evaluates every branch on every call.
- Eight scattered `If Object_Id ... Drop Table` blocks at the top of the procedure.
- No indexes on intermediate temp tables that participate in downstream joins.
- `Where ShipmentId Not In (Select ShipmentId From SrtShipToteShipmentAssoc)` for the inducted-but-not-sorted filter. NOT EXISTS is the safer idiom.
- Unexplained `Option (MaxDop 1)` hint on the first SELECT INTO. No comment in the body.
- Two-step "build + delete-non-matching" pattern on `#HistoryExceptionRxPackageShipmentIds`. One SELECT with both conditions is cleaner.

## Recommendation

Ship the proposed refactor as a single deployment unit. The package replaces a multi-branch aggregation with five focused SELECTs, swaps `Not In` for `Not Exists`, indexes the intermediate temp tables, and consolidates the cleanup block. No schema change, no index change on base tables.

1. Replace the UNION-with-placeholders aggregation with five direct `Select @OutputVar = ...` statements, one per OUTPUT counter. Each statement is a focused query the optimizer can plan independently.
2. Collapse the eight `If Object_Id ... Drop Table` blocks into a single `Drop Table If Exists` list at the top of the procedure.
3. Add clustered indexes to `#SorterTotePackageRxs`, `#AllShipmentIds`, and `#NumOfPackagesInducted` on the columns used in downstream joins.
4. Convert the `Not In` subquery in the inducted-but-not-sorted aggregate to `Not Exists` with the correlation predicate inside the inner WHERE.
5. Preserve `Option (MaxDop 1)` on the first SELECT INTO. The hint carries no comment; removing it without evidence is a net-new risk. Flag for re-evaluation in Section 11 of the analysis.
6. Collapse the build + delete on `#HistoryExceptionRxPackageShipmentIds` into a single SELECT INTO with both conditions in the WHERE.

The full proposed body is in `Refactored.sql`. The original body is in `Original.sql` for diff and rollback.

## First Principles

**NOT IN is unsafe against NULL-able subquery columns.** From `masterclass/NOT IN vs NOT EXISTS.md`:

> `NOT IN` against a subquery is unsafe in the presence of NULL. If any single row in the subquery returns NULL for the compared column, the entire `NOT IN` predicate evaluates to UNKNOWN, which excludes every outer row from the result. The query silently returns zero rows, or fewer rows than expected, with no error and no warning. `NOT EXISTS` does not have this problem and produces the result the author almost always intended.

Even if `SrtShipToteShipmentAssoc.ShipmentId` is NOT NULL today, the `Not Exists` form is the codebase-wide convention and the safer default for any future schema change. The mechanical conversion moves the correlation predicate (`TSA.ShipmentId = PI.ShipmentId`) into the inner WHERE.

**Indexes on intermediate temp tables.** From `masterclass/Index Key Columns vs Included Columns.md`:

> Included columns sit at the leaf only, so they cost storage but not navigation overhead.

`#SorterTotePackageRxs`, `#AllShipmentIds`, and `#NumOfPackagesInducted` each feed downstream joins on a small number of key columns. A clustered index on the join column gives the optimizer real cardinality and lets it choose hash or merge based on the populated row count. The "insert then index" pattern (populate the temp table, then create the index on the populated data) is the standard form.

**Direct OUTPUT assignment over UNION-with-placeholders.** The UNION-with-placeholders pattern forces the optimizer to evaluate every branch even though each branch only contributes to one OUTPUT variable. Five focused SELECT statements, each assigning directly to its OUTPUT variable, give the optimizer one row source per statement and one cached plan per statement. Each branch is small and independent; the structural simplification is the win, not a reduction in leaf reads.

**Consolidate build-and-delete into one SELECT.** The two-step pattern on `#HistoryExceptionRxPackageShipmentIds` was likely a defensive accommodation when the original WHERE clause was complex. The conditions are well-understood now (`SR.HistoryDtTm <> '12/31/9999'` and `O.OrderStatus = 'Canceled'`); both belong in a single SELECT INTO with both predicates in the WHERE.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Existence checks. [EXISTS (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql) documents the NULL-safe semantics that make `Not Exists` the correct replacement for `Not In` here.
- Index support. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) and [Create Indexes with Included Columns](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-indexes-with-included-columns) cover the indexed temp tables.
- Plan control. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) documents the `MaxDop` hint preserved on the first SELECT INTO and the `Recompile` hints added to the parameter-sensitive aggregates.

## Risk Note

The five direct OUTPUT assignments produce the same five values as the original UNION pattern by construction; each branch's filter is reproduced verbatim in the focused SELECT. The `Not Exists` substitution is functionally identical when the subquery column is NOT NULL and preserves correctness when it is NULL. The `Option (MaxDop 1)` hint is preserved on the first SELECT INTO because removing it without evidence is a net-new risk; the hint carries no comment explaining its rationale, so the conservative choice is to keep it and flag it for re-evaluation after deployment. Watch the first 24 hours for per-OUTPUT-variable value parity with the original, plan operator count (expected: substantially lower), and elapsed time per call. Rollback is a pure DDL revert; redeploy the original from `Original.sql`.
