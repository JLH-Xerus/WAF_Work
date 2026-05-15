# Refactor Recommendation: lsp_SrtGetSortationItems

**Date:** 2026-05-07
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** v2 in production across 13 of 14 reporting MFCs. v3 proposed, in progress.

---

## Problem

v2 of `dbo.lsp_SrtGetSortationItems` is the single largest I/O contributor in the fleet. The cross-MFC Query Store capture shows 1.84 trillion logical reads per month across 17.3 million executions, with Bolingbrook alone burning 400 billion reads. Avg reads per execution averages 106K against a four-column table whose typical result is one row. CPU time at Bolingbrook runs about 47 days of CPU per month on this single procedure. Plan stability is the tell: every site returns exactly one plan variant for one query_id, with no parameter sniffing volatility. That is the catch-all query anti-pattern's signature.

The single issue:

- Catch-all WHERE clause forces a scan plan. `(@Barcode Is Null Or Barcode = @Barcode) And (@SortationCode Is Null Or SortationCode = @SortationCode)` cannot be planned for any specific parameter shape at compile time. The optimizer compiles one scan plan that handles every parameter combination, and every call pays the scan cost.

## Recommendation

Ship v3 as a single deployment unit. The procedure has two optional parameters and four parameter combinations, which is the cleanest case for IF/ELSE branching. No schema change. The win depends on `SrtSortationItem` having seekable indexes on `Barcode` and `SortationCode`; pull the index DDL before deployment (Section 11.1.A of the analysis covers this).

1. Replace the single catch-all SELECT with four IF/ELSE branches, one per parameter combination. Each branch is a standalone SELECT with the predicate hard-coded for that combination.
2. Preserve the proc's parameter signature and result-set shape. No caller-visible contract change.

The full v3 body is in `Refactored.sql`. The v2 body is in `Original.sql` for diff and rollback. The expected drop is from roughly 95K reads per execution to under 10 for single-parameter calls, fleet-wide.

## First Principles

**Catch-all queries defeat the optimizer at compile time.** From `masterclass/Catch-All Query Anti-Pattern.md`:

> The optimizer compiles plans at compile time, not at runtime. When it sees `(@Param Is Null Or Col = @Param)`, it cannot evaluate `@Param Is Null` until the query runs. Compile time has to plan for both branches of the OR. Both branches have to be in the plan, which means the predicate cannot be pushed into an index seek that requires `Col = constant`. The result is a full scan of the table or its widest covering index, every call.

The Query Store evidence confirms the diagnosis exactly. Every site shows one plan variant, avg reads per execution scales with table size rather than result size, and avg duration is uniformly high for what should be a single-row lookup. The catch-all pattern produces consistently bad performance, not parameter-sniffing volatility.

**IF/ELSE branching is the right fix when parameter count is small.** From `masterclass/Catch-All Query Anti-Pattern.md`:

> Best when the number of parameters is small (two or three) and the number of combinations is manageable (four to eight). Each combination becomes its own SELECT, each with a clean cached plan.

> The optimizer sees four separate queries. Each one compiles a plan suited to its actual filter, with no NULL guard interfering. Each branch caches its own plan and reuses it across calls. Index seeks become possible.

With two optional parameters, v3 produces four cached plans. Three of them are single-row index seeks (barcode only, sortation code only, both). The fourth (no parameters supplied) is a scan, which is correct because it is asking for the entire table. The plan-cache footprint stays small, the plans stay good, and each branch only ever sees calls that match its parameter combination.

**Plan-cache reuse versus plan adaptability.** A single cached plan that is wrong for every call is worse than four cached plans, each tuned to its parameter shape. There is no parameter-sniffing concern in the v3 form because each plan only ever sees calls that match its branch. Calls with both parameters never enter the "barcode only" branch, and vice versa.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Branch-driven plan selection. [SELECT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-transact-sql) and [Predicates (SARGability reference)](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) cover the predicate forms that the four branches use to enable index seeks.
- Index seek path. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) documents the index structure that v3's first three branches rely on for the seek.
- Fallback if the index assumption is wrong. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) covers `RECOMPILE` and other plan-control options if post-deployment data shows v3 is not seeking as expected.

## Risk Note

The win depends on `SrtSortationItem` having seekable indexes on `Barcode` and on `SortationCode`. Without them, v3's branches still scan, just in four cached plans rather than one, and the cost would be similar to v2. Pull the index DDL for the table before deployment and confirm the indexes exist; if they do not, add them as part of the v3 deployment package or in an immediate follow-on. The caller contract is unchanged. Watch Query Store for the first 24 hours: avg reads per execution should drop from roughly 95K to under 10 for parameter-supplied calls, avg duration should drop to single-digit milliseconds, and the plan cache should show four plans (one per branch). Rollback is a pure DDL revert; redeploy v2 from `Original.sql`.
