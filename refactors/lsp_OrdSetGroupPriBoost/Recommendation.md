# Refactor Recommendation: lsp_OrdSetGroupPriBoost

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** v11 in production. v12 proposed by the MFC DBA team, awaiting iA review.

---

## Problem

v11 of `dbo.lsp_OrdSetGroupPriBoost` updates the priority-boost digit (character position 4 of `PriInternal`) on every Rx in a group or split-package super-group whose current boost is lower than the target boost, then logs both a group-level event and a per-Rx event for audit. The procedure appears on two of four expense lists (Duration #12, Expense #8) with "algorithmic / plan rewrite candidate" as the dominant heuristic. Parameter sniffing is not the cost driver; the structural issues are.

The five issues:

- OR predicate combining equality and LIKE on the same column. `(GroupNum = @GroupNum Or GroupNum Like @SuperGroupNum + '[A-Z]')` cannot be satisfied with a single clean seek into the GroupNum index. The optimizer either scans or falls back to an index-scan-with-residual pattern.
- Three reads of `OeOrder` for the same row set. `#GroupRxs` carries only `OrderId`, so the UPDATE and the per-Rx event INSERT both re-read `PriInternal` from `OeOrder`.
- Non-SARGable `SubString(PriInternal, 4, 1) > Cast(@PriBoost As Char(1))` predicate. The Substring on the column defeats any index seek on `PriInternal`. Unavoidable without a schema change; mitigated by narrowing the row count before the predicate evaluates.
- Misleading `with (nolock)` hint on the UPDATE target. The engine ignores NoLock on update targets; the hint is documentation drift rather than effective behavior.
- No local-variable indirection for the input parameters.

## Recommendation

Ship v12 as a single deployment unit. The package splits the OR into two focused INSERTs that each seek into the GroupNum index, carries `PriInternal` forward through `#GroupRxs` to eliminate two re-reads, removes the misleading UPDATE NoLock hint, and adds local-variable indirection. No schema change. The procedure is fast in absolute terms; the win is structural.

1. Add local variables for the input parameters and precompute `@SuperGroupLikePattern` once at the top of the procedure.
2. Replace the OR-combined predicate with two INSERTs into `#GroupRxs`. The first INSERT seeks by `GroupNum = @LocalGroupNum`; the second seeks by `GroupNum Like @SuperGroupLikePattern` and uses a `Left Join #GroupRxs ... Where Is Null` anti-join to prevent duplicates.
3. Declare `#GroupRxs` with `(OrderId, PriInternal)` and a primary key on `OrderId`. The PriInternal column carries the pre-update value forward, so the UPDATE no longer needs to re-read it from `OeOrder`.
4. Remove the `with (nolock)` hint from the UPDATE target.
5. The per-Rx event INSERT reads the post-update `PriInternal` from `OeOrder` once (the UPDATE has already written the new value in the same transaction).

The full v12 body is in `Refactored.sql`. The v11 body is in `Original.sql` for diff and rollback.

## First Principles

**OR across different access paths defeats a clean seek.** From `masterclass/LEFT JOIN OR Anti-Pattern.md`:

> The optimizer needs to satisfy `Condition_A OR Condition_B` where A and B involve different tables. It can't seek on table X's index AND table M's index simultaneously for an OR.

> The textbook refactor splits the query into two branches, one per join path, and combines them with UNION:

The textbook fix is UNION ALL across the two paths. The same principle applies here even though the OR is on a single table: two focused INSERTs into `#GroupRxs`, one per branch of the OR, let the optimizer seek into the GroupNum index for each and avoid the scan-or-fallback that the combined predicate forces. The anti-join in the second INSERT prevents duplicates in the edge case where a GroupNum matches both predicates.

**Non-SARGable predicates: narrow before you evaluate.** From `masterclass/Non-SARGable Predicates.md`:

> Not every non-SARGable predicate needs to be fixed. If the table is small (under 10,000 rows), a scan is fine. Don't over-optimize. If the predicate is on a joined table (not the driving table), the scan cost may be negligible.

The `SubString(PriInternal, 4, 1) > @PriBoostChar` predicate is fundamentally non-SARGable without a schema change. The mitigation is to bound the row count the predicate evaluates against. v12's two-branch structure filters by GroupNum first (seekable), leaving a small intermediate set on which the Substring is cheap. The schema-level fix (a persisted computed column on the boost digit, indexed) is documented in Section 11.1 of the analysis.

**Carry forward needed columns through the temp table.** A temp table that holds only a join key forces every downstream consumer to re-read the source for any additional column. `#GroupRxs` in v11 carries only `OrderId`, so the UPDATE and the per-Rx event INSERT each re-read `PriInternal` from `OeOrder`. Carrying `PriInternal` forward into `#GroupRxs` eliminates one full read of `OeOrder` for the UPDATE's read of the existing value. The pre-update `PriInternal` is in cache from the temp-table population; the post-update value is computed in the UPDATE and read once from `OeOrder` for the event log.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Set operators. [UNION (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-union-transact-sql) covers the principle behind the two-INSERT pattern that replaces the OR predicate.
- Predicates. [Predicates (SARGability reference)](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) describes the SARGability rules that the GroupNum predicates satisfy and the Substring predicate violates.
- Index support. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) covers the primary key on `#GroupRxs.OrderId` that supports the downstream joins.
- Table hints. [Table Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table) documents that NoLock has no effect on update targets, which is the basis for removing the misleading hint.

## Risk Note

The two-branch UNION ALL preserves row identity through the primary key on `#GroupRxs.OrderId`. The anti-join in the second INSERT prevents duplicates in the edge case where a GroupNum somehow matches both the equality and the LIKE pattern (which should not occur given the trailing-letter convention, but the anti-join is defensive). The Substring predicate is preserved in both branches; the row count it evaluates against is bounded by the GroupNum-filtered intermediate set, which is typically dozens. Removing the UPDATE NoLock hint is mechanical; the engine ignores NoLock on update targets, so the hint had no effect on locking behavior. Watch the first 24 hours for plan shape on the two `#GroupRxs` INSERTs (expected: two clean index seeks into GroupNum), row count parity between v11 and v12 on the same data state, and same PriInternal values written. Rollback is a pure DDL revert; redeploy v11 from `Original.sql`.
