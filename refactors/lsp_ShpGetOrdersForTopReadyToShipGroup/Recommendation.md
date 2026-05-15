# Refactor Recommendation: lsp_ShpGetOrdersForTopReadyToShipGroup

**Date:** 2026-05-07
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** Pilot. v25 captured at Tolleson on 2026-05-07 against a near-empty driver state; plan-shape evidence is conclusive, cost numbers require a representative-driver re-capture before fleet rollout. A Maxdop discrepancy between the test body and `Refactored.sql` must be resolved before deploy.

---

## Problem

v24 of `dbo.lsp_ShpGetOrdersForTopReadyToShipGroup` is responsible for roughly 950 billion logical reads per month across 13 of 14 reporting sites, on 33 million executions. The cross-MFC per-execution range is 37x (1,685 reads at Kent to 63,175 at NorthLake), with six sites returning multiple plan variants for the same `query_id` and BrooklynPark showing a 2x duration swing driven entirely by plan choice. The dominant cost driver is structural: the `STUFF((... FOR XML PATH('')), 1, 1, '')` correlated subquery is repeated identically across three UNION branches in Part 4, and a multi-vial split parent check is run as a correlated scalar subquery from five separate call sites (Parts 2 and 3, plus three times inside the Part 4 FOR XML PATH expressions).

The structural issues in v24:

- Triple-repeated FOR XML PATH (lines 184-195, 199-210, 213-225). One operator's worth of work runs three times because the same expression is pasted into three UNION branches. At 60K-80K reads per execution and three executions per call, this is the dominant cost driver.
- UNION at the top of Part 4 with mutually exclusive `GroupType` labels. The dedup sort cannot ever match a row across branches, so the work is wasted.
- Correlated scalar subquery for the multi-vial split parent check (lines 138 and 172). `(Select 1 ... ) = 1` against `OeOrder` filtered by status, evaluated once per outer row. The same subquery shape repeats in five places.
- Three separate driver temp tables (`#GroupsHavingAllReadyToShipRx`, `#GroupsHavingTimedOutInLocRx`, `#GroupsHavingTimedOutInToteRx`) each produced by a `SELECT INTO` with identical column shapes. The `GroupType` label already disambiguates the source.
- Legacy `Object_Id` temp-table existence checks (lines 85-86, 124-125, 158-159). Cosmetic, replaced by `Drop Table If Exists`.
- Mixed `With (NoLock)` capitalization. Cosmetic.

## Recommendation

Apply the v25 package. The semantic-risk-minimizing changes are intentional: the `Maxdop 1` hint on Part 1 is preserved, the where-clause logic of each branch is unchanged, and the FOR XML PATH expression is structurally untouched. What changes is the number of times each expensive operator runs.

1. Materialize the multi-vial split parent set once into `#MultiVialReadyParents (OrderId)` with a clustered index, near the top of the proc. Probe via `Exists` from Parts 2, 3, and the consolidated FOR XML PATH in Part 4. Replaces five correlated scalar subqueries with five cheap clustered-index seeks against a small temp table.
2. Consolidate the three driver `SELECT INTO` statements into a single CTE with three `UNION ALL` branches, materialized into `#AllQualifyingGroups`. The `GroupType` label guarantees no overlap, so `UNION ALL` is correct and the v24 dedup sort goes away.
3. Run the FOR XML PATH expression once against `#AllQualifyingGroups`. The expression itself is unchanged; only its outer driver changes. Three operators collapse to one.
4. Rewrite `(Select 1 ...) = 1` as `Exists (Select 1 ...)` everywhere.
5. Replace the three legacy `Object_Id` existence checks with `Drop Table If Exists`. Normalize `With (NoLock)` capitalization.
6. Explicit drops at the end of the proc for `#AllQualifyingGroups` and `#MultiVialReadyParents`.

The full v25 body is in `Refactored.sql`. The v24 body is in `Original.sql` for diff and rollback. The Tolleson capture confirmed the plan-shape prediction (operators 140 to 87, UDX nodes 6 to 2, TOP 3 to 1) but ran against a near-empty driver state, so the cost-side evidence is inconclusive. Section 9.5 of `Analysis.docx` lists the three clean paths to close out the comparison.

## First Principles

**Consolidate identical FOR XML PATH expressions into a single execution.** From `masterclass/FOR XML PATH Consolidation.md`:

> Each FOR XML PATH expression is itself a correlated subquery. The engine evaluates it once per outer row. When the same expression appears in three UNION branches, you're running three separate sets of correlated lookups against the same underlying tables.

The note's prescription maps exactly onto v25: materialize all branches into one temp table via `UNION ALL`, add a covering index on the correlation column, and run the FOR XML PATH once against the consolidated set. The biggest practical win is the cardinality estimate improvement; the optimizer plans the inner lookup once with the full combined cardinality in view, rather than three times against smaller driver sets that could each pick a different (and often worse) join order.

**Materialize-once for repeated correlated lookups.** From `masterclass/Correlated Subqueries to CTEs.md`:

> CTEs are not materialized in SQL Server (unlike temp tables). The optimizer may re-evaluate a CTE each time it's referenced. If you reference the same CTE in multiple places, consider materializing it into a `#temp` table instead. For a single reference, CTEs are ideal.

The multi-vial split parent set is referenced from five places. A CTE would be re-evaluated each time; a temp table with a clustered index on `OrderId` is built once and probed cheaply. v25 takes the temp-table path, which is the right call for five-call-site reuse on a small result.

**UNION versus UNION ALL when label columns prevent overlap.** `GroupType` carries one of three distinct values per branch in Part 4, which guarantees no row produced by branch A could ever match a row from branch B. The v24 UNION at the top forced a dedup sort that could never remove anything. v25 uses `UNION ALL` inside the materialization, which skips the dedup. The optimizer cannot infer the mutual exclusion on its own because it does not know that the labels are mutually exclusive, so the human change is required.

**`Exists` over `(Select 1 ...) = 1` for existence semantics.** The scalar comparison form does not short-circuit on the first matching row, raises a runtime error on multi-row results, and reads less clearly than `Exists`. The intent in every one of the five call sites is "does at least one matching row exist," which is the canonical case for `Exists`. v25 uses the canonical form consistently.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- CTE materialization into temp tables. [WITH common_table_expression (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) and [SELECT (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-transact-sql) describe the CTE-into-SELECT-INTO pattern used to populate `#AllQualifyingGroups`.
- UNION semantics. [Set Operators - UNION (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-union-transact-sql) covers the UNION-versus-UNION-ALL distinction that justifies replacing the v24 top-of-Part-4 UNION with `UNION ALL` inside the consolidation.
- Existence checks. [EXISTS (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql) documents the short-circuit semantics that v25's `Exists` probes against `#MultiVialReadyParents` rely on. The page is explicit that EXISTS returns TRUE on the first qualifying row.
- Indexed temp tables. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) covers the clustered index on `#MultiVialReadyParents.OrderId` that supports the five `Exists` probes.
- Hint placement. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) documents the `Maxdop 1` hint preserved on Part 1; the hint cannot legally sit inside a CTE branch, which is the discrepancy currently in `Refactored.sql` that Section 11.2.C of `Analysis.docx` resolves.

## Risk Note

Semantic risk on the `Exists` substitution is bounded; the v24 scalar form returned false when the subquery returned no rows and v25's `Exists` returns false in the same case. Plan-choice risk on the consolidated CTE is real but small: the three v24 driver SELECTs each had their own cardinality estimate, and v25 plans with a combined estimate. If one branch has a dramatically different cardinality from the combined average, the optimizer may choose a slightly suboptimal join order for the smaller branches. The `Maxdop 1` hint preservation matters; the test body had the hint commented out while `Refactored.sql` has it active, and Part 1 is the largest cost in the consolidated CTE (cost 30.09 of 34.83 in the v25 plan). Resolve before deploy. Watch Query Store average reads, average duration, and plan count for the proc over 24-48 hours post-deployment; plan count should stabilize at 1 or 2 once the new shape is cached, and the absolute drop in avg reads/exec at the top-5 sites (Bolingbrook, Orlando, Tolleson, Mechanicsville, NorthLake) is the headline signal. Rollback path: redeploy v24 from `Original.sql`. No data state is mutated.
