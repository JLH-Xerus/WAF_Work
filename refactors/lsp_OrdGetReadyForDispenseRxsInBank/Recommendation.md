# Refactor Recommendation: lsp_OrdGetReadyForDispenseRxsInBank

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. Refactored.sql is a proposed v21 awaiting iA review.

---

## Problem

v20 returns the list of Counted auto-fill Rxs ready for dispense in a cabinet bank, ordered by a derived priority that accounts for the highest-priority same-product upstream queued Rx. The procedure is on the optimization list because the cross-list capture reports it on three of four expense lists (Volume #19, Duration #13, Expense #11) with "moderate read volume" and "review indexing and join strategy" as the dominant heuristics. The Row 21 analysis (`lsp_RbtDetermineNextVialToProcess`) identifies this procedure as the dominant cost driver for that procedure; refactoring here delivers compounding benefit to both rows.

- Two near-identical reads of `OeOrder` for the upstream candidate set. The reads differ only in the `AddrBank` value (`@AddrBank` versus `'-'`); the predicate shape is otherwise identical.
- Two-step min-then-join pattern for the top upstream Rx per product. The first temp table computes the min `PriInternal` per product; the second re-joins to find the OrderId at that min. Classic candidate for a `Row_Number()` window function.
- Two intermediate temp tables (`#CountedRxsWithTopUpstreamRxData01` and `#CountedRxsWithTopUpstreamRxData02`) that derive related expressions over the same base join. Collapse.
- Seven scattered `If Object_Id ... Drop Table` legacy cleanup blocks.
- No local-variable indirection for the input parameters. Program convention.
- No indexes on the intermediate temp tables that participate in downstream joins.

## Recommendation

Apply the v21 package. The dominant lever is the consolidated upstream-candidate scan; the structural cleanup compounds at scale.

1. Declare locals for the input parameters at the top of the procedure.
2. Combine the two `OeOrder` upstream-candidate reads into one scan with `O.AddrBank In (@LocalAddrBank, '-')`. Filter to candidate products via `Exists` against `#CountedRxs`.
3. Replace the two-step min-then-join pattern with a `Row_Number() Over (Partition By ProductId Order By PriInternal Asc, OrderId Asc)` CTE; project the rank-1 row into `#TopUpstreamPerProduct`.
4. Collapse `#CountedRxsWithTopUpstreamRxData01` and `#CountedRxsWithTopUpstreamRxData02` into a single SELECT via a `Cross Apply` that names the adjusted-upstream value once and references it in the projection and the two derived expressions.
5. Add an explicit index to each remaining temp table sized for its downstream join.
6. Consolidate the seven legacy `Object_Id` blocks into a single `Drop Table If Exists` tail.
7. Apply `Option (Recompile)` to the combined upstream scan so the optimizer sees the actual `AddrBank` selectivity at compile time.

The full v21 body is in `Refactored.sql`. The v20 body is in `Original.sql` for diff and rollback.

## First Principles

**Consolidate near-identical scans.** From `masterclass/LEFT JOIN OR Anti-Pattern.md`:

> Each branch uses INNER JOINs, so the optimizer can push predicates into the join tree and use index seeks.

The v20 two-read pattern is the inverse problem: two scans that should be one. The combined `AddrBank In (@LocalAddrBank, '-')` predicate is SARGable against an index keyed by `AddrBank`; the optimizer can produce a seek-or-scan of the index for each of the two values and short-circuit the empty side. The `Exists` clause against `#CountedRxs` restricts the scan footprint to candidate products.

**Window function replaces find-row-with-min-value-per-group.** From `masterclass/Correlated Subqueries to CTEs.md`:

> A correlated subquery references columns from the outer query, which means the database engine must evaluate the subquery once per row of the outer result set. The subquery is logically correlated to each outer row.

> The key insight: the CTE scans `CfStoreDeliveryCourierCutOff` once, groups by pharmacy, and produces a small result set.

The two-temp-table v20 pattern computes `Min(PriInternal)` per product, then re-joins to find the OrderId at that min. `Row_Number() Over (Partition By ProductId Order By PriInternal Asc, OrderId Asc) = 1` produces both values in one pass. The tie-breaker on `OrderId` matches the v20 `Min(OrderId)` semantics. One scan over `#UpstreamCandidates` instead of one scan plus one re-join.

**Cross Apply to name a derived expression once.** v20 stages two temp tables (`#CountedRxsWithTopUpstreamRxData01` and `#CountedRxsWithTopUpstreamRxData02`) where each computes a related expression over the same base join. The `Cross Apply` form produces the intermediate `AdjustedUpstream` value once and names it for reference in the column projection and the two derived expressions (`ActOnAsPriInternal` and `FillToMakeWayForUpstreamRxOrderId`). The v20 form had to repeat the long substring expression three times; v21 expresses it once.

**Indexed temp tables for downstream joins.** From `masterclass/Index Key Columns vs Included Columns.md`:

> The b-tree only needs to sort by the columns the optimizer will actually use to navigate. Every column you add to the key list makes the tree wider at every level, which means more pages, more memory, and slower writes. Included columns sit at the leaf only, so they cost storage but not navigation overhead.

Each temp table that participates in a downstream join gets an explicit index sized for that join. `#TopUpstreamPerProduct` is keyed on `ProductId` with `PriInternal` and `OrderId` Included so the Left Join from `#CountedRxs` is a leaf-only probe.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Window aggregation. [OVER clause (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-over-clause-transact-sql) documents the `Row_Number() Over (Partition By ...)` shape that collapses the min-then-join pattern.
- CTE and APPLY. [WITH common_table_expression (CTE)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) documents the `Ranked` CTE; [FROM clause plus JOIN, APPLY, PIVOT](https://learn.microsoft.com/en-us/sql/t-sql/queries/from-transact-sql) covers the `Cross Apply` form.
- Combined SARGable predicate. [Predicates (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) covers the SARGability of `AddrBank In (@LocalAddrBank, '-')`.
- Indexed temp tables. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) and [Create Indexes with Included Columns](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-indexes-with-included-columns) document the key-versus-include split.

## Risk Note

The combined `AddrBank` scan changes the `OeOrder` access pattern. The `In` predicate is SARGable against an index keyed by `AddrBank`; the optimizer should produce a seek-or-scan for each value and short-circuit empty ranges. The `Row_Number()` form changes the cardinality estimate the optimizer sees for the upstream-top-per-product set; the set is bounded by the number of distinct products in `#CountedRxs`, which is typically small. The `Cross Apply` form is equivalent to the v20 two-temp-table form by construction. In the first 24 hours watch `OeOrder` reads on the upstream-candidate scan (expected to drop materially), temp-table count (expected to drop from 5 to 4), result-set parity with v20, and the compounding effect on Row 21's `INSERT ... EXEC` cost. Rollback path: redeploy v20 from `Original.sql`. No schema change, no index change.
