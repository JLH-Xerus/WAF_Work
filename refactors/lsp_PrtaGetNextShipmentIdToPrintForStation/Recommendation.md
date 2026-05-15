# Refactor Recommendation: lsp_PrtaGetNextShipmentIdToPrintForStation

**Date:** 2026-05-08
**Companion analysis:** `Analysis.docx` in this folder.
**Deployment state:** Cataloged. v18 proposed by the MFC DBA team, awaiting iA review.

---

## Problem

v17 of `dbo.lsp_PrtaGetNextShipmentIdToPrintForStation` is the paperwork printer's next-shipment selector. The cross-list capture reports it on all four expense lists (Volume rank 9, Duration 8, Expense 20, Plan 9) with a 19,839x worst-case-to-average ratio, the largest plan-volatility signal in the rows 22-25 cohort. The procedure materializes nine temp tables in sequence and the plan shape is sensitive to the cardinality of every intermediate set. Volatility is driven by parameter sniffing on `@StationId` interacting with structural complexity.

The five structural issues:

- Parameter sniffing on `@StationId`. The per-station candidate population varies by orders of magnitude across workstations within a single MFC.
- Doubly-evaluated correlated scalar subquery against `OeOrderShipmentAssoc` joined to `OeOrderHistory` (non-canceled Rx count per shipment, at lines 237 and 278). Same shape, two distinct WHERE/HAVING clauses, both per-row.
- `STR(ShipmentId)` join key against `OeWorkflowStepInProgress.ObjectKey` (lines 235, 255). Per-row cast defeats any seek on an `ObjectKey` index.
- No indexes on the intermediate temp tables. Six of the nine participate in downstream joins as heaps.
- Nine separate legacy `If Object_Id ... Drop Table` blocks. Modern equivalent is a single consolidated `Drop Table If Exists`.

The per-statement `Option (MaxDop 1)` hint is preserved unchanged. The hint is a documented operational workaround for parallel-thread starvation at a fielded system, and Section 11.1 of the analysis carries the follow-up to re-evaluate it.

## Recommendation

Apply v18 as a single deployment unit. No schema change. No base-table index change. The package handles sniffing, correlation, the cast pattern, the temp-table heap problem, and the cleanup idiom together.

1. Assign `@LocalStationId = @StationId` and reference the local in every downstream filter.
2. Pre-materialize the in-progress shipment IDs once: `Select Distinct Try_Cast(ObjectKey As Int) Into #InProgressShipments From OeWorkflowStepInProgress Where Try_Cast(ObjectKey As Int) Is Not Null`. Add `IX_InProg_ShipmentId` after population.
3. Pre-materialize the non-canceled count once into `#ShipmentNonCanceledCount` restricted to the candidate ShipmentIds. Add `IX_SNC_ShipmentId` after population.
4. Replace both correlated subqueries with joins to `#ShipmentNonCanceledCount`.
5. Replace the `STR(ShipmentId) = ObjectKey` join with a typed equality against `#InProgressShipments.ShipmentIdAsKey`.
6. Add indexes to the other temp tables that participate in downstream joins.
7. Consolidate the nine legacy `Object_Id` drops into one `Drop Table If Exists`.
8. Add `Option (Recompile)` to the parameter-sniffing-prone candidate-building queries. Preserve `Option (MaxDop 1)` on every statement.

The full v18 body is in `Refactored.sql`. The v17 body is in `Original.sql` for diff and rollback.

## First Principles

**Local variables stabilize plan choice across a sniffing-prone parameter.** From `masterclass/Parameter Sniffing.md`:

> Local variables give you plan stability at the cost of plan optimality. You will never get the best possible plan for a specific value, but you will also never get a catastrophically bad plan. For high-frequency OLTP procedures, stability almost always wins.

`@StationId` selectivity varies by orders of magnitude across workstations. `@LocalStationId` gives the optimizer a density-based plan that lands acceptably across the parameter spread. `Option (Recompile)` on the candidate-building queries provides the additional safety net where the local-variable plan alone produces a poor estimate for the candidate-set size.

**Correlated subqueries become pre-materialized aggregates.** From `masterclass/Correlated Subqueries to CTEs.md`:

> A correlated subquery references columns from the outer query, which means the database engine must evaluate the subquery once per row of the outer result set. The subquery is logically correlated to each outer row.

The non-canceled count appears in two places in v17, both as the same correlated subquery shape. v18 computes the count once per candidate ShipmentId into `#ShipmentNonCanceledCount`, restricted to the candidate set that survives the upstream filters. Both downstream consumers join on `ShipmentId` and read `NonCanceledCount` directly. The cost moves from "per-row, twice" to "per-set, once."

**Conversions on a join key belong on the source side, not on each pairing.** From `masterclass/Non-SARGable Predicates.md`:

> A predicate is SARGable if the query optimizer can use an index seek to evaluate it. The column must appear naked on one side of the comparison, with no functions, no computations, and no wrapping.

`WFS.ObjectKey = STR(t.ShipmentId)` is the inverse-direction form of the same problem: the cast is on the outer-row side rather than the column side, but the optimizer still materializes the cast per pairing and cannot match it against any index keyed on `ObjectKey`. Pre-materializing `Try_Cast(ObjectKey As Int) As ShipmentIdAsKey` into `#InProgressShipments` once and joining on the typed key makes the downstream left-join a clean equality.

**Indexing temp tables that get joined is among the cheapest wins.** Six of v17's temp tables participate in joins as heaps. v18 adds an explicit index after population on each one keyed at the natural join column. Statistics on the temp table after the indexed populate give the optimizer real cardinality and let it pick hash or merge joins where the data shape supports them rather than nested loops everywhere.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Plan stability hints. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) and [Recompile a Stored Procedure](https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/recompile-a-stored-procedure) cover the `Option (Recompile)` and `Option (MaxDop 1)` semantics.
- Temp-table indexing. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) is the canonical reference for the index additions on the intermediate temp tables.
- Predicate SARGability. [Predicates](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) describes the seek-versus-scan distinction that the `STR(ShipmentId)` cast defeats in v17.
- Aggregation. [Aggregate Functions](https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql) and the [WITH common_table_expression (CTE)](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) reference cover the non-canceled-count materialization.

## Risk Note

The `Try_Cast` filter in `#InProgressShipments` is non-throwing: malformed `ObjectKey` values return NULL and the `Is Not Null` guard drops them. The count-materialization scope (`OSA.ShipmentId In (#TopShipmentIdInBin Union #CvyRxsArrivedForShipmentId)`) preserves v17's semantics when either source is empty for a given call. The `Option (MaxDop 1)` hint is preserved on every statement to keep the runtime characteristic the workaround addresses unchanged; revisiting it is the Section 11.1 follow-up. First 24 hours: plan variant count per site, max-vs-avg ratio per site, reads on `OeOrderShipmentAssoc + OeOrderHistory` (expected to drop from "twice per-row" to "once per-set"), and final ShipmentId parity with v17. Rollback is `Alter Procedure` from `Original.sql`. No schema or base-table index change to revert.
