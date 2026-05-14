# Refactor Recommendation: lsp_OrdGetRxNumsMedsFromPo

**Date:** 2026-05-08
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Cataloged. v23 proposed by the MFC DBA team, awaiting iA review.

---

## Problem

v22 of `dbo.lsp_OrdGetRxNumsMedsFromPo` is the label-data generator's PO-to-Rxs lookup (called from `OrdrGetLabelData`). The cross-list capture reports it on two of four expense lists (Duration #24, Plan #21) with a 1,720x worst-case-to-average ratio and "algorithmic / plan rewrite candidate" as the dominant heuristic. The ratio is consistent with PoNum-selectivity sniffing: some PO numbers map to a single Rx, others to a multi-vial parent set, and a single cached plan cannot serve both selectivities.

The structural issues:

- Parameter sniffing on `@PoNum`. The PoNum filter selectivity varies materially across calls.
- Constructed parent-OrderId derivation in the final SELECT's secondary-data join (lines 108-114). The join condition is `((O.OrderId Like '%-[0-9][0-9]' And O2.OrderId = Left(O.OrderId, Len(O.OrderId) - 3)) Or (O.OrderId Not Like '%-[0-9][0-9]' And O2.OrderId = O.OrderId))`. The Or-with-function-on-column shape defeats clean index seeks on `OeOrderSecondaryData.OrderId`.
- Repeated `OrderId In (Select OrderId From #tmpPoOrdrId)` subqueries. The temp table is small but the IN form defeats clean join behavior, and the temp table is unindexed.
- Mixed-convention temp-table cleanup. Three legacy `If Object_Id ... Drop Table` blocks at entry and three modern `Drop Table If Exists` at exit.
- No indexes on the three temp tables.

## Recommendation

Apply v23 as proposed. No schema change. No base-table index change. The package handles sniffing, the constructed-key join, the IN-to-join rewrite, and the temp-table indexing in a single deployment unit.

1. Assign `@LocalPoNum = @PoNum` and reference the local in the PO-to-OrderId materialization.
2. Index `#tmpPoOrdrId(OrderId)` after population.
3. Compute `ParentOrderId` once per row at `#tmpOrders` materialization time: `Case When O.OrderId Like '%-[0-9][0-9]' Then Left(O.OrderId, Len(O.OrderId) - 3) Else O.OrderId End`. Apply the same derivation to the OeOrder and OeOrderHistory branches that feed `#tmpOrders`.
4. Index `#tmpOrders(OrderId, HistoryDtTm) Include (RxNum, Medication, PatCustId, GroupNum, ParentOrderId)` after population.
5. Replace the three `OrderId In (...)` subqueries with direct inner joins to `#tmpPoOrdrId`.
6. Replace the constructed-parent Or-clause in the secondary-data join with a clean equality on `O2.OrderId = O.ParentOrderId`.
7. Add `Option (Recompile)` on the three parameter-sensitive INSERTs.
8. Consolidate the entry/exit cleanup into a single `Drop Table If Exists` at the top of the procedure.

The full v23 body is in `Refactored.sql`. The v22 body is in `Original.sql` for diff and rollback.

## First Principles

**Functions on join columns prevent index seeks.** From `masterclass/Non-SARGable Predicates.md`:

> A predicate is SARGable if the query optimizer can use an index seek to evaluate it. The column must appear naked on one side of the comparison, with no functions, no computations, and no wrapping.

The v22 secondary-data join wraps the join condition in an Or-clause where one branch uses `Left(O.OrderId, Len(O.OrderId) - 3)` and the other compares OrderId directly. Even though the optimizer can sometimes peel apart Or-branches, the function call on the column side forces the optimizer to materialize the cast per pairing. v23 computes the derivation once per row at `#tmpOrders` materialization time into the `ParentOrderId` column. The secondary-data join becomes `O2.OrderId = O.ParentOrderId`, a clean equality that matches a clustered or nonclustered index on `OeOrderSecondaryData.OrderId` directly.

**Local variables stabilize plan choice across PoNum selectivity.** From `masterclass/Parameter Sniffing.md`:

> Local variables give you plan stability at the cost of plan optimality. You will never get the best possible plan for a specific value, but you will also never get a catastrophically bad plan. For high-frequency OLTP procedures, stability almost always wins.

`@LocalPoNum` gives the optimizer a density-based plan rather than a histogram-based plan tied to whatever PoNum the first caller passed. `Option (Recompile)` on the three INSERTs pays the per-call compile cost in exchange for an accurate selectivity estimate against the actual `#tmpPoOrdrId` size.

**Temp-table indexes give the optimizer real cardinality and seek-capable joins.** From `masterclass/Table Variables vs Temp Tables.md`:

> The "insert then index" pattern is a best practice. Building the index on the complete dataset is a single efficient sort, versus maintaining the b-tree during every insert.

`#tmpPoOrdrId` is small (typically a handful of OrderIds per PO) but it participates in three downstream operations in v22. v23 indexes it on `OrderId` after population and the three IN-subqueries become direct inner joins on the indexed key. `#tmpOrders` carries a composite covering index keyed on `(OrderId, HistoryDtTm)` with the consumed columns in the INCLUDE list, eliminating key lookups during the final SELECT.

**Direct joins beat IN-subqueries on small indexed candidate sets.** The three `OrderId In (Select OrderId From #tmpPoOrdrId)` subqueries are functionally equivalent to inner joins on an indexed key. The join form gives the optimizer more flexibility on join order and method, and produces a cleaner plan.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Predicate SARGability. [Predicates](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) describes the seek-versus-scan distinction that the constructed-key Or-clause join defeats in v22.
- Temp-table indexing with INCLUDE. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) and [Create Indexes with Included Columns](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-indexes-with-included-columns) cover the `#tmpOrders` covering-index pattern.
- Plan hints. [Query Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query) and [Recompile a Stored Procedure](https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/recompile-a-stored-procedure) cover the `Option (Recompile)` semantics on the parameter-sensitive INSERTs.
- Join shape. [FROM clause plus JOIN, APPLY, PIVOT](https://learn.microsoft.com/en-us/sql/t-sql/queries/from-transact-sql) covers the inner-join shape that replaces the v22 IN-subqueries.

## Risk Note

The constructed-parent derivation in v23 is logically equivalent to the v22 Or-clause: a multi-vial portion Rx has the form `parent-NN` and uses `Left(OrderId, Len(OrderId) - 3)`; a single-vial Rx uses OrderId directly. The Case expression produces the same value the Or-clause expected on each side of the disjunction. The `Select Distinct` in the final SELECT is preserved as a safety net for duplicate rows arising from the Left Joins into `OeGroupUserDef`; Section 11.3 of the analysis carries the recommendation to confirm whether `OeGroupUserDef` is unique per GroupNum, in which case the Distinct can drop. First 24 hours: plan variant count per site (expected drop from many to one), max-vs-avg ratio (expected to fall substantially from 1,720x), reads on `OeOrderSecondaryData` (expected: indexed seek on ParentOrderId), and Distinct row-count parity with v22. Rollback is `Alter Procedure` from `Original.sql`.
