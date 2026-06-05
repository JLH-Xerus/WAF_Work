# Refactor Recommendation: lsp_RxFillingHistory_V4

**Date:** 2026-06-05
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** Draft, not deployed. v2 candidate awaiting Orlando captures.

---

## Problem

`dbo.lsp_RxFillingHistory_V4` builds the DSCSA filling history report: one row per Rx filled since the `CustomTask.DSCSAReport` watermark, with lot, expiration, serial, GTIN, and dispenser serial detail aggregated into delimited strings. The 2026-05-13 Query Store delta attributes 64.23 CPU hours across 2,879 executions (roughly 80 CPU-seconds per run) and 53 distinct plans to this proc, worst on `pwdorsymp-LST01` (Orlando). The cost concentrates in the multi-vial reconciliation and in repeated full-set deduplication.

The structural issues:

- Non-SARGable LIKE prefix joins. Four statements join the order tables to `#MultiVialOrders` with `O.OrderId Like M.OrderId + '%-[0-9][0-9]'`. The pattern is built per outer row, so each statement nested-loop scans `OeOrder` or `OeOrderHistory` once per multi-vial order.
- UNION where UNION ALL suffices. All four UNIONs feed consumers that dedupe again (MAX, DISTINCT, or split-and-dedupe), so the distinct sort over each full intermediate set is paid twice.
- Dead joins. `SecUser` (twice), `Pharmacy`, and `InvPool` are left-joined and `OePatientCust` and `OeGroup` inner-joined in all four detail branches; none contributes an output column.
- Aggregate, split, distinct, re-aggregate round trips. Serial and dispenser values are STRING_AGGed into a CSV, split back apart with STRING_SPLIT, deduped, and re-aggregated, twice over for the lot strings.
- Redundant final re-aggregation. `#Grouped` runs a GROUP BY and a second split round trip over inputs that are already one row per RxNum.
- Unreachable predicate. `Not (OrderStatus = 'Canceled' And ...)` sits behind `OrderStatus <> 'Canceled'`.

## Recommendation

Apply the v2 package as a single deployment unit. Every change is internal to the proc body: no schema change, no index change, and the output contract (column list, names including the `DisacardDate` spelling, dedupe and NULL semantics, ordering) is preserved.

1. Replace the four LIKE prefix joins with one scan per order table restricted to `OrderId Like '%-[0-9][0-9]'`, deriving the base id with `Left(OrderId, Len(OrderId) - 3)` and joining to `#MultiVialOrders` on equality.
2. Convert all four UNIONs to UNION ALL.
3. Remove the `SecUser`, `Pharmacy`, and `InvPool` joins; convert `OePatientCust` and `OeGroup` to `Exists` filters.
4. Dedupe rows with DISTINCT before aggregating, then run STRING_AGG once; delete the STRING_SPLIT round trips.
5. Build `#Grouped` with plain 1:1 joins and a single STRING_AGG over the lot strings; drop the GROUP BY.
6. Remove the unreachable Canceled/StatusReason predicate, write the effective-inner `OeLotcode` join as an inner join, replace the legacy `Object_Id` temp checks with `Drop Table If Exists`, and add a unique clustered index on `#LatestFilled (RxNum)`.

The full v2 body is in `Refactored.sql`. The v1 body is in `Original.sql` for diff and rollback.

## First Principles

**Non-SARGable predicates.** From `masterclass/Non-SARGable Predicates.md`:

> SARG stands for Search ARGument. A predicate is SARGable if the query optimizer can use an index seek to evaluate it. The column must appear naked on one side of the comparison, with no functions, no computations, and no wrapping.

The multi-vial joins are the worst form of the violation: the LIKE pattern is constructed from the outer row, so no index on `OrderId` can ever seek and the inner table is scanned per row. Deriving the base id once and joining on equality turns four scan loops into four single scans with hash joins.

**Dead join elimination.** From `masterclass/Dead Join Elimination.md`:

> An inner join with no referenced columns is an existence filter in disguise: it removes rows that have no match, and it multiplies rows when the join key is duplicated on the other side. Rewrite it as EXISTS. EXISTS preserves the filtering, cannot multiply rows, and lets the optimizer use a semi join, which stops probing after the first match.

Six tables leave the FROM clause of every detail branch with no change to the output: four removed outright, two converted to EXISTS. The reads, memory grant, and optimizer search space they consumed were pure overhead on every execution.

**String aggregate split round trip.** From `masterclass/String Aggregate Split Round Trip.md`:

> This is row deduplication performed in string space. The work the author wanted was "aggregate the distinct values." The work the engine performs is: build a varchar(max) LOB per group, feed each LOB through a table-valued function, sort the fragments for the DISTINCT, then aggregate again.

The proc runs this pattern four times, including a second pass over lot strings that are already unique within each Rx (the LotCode prefix guarantees it), making that entire round trip a no-op. Deduping rows first and aggregating once produces identical strings in a single pass.

**UNION pays for deduplication; only buy it once.** UNION is UNION ALL plus a distinct sort over the combined set. Every UNION in this proc feeds a consumer that dedupes again by MAX, DISTINCT, or string-space dedupe, so the sorts over the four largest intermediate sets buy nothing. UNION ALL drops them with no change to the final result.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- SARGability. [Predicates](https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates) covers predicate evaluation; the per-row pattern construction in the LIKE joins is the textbook case of a predicate the optimizer cannot turn into a seek.
- Set operators. [UNION (set operators)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-union-transact-sql) documents that UNION removes duplicates while UNION ALL does not, which is the entire cost difference the conversion recovers.
- Existence filters. [EXISTS (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql) specifies that EXISTS returns TRUE on the first qualifying row, the semi-join short-circuit the `OePatientCust`/`OeGroup` rewrite depends on.
- String aggregation. [STRING_AGG](https://learn.microsoft.com/en-us/sql/t-sql/functions/string-agg-transact-sql) and [STRING_SPLIT](https://learn.microsoft.com/en-us/sql/t-sql/functions/string-split-transact-sql) document the aggregate and the table-valued function; the refactor keeps one STRING_AGG per rollup and removes every STRING_SPLIT.

## Risk Note

The highest semantic risk is the multi-vial equality join: the v1 pattern's embedded `%` let a base id cross-match longer OrderIds sharing its prefix (`ABC` matching `ABC123-01`), and v2 tightens this to exact base-id matches. This is judged a latent bug fix, but it must be confirmed against a window with multi-vial orders present. The UNION ALL and EXISTS conversions rely on downstream dedupe and on parent-key uniqueness respectively; the identical-result-set check (row count plus row identities ordered by DateFilled, OrderId) is the gate for both. Watch report row counts and CPU per execution in Query Store on Orlando for the first 24 hours. Rollback is redeploying `Original.sql`; no schema objects change. Separately, verify the `CustomTask.DSCSAReport` watermark is advancing: if it is stale, the scan window grows without bound and no body change fixes that.
