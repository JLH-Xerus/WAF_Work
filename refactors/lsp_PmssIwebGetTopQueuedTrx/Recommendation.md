# Refactor Recommendation: lsp_PmssIwebGetTopQueuedTrx

**Date:** 2026-05-07
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** v2 deployed across the MFC fleet. The recommended change is an index addition on `PmssCapturedXmlTrxs`, not a body change. The proc body in v2 stays as-is.

---

## Problem

`dbo.lsp_PmssIwebGetTopQueuedTrx` is the queue poller for the PMSS IA Web Services interface. The proc body is four lines of canonical SQL: `Select Top 1`, `Where TrxStateCode = 'Q' And Trx In (...)`, `Order By Id`. The cross-MFC capture from 2026-05-07 reports approximately 825 million executions per month, returning 278 trillion logical reads fleet-wide, with avg reads per execution ranging from 20 (West Jordan) to 1,392,722 (Orlando), a 70,000x spread. Plan instability is the dominant signature: same parameterless query text, multiple plan variants per site, 18x to 53x avg-reads spreads within a single site.

The structural issue is not in the proc body. There is no anti-pattern to fix. The query expresses exactly what an indexable plan would want:

- Two predicates, both equality (one direct, one `In` list).
- A clean `Order By Id` matching the natural seek order.
- A four-column SELECT list, no functions, no wrapping.
- `(NoLock)` is appropriate for a queue-poller read where the application rechecks on consumption.

The actual issue is index coverage on `PmssCapturedXmlTrxs`. The 70,000x avg-reads spread across sites and the plan instability on a parameterless query are the optimizer signaling, in plan-cache form, that no existing index covers the predicate and order-by cleanly. The optimizer is choosing different non-covering structures at different compilations because none of them are tight.

## Recommendation

Leave the proc body unchanged. Deploy a filtered nonclustered index that matches the query shape exactly. The body is canonical; adding a code change for its own sake dilutes review attention from the index, where the entire win lives.

1. Pull the index DDL for `PmssCapturedXmlTrxs` against Tolleson to confirm no near-miss index exists that should be reshaped rather than supplemented.
2. Create the filtered covering index:

```sql
Create NonClustered Index IX_PmssCapturedXmlTrxs_QueuedFillCancel
   On dbo.PmssCapturedXmlTrxs (Id)
   Include (TrxKey, Trx, TrxStream)
   Where TrxStateCode = 'Q'
     And Trx In ('fillRequest', 'cancelRxRequest', 'cancelOrderRequest')
   With (Fillfactor = 92, Online = On);
```

3. If `TrxStream` is a multi-KB LOB column, drop it from the INCLUDE list and accept a single key lookup per returned row.
4. Deploy first to Orlando (highest per-call cost). Capture before/after Query Store metrics over 24 hours. Roll forward to NorthLake, Bolingbrook, Mansfield, Tolleson, then the rest of the fleet.

`Refactored.sql` is byte-for-byte identical to `Original.sql` except for an expanded comment block in the header. There is no proc-body diff to review.

## First Principles

**The index shape is determined by the query shape.** From `masterclass/Index Key Columns vs Included Columns.md`:

> A column should be a key column when at least one of these is true:
>
> 1. The optimizer will use it to seek (an equality or range predicate is applied to it).
> 2. The optimizer will use it to satisfy an ORDER BY without a separate Sort operator.
> 3. The optimizer will use it to satisfy a GROUP BY in stream aggregate fashion.
> 4. It contributes to uniqueness (you're declaring this index unique on the combination of these columns).

The query has equality predicates on `TrxStateCode` and `Trx`, an `Order By Id`, and a four-column SELECT list. The filter form keys on `Id` (to satisfy the `Order By` from the leaf without a Sort), filters on `TrxStateCode = 'Q' And Trx In (...)` (so the index only contains queued rows for the three known Trx kinds), and includes `TrxKey, Trx, TrxStream` so the entire SELECT is satisfied from the leaf without a key lookup. The filter set is bounded by the queue depth, which is small because the IA Web Services interface dequeues continuously.

**Plan instability on a parameterless query is an indexing signal.** From `masterclass/Parameter Sniffing.md`:

> The telltale sign is plan instability in [[Query Store Triage]]: the same `query_id` appears with multiple `plan_id` values, and their performance metrics vary by 10x to 100x. Every time the plan gets evicted and recompiled (memory pressure, schema change, statistics update), a new first-caller value produces a different plan shape.

The sniffing mechanism is named for parameter values, but the broader story is that the optimizer compiles against whatever it knows at compile time. With no parameters, the things that vary across compilations are statistics, row-count estimates, and the optimizer's choice of which non-covering structure to use. Once a single covering index exists, plan choice collapses to one tight option and the variance disappears. This is also how the index stabilizes the proc, not just speeds it up.

**No proc-body change rescues a missing index.** An `Option (Fast 1)` hint on `Top 1 ... Order By Id` is redundant when the index supports the row-goal naturally and useless when no index does. The decision tree on body-level changes has the same conclusion at every branch: not the lever.

## MS Learn Citations

The techniques in this refactor anchor to the following Transact-SQL reference pages.

- Filtered index design. [Create Filtered Indexes](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-filtered-indexes) covers the syntax and the optimizer's expression-equivalence matching, which is the property that makes the filter dependency on the literal `Trx` IN list both a feature (the index is purpose-built) and a constraint (any future IN-list change would silently bypass it).
- Covering indexes with INCLUDE. [Create Indexes with Included Columns](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-indexes-with-included-columns) covers the trade-off on `TrxStream`: include for zero key lookups, exclude for narrower leaf pages.
- Index syntax. [CREATE INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql) is the canonical reference for the DDL.
- Top-N ordering. [TOP (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/top-transact-sql) and the [ORDER BY clause](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql) document the row-goal interaction that lets `Top 1 ... Order By Id` read the leaf in natural order and stop after one row.

## Risk Note

No proc-body risk. The body is unchanged. Index-deployment risk: writes to `PmssCapturedXmlTrxs` pay the maintenance cost of one additional index, but the filter narrows that cost to inserts at `TrxStateCode = 'Q'` (the typical case for new inserts) and updates that change `TrxStateCode` away from `'Q'` (the dequeue path). Filtered-index match risk: any future expansion of the `Trx` IN list silently bypasses the filter; if operations is concerned, alternative shape B from Section 11.1.B of the analysis (`(TrxStateCode, Trx, Id)` keyed, `(TrxKey, TrxStream)` included, no filter) tolerates query-text drift at a slightly larger footprint. First 24 hours: per-call reads at Orlando, NorthLake, Bolingbrook, Mansfield, Tolleson (expected drop from 1.4M and 27K to single digits), plan count per site (should collapse to 1), insert latency on the IA Web Services capture path, and index size growth. Rollback is `Drop Index`. No data state mutated.
