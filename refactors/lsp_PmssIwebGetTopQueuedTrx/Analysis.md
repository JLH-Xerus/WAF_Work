# lsp_PmssIwebGetTopQueuedTrx: Refactor Analysis (v2, no proc-body change)

**Date:** 2026-05-07
**Tracking sheet row:** 20 (Priority P3, status Not Started)

This analysis is unusual. The proc body is well-written and v2 is the recommended deployment. The win is an index addition on `PmssCapturedXmlTrxs`, not a code change. Section 6 contains no proc-body diff; Section 11.1 carries the heavy lifting. Section 9's verdict states this directly.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_PmssIwebGetTopQueuedTrx`

**Purpose in one line:** Return the oldest queued PMSS captured XML transaction whose Trx kind is one of Fill, Cancel-Rx, or Cancel-Order. Called by the PMSS IA Web Services interface as the queue poller.

**Tables touched:**

- `PmssCapturedXmlTrxs` (the only table; 9 columns per the tracking sheet entry, returns `Id`, `TrxKey`, `Trx`, `TrxStream`).

**Indexes used:** to be confirmed against the Tolleson plan once captured. The Query Store evidence in Section 3.1 shows three distinct plan variants at Tolleson with avg reads per execution ranging from 19,551 to 366,120, an 18x spread on a parameterless single-statement query. That spread is the primary signal that no single index covers the predicate cleanly. The optimizer is choosing different plans across compilations, none of them tight.

**Index DDL gap.** The pilot `IndexExtract.xlsx` covers only the `OeOrder` family; `PmssCapturedXmlTrxs` is not included. This is the gating dependency for finalizing Section 11.1's index recommendation. Run `extract_index_ddl.sql` against Tolleson with `@Tables` set to `('PmssCapturedXmlTrxs')` to fill this in. Until that data is in hand, the recommendation in Section 11.1 is shaped by the query body (which is unambiguous about the seek shape it wants) and will need a final pass to confirm there is not already a redundant or near-miss index that should be replaced rather than supplemented.

**Callers:** the PMSS IA Web Services interface, calling as parameterless ad-hoc T-SQL. Query Store at most sites captures the executions under `objectname = '<ad-hoc>'` rather than under the proc name, even though the text is byte-for-byte identical to the proc body (minus the `Top 1` keyword in the captured plan-cache form). This is documented further in Section 2.

---

## 2. Overview of Performance

The cross-MFC view from the 2026-05-07 Query Store capture shows this is the highest-volume statement in the fleet by execution count, by a wide margin. **Roughly 825 million executions per month, returning 278 trillion logical reads, fleet-wide.** That is more than 25 times the call volume and roughly 150 times the read load of the Row 15 pilot, and it is happening on a query that should cost single-digit reads per call.

The tracking sheet's headline "6.8M to 9M execs/month" was clearly drawn from a single-site, single-snapshot view. The cross-MFC reality is closer to 825M/month. The detailed per-site breakdown is in Section 3.1.

The dominant cost driver is index coverage, not query shape. The query is canonical: `Select Top 1 ... From T Where Col1 = 'Q' And Col2 In (...) Order By Id`. With the right index, this is one or two reads per execution. With the current indexes (PK on `Id` plus whatever else exists, to be confirmed), the optimizer is forced into one of several plan shapes that all do far more work than the result requires. The signature is plan instability: same query text, same `query_id`, multiple plan variants per site, with avg reads per execution ranging from 20 (West Jordan) to 1,392,722 (Orlando), a 70,000x spread. A well-indexed plan would not show that variance.

The polling-frequency angle is also worth flagging here, even though it is out of scope for v2. The IA Web Services interface is calling this query on the order of 30 to 50 times per second per site at the busiest MFCs (Bolingbrook, Canal, Liberty all show 100M+ executions in 30 days). That is a tight polling loop. An index reduces per-call cost; a Service Broker queue or signal-based handoff would eliminate the polling. Section 11.3 carries that recommendation.

---

## 3. Evidence of Original (v2)

### 3.1 Query Store, cross-MFC view

**Source:** `diagnostics/querystore_outputs/QueryStore_20260507.xlsx`. Top 50 offenders per site, 30-day lookback, captured 2026-05-07. One tab per site.

**Aggregation method:** the proc executes as ad-hoc text from the IA Web Services interface, so `objectname` is `<ad-hoc>` rather than `lsp_PmssIwebGetTopQueuedTrx`. Rows are matched by query text containing `PmssCapturedXmlTrxs`, the literal `'fillRequest'`, and the predicate on `TrxStateCode`. Multiple rows with the same `query_id` indicate plan instability.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------------|----------------|
| Orlando | 1 | 1 | 81,339,798 | 113,283,697,761,613 | 1,392,722 | 190,568 | 1,031,068,229,920 |
| NorthLake | 2 | 1 | 80,473,079 | 72,474,088,033,007 | 900,600 | 130,157 to 132,063 | 670,049,210,168 |
| Bolingbrook | 2 | 1 | 78,942,671 | 56,443,083,889,520 | 714,988 | 87,813 to 674,510 | 488,420,381,221 |
| Mansfield | 2 | 1 | 92,730,066 | 31,521,133,676,597 | 339,924 | 29,266 to 1,797,589 | 252,471,706,419 |
| Tolleson | 3 | 1 | 102,682,975 | 2,777,436,589,621 | 27,049 | 3,529 to 74,365 | 25,620,330,432 |
| Canal | 1 | 1 | 120,395,621 | 928,903,092,466 | 7,715 | 1,020 | 7,945,402,914 |
| Liberty | 1 | 1 | 110,056,280 | 736,225,912,623 | 6,690 | 1,044 | 6,533,128,000 |
| BrooklynPark | 1 | 1 | 75,081,058 | 294,199,747,602 | 3,918 | 500 | 1,928,253,127 |
| West Jordan | 1 | 1 | 82,914,086 | 1,666,124,594 | 20 | 0.3 | 13,232,332 |
| Denver | | | | | | | |
| Indy | | | | | | | |
| Kent | | | | | | | |
| Memphis | | | | | | | |
| Mechanicsville | | | | | | | |

**Roll-up across the 9 reporting sites:** 824,615,634 executions, **278,460,434,827,643 total logical reads** (337,685 avg reads/exec), 1,057,825,163,742 total CPU ms (about 12,243 days of CPU per month). Five sites (Denver, Indy, Kent, Memphis, Mechanicsville) did not surface this query in the top 50. At those sites the per-site top-50 cut sits between 8B and 17B reads, so the proc is running at sub-top-50 cost there. Worth pulling a focused capture on those sites to confirm whether they have a better index already in place or simply a smaller `PmssCapturedXmlTrxs` population.

**Observations:**

- **Plan instability is the dominant signature.** Tolleson has three plan variants with avg reads ranging 19,551 to 366,120 (an 18x spread). Mansfield has two variants spanning 151,461 to 8,029,798 (a 53x spread). Bolingbrook spans 625,970 to 5,305,120 (an 8x spread). On a parameterless query the only thing that varies between compilations is the optimizer's view of the table at compile time: row counts, statistics, and whatever predicates the optimizer decides to push into a seek versus evaluate as a residual. None of these would matter if there were a single covering index that exactly matched the query shape.
- **Per-site avg reads scales with table size and plan choice, not call volume.** Orlando at 1.39M reads/exec is doing what looks like a full scan plus a sort to satisfy `Order By Id`. West Jordan at 20 reads/exec is doing what every site should do: a tight seek. The fact that the same statement cost varies by 70,000x across sites is the strongest possible evidence that this is an indexing problem rather than a query problem.
- **The five high-cost sites carry 99.6% of the reads.** Orlando, NorthLake, Bolingbrook, Mansfield, and Tolleson together account for 276.5T of the 278.5T total. An index deployed to those five sites first would capture nearly the entire fleet-wide win.
- **Five sites have the proc below the top-50 cut.** Denver, Indy, Kent, Memphis, and Mechanicsville did not surface the proc in their top 50 offenders. The cut at those sites ranges from 8B to 17B reads, so the proc is already costing less than that at those sites. Likely they either have a more selective index in place or a smaller queue table. Pull a targeted Query Store capture for the proc at those sites to confirm.
- **Avg duration at Orlando is 190 seconds per call.** That number is striking enough to be worth restating. A queue-poller statement that the application is calling tens of times per second is taking on average more than three minutes to return per call at one site. If that number is real and not an aggregation artifact, the IA Web Services interface is almost certainly running with massive concurrent in-flight pollers competing for the same scan, which compounds the problem because each call holds the (NoLock) read for longer.

### 3.2 SET STATISTICS IO, TIME from a representative MFC

I capture STATS IO/TIME from Tolleson (or another representative high-cost MFC such as Orlando or NorthLake) running the v2 statement, ideally back to back against three states:

- A queue with at least one `Q`-state Fill request in `Trx`. Returns one row.
- A queue with no `Q`-state requests. Returns zero rows.
- A queue with the row at the front of the order being a non-matching `Trx` value (something other than the three in the IN list), behind which there are matching rows. Returns one row, but the seek has to skip over non-matching rows.

The third state is the one that exposes whether the chosen index lets the engine evaluate the `Trx In (...)` predicate as a seek predicate or a residual. With a filtered index on `TrxStateCode = 'Q'` keyed on `(Trx, Id)` or `(Id)` plus an INCLUDE for `Trx`, the engine will short-circuit; without it, the engine has to read rows that do not match `Trx` and discard them.

```
(STATS IO + STATS TIME output for v2 goes here, ideally one block per state.
 Capture both warm-cache and cold-cache; report warm-cache numbers in Section 9.)
```

---

## 4. Issue Identification (v2)

The proc has no anti-patterns. The body is four lines of canonical SQL: `Select Top 1`, two predicates, `Order By Id`. The line numbers below reference `Original.sql`.

### 4.1 No issue: query shape is canonical (lines 32 to 40)

```sql
Select Top 1
  Id, TrxKey, Trx, TrxStream
From
  PmssCapturedXmlTrxs With (NoLock)
Where
  TrxStateCode = 'Q' And
  Trx In ('fillRequest','cancelRxRequest','cancelOrderRequest')
Order By
  Id
```

This is what I would write if I were writing this proc from scratch. `Top 1` plus `Order By Id` gives the optimizer a clear row goal. The two predicates are both simple equality (one direct, one `In` list). `(NoLock)` is appropriate for a queue-poller read where the application is going to recheck on consumption. The predicate columns are not wrapped in functions. There is nothing to refactor in the body.

### 4.2 The actual issue: index coverage on `PmssCapturedXmlTrxs` is unknown and almost certainly missing

The Query Store evidence in Section 3.1 demonstrates that no single existing index covers this query. The 70,000x spread in avg reads per execution across sites is the optimizer signaling, in plan-cache form, that it cannot find a tight plan. Section 11.1 carries the index recommendation. There is no line number in the proc to point at; the issue is a physical-design gap, not a code defect.

---

## 5. First Principles

### 5.1 Index Key Columns vs Included Columns

The central principle for this proc. The query shape exactly determines the optimal index shape. The predicates are `TrxStateCode = 'Q' And Trx In (...)`. The order-by is `Id`. The select list is `Id, TrxKey, Trx, TrxStream`. A nonclustered index keyed to support the seek and the order-by, including the rest of the columns at the leaf, lets the engine satisfy the entire query without a key lookup.

There are three viable shapes, in increasing order of specificity.

1. A non-filtered nonclustered index keyed on `(TrxStateCode, Trx, Id)`, including `(TrxKey, TrxStream)`. The engine seeks to the `'Q'` partition, then within that to the three matching `Trx` values, and reads `Id` ascending until it has one row. Works for any current or future `TrxStateCode` value.
2. A filtered nonclustered index keyed on `(Trx, Id)`, including `(TrxKey, TrxStream)`, with `Where TrxStateCode = 'Q'`. The filter eliminates the `Q`-state column from the key, making the index narrower and the leaf pages denser. Only useful if `Q` is consistently a small fraction of the table.
3. A filtered nonclustered index keyed on `(Id)` only, including `(Trx, TrxKey, TrxStream)`, with `Where TrxStateCode = 'Q' And Trx In ('fillRequest','cancelRxRequest','cancelOrderRequest')`. Smallest possible index. Only matched when the query's predicate is exactly the same as the filter, so any new `Trx` value added to the IN list breaks the match.

See [[Index Key Columns vs Included Columns]] for the full pattern. The choice between these three is driven by what the row-count distribution looks like across `TrxStateCode` and `Trx` values, which is the data point I need from the index DDL extraction. Section 11.1 documents the recommended choice and the fallback options.

The `TrxStream` column is the one to examine carefully. The tracking sheet entry calls out that it may be a large column (XML or `varchar(max)`). If it is, including it in the leaf bloats the index on every queued row, even though the proc only ever needs to read it for the one row at the front of the queue. There are two responses to that constraint: drop `TrxStream` from the INCLUDE list and accept a cheap key lookup back to the clustered index for that one column, or include it and pay the leaf-page cost. The recommendation in Section 11.1 includes `TrxStream` because at most one row will be returned per call and the pages stay cache-resident, but if `TrxStream` is multi-MB per row and the queue routinely holds thousands of `Q`-state rows, the trade-off flips.

### 5.2 Plan Instability on a Parameterless Query is an Indexing Signal

[[Parameter Sniffing]] is named for parameter values, but the underlying mechanism is broader: the optimizer compiles a plan based on whatever it knows at compile time, and "what it knows" includes more than just sniffed parameters. On a parameterless query like this one, the things that vary across compilations are statistics, row-count estimates, and the optimizer's choice of which index to use when no single index covers the query.

The 18x to 70,000x avg-reads spreads in Section 3.1 are not parameter sniffing in the strict sense. They are the optimizer choosing different non-covering indexes (or scans) at different compilation moments because no covering index exists. Once the index in Section 11.1 is in place, plan choice collapses to the one tight option and the variance disappears. This is also the mechanism by which the index addition stabilizes the proc, not just speeds it up.

---

## 6. Refactor (commented)

`Refactored.sql` is byte-for-byte identical to `Original.sql` except for an expanded comment block in the header documenting why no body change is being made. There is no code diff to walk through.

The reasoning is summarized in the file header and detailed in Section 9. The win for this proc lives at the index layer, not the proc layer. The query body already expresses exactly what a good index would let the optimizer compile efficiently. Adding code change for its own sake would dilute review attention from where it actually matters, which is the index recommendation in Section 11.1.

The only proc-level change considered and rejected was an `Option (Fast 1)` hint on the SELECT. The rationale for rejection: `Fast 1` tells the optimizer to prefer a plan that returns the first row quickly even at the cost of higher total cost. With a good index in place, the optimizer already gets that for free because of the `Top 1` plus `Order By Id` shape. With a bad index in place (the current state), `Fast 1` cannot help because there is no fast plan available to optimize toward. The hint would be a code change without a behavior change, which is exactly the kind of churn that is worth resisting.

---

## 7. Risk & Rollback

### Risks

- **No proc-body risk.** The proc is unchanged.
- **Index-deployment risk** (when Section 11.1 is applied): adding an index changes plan choice for any other query that touches `PmssCapturedXmlTrxs`. The table is presumably written by the IA Web Services interface (every captured Fill/Cancel request is inserted) and read by this proc and possibly others. Insert-side cost goes up by the maintenance cost of one additional index on every write. With a filtered index, that cost only applies to inserts whose `TrxStateCode` is `'Q'` (the typical case for new inserts) and to updates that change `TrxStateCode` away from `'Q'` (the dequeue path). Both are exercised at high frequency, so the maintenance cost is real but small per-row.
- **Filtered-index match risk.** If the query text ever changes (a new `Trx` value added, the predicate widened to `TrxStateCode In ('Q', 'X')`, etc.), a filtered index whose filter does not match the new query exactly will be silently bypassed and the proc returns to its current cost profile. This is a known property of filtered indexes; document the dependency in the index's extended properties when deploying.

### Rollback

The proc is unchanged. If the index in Section 11.1 is deployed and causes regressions on writes or on other queries against `PmssCapturedXmlTrxs`:

```sql
Drop Index IX_PmssCapturedXmlTrxs_QueuedFillCancel On dbo.PmssCapturedXmlTrxs;
```

No data state is mutated by either the index creation or its rollback. Index drops and creates on `PmssCapturedXmlTrxs` are pure metadata-and-leaf operations.

### Monitoring Window

For the first 24 hours after the index is deployed:

- Query Store avg reads per execution for the `<ad-hoc>` PmssCapturedXmlTrxs statement at the five high-cost sites (Orlando, NorthLake, Bolingbrook, Mansfield, Tolleson). Expected drop is from 27K to 1.4M down to single digits.
- Plan count per site. Should collapse to 1 (one plan per site, all sites). Plan instability is a load-bearing diagnostic here.
- Insert latency on the IA Web Services capture path. The maintenance cost of one additional index on a high-write table is the principal regression risk.
- Index size growth on `PmssCapturedXmlTrxs`. With a filtered index on `Q`-state rows, growth should plateau quickly because the filtered set is bounded by the queue depth at any given moment.

---

## 8. Evidence of Refactor (post-index)

There is no Refactored.sql diff to capture STATS IO/TIME against. The evidence pass for this proc is index-level: capture STATS IO/TIME for the same statement against the same data state with the new index in place, confirm the optimizer chose it, and confirm the avg reads per execution dropped to single digits.

```
(STATS IO + STATS TIME output post-index goes here, one block per state matching Section 3.2.
 Save the .sqlplan file to confirm the new index was chosen.)
```

---

## 9. Comparison & Improvement

### 9.1 Verdict: the win is an index addition, not a proc change

The proc body in v2 is canonical and there is no code path that would deliver a meaningful improvement to it. The 70,000x cross-MFC variance in avg reads per execution, the plan instability on a parameterless query, and the simple two-predicate query shape all point at the same diagnosis: missing index coverage.

The estimated impact, once the index in Section 11.1 is in place, is from 337,685 avg reads per execution fleet-wide down to single digits (5 to 10). At 825 million monthly executions, that is approximately **278 trillion logical reads saved per month, fleet-wide**, on this single index addition. The savings are roughly 100 times the savings from the Row 16 Catch-All fix and roughly 290 times the savings from the Row 15 FOR XML PATH consolidation. This is the largest single-change win identified in the refactor batch so far.

The polling-frequency angle in Section 11.3 is a longer-horizon multiplier on top of the index win. The IA Web Services interface is calling this query roughly 30 to 50 times per second per site at peak. A Service Broker queue or a signal-driven handoff would eliminate the polling entirely, dropping the call count from 825M per month to something proportional to the actual transaction rate (likely in the low millions). That is an application-side change, not a database-side change, and it is out of scope for v2.

### 9.2 What the data supports today, before the index ships

| Metric | v2 (no index) | v2 (with index) | Delta |
|--------|---------------|-----------------|-------|
| Avg reads per execution (fleet) | 337,685 | (predicted 5 to 10) | predicted -99.997% |
| Plan variants per site | 1 to 3 | 1 | stable |
| Avg duration at Orlando | 190,568 ms | (predicted single-digit ms) | predicted >99.99% |
| Total monthly reads (fleet) | 278.5T | (predicted ~5B at 6 reads/exec) | predicted -99.998% |
| Proc body | unchanged | unchanged | tie |

The "predicted" cells are the gap to be closed by the STATS IO/TIME captures in Section 3.2 and Section 8 once the index is deployed at one MFC.

### 9.3 What I need next

1. **Pull the index DDL** for `PmssCapturedXmlTrxs` so I can see what indexes exist today and whether any of them are near-misses worth replacing instead of supplementing. This is the gating dependency on Section 11.1's final form.
2. **Confirm `TrxStream` column type and typical row size.** If it is a `varchar(max)` or `xml` column with multi-KB rows, the INCLUDE choice in Section 11.1 needs to be revisited (drop `TrxStream` from INCLUDE, accept a key lookup for the one returned row).
3. **Deploy to one high-cost MFC and measure.** Orlando is the obvious target because the per-call cost is so high that any plan-shape change will be visible in Query Store within minutes. Capture before/after Query Store metrics over a 24-hour window.
4. **Confirm the five sites below the top-50 cut** (Denver, Indy, Kent, Memphis, Mechanicsville) are running the proc cheaply because they already have a usable index, not because they have a smaller queue. If they already have one, that is the index pattern to standardize across the fleet.

---

## 10. Validation Checklist

Per `tasks/lessons.md`. Marked against the future post-index capture; the proc-body checklist degenerates because the body is unchanged.

- [ ] **Same data state.** v2 statement run twice, once with the existing indexes and once after the index in Section 11.1 is created, both back to back, no intervening queue churn beyond what production naturally produces.
- [ ] **Warm cache only.** Run twice per state, discard cold-cache numbers.
- [ ] **Non-zero result set.** At least one queued `'Q'`-state Fill or Cancel request present in the test data. Worth running a second pass with zero matching rows to confirm the empty-queue path is also tight.
- [ ] **Identical result set.** Same row count and same row identity (same `Id`) before and after.
- [ ] **Plan shape matches prediction.** New plan uses the index in Section 11.1 as the seek source. Confirmed against `.sqlplan`.
- [ ] **No new error or warning messages.** No cardinality errors, no lock-escalation warnings on the index create.
- [ ] **Warm-cache elapsed time at or below original.** v2 elapsed time post-index should drop from the per-call multi-second range at the high-cost sites to single-digit milliseconds.

The validation checklist for the proc body itself is automatically passed: the body is unchanged so result-set parity is identical by definition.

**Net call:** the proc body is not the lever. v2 is the recommended deployment as-is. The index in Section 11.1 is the change that captures the fleet-wide win.

---

## 11. Open Items / Future Improvements

### 11.1 Index Recommendations (the entire win lives here)

#### A. Filter-or-composite index on `PmssCapturedXmlTrxs` for the Queued Fill/Cancel query

**Gating dependency:** the index DDL for `PmssCapturedXmlTrxs` is not in `IndexExtract.xlsx`. The pilot extract covered the `OeOrder` family only. Run `extract_index_ddl.sql` against Tolleson (or another high-cost site) with `@Tables` set to `('PmssCapturedXmlTrxs')` before deploying. If a near-miss index already exists (for example, `(TrxStateCode)` keyed only, or `(Id)` clustered with no helpful nonclustered), the recommendation may be to reshape the existing index rather than add a new one, which preserves the plan-cache reference.

The recommended shape, assuming no existing near-miss:

```sql
Create NonClustered Index IX_PmssCapturedXmlTrxs_QueuedFillCancel
   On dbo.PmssCapturedXmlTrxs (Id)
   Include (TrxKey, Trx, TrxStream)
   Where TrxStateCode = 'Q'
     And Trx In ('fillRequest', 'cancelRxRequest', 'cancelOrderRequest')
   With (Fillfactor = 92, Online = On);
```

Rationale:

- The filter predicate matches the query's `Where` clause exactly. The optimizer will use this index whenever this proc runs and will not use it for other queries against `PmssCapturedXmlTrxs` (which is the desired behavior; the index is purpose-built for this one statement).
- `Id` is the key so that `Top 1 ... Order By Id` is satisfied by reading the leaf in its natural order. No sort operator required.
- `TrxKey`, `Trx`, and `TrxStream` are in INCLUDE so the entire SELECT list is satisfied from the leaf without a key lookup. `Trx` is technically redundant with the filter (it is one of three known values) but including it lets the engine return the column without having to inject a literal at runtime.
- The filter is narrow. The set of rows in `Q` state for one of three `Trx` kinds is by definition the queue depth at any instant, which is small (the IA Web Services interface dequeues continuously). The index will hold a few thousand rows at most and stay cache-resident.

**Expected impact:** per-call reads drop from the 337,685 fleet-wide average to single digits (a Top 1 read against a small, ordered, filtered, covering index is one or two leaf pages). At 825M monthly executions, that is approximately **278 trillion logical reads saved per month, fleet-wide**.

#### B. Alternative shape if filter-index dependency is fragile

If operations is concerned about the filter predicate's coupling to the literal `Trx` IN list (any future expansion of the IN list would silently bypass this index), use a non-filtered composite instead:

```sql
Create NonClustered Index IX_PmssCapturedXmlTrxs_StateTrxId
   On dbo.PmssCapturedXmlTrxs (TrxStateCode, Trx, Id)
   Include (TrxKey, TrxStream)
   With (Fillfactor = 92, Online = On);
```

Wider index, slightly larger footprint, but it tolerates query-text drift on the IN list because the seek predicate `TrxStateCode = 'Q' And Trx In (...)` matches the leading key columns whatever `Trx` values are in scope. Choose A if the IN list is stable and operations is comfortable with the filter dependency. Choose B if the IN list is expected to evolve.

#### C. Conditional: drop `TrxStream` from INCLUDE if the column is a large LOB

If `TrxStream` is a multi-KB or multi-MB column (the tracking sheet entry suggests it may be XML or `varchar(max)`), including it in the leaf will bloat the index on every queued row. The leaf will hold one row per queue entry but each row will be huge.

Trade-off:

- **Include `TrxStream`** (recommendation A as written): the proc completes with zero key lookups. Per-call reads are 1 to 2 leaf pages plus the INCLUDE row, which may itself be multiple pages if `TrxStream` is large.
- **Exclude `TrxStream`**: the proc reads 1 to 2 leaf pages plus one key lookup back to the clustered index (`PK_PmssCapturedXmlTrxs (Id)` presumably). The key lookup pulls `TrxStream` from the clustered leaf. Per-call reads stay in single digits as long as the clustered index is sane.

If `TrxStream` is multi-KB on average, exclude it. If it is small (a few hundred bytes typical), include it. The DDL extraction in 11.1.A's gating dependency should report the column type and average row size, which determines the choice.

### 11.2 Proc-Level Changes

None recommended for v2. The proc body is canonical. The decision tree for proc-level changes (a hint, an `Option (Fast 1)`, a rewrite to use a CTE or a different query structure) is documented in Section 6 and the conclusion is the same in every branch: no code change adds value if the index is right, and no code change rescues a missing index.

If for some reason the index recommendation in 11.1 cannot be deployed (operations objection on insert-side maintenance cost, for example), an `Option (Fast 1)` hint on the SELECT would not deliver meaningful improvement on its own but is harmless. The right escalation in that case is a Section 11.3 change (Service Broker), not a Section 11.2 change (proc-body hint).

### 11.3 Schema-Level Changes (longer horizon)

#### A. Replace polling with Service Broker or equivalent signal-driven handoff

The IA Web Services interface is polling this queue at roughly 30 to 50 times per second per site at the busiest MFCs. Even with the perfect index in place (per Section 11.1), the database is being asked the same question many tens of millions of times per month at every site, and the answer is "no rows" the overwhelming majority of the time (the queue is usually nearly empty because consumption keeps up with insertion).

A signal-driven pattern would eliminate the polling:

- **Service Broker**: convert `PmssCapturedXmlTrxs` writes to send a Service Broker message to a queue, and have the IA Web Services interface use `Waitfor (Receive ...)` to block until a message arrives. This collapses the call count from 825M/month to one call per actual transaction (likely low millions per month).
- **`Waitfor Delay` polling**: less invasive but lower payoff. Have the IA Web Services interface insert a `Waitfor Delay '00:00:00.500'` between polls so the loop runs twice per second instead of dozens of times per second. Reduces call count by roughly 25x.
- **Trigger plus signal table**: insert trigger on `PmssCapturedXmlTrxs` writes to a signal table that the application watches; less elegant than Service Broker but compatible with the existing dequeue logic.

Any of these dwarfs the index-level win, but they require application-side change. Out of scope for v2. Worth raising with the IA Web Services team as a separate workstream.

#### B. Confirm the `TrxStream` column type and consider compression

If `TrxStream` is a large LOB column and is the determining factor in 11.1.C's INCLUDE choice, consider whether `Page` compression on the table or on the index would mitigate the leaf-bloat cost. SQL Server compresses XML and text columns reasonably well at the page level. This is a tuning decision driven by the actual column size distribution, which the DDL extraction in 11.1.A will reveal.
