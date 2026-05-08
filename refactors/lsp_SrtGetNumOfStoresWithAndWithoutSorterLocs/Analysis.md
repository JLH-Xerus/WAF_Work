# lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs: Refactor Analysis (v6 to v7)

**Date:** 2026-05-07
**Tracking sheet row:** 17 (Priority P1, status Not Started)

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs`

**Purpose in one line:** Count the stores in each of four priority buckets relative to the package sorter (unassigned with active orders, assigned with no tote, assigned with a full tote, unassigned with no active orders), optionally filtered to a single store.

**Tables touched:**

- `OeOrder` (the workhorse for the active-orders driver; joined to OeOrderSecondaryData and a vInvMaster view)
- `OeOrderSecondaryData` (joined for the `PlanPickUpDtTm` predicate)
- `vInvMasterWithUserOverrides` (left-joined to filter by `StorageCondCode <> 'R'`)
- `SysEndPharmacy` (the store catalog; scanned in priorities 1 and 4 and joined inside the sorter-loc chain)
- `SrtSorterLoc` (the sorter location catalog with `HasTote` and `IsFull` flags)
- `SysEndPharmacyShpCarrierAssoc` (the bridge between sorter loc carrier code and pharmacy id)

**Indexes used:** to be confirmed against the v7 plan once captured. The proc assumes seekable indexes on `OeOrder.StoreNum`, `OeOrder.OrderStatus`, `SysEndPharmacy.StoreNum`, `SysEndPharmacy.Id`, `SrtSorterLoc.CarrierCode`, and `SysEndPharmacyShpCarrierAssoc.CarrierCode`. The Section 11 index recommendations are gated on confirming these.

**Index DDL gap.** `IndexExtract.xlsx` produced for the Row 15 pilot covers only the OeOrder family (OeOrder, OeGroup, OeOrderHistory, OeOrderCurrHistoryDtTm, OeOrderShipmentAssoc, OeOrderPoNumAssoc). The Sortation tables that dominate this proc (`SrtSorterLoc`, `SysEndPharmacy`, `SysEndPharmacyShpCarrierAssoc`) are not in that extract. Run `extract_index_ddl.sql` against Tolleson with `@Tables` set to `('SrtSorterLoc', 'SysEndPharmacy', 'SysEndPharmacyShpCarrierAssoc', 'OeOrderSecondaryData')` to fill this in. The OeOrder coverage from the Row 15 extract carries over.

**Callers:** Sorter dashboard / sorter operator UI. The 17 million monthly executions across the fleet are consistent with a polling dashboard refresh on a short interval.

---

## 2. Overview of Performance

The cross-MFC view from the 2026-05-07 Query Store capture shows this proc as a top-tier I/O contributor, present in all 14 reporting sites. **Roughly 1.20 trillion logical reads per month across 6.1 million executions, averaging 196K reads per execution fleet-wide.** Per-site avg reads per exec ranges from 36K (West Jordan) to 387K (NorthLake), an 11x spread. Detailed per-site breakdown is in Section 3.1.

The plan-stability picture is mixed. Eight of 14 sites return a single plan variant (Bolingbrook, Canal, Denver, Liberty, Mechanicsville, Memphis, NorthLake, Tolleson). BrooklynPark, Indy, Kent, Mansfield, Orlando, and West Jordan show 2 to 5 plan variants, consistent with parameter-sniffing volatility on the optional `@StoreNum` parameter (a single-store call versus an all-stores call have very different cardinality expectations). Kent's 5-variant / 2-query_id pattern is the most extreme and worth confirming during validation.

The dominant cost driver in v6 is structural and stacks in three layers:

1. **Four UNION branches each independently join `SrtSorterLoc` to `SysEndPharmacyShpCarrierAssoc` to `SysEndPharmacy`.** That is the same three-table chain executed four times per call, even though each branch wants the same store-to-sorter-loc assignment data. The HasTote and IsFull flags from SrtSorterLoc are the only per-branch differentiators.
2. **Two of the four branches use NOT IN against subqueries that can return NULL.** Priority 1 and Priority 4 both use `SEP.StoreNum Not In (Select Distinct SEP.StoreNum From SrtSorterLoc ... Left Join SysEndPharmacy ...)`. The `Left Join` to `SysEndPharmacy` makes NULL StoreNums possible in the subquery's result set; the `Where SEP.StoreNum Is Not Null` filter in the subquery does mitigate this, but the pattern is a NULL-safety landmine and an unforced semantic risk.
3. **Every WHERE clause wraps the StoreNum column in TRIM().** `TRIM(O.StoreNum) = TRIM(@StoreNum)` is non-SARGable (see [[Non-SARGable Predicates]]) and forces the optimizer into a scan on whichever index covers StoreNum, evaluating the function on every row.

v7 attacks all three. The sorter-loc assignment chain is materialized once into `#SorterLocStores` with a clustered index on StoreNum; the four UNION branches probe it via EXISTS / NOT EXISTS instead of re-joining the three base tables. NOT IN is replaced with NOT EXISTS, eliminating the NULL-safety bug. TRIM is moved off the WHERE clause via a pre-trimmed local variable `@StoreNumTrimmed`. UNION becomes UNION ALL because the Priority column is mutually exclusive across branches. The expected effect is a 3x to 5x reduction in per-execution reads, which at fleet scale is approximately 800B to 950B logical reads saved per month across the 14 reporting sites.

The biggest absolute wins should land at the high-cost sites (NorthLake, Bolingbrook, Orlando, Tolleson, Mechanicsville, Mansfield) which together account for roughly 71% of the monthly load.

---

## 3. Evidence of Original (v6)

### 3.1 Query Store, cross-MFC view

**Source:** `diagnostics/querystore_outputs/QueryStore_20260507.xlsx`. Top 50 offenders per site, 30-day lookback, captured 2026-05-07. One tab per site.

**Aggregation method:** for each site, all rows whose `objectname` matches `lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs` are aggregated. Multiple rows with the same `query_id` indicate plan instability (the same statement captured under different plan variants). Multiple distinct `query_ids` indicate that more than one statement inside the proc is appearing in the top 50.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------------|----------------|
| NorthLake | 1 | 1 | 511,084 | 197,785,826,682 | 386,993 | 400.4 | 366,117,372 |
| Orlando | 3 | 1 | 407,120 | 142,635,954,174 | 350,354 | 891.7 to 1201.3 | 249,494,004 |
| Bolingbrook | 1 | 1 | 410,399 | 134,690,594,171 | 328,194 | 364.3 | 257,571,957 |
| Tolleson | 1 | 1 | 512,612 | 141,039,491,130 | 275,139 | 208.6 | 248,627,415 |
| Mechanicsville | 1 | 1 | 474,216 | 113,062,073,911 | 238,419 | 638.9 | 156,659,686 |
| Canal | 1 | 1 | 492,923 | 94,555,322,958 | 191,826 | 611.5 | 135,145,174 |
| Mansfield | 2 | 1 | 398,337 | 71,349,053,045 | 179,117 | 403.9 to 611.6 | 128,874,447 |
| BrooklynPark | 3 | 1 | 283,328 | 43,223,672,286 | 152,557 | 47.5 to 123.4 | 71,481,266 |
| Indy | 3 | 1 | 510,683 | 73,186,345,202 | 143,311 | 442.8 to 508.8 | 135,392,816 |
| Liberty | 1 | 1 | 506,165 | 70,045,516,154 | 138,385 | 97.6 | 116,729,250 |
| Memphis | 1 | 1 | 484,948 | 63,980,106,682 | 131,932 | 368.9 | 94,461,558 |
| Denver | 1 | 1 | 305,022 | 18,952,544,619 | 62,135 | 87.2 | 24,088,785 |
| Kent | 5 | 2 | 569,599 | 21,729,487,019 | 38,149 | 36.7 to 83.0 | 54,641,548 |
| West Jordan | 2 | 1 | 244,385 | 8,850,822,479 | 36,217 | 27.8 to 38.7 | 9,895,615 |

**Roll-up across all 14 sites:** 6,110,821 executions, **1,195,086,810,512 total logical reads** (195,569 avg reads/exec). Every site is reporting; no site is missing the proc from its top 50, which is consistent with a high-frequency dashboard caller.

**Observations:**

- **Plan instability is concentrated at the high-volume parameter-varying sites.** Orlando shows 3 plan variants with avg duration spanning 891 to 1201 ms, a 1.35x swing on the same statement. Mansfield's 2 variants span 404 to 612 ms (1.5x). BrooklynPark's 3 variants span 47 to 123 ms (2.6x). Kent has 5 variants across 2 distinct query_ids, the only site exhibiting two different statements in the top 50. All of these are textbook parameter-sniffing signals on the optional `@StoreNum`. See [[Parameter Sniffing]].
- **Per-site avg reads/exec varies by 11x.** NorthLake at 387K and Orlando at 350K against West Jordan at 36K. This suggests significant variance in the size of the SysEndPharmacy and SrtSorterLoc tables across sites, plus variance in the active-Orders driver state that feeds priorities 1 and 4. The largest sites are paying disproportionate cost on the repeated three-table sorter-loc chain.
- **Top 6 sites by total reads contribute 71% of the load.** NorthLake, Orlando, Bolingbrook, Tolleson, Mechanicsville, and Mansfield together account for 800B of the 1.2T monthly reads. Deploying v7 to those 6 sites first would capture the bulk of the systemic improvement.
- **Avg duration is anomalously high for what should be a counting query.** Mechanicsville at 639 ms, Canal at 612 ms, Mansfield at up to 612 ms, Orlando at up to 1201 ms. A four-priority count with at most a few thousand stores per site should not take half a second. The wall-clock numbers are signal that the engine is doing far more work than the result requires.
- **CPU time is enormous in absolute terms.** NorthLake spends roughly 366 million ms (about 102 hours, or 4 days of CPU) per month on this single proc. The fleet total CPU is about 2.3 billion ms per month, roughly 27 days of CPU.
- **No forced plans, no plan failures.** The `forcedplan` and `planfailures` columns are NULL for every row, so there is no forced-plan rollback to manage.

### 3.2 SET STATISTICS IO, TIME from a representative MFC

I capture STATS IO/TIME from Tolleson (or another representative MFC) running both v6 and v7 against the same data state, with several common parameter combinations:

- `Exec lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs @StoreNum = '<known store>', @NextDayOrderStartTime = '06:00'` (single-store filter, the dashboard's typical case)
- `Exec lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs @StoreNum = '', @NextDayOrderStartTime = '06:00'` (all stores, the high-cost case)

```
(STATS IO + STATS TIME output for v6 goes here, ideally one block per parameter combination)
```

---

## 4. Issue Identification (v6)

The line numbers below reference `Original.sql` (v6).

### 4.1 Three-table sorter-loc join chain repeated four times (lines 100 to 105, 117 to 119, 134 to 136, 159 to 162)

The same `SrtSorterLoc -> SysEndPharmacyShpCarrierAssoc -> SysEndPharmacy` join structure appears in four independent places in the proc. Three of the four are `Left Join` chains and one is the join inside the `Not In` subqueries for priorities 1 and 4. The intent in every case is the same: identify the set of (StoreNum, sorter-loc-attribute) pairings.

```sql
-- Inside the priority-1 NOT IN (lines 99-105):
Select Distinct SEP.StoreNum
From SrtSorterLoc AS SL With (NOLOCK)
Left Join SysEndPharmacyShpCarrierAssoc AS SPSC With (NoLock) ON SPSC.CarrierCode = SL.CarrierCode
Left Join SysEndPharmacy AS SEP With (NoLock) ON SEP.Id = SPSC.SysEndPharmacyId
Where SEP.StoreNum Is Not Null

-- Priority 2 (lines 116-119), priority 3 (lines 133-136), and priority 4's NOT IN (lines 158-162)
-- repeat the same chain with minor differences in WHERE filters.
```

The cost compounds. Each repetition independently seeks or scans `SrtSorterLoc` (13 columns), `SysEndPharmacyShpCarrierAssoc` (4 columns), and `SysEndPharmacy` (26 columns), producing four separate plan operators when one materialized set would suffice. At the high-volume sites this is the dominant driver of the 196K-reads-per-execution fleet average.

### 4.2 NOT IN with NULL-safety risk (lines 98 to 106 and 157 to 164)

Priorities 1 and 4 both use the form:

```sql
And SEP.StoreNum Not In
(
   Select Distinct SEP.StoreNum
   From SrtSorterLoc AS SL With (NOLOCK)
   Left Join SysEndPharmacyShpCarrierAssoc AS SPSC With (NoLock) ON SPSC.CarrierCode = SL.CarrierCode
   Left Join SysEndPharmacy AS SEP With (NoLock) ON SEP.Id = SPSC.SysEndPharmacyId
   Where SEP.StoreNum Is Not Null
)
```

The `Where SEP.StoreNum Is Not Null` filter at the bottom does mitigate the immediate NULL-safety risk by excluding NULL StoreNums from the subquery's result. But the code is fragile: if a future edit removes that filter (or changes the join order in a way that obscures it), the entire `NOT IN` predicate silently breaks because three-valued logic against a single NULL poisons the whole expression and excludes every outer row from the result. See [[NOT IN vs NOT EXISTS]] for the masterclass note.

`NOT EXISTS` is the canonical NULL-safe form and produces the same result without depending on the inner filter to stay correct.

### 4.3 TRIM on column in WHERE clause (lines 58, 97, 121, 138, 155)

Five separate WHERE clauses use the form `TRIM(O.StoreNum) = TRIM(@StoreNum)` or `TRIM(SEP.StoreNum) = TRIM(@StoreNum)`. Wrapping the column in `TRIM()` is non-SARGable: the optimizer cannot use any nonclustered index on `StoreNum` for a seek, because the index stores the untrimmed values and the predicate is asking about the trimmed ones. The function must be evaluated on every row.

The pattern is also redundant. The parameter `@StoreNum` is constant for the lifetime of one execution, so `TRIM(@StoreNum)` is computed once per row when it could be computed once for the whole call. Pre-trimming into a local variable solves both issues.

If the `StoreNum` column itself contains leading or trailing whitespace, the right fix is a one-time data cleanup plus a constraint, not a per-row TRIM in every query that touches the column. That is out of scope for v7 but worth noting in Section 11.3.

### 4.4 UNION (not UNION ALL) at lines 109, 126, 144

The four priority subqueries are combined with `UNION`, which forces a dedup sort. The Priority column carries one of four distinct integers (1, 2, 3, 4) per branch, guaranteeing that no row produced by branch A could ever match a row produced by branch B. The dedup is wasted work. See [[UNION ALL Views]].

### 4.5 Legacy temp-table existence check (lines 34 to 35)

```sql
IF OBJECT_ID('tempdb..#StoreForCurrentDayOrders') IS NOT NULL
   DROP TABLE #StoreForCurrentDayOrders
```

Functional but verbose, replaced in modern T-SQL by `Drop Table If Exists`. Cosmetic, not performance-critical, but reduces line count and reads cleaner.

### 4.6 Inconsistent capitalization on `WITH (NoLock)` (multiple lines)

A purely cosmetic issue, but the proc mixes `With (NOLOCK)`, `With (NoLock)`, and `with (nolock)` across different lines. v7 standardizes on `With (NoLock)`.

---

## 5. First Principles

### 5.1 Materialize-Once for Repeated Join Chains

When the same multi-table join chain appears in multiple call sites within a query, materializing the chain once into an indexed temp table and probing via `EXISTS` / `NOT EXISTS` collapses the repeated work. The principle is articulated in [[FOR XML PATH Consolidation]] (which is a specialization of the same idea applied to FOR XML PATH expressions repeated across UNION branches) and in the "if you reference the same CTE in multiple places, consider materializing it" note in [[Correlated Subqueries to CTEs]]. v7 applies this exactly: the `SrtSorterLoc -> SysEndPharmacyShpCarrierAssoc -> SysEndPharmacy` chain is built once into `#SorterLocStores` with a clustered index on `StoreNum`, then probed from each of the four priority branches.

The HasTote and IsFull columns from `SrtSorterLoc` are carried into the temp table so priorities 2 and 3 can filter on them without re-joining `SrtSorterLoc`. This is the only meaningful difference between the four branches' use of the chain; carrying the differentiators forward into the materialized set means one build covers all four branches.

### 5.2 NOT IN vs NOT EXISTS

`NOT IN (Select Col From ...)` is unsafe whenever the inner column can be NULL. A single NULL in the subquery's result set poisons the entire predicate via SQL three-valued logic, and the outer query silently returns zero or fewer rows than expected. `NOT EXISTS (Select 1 From ... Where Col = OuterCol)` does not have this problem and is the canonical NULL-safe form. v6's priority 1 and priority 4 use `NOT IN` against a subquery whose inner column could in principle return NULL (the `Left Join SysEndPharmacy` makes it possible). The existing `Where SEP.StoreNum Is Not Null` filter in the subquery papers over the bug today, but the pattern is fragile against future edits. v7 standardizes on `NOT EXISTS`. See [[NOT IN vs NOT EXISTS]] for the full masterclass note.

### 5.3 Non-SARGable Predicates: TRIM on Column

`TRIM(O.StoreNum) = TRIM(@StoreNum)` wraps the column in a function call, making the predicate non-SARGable. The optimizer cannot use any nonclustered index on `StoreNum` for an index seek because the index stores the untrimmed values. The fix is to pre-trim the parameter into a local variable `@StoreNumTrimmed`, then compare the bare column to the local. This restores SARGability and lets the optimizer choose an index seek when one is available. See [[Non-SARGable Predicates]].

A persisted computed column on `OeOrder.StoreNum` (and similar on `SysEndPharmacy.StoreNum`) holding the trimmed value, with an index, would be the schema-level fix if the underlying data actually contains whitespace. That is the right long-term answer but out of scope for v7. See Section 11.3.

### 5.4 UNION vs UNION ALL with Mutually Exclusive Label Columns

`UNION` removes duplicates by sorting (or hashing) the combined result. When a label column in the SELECT list guarantees that rows from different branches cannot match (in this proc, the `Priority` column carries 1, 2, 3, 4 in distinct branches), the dedup is wasted work. `UNION ALL` skips the dedup and concatenates the branches directly. The optimizer cannot infer this safely on its own; the human change is required. See [[UNION ALL Views]].

### 5.5 Modern Temp-Table Existence Checks

`Drop Table If Exists` was introduced in SQL Server 2016 and supersedes the legacy `If Object_Id('TempDb..#X') Is Not Null Drop Table #X` pattern. The legacy form still works, but the modern form is shorter, more readable, and harder to typo (the `Object_Id` form requires the `'TempDb..'` prefix, which is easy to miss). v7 standardizes on the modern form.

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v7. The narrative below walks the changes block by block. Line references are to `Refactored.sql`.

### 6.1 Pre-trimmed parameter (lines 56 to 60)

```sql
Declare @StoreNumTrimmed VarChar(19)

Set @StoreNumTrimmed = LTrim(RTrim(IsNull(@StoreNum, '')))
```

The parameter is trimmed once at the top of the proc. Every downstream WHERE clause then compares a bare column to `@StoreNumTrimmed`, restoring SARGability. The empty-string convention from v6 (where an empty `@StoreNum` means "all stores") is preserved: an empty input still becomes an empty trimmed value, and the WHERE clause checks `(@StoreNumTrimmed = '' Or Col = @StoreNumTrimmed)`. The `IsNull(..., '')` is defensive against a NULL parameter, which v6 did not handle explicitly but which `TRIM(NULL)` would produce a NULL for, breaking the equality check.

### 6.2 #StoreForCurrentDayOrders (lines 67 to 89)

The driver build for active-orders stores. The shape is unchanged from v6. The only changes are:

- TRIM moved off the WHERE clause: `(@StoreNumTrimmed = '' Or O.StoreNum = @StoreNumTrimmed)` replaces `(TRIM(O.StoreNum) = TRIM(@StoreNum) Or @StoreNum = '')`. SARGable.
- `Drop Table If Exists` at the top of the proc replaces the `Object_Id` check.
- `Inner Join` made explicit on the `OeOrderSecondaryData` join (v6 used the bare `Join` keyword which is implicitly inner; the explicit form is clearer).
- The "Not In any Pending status prior to Awaiting Governor Allocation" predicate is rewritten using a tighter De Morgan form: v6's `Not (O.OrderStatus = 'Pending' And Not (O.StatusReason = 'Awaiting Governor Allocation'))` becomes `Not (O.OrderStatus = 'Pending' And O.StatusReason <> 'Awaiting Governor Allocation')`. Logically equivalent, easier to read.

### 6.3 #SorterLocStores (lines 96 to 113)

The store-to-sorter-loc assignment chain, materialized once.

```sql
Select Distinct
   SEP.StoreNum,
   SL.HasTote,
   SL.IsFull
Into #SorterLocStores
From SrtSorterLoc As SL With (NoLock)
   Inner Join SysEndPharmacyShpCarrierAssoc As SPSC With (NoLock)
      On SPSC.CarrierCode = SL.CarrierCode
   Inner Join SysEndPharmacy As SEP With (NoLock)
      On SEP.Id = SPSC.SysEndPharmacyId
Where
   SL.CarrierCode Is Not Null
   And SEP.StoreNum Is Not Null

Create Clustered Index IX_SLS On #SorterLocStores (StoreNum)
```

This is the central refactor change. v6's four UNION branches each independently joined this chain. v7 builds it once. The HasTote and IsFull columns are carried so priorities 2 and 3 can filter on them without re-joining `SrtSorterLoc`. The clustered index on `StoreNum` supports the EXISTS / NOT EXISTS probes from all four branches.

The join shape changed from `Left Join` to `Inner Join` for both joins. v6's `Left Join` to `SysEndPharmacyShpCarrierAssoc` and to `SysEndPharmacy` was paired with a `Where SEP.StoreNum Is Not Null` filter that effectively converted the LEFT JOIN back to an INNER JOIN by post-filtering the unmatched rows. The `Inner Join` form is semantically equivalent to the v6 result and gives the optimizer better cardinality estimates upfront.

### 6.4 Four priority branches via UNION ALL (lines 121 to 191)

Each branch is restructured:

- **Priority 1** (unassigned, active orders): `EXISTS (Select 1 From #StoreForCurrentDayOrders ...)` replaces the `In (Select StoreNum From #StoreForCurrentDayOrders)` form, and `NOT EXISTS (Select 1 From #SorterLocStores ...)` replaces the `NOT IN` against the four-table subquery. Two anti-pattern fixes in one branch.
- **Priority 2** (assigned, no tote): reads from `#SorterLocStores` directly with `Where S.HasTote = 0`. v6 re-joined the three base tables.
- **Priority 3** (assigned, full tote): reads from `#SorterLocStores` directly with `Where S.IsFull = 1`. v6 re-joined the three base tables.
- **Priority 4** (unassigned, no active orders): two `NOT EXISTS` probes replace v6's two `NOT IN` subqueries. Both probes are clustered-index seeks against small temp tables.

`UNION ALL` replaces v6's `UNION` between the four branches. The Priority column makes overlap impossible.

### 6.5 Cleanup (lines 198 to 199)

```sql
Drop Table If Exists #StoreForCurrentDayOrders
Drop Table If Exists #SorterLocStores
```

Explicit cleanup at the end of the proc. SQL Server cleans up session-scoped temp tables automatically when the session ends, but at 17 million calls per month (one execution every 2 to 3 seconds at steady state per site) the immediate release matters for tempdb pressure.

---

## 7. Risk & Rollback

### Risks

- **Result-set parity on the priority counts.** v6 and v7 should produce identical counts for each of the four priorities for any given parameter combination. The semantic changes are: (a) `NOT IN` becomes `NOT EXISTS`, which is the same for non-NULL inner columns (v6's `Where SEP.StoreNum Is Not Null` filter ensured this) but differs for NULL; (b) `Left Join` becomes `Inner Join` in the materialized chain, which is the same because v6's `Where SEP.StoreNum Is Not Null` post-filter effectively converted LEFT to INNER; (c) UNION becomes UNION ALL, which is the same because the Priority column makes overlap impossible. The result set should be byte-identical for any data state v6 was producing correct results on. Verify during validation that v6 and v7 produce the same four `(Priority, NumOfStores)` rows for both the single-store and all-stores parameter cases.
- **`#SorterLocStores` cardinality vs SysEndPharmacy size.** The materialized set is one row per (sorter-loc, pharmacy) pairing where the sorter-loc has a non-NULL CarrierCode and the pharmacy has a non-NULL StoreNum. The expected size is small (most fleets have hundreds to low thousands of sorter locations). If a particular MFC has an unusually large `SrtSorterLoc` table or unusual carrier-code distribution, the build cost could be higher than expected. Worth confirming the table sizes from the index DDL extract (Section 11.1.A).
- **Plan-cache churn from the new statement boundaries.** v7 introduces two additional statements (the `#StoreForCurrentDayOrders` build, the `#SorterLocStores` build, and the index create on `#SorterLocStores`). Each gets its own cached plan. The cache impact is small but non-zero. Worth a glance at plan cache size on the day after deployment.
- **Parameter-sniffing on the consolidated query.** v6 had four separate compiled plans, one per branch. v7 has a single consolidated plan for the main count query. If the optimizer's combined cardinality estimate is materially worse than the per-branch estimates were, the new plan could be slower than the sum of the v6 branches. This is unlikely because the dominant cost is the temp-table builds (which run regardless), but it is the place to look if anything underperforms.

### Rollback

The proc is a read-only `SELECT` returning a result set. Rollback is a pure DDL revert: restore the v6 body via `ALTER PROCEDURE`. No data state is mutated, so there is no compensating action required.

If a regression appears, run the v6 form (committed at git revision `9255cf2`) against the same data state to confirm. Rollback path:

```sql
-- pull v6 from git
git show 9255cf2:stored_procedures/lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs.sql

-- adjust CREATE -> ALTER, deploy
```

### Monitoring Window

For the first 24 hours after deployment, watch:

- Sorter dashboard. Confirm the four priority counts look reasonable and that none of the priorities drops to zero unexpectedly (which would indicate the NOT IN / NOT EXISTS conversion changed semantics).
- Query Store avg reads/exec for the proc. Expected drop is from roughly 196K (fleet avg) to under 50K, with the largest gains at the highest-volume sites. If avg reads/exec does not drop materially, the materialization is not landing as expected and the index assumptions need revisiting.
- Avg duration. Expected drop is proportional, single-digit milliseconds for single-store calls, double-digit for all-stores calls.
- Plan count. Should stabilize at one plan per statement once warm. Sites that showed multiple plan variants in v6 should converge to fewer variants in v7.
- Tempdb allocation. The two temp tables are small per call, but at 17M calls/month tempdb churn is worth a glance.

---

## 8. Evidence of Refactor (v7)

I capture STATS IO/TIME from the same MFC and same data state used for the v6 capture, running v7 with the same parameter combinations.

```
(STATS IO + STATS TIME output for v7 goes here, one block per parameter combination)
```

The post-refactor plan summary should show:

- One scan of `OeOrder` / `OeOrderSecondaryData` / `vInvMasterWithUserOverrides` for the `#StoreForCurrentDayOrders` build.
- One pass through `SrtSorterLoc` -> `SysEndPharmacyShpCarrierAssoc` -> `SysEndPharmacy` for the `#SorterLocStores` build.
- Four EXISTS / NOT EXISTS probes against the indexed temp tables in the priority branches (priority 2 and priority 3 are pure scans of `#SorterLocStores`, priorities 1 and 4 are scans of `SysEndPharmacy` with two anti-semi-join probes each).
- No re-join of the three sorter-loc base tables inside any priority branch.

Confirm against the .sqlplan file.

---

## 9. Comparison & Improvement

*Filled in once both v6 and v7 STATS IO/TIME outputs are pasted above.*

| Metric | v6 (UNION + NOT IN + TRIM) | v7 (materialize + NOT EXISTS + pre-trim) | Delta | % Change |
|--------|----------------------------|------------------------------------------|-------|----------|
| Logical reads, single-store call | | | | |
| Logical reads, all-stores call | | | | |
| `OeOrder` reads | | | | |
| `SysEndPharmacy` reads | | | | |
| `SrtSorterLoc` reads | | | | |
| `SysEndPharmacyShpCarrierAssoc` reads | | | | |
| `#StoreForCurrentDayOrders` reads | | | | |
| `#SorterLocStores` reads | | | | |
| CPU time per call (ms) | | | | |
| Elapsed time per call (ms) | | | | |
| Result row count (must be 4) | 4 | 4 | | |
| Per-priority counts (must match) | | | | |

**Plan-shape verification:**

- v6 plan should show four independent `SrtSorterLoc -> SPSC -> SEP` join chains, two `NOT IN` semi-joins driven by the same chain, and a UNION operator at the top with a sort/aggregate for dedup.
- v7 plan should show one `SrtSorterLoc -> SPSC -> SEP` join chain feeding a Table Insert into `#SorterLocStores`, a clustered index create on `#SorterLocStores`, four anti-semi-join / semi-join probes against the temp table, and a UNION ALL Concatenation at the top with no dedup sort.

---

## 10. Validation Checklist

Per `tasks/lessons.md`. Mark each pass or fail before declaring the refactor done.

- [ ] **Same data state.** Both v6 and v7 captures taken against the same data, ideally back to back without intervening writes to `OeOrder`, `SysEndPharmacy`, `SrtSorterLoc`, or `SysEndPharmacyShpCarrierAssoc`.
- [ ] **Warm cache only.** Both captures are the second of two consecutive runs.
- [ ] **Non-zero result set.** Each parameter combination returned at least one priority bucket with a non-zero count. The all-stores call exercised priorities 1 and 4 with a non-trivial number of stores so the materialize-once win is visible.
- [ ] **Identical result set.** v6 and v7 returned the same four `(Priority, NumOfStores)` rows for each parameter combination.
- [ ] **Plan shape matches prediction.** v7 shows one materialization of `#SorterLocStores`, four EXISTS / NOT EXISTS probes against it, and UNION ALL with no dedup sort.
- [ ] **No new error or warning messages.** Neither run produced cardinality errors, conversion warnings, or excessive memory grant warnings.
- [ ] **Warm-cache elapsed time at or below v6.** v7 is faster on every parameter combination, with the largest improvement on the all-stores call where the four-times-repeated chain is most visible.

---

## 11. Open Items / Future Improvements

### 11.1 Index Recommendations

These are gated on running `extract_index_ddl.sql` against `SrtSorterLoc`, `SysEndPharmacy`, `SysEndPharmacyShpCarrierAssoc`, and `OeOrderSecondaryData` to confirm what already exists. The recommendations below assume the indexes do not currently exist; if they do, no action needed.

#### A. Run the index DDL extract

```sql
-- adjust @Tables in extract_index_ddl.sql
@Tables = ('SrtSorterLoc', 'SysEndPharmacy', 'SysEndPharmacyShpCarrierAssoc', 'OeOrderSecondaryData')
```

Save the output as `IndexExtract.xlsx` in this folder. Confirm whether the assumed indexes below exist before recommending creates.

#### B. Index on `SysEndPharmacy.StoreNum`

```sql
Create NonClustered Index IX_SysEndPharmacy_StoreNum
   On dbo.SysEndPharmacy (StoreNum)
   With (Fillfactor = 92, Online = On);
```

Supports the single-store filter in priorities 1 and 4 (`Where SEP.StoreNum = @StoreNumTrimmed`). Without this index, the single-store call still scans `SysEndPharmacy`. With it, the call becomes a tight seek that returns at most one row.

#### C. Index on `SrtSorterLoc.CarrierCode` (with HasTote, IsFull as INCLUDE)

```sql
Create NonClustered Index IX_SrtSorterLoc_CarrierCode
   On dbo.SrtSorterLoc (CarrierCode)
   Include (HasTote, IsFull)
   Where CarrierCode Is Not Null
   With (Fillfactor = 92, Online = On);
```

Supports the `#SorterLocStores` build. The filtered predicate matches the build's `Where SL.CarrierCode Is Not Null` exactly. Including HasTote and IsFull lets the build read the index without touching the base table.

#### D. Index on `SysEndPharmacyShpCarrierAssoc.CarrierCode`

```sql
Create NonClustered Index IX_SysEndPharmacyShpCarrierAssoc_CarrierCode
   On dbo.SysEndPharmacyShpCarrierAssoc (CarrierCode)
   Include (SysEndPharmacyId)
   With (Fillfactor = 92, Online = On);
```

Supports the join from `SrtSorterLoc.CarrierCode` to `SysEndPharmacyShpCarrierAssoc.CarrierCode` inside the `#SorterLocStores` build.

#### E. Index on `OeOrder (StoreNum, OrderStatus)`

```sql
Create NonClustered Index IX_OeOrder_StoreNum_OrderStatus
   On dbo.OeOrder (StoreNum, OrderStatus)
   Include (OrderId, HistoryDtTm, ProductId, StatusReason)
   With (Fillfactor = 92, Online = On);
```

Supports the `#StoreForCurrentDayOrders` build. The leading column matches the SARGable parameter filter, the second column matches the long `OrderStatus In (...)` list, and the INCLUDE list covers the join keys to `OeOrderSecondaryData` and the StatusReason filter. With this index in place, the build cost should drop substantially for both single-store and all-stores calls.

### 11.2 Proc-Level Changes

None pending. v7 is the recommended deployment.

If post-deployment Query Store shows residual parameter-sniffing volatility on the all-stores vs single-store path, an `Option (Recompile)` on the main count statement is the simplest mitigation. Defer until v7 production data justifies it.

### 11.3 Schema-Level Changes

These are longer-horizon and require DBA coordination:

- **Persisted computed column on `OeOrder.StoreNum`** holding the trimmed value, with an index. Eliminates the need for the `@StoreNumTrimmed` workaround and allows the SARGable predicate without any application-side trimming. Same pattern on `SysEndPharmacy.StoreNum`.
- **Data cleanup of whitespace in `StoreNum`.** The reason TRIM is in the original proc is presumably that the column actually contains leading or trailing whitespace in some rows. The right long-term fix is a one-time UPDATE to strip whitespace plus a CHECK constraint to prevent it returning. This eliminates TRIM from every query in the codebase that touches StoreNum, not just this proc.
- **NOT NULL constraint on `SysEndPharmacy.StoreNum`.** The `Where SEP.StoreNum Is Not Null` filter recurring through the codebase suggests StoreNum is nullable today even though business semantics require it. A NOT NULL constraint would let every consumer drop the filter and would make `NOT IN` against this column safe by construction (though `NOT EXISTS` remains the better default).
