# lsp_ShpGetOrdersForTopReadyToShipGroup — Refactor Analysis (v24 → v25)

**Date:** 2026-05-07
**Tracking sheet row:** 15 (Priority P3, status In Progress)
**Pilot:** Yes — first procedure in the deep-dive series, used to validate the workflow.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_ShpGetOrdersForTopReadyToShipGroup`

**Purpose in one line:** Returns the list of Rx Groups that are ready (or timed-out) to be picked up by the Auto Shipper, ordered by `DateFilled`, with a comma-separated list of qualifying OrderIds per group.

**Tables touched:**

- `OeOrder` (the workhorse — 44 columns, joined or scanned in every part of the proc)
- `OeGroup` (12 columns; joined in Part 1 to filter on Group Status)
- `OeOrderShipmentAssoc` (5 columns; joined as anti-join to filter out orders that already have a shipment)
- `OeOrderCurrHistoryDtTm` and `OeOrderHistory` (both used in the `NOT EXISTS` rescheduling-window check in Part 1)
- `OrdWillCallBin` (joined in Part 2 to find Rxs that have been in a will-call bin past the timeout)
- `OrderToteAssoc` (joined in Part 3 to find Rxs that have been in a tote past the timeout)
- `OeOrderPoNumAssoc` (4 columns; joined inside the FOR XML PATH expression in Part 4)
- `vCfgSystemParamVal` (the system-parameter view, read once at the top to retrieve `PartialShipmentsTimeoutInMinutes`)

**Indexes used (extracted from the v25 plan file):**

| Table | Index | Used by |
|---|---|---|
| `OeOrder` | `PK_OeOrder` (clustered, `(OrderId, HistoryDtTm)`) | 6 references — Part 4 lookups, key lookups |
| `OeOrder` | `ByGroupNum` (nonclustered, key shape unknown) | 1 reference — FOR XML PATH inner |
| `OeOrder` | `ByOrderId` (nonclustered, key shape unknown) | 1 reference |
| `OeOrder` | `ByOrderStatus` (nonclustered, key shape unknown) | 1 reference — `#MultiVialReadyParents` build |
| `OeOrderHistory` | `ByGroupNum` (nonclustered, key shape unknown) | 1 reference — Part 1 NOT EXISTS, the 4,223-scan loop |
| `OeOrderCurrHistoryDtTm` | `PK_OeOrderCurrHistoryDtTm` (clustered) | 1 reference — Part 1 NOT EXISTS |
| `OeGroup` | `PK_OeGroup` (clustered, `(GroupNum)`) | 1 reference |
| `OeOrderShipmentAssoc` | `PK_OeOrderShipmentAssoc` (clustered) | 4 references — every LEFT JOIN |
| `OeOrderPoNumAssoc` | `PK_OeOrderPoNumAssoc` (clustered) | 1 reference — FOR XML PATH inner |

The new `#MultiVialReadyParents` temp table introduces a clustered index `IX_MVRP` on `OrderId` to support the EXISTS lookups in Parts 2, 3, and 4 (the v25 plan shows 274 scans / 549 logical reads on this index).

**Index DDL gap.** `MFCs/Tolleson/Tolleson_PA_tables.sql` contains only clustered primary keys. The four nonclustered indexes above (`OeOrder.ByGroupNum`, `ByOrderId`, `ByOrderStatus`, and `OeOrderHistory.ByGroupNum`) are visible in the execution plan but their key column orders and INCLUDE lists are not in the workspace. Specific index recommendations (covering, filtered, key-column reordering) require pulling this DDL via `sys.indexes`/`sys.index_columns` against Tolleson before they can be made with confidence.

**Callers:** Auto Shipper job. The proc is called frequently during shipment processing.

---

## 2. Overview of Performance

The tracking sheet flagged this proc on March/April single-site numbers (10.0B and 10.3B total reads, 259K-274K executions). The current cross-MFC view from the 2026-05-07 Query Store capture shows the systemic picture: **roughly 950 billion logical reads per month across 33 million executions, spread across 13 of 14 reporting sites**. This puts the proc in the top tier of I/O contributors fleet-wide, not just at any single site. The detailed per-site breakdown is in Section 3.1.

Plan instability is widespread. Six of 13 sites returned multiple plan variants for the same `query_id` in the top 50, with avg duration swings as large as 2x on identical statements (BrooklynPark: 16.9 to 32.5 ms). This is a textbook parameter-sniffing signal independent of the structural refactor. Per-site avg reads per execution ranges from 1,700 (Kent) to 63,000 (Bolingbrook, NorthLake), a 37x spread that suggests significant data-shape variance across MFCs — the sites with the most groups in 'Ready' status and the most multi-vial portions pay disproportionate cost.

The dominant cost driver in v24 is structural: the same `STUFF((... FOR XML PATH('')), 1, 1, '')` correlated subquery is repeated identically across three UNION branches in Part 4, and the multi-vial split parent check is a correlated scalar subquery repeated in Parts 2, 3, and inside each of the three FOR XML PATH expressions. The proc is doing the same lookups several times when one would suffice. The repeated work hits hardest at the high-volume, high-data-shape sites (Bolingbrook, Orlando, Tolleson, Mechanicsville, NorthLake) where the FOR XML PATH inner cost is largest and the call volume highest.

v25 consolidates the three driver sets into one, materializes the multi-vial parents once into a small indexed temp table, and runs the FOR XML PATH expression a single time against the consolidated set. A 30 to 50 percent reduction in per-execution reads — the target outlined in the tracking sheet — applied across the cross-MFC scale would save roughly 285 to 475 billion logical reads per month fleet-wide. The biggest absolute wins should land at the top 5 sites by reads (Bolingbrook, Orlando, Tolleson, Mechanicsville, NorthLake), which together account for 67 percent of the current load. Validation should sample at least one of the high-cost stable-plan sites (e.g., Tolleson at 119.9 ms steady) and one of the unstable-plan sites (e.g., BrooklynPark with the 2x swing) to confirm both the structural win and that the consolidated plan is more stable than the v24 form.

---

## 3. Evidence of Original (v24)

### 3.1 Query Store — cross-MFC view

**Source:** `diagnostics/querystore_outputs/QueryStore_20260507.xlsx` — top 50 offenders per site, 30-day lookback, captured 2026-05-07. One tab per site.

**Aggregation method:** for each site, all rows whose `objectname` matches `lsp_ShpGetOrdersForTopReadyToShipGroup` are aggregated. Multiple rows with the same `query_id` indicate plan instability (the same statement captured under different plan variants). Multiple distinct `query_ids` indicate that more than one statement inside the proc is appearing in the top 50.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------------|----------------|
| Bolingbrook | 1 | 1 | 2,659,837 | 167,930,591,506 | 63,136 | 121.7 | 233,316,100 |
| Orlando | 2 | 1 | 2,544,734 | 149,904,971,316 | 58,908 | 81.2 – 119.7 | 179,072,807 |
| Tolleson | 1 | 1 | 3,066,114 | 140,438,472,873 | 45,803 | 119.9 | 170,407,412 |
| Mechanicsville | 2 | 1 | 3,446,627 | 136,051,791,714 | 39,474 | 48.0 – 54.1 | 148,242,100 |
| NorthLake | 1 | 1 | 1,281,700 | 80,970,791,854 | 63,175 | 119.0 | 109,833,178 |
| Memphis | 2 | 1 | 2,790,182 | 64,585,568,648 | 23,147 | 19.3 – 31.7 | 65,025,323 |
| Canal | 1 | 1 | 3,040,859 | 63,776,537,548 | 20,973 | 27.4 | 74,375,708 |
| Mansfield | 2 | 1 | 2,810,204 | 53,471,198,380 | 19,028 | 32.4 – 43.8 | 77,065,788 |
| BrooklynPark | 3 | 1 | 2,290,413 | 38,248,356,159 | 16,699 | 16.9 – 32.5 | 51,500,345 |
| Liberty | 1 | 1 | 1,535,723 | 29,708,912,425 | 19,345 | 27.6 | 35,188,803 |
| Denver | 1 | 1 | 1,301,953 | 11,001,118,002 | 8,450 | 14.9 | 17,098,656 |
| Kent | 3 | 3 | 5,160,418 | 8,695,281,536 | 1,685 | 1.8 – 17.1 | 15,163,827 |
| West Jordan | 2 | 1 | 1,213,881 | 5,048,812,688 | 4,159 | 5.6 – 8.4 | 8,635,229 |
| Indy | — | — | — | — | — | — | — |

**Roll-up across the 13 reporting sites:** 33,142,645 executions, **949,832,404,649 total logical reads** (28,659 avg reads/exec). Indy is the only site where the proc did not appear in the top 50, suggesting either lower call volume or a plan that runs cheaply enough to fall outside the top offenders cut. Worth confirming whether Indy's call volume is lower or whether something about its data shape is fundamentally different.

**Observations:**

- **Plan instability is real and widespread.** Six of 13 sites returned multiple plan variants for the same `query_id`: Mansfield, Mechanicsville, Orlando, Memphis, BrooklynPark, West Jordan. BrooklynPark is the most extreme with three variants spanning 16.9 to 32.5 ms avg duration on an identical statement — almost a 2x swing driven entirely by plan choice. This is a strong parameter-sniffing signal even before we look at the v25 refactor.
- **Per-site avg reads/exec varies by 37x.** Bolingbrook and NorthLake sit at 63K reads/exec while Kent sits at ~1,700. Same proc, same code, dramatically different cost. This is consistent with significant data-shape variance across sites — likely driven by the count of `OeOrder` rows in `Ready To Ship` status, the count of multi-vial parents in `Split/MV`, and the prevalence of timed-out Rxs in will-call bins or totes. The v25 refactor's wins should compound at the sites with the highest avg reads/exec, since those are the sites where the FOR XML PATH consolidation has the most repeated work to eliminate.
- **Top 5 sites by total reads contribute 67% of the load.** Bolingbrook, Orlando, Tolleson, Mechanicsville, and NorthLake together account for 675B of the 950B reads. If we deploy v25 to those 5 sites first and capture a clean before/after, we will see the bulk of the systemic improvement.
- **No forced plans, no plan failures.** The `forcedplan` and `planfailures` columns are NULL for every row, so we don't have a forced-plan rollback to manage. Clean ground to work on.
- **Kent is an outlier.** Three distinct `query_ids` in top 50 (the only site with this pattern), but at very low avg reads/exec (~1,700) and very high exec count (5.1M). Kent's call volume is the highest in the fleet by a wide margin, but each call is cheap. The dominant cost driver at Kent will be exec count, not per-exec work. v25 will help here too but the win shape will be different (modest per-exec reduction × very high call count).

### 3.2 SET STATISTICS IO, TIME — Tolleson run, 2026-05-07 14:18 CT

Source: `lsp_ShpGetOrdersForTopReadyToShipGroup_results_Tolleson_20260507.txt` (paired with `lsp_ShpGetOrdersForTopReadyToShipGroup_original_plan.sqlplan`).

Per-table aggregated logical reads across all 4 statements (Parts 1, 2, 3, 4):

| Table | Logical reads | Scan count |
|-------|---------------|------------|
| `CfgSystemParamVal` | 2 | 0 |
| `CfgSystemParamDef` | 2 | 0 |
| `OeOrderShipmentAssoc` | 19,258 | 0 |
| `OeOrderHistory` | 16,823 | 4,187 |
| `OeGroup` | 17,368 | 0 |
| `OeOrder` | 13,054 | 1 + 230 + 892 = 1,123 |
| `OrdWillCallBin` | 7 | 1 |
| `OrderToteAssoc` | 24 | 2 |
| `OeOrderPoNumAssoc` | 0 | 0 |
| `#GroupsHavingAllReadyToShipRx` | 1 | 1 |
| `#GroupsHavingTimedOutInLocRx` | 1 | 1 |
| `#GroupsHavingTimedOutInToteRx` | 1 | 1 |
| **Total logical reads** | **~66,541** | |

Time per statement:

| Statement | CPU (ms) | Elapsed (ms) | Notes |
|-----------|----------|--------------|-------|
| Parse/compile (statement 1) | 120 | 120 | First-call compile |
| Part 1 build | 63 | 74 | `#GroupsHavingAllReadyToShipRx` |
| Part 2 build | 16 | 1 | `#GroupsHavingTimedOutInLocRx` |
| Part 3 build | 0 | 4 | `#GroupsHavingTimedOutInToteRx` |
| Part 4 build | 15 | 14 | (statement boundary) |
| Parse/compile (Part 4 select) | 31 | 37 | |
| Part 4 select | 0 | 25 | UNION of 3x FOR XML PATH |
| **Total execution** (excluding compile) | **94** | **118** | |

Plan shape from `_original_plan.sqlplan`: 140 RelOp operators total, including 6 UDX (XML PATH) nodes, 3 TOP, 5 Concatenation (UNION), 36 Nested Loops, 24 Clustered Index Seeks, 7 Index Seeks.

---

## 4. Issue Identification (v24)

The line numbers below reference `Original.sql` (v24).

### 4.1 Three legacy `Object_Id` temp-table existence checks (lines 85–86, 124–125, 158–159)

Each of the three temp tables is preceded by:

```sql
If Object_Id('TempDb..#GroupsHavingAllReadyToShipRx') Is Not Null
   Drop Table #GroupsHavingAllReadyToShipRx
```

Functional but verbose, and replaced in modern T-SQL by `Drop Table If Exists`. Cosmetic, not performance-critical, but reduces the line count and reads more cleanly.

### 4.2 Three separate driver temp tables (lines 88–109, 127–143, 161–177)

The proc creates `#GroupsHavingAllReadyToShipRx`, `#GroupsHavingTimedOutInLocRx`, and `#GroupsHavingTimedOutInToteRx` as three separate `SELECT INTO` operations. Each produces an identically shaped result (`GroupType`, `GroupNum`, `PriInternal`, `DateFilled`). The shapes already match, and the `GroupType` label already disambiguates the source, so there is no semantic reason to keep them separate.

### 4.3 Correlated scalar subquery for multi-vial split parent check (lines 138 and 172)

In Parts 2 and 3, the predicate for matching multi-vial split portions reads:

```sql
(O.OrderId Like '%-[0-9][0-9]'
   And (Select 1 From OeOrder With (NOLOCK)
        Where OrderId = Left(O.OrderId, Len(O.OrderId) - 3)
          And OrderStatus = 'Split/MV'
          And StatusReason = 'Ready To Ship') = 1)
```

This is a scalar subquery against `OeOrder` evaluated once per row of the outer query. Two issues compound:

- The scalar form `(Select 1 ...) = 1` is harder to read than the canonical `Exists (Select 1 ...)` form, has subtle NULL semantics if the subquery returns no rows, and produces a runtime error if it returns more than one row. The intent is exactly what `EXISTS` expresses cleanly.
- The same subquery shape (filter on `OrderStatus = 'Split/MV' And StatusReason = 'Ready To Ship'` against `OeOrder`) is repeated in Part 2, Part 3, and inside each of the three FOR XML PATH expressions in Part 4. That's five separate places where the engine recomputes the same lookup against the same table, on every execution.

### 4.4 Triple-repeated FOR XML PATH correlated subquery (lines 184–195, 199–210, 213–225)

Part 4 is structurally three nearly-identical `Select` blocks unioned together. Each block reads the same FOR XML PATH expression, joining `OeOrder` to `OeOrderPoNumAssoc` and `OeOrderShipmentAssoc`, with the same WHERE clause and the same multi-vial split correlated subquery embedded inside. The only thing that differs across the three blocks is which driver temp table they correlate to (`#GroupsHavingAllReadyToShipRx`, `#GroupsHavingTimedOutInLocRx`, or `#GroupsHavingTimedOutInToteRx`).

The cost of one FOR XML PATH execution is multiplied by three, even though the FOR XML PATH logic itself is identical. Each execution generates its own seeks/scans against `OeOrder`, `OeOrderPoNumAssoc`, and `OeOrderShipmentAssoc`. At 60K-80K reads per execution and three executions per call, this is the proc's dominant cost driver.

### 4.5 UNION (not UNION ALL) at the top of Part 4 (lines 197 and 212)

The three Part 4 blocks are combined with `UNION`, which forces the engine to deduplicate the result. The `GroupType` column carries one of three distinct labels per branch, guaranteeing that no row produced by branch A could ever match a row produced by branch B. The dedup sort is wasted work.

### 4.6 Inconsistent capitalization on `WITH (NoLock)` and `nolock` (multiple lines)

A purely cosmetic issue, but the proc mixes `With (NoLock)` and `with (nolock)` (and on line 138, the embedded subquery uses `With (NOLOCK)`). Consistency improves readability and reduces the chance of someone missing one in a future edit.

---

## 5. First Principles

### 5.1 FOR XML PATH Consolidation

When the same FOR XML PATH expression appears in multiple UNION branches that produce the same column shape, the engine evaluates the expression once per branch. Consolidating the branches into a single materialized driver set (via UNION ALL into a temp table) and running the FOR XML PATH once against the consolidated set replaces three plan operators with one, eliminates the UNION's dedup sort, and gives the optimizer a single cardinality estimate to plan against rather than three independent ones. See [[FOR XML PATH Consolidation]] for the full pattern.

### 5.2 Materialize-Once for Repeated Correlated Lookups

When a correlated subquery references the same table with the same filter from multiple call sites, materializing the lookup once into an indexed temp table and probing via `EXISTS` collapses the repeated work. The general principle is in [[Correlated Subqueries to CTEs]], with the specific note that "if you reference the same CTE in multiple places, consider materializing it into a #temp table instead." That is exactly what v25 does for `#MultiVialReadyParents` — the multi-vial split parent set is materialized once with a clustered index on `OrderId`, then probed via `EXISTS` from Part 2, Part 3, and the consolidated FOR XML PATH in Part 4.

### 5.3 `(Select 1 ...) = 1` vs `EXISTS (Select 1 ...)`

The scalar subquery comparison form is functionally similar to `EXISTS` for the case where the subquery returns either zero rows or one row containing the value 1, but it has three drawbacks compared to `EXISTS`:

- It does not allow the engine to short-circuit on the first matching row. `EXISTS` stops reading as soon as it finds a single row; the scalar form has to evaluate the expression and return a value.
- If the subquery returns more than one row, the scalar form raises a runtime error. `EXISTS` just returns true.
- The intent is less clear to a reader. `EXISTS (Select 1 From ... Where ...)` reads as "does a matching row exist?" The scalar form reads as "does the result of this lookup equal 1?", which forces the reader to reason about both the value and the existence simultaneously.

`EXISTS` is the canonical form for "does at least one matching row exist," and v25 uses it consistently.

### 5.4 UNION vs UNION ALL when Label Columns Prevent Overlap

`UNION` removes duplicates by sorting (or hashing) the combined result. When a label column in the SELECT list guarantees that rows from different branches cannot match (in this case, `GroupType` always carries one of three distinct values), the dedup is wasted work. `UNION ALL` skips the dedup and concatenates the branches directly. The optimizer cannot infer this safely on its own — it does not know that the labels are mutually exclusive — so the human change is required.

### 5.5 Modern Temp-Table Existence Checks

`Drop Table If Exists` was introduced in SQL Server 2016 and supersedes the legacy `If Object_Id('TempDb..#X') Is Not Null Drop Table #X` pattern. The legacy form still works, but the modern form is shorter, more readable, and harder to typo (the `Object_Id` form requires the `'TempDb..'` prefix, which is easy to miss). v25 standardizes on the modern form.

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v25. The narrative below walks the changes block by block. Line references are to `Refactored.sql`.

### 6.1 New Block: Materialize multi-vial split parents (lines 71–80)

```sql
Drop Table If Exists #MultiVialReadyParents

Select OrderId
Into #MultiVialReadyParents
From OeOrder With (NoLock)
Where OrderStatus = 'Split/MV'
  And StatusReason = 'Ready To Ship'

Create Clustered Index IX_MVRP On #MultiVialReadyParents (OrderId)
```

This block is net-new in v25. It runs once at the top of the proc and produces a small temp table containing every `OrderId` whose order status is `Split/MV` and status reason is `Ready To Ship`. A clustered index on `OrderId` supports the `EXISTS` probes that follow. This single materialization replaces the five separate correlated subqueries that v24 had against this same row set (two in Parts 2 and 3, three more inside the Part 4 FOR XML PATH expressions).

### 6.2 Consolidated Driver: #AllQualifyingGroups (lines 86–167)

```sql
Drop Table If Exists #AllQualifyingGroups

;With QualifyingGroups As
(
    -- Part 1: All-ready-Rxs Groups
    Select 'All Rxs Ready' As GroupType, ... From OeOrder O ... Where ...
    Group By O.GroupNum
    Option (Maxdop 1)

    Union All

    -- Part 2: Rx timed-out in storage location
    Select 'Timed Out In Loc' As GroupType, ... From OeOrder O ... Where ...
       And (... Or (O.OrderId Like '%-[0-9][0-9]'
                    And Exists (Select 1 From #MultiVialReadyParents P
                                Where P.OrderId = Left(O.OrderId, Len(O.OrderId) - 3))))
    Group By O.GroupNum

    Union All

    -- Part 3: Rx timed-out in tote
    Select 'Timed Out In Tote' As GroupType, ... From OeOrder O ... Where ...
       And (... Or (O.OrderId Like '%-[0-9][0-9]'
                    And Exists (Select 1 From #MultiVialReadyParents P
                                Where P.OrderId = Left(O.OrderId, Len(O.OrderId) - 3))))
    Group By O.GroupNum
)
Select GroupType, GroupNum, PriInternal, DateFilled
Into #AllQualifyingGroups
From QualifyingGroups
```

Three v24 changes are folded together here:

- The three `SELECT INTO` statements collapse into a single CTE with three `UNION ALL` branches, then materialize once into `#AllQualifyingGroups`. The `GroupType` label disambiguates rows from each branch.
- `UNION ALL` is correct because `GroupType` makes overlap impossible. The implicit dedup of the v24 `UNION` was wasted work.
- The two correlated multi-vial scalar subqueries become `EXISTS` probes against `#MultiVialReadyParents`. Each probe is a clustered-index seek against a small temp table — far cheaper than re-scanning `OeOrder` filtered on status.

The `Maxdop 1` hint on Part 1 is preserved exactly as it was in v24. It is presumably there to control concurrency or work around a parameter-sniffing parallel-plan issue specific to that branch. Removing it or extending it to the other branches is out of scope for v25; we are intentionally minimizing semantic-risk changes.

### 6.3 Single FOR XML PATH (lines 175–195)

```sql
Select GRx.GroupType, GRx.GroupNum, GRx.PriInternal, GRx.DateFilled,
       Stuff((Select N',' + O.OrderId
              From OeOrder O With (NoLock)
              Inner Join OeOrderPoNumAssoc OP With (NoLock) On OP.OrderId = O.OrderId
              Left Join OeOrderShipmentAssoc OS With (NoLock) On OS.OrderId = O.OrderId
                                                                And OS.HistoryDtTm = O.HistoryDtTm
              Where O.GroupNum = GRx.GroupNum
                And OS.ShipmentId Is Null
                And ( (O.OrderStatus = 'Ready To Ship' And Not O.OrderId Like '%-[0-9][0-9]')
                      Or (O.OrderId Like '%-[0-9][0-9]'
                          And Exists (Select 1 From #MultiVialReadyParents P
                                      Where P.OrderId = Left(O.OrderId, Len(O.OrderId) - 3))))
              Order By O.OrderId
              For Xml Path(''), Type).value(N'.[1]', N'nVarChar(max)'), 1, 1, N'') As Orders
From #AllQualifyingGroups GRx
Order By GRx.DateFilled Asc
```

The FOR XML PATH expression is unchanged structurally. What changed is the outer driver: it now reads from `#AllQualifyingGroups` (which carries all three categories with their `GroupType` labels) rather than from one of three separate temp tables joined via `UNION`. The expression evaluates once over the combined driver set instead of three times over three separate driver sets. The embedded multi-vial check uses the same `EXISTS` against `#MultiVialReadyParents` as the consolidated driver, eliminating three more correlated subqueries.

### 6.4 Cleanup (lines 198–199)

```sql
Drop Table If Exists #AllQualifyingGroups
Drop Table If Exists #MultiVialReadyParents
```

Explicit cleanup at the end of the proc. SQL Server cleans up session-scoped temp tables automatically when the session ends, but explicit drops free tempdb pages immediately and avoid carrying state across rapid back-to-back invocations of the proc.

---

## 7. Risk & Rollback

### Risks

- **Semantic drift on the multi-vial check.** The v24 form `(Select 1 ... ) = 1` returns false when the subquery returns no rows (because `NULL = 1` is false). The v25 form `Exists (...)` returns false in the same case. They should be equivalent. The risk is that the v24 form had been silently masking a multi-row subquery (which would have raised an error); if so, v25 may now match rows that v24 was erroring on. We expect this risk to be zero in practice — the filter on `OrderStatus = 'Split/MV' And StatusReason = 'Ready To Ship'` against `OrderId = Left(...)` should produce at most one match — but it is worth confirming during validation that v24's representative run did not raise any subquery errors.
- **Plan choice on the consolidated CTE.** The three v24 driver SELECTs each had their own plan tuned to their own cardinality. The v25 CTE materialization will be planned with a single combined cardinality estimate. If any one of the three branches has a cardinality that is dramatically different from the combined average (for example, Part 1 returns thousands of rows but Parts 2 and 3 return tens), the optimizer may choose join orders inside the branches that are slightly suboptimal for the smaller branches. This is unlikely to be a net regression because the dominant cost is the FOR XML PATH consolidation, but it is the place to look if anything underperforms.
- **The Maxdop 1 hint on Part 1.** Preserving this hint inside a CTE branch is correct syntactically, but its effect on the overall consolidated plan is worth verifying. If the optimizer applies `Maxdop 1` to the entire consolidated plan rather than just Part 1's contribution, we may unintentionally serialize work that was previously parallel in Parts 2 and 3.

### Rollback

The proc is a read-only `SELECT` returning a result set. Rollback is a pure DDL revert: restore the v24 body via `ALTER PROCEDURE`. No data state is mutated, so there is no compensating action required.

If a regression appears, run the v24 form (committed at git revision `b4bf42c`) against the same data state to confirm. Rollback path:

```sql
-- pull v24 from git
git show b4bf42c:stored_procedures/lsp_ShpGetOrdersForTopReadyToShipGroup.sql

-- adjust ALTER -> CREATE if needed, deploy
```

### Monitoring Window

For the first 24 hours after deployment, watch:

- Auto Shipper job log for any errors or unexpectedly empty result sets.
- Query Store for `lsp_ShpGetOrdersForTopReadyToShipGroup` average duration, average reads, and plan count. Plan count should stabilize at 1 or 2 once the new shape is cached.
- Tempdb allocation. The two new temp tables are small, but on a high-call-volume system (270K execs/month means roughly one execution every 9 to 10 seconds at steady state), tempdb churn is worth a glance.

---

## 8. Evidence of Refactor (v25)

Source: same results file as Section 3.2, second half. Tolleson run, 2026-05-07 14:20 CT (about 2.5 minutes after the v24 capture). Paired with `lsp_ShpGetOrdersForTopReadyToShipGroup_refactor_plan.sqlplan`.

**Important deployment-vs-test discrepancy.** The v25 body that was tested has the `Option (Maxdop 1)` hint on Part 1 commented out (`--Option (Maxdop 1)`). The body committed in `Refactored.sql` has the hint active. We need to align these before deployment — see Section 9 for implications.

Per-table aggregated logical reads across all 5 statements (#MVRP build, index, CTE consolidation, Part 4 select):

| Table | Logical reads | Scan count |
|-------|---------------|------------|
| `CfgSystemParamVal` | 2 | 0 |
| `CfgSystemParamDef` | 2 | 0 |
| `OeOrder` (build #MVRP) | 4,751 | 1 |
| `#MultiVialReadyParents` (index create) | 1 | 1 |
| `OeOrderShipmentAssoc` | 19,266 | 0 |
| `OeOrderHistory` | 16,967 | 4,223 |
| `OeGroup` | 17,514 | 0 |
| `OeOrder` (CTE + Part 4) | 11,330 | 802 + 1 = 803 |
| `OrdWillCallBin` | 7 | 1 |
| `OrderToteAssoc` | 24 | 2 |
| `OeOrderPoNumAssoc` | 4 | 0 |
| `#MultiVialReadyParents` (EXISTS probes) | 549 | 274 |
| `#AllQualifyingGroups` | 1 | 1 |
| **Total logical reads** | **~70,418** | |

Time per statement:

| Statement | CPU (ms) | Elapsed (ms) | Notes |
|-----------|----------|--------------|-------|
| Build `#MultiVialReadyParents` | 16 | 16 | Scan of OeOrder filtered to Split/MV |
| Create clustered index on #MVRP | 0 | 2 | |
| Parse/compile (CTE build) | 147 | 147 | First compile of consolidated CTE |
| Build `#AllQualifyingGroups` | 78 | 84 | UNION ALL of 3 branches into temp |
| Parse/compile (Part 4 select) | 19 | 19 | |
| Part 4 select | 0 | 0 | Single FOR XML PATH |
| **Total execution** (excluding compile) | **94** | **102** | |

Plan shape from `_refactor_plan.sqlplan`: 87 RelOp operators total, including 2 UDX (XML PATH) nodes, 1 TOP, 3 Concatenation (UNION ALL), 21 Nested Loops, 16 Clustered Index Seeks, 4 Index Seeks. Operator count is 38% lower than v24 and UDX count is 67% lower (6 → 2). The plan-shape prediction is exactly what we expected.

---

## 9. Comparison & Improvement

### 9.1 Verdict — this run is structurally promising but not conclusive on cost

The plan-shape changes match the prediction precisely: operator count dropped from 140 to 87 (–38%), UDX (FOR XML PATH builder) operators dropped from 6 to 2 (–67%), TOP operators from 3 to 1, UNION Concatenation from 5 to 3. The structural refactor landed exactly as designed.

The cost numbers, however, **cannot support a "v25 is faster" claim from this single run**, and per `tasks/lessons.md` we do not declare a winner from non-representative data. Two issues with the evidence:

1. **The result set was effectively empty.** Both v24 and v25 produced essentially zero work on Part 4 (the FOR XML PATH execution). v24's Part 4 cost only 25 ms elapsed with no reads on `OeOrder`, `OeOrderPoNumAssoc`, or `OeOrderShipmentAssoc` from inside the FOR XML PATH expressions — meaning the driver temp tables held zero or near-zero qualifying rows. The dominant cost driver of v24, namely the triple-repeated FOR XML PATH inner work, was never exercised. From production Query Store we know Tolleson averages ~46K reads per execution over 30 days, so 70K total reads in this test run is below the per-call average — the test caught a quiet moment.
2. **The Maxdop hint is commented out in the test version but active in `Refactored.sql`.** This is a real divergence we need to resolve before deployment. Either (a) the hint should be removed from `Refactored.sql`, or (b) the test should be re-run with the hint active. The Part 1 build subtree is the largest cost in the consolidated CTE (cost 30.09 of the 34.83 v25 plan), so the hint's effect on that subtree matters.

### 9.2 What the data does support

| Metric | v24 | v25 | Delta |
|--------|-----|-----|-------|
| Plan operators total | 140 | 87 | **–38%** |
| UDX (XML PATH builder) | 6 | 2 | **–67%** |
| TOP operators | 3 | 1 | –67% |
| Concatenation operators | 5 | 3 | –40% |
| Nested Loops | 36 | 21 | –42% |
| Statements in plan | 5 | 5 | tie |
| Estimated subtree cost (sum) | ~33.5 | ~34.8 | +4% |

The plan is structurally simpler in exactly the ways v25 was supposed to make it simpler. The estimated subtree costs are nearly identical, which is a feature: for a near-empty driver state, both plans should compile to roughly equal cost, because the dominant savings of v25 come from collapsing repeated work that scales with driver row count.

### 9.3 Per-table comparison on this run

| Table / object | v24 reads | v25 reads | Delta | Note |
|----------------|-----------|-----------|-------|------|
| `OeOrder` (driver builds) | 13,054 | 16,075 | +3,021 | v25 includes 4,751-read scan to materialize #MVRP |
| `OeOrderShipmentAssoc` | 19,258 | 19,266 | +8 | flat |
| `OeOrderHistory` | 16,823 | 16,967 | +144 | flat (Part 1 logic unchanged) |
| `OeGroup` | 17,368 | 17,514 | +146 | flat (Part 1 logic unchanged) |
| `OrdWillCallBin` | 7 | 7 | 0 | tie |
| `OrderToteAssoc` | 24 | 24 | 0 | tie |
| `OeOrder` (Part 4) | 0 | 6 | +6 | both effectively zero — empty driver |
| `OeOrderPoNumAssoc` (Part 4) | 0 | 4 | +4 | both effectively zero — empty driver |
| `#MultiVialReadyParents` | n/a | 549 | +549 | EXISTS probes (274 scans) |
| 3 v24 driver temp tables | 3 | n/a | –3 | |
| `#AllQualifyingGroups` | n/a | 1 | +1 | |
| **Total logical reads** | **66,541** | **70,418** | **+3,877 (+5.8%)** | |
| **CPU time (ms)** | **94** | **94** | tie | |
| **Elapsed time (ms)** | **118** | **102** | **–14%** | wall clock |

The +5.8% reads on v25 are explained almost entirely by two new costs that are paid up front and that v24 didn't pay: the 4,751 reads to scan `OeOrder` and build `#MultiVialReadyParents`, and the 549 reads of `#MultiVialReadyParents` from the EXISTS probes. These are fixed-overhead costs that v25 takes regardless of driver row count. The win — the FOR XML PATH consolidation — only shows up when the driver row count is non-trivial. With ~0 driver rows, the consolidation can't save anything because there was nothing to repeat.

### 9.4 Result-set verification

The output text file does not include the `(N rows affected)` lines for either run, so I can't confirm row count parity from this evidence alone. Need to add `Set NoCount Off` to the test harness, or capture the result grid alongside, before declaring identical-result-set.

### 9.5 What we need next

To close out the comparison cleanly, one of:

1. **Re-run on a representative driver state.** Either capture during a busy window when the proc has many qualifying groups, or stage data so Part 4 has a meaningful driver row count. Targets: at least 50-100 driver rows total across the three branches, so the FOR XML PATH inner work runs at scale. Run both v24 and v25 back to back, both warm cache, against the same data state.
2. **Deploy v25 to a single MFC and measure in production.** Watch Query Store avg reads/exec for the proc over 24-48 hours post-deployment. The cross-MFC numbers in Section 3.1 give a strong baseline (Tolleson at 45,803 avg reads/exec) — a meaningful drop in that number will validate the refactor at scale. The plan-shape evidence above is enough to justify the production deployment as a measured experiment.
3. **Resolve the Maxdop discrepancy.** Decide whether the v25 deployment includes `Option (Maxdop 1)` on Part 1 or not. If yes, re-test with the hint active. If no, remove the commented line from `Refactored.sql`.

---

## 10. Validation Checklist

Per `tasks/lessons.md`. Marked against the Tolleson 2026-05-07 run.

- [⚠️] **Same data state** — captures taken ~2.5 minutes apart on the same instance, no explicit data freeze. Probably close to identical, but not guaranteed. Acceptable for this exploratory run; should be tightened for the re-run.
- [⚠️] **Warm cache only** — uncertain. The output does not show whether a warm-up execution preceded each capture. Physical reads were zero on both, which suggests warm cache (or cached pages from prior workload), but not strict proof. Tighten in the re-run with an explicit warm-up step.
- [❌] **Non-zero result set** — fails. Part 4 effectively returned zero qualifying groups, evidenced by zero reads on `OeOrder` / `OeOrderPoNumAssoc` / `OeOrderShipmentAssoc` from inside the v24 FOR XML PATH expressions and a 25 ms elapsed Part 4 statement. The dominant cost driver of v24 was not exercised, so the comparison cannot speak to the FOR XML PATH consolidation win.
- [?] **Identical result set** — cannot verify from this output. The `(N rows affected)` lines are not present. Need `Set NoCount Off` or a result-grid capture in the re-run.
- [✅] **Plan shape matches prediction** — passes cleanly. v25 plan has 2 UDX operators (vs v24's 6), 1 TOP (vs 3), 3 Concatenation operators (vs 5 — and the v25 ones are UNION ALL inside the CTE rather than the v24 UNION at the top). EXISTS probes against `#MultiVialReadyParents` are visible in the v25 plan as 274 scans / 549 logical reads. This was the biggest single-checkpoint risk on the refactor and it passed.
- [✅] **No new error or warning messages** — neither run produced cardinality errors, conversion warnings, or excessive memory grant warnings.
- [✅] **Warm-cache elapsed time is at or below v24** — v25 is 102 ms vs v24's 118 ms (–14%), even with the 5.8% read overhead and an extra parse/compile pass for the CTE. CPU is a tie at 94 ms each. The wall-clock improvement on a near-empty driver state is consistent with the operator-count reduction translating into less coordination overhead.

**Net call:** plan shape and structural refactor are validated. Cost numbers are inconclusive due to the empty driver state. Two clean paths forward are listed in Section 9.5.

---

## 11. Open Items / Future Improvements (out of scope for v25)

These are observations made during the deep dive that are intentionally not addressed in v25. They are noted here so they don't get lost.

- **Non-SARGable `OrderId Like '%-[0-9][0-9]'` predicate.** Used in Parts 2, 3, and inside the FOR XML PATH. This forces a scan over the predicate column. A persisted computed column with an index, or a separate `IsMultiVialPortion` flag column maintained on insert, would convert these scans to seeks. Schema change required, so out of scope here.
- **Non-SARGable `Left(O.OrderId, Len(O.OrderId) - 3)` parent lookup.** Same kind of issue. A persisted computed column for `ParentOrderId` would let the EXISTS probe become a direct seek without the string manipulation. Schema change required.
- **`Maxdop 1` on Part 1.** The hint is preserved from v24 with the assumption it was added intentionally. Worth investigating whether it's still needed in the consolidated plan, or whether it can be lifted to allow parallelism on Part 1.
- **The Part 1 `Not Exists` block.** The 2-table join inside the EXISTS (`OeOrderCurrHistoryDtTm` + `OeOrderHistory`) is unchanged from v24. Worth checking index coverage as a follow-up if v25's per-execution reads are still high.
