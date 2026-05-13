# lsp_PrtaGetNextShipmentIdToPrintForStation: Refactor Analysis (v17 to v18)

**Date:** 2026-05-08
**Tracking sheet row:** 24 (Priority P0, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v18 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_PrtaGetNextShipmentIdToPrintForStation`

**Purpose in one line:** For a given workstation station Id, return the next shipment whose paperwork should be printed and the list of orders belonging to that shipment, choosing the shipment by internal priority among the shipments that have every non-canceled Rx accounted for (either in a will-call bin or in a station tote queue).

**Tables touched:**

- `OeOrder` (the workhorse; queried in the bin scan, in all three pack-queue scans, and in the final order-list output).
- `OeOrderShipmentAssoc` (the order-to-shipment association; queried in the bin scan, the bin priority lookup, all three pack-queue scans, the non-canceled-count materialization, and the final order list).
- `OeOrderHistory` (queried in the original procedure's correlated subqueries against non-canceled count; v18 materializes the join once into `#ShipmentNonCanceledCount`).
- `OeOrderPoNumAssoc` (joined for PoNum in the final output).
- `OrderToteAssoc` (joined in the pack-queue scans and the final output).
- `OrdWillCallBin` (joined in the bin scan).
- `ShpStationWillCallBinAssoc` (joined in the bin scan to filter by the station's bin prefixes).
- `PwkPrinterTray` (left-joined throughout to exclude shipments already printed).
- `OeWorkflowStepInProgress` (left-joined in the in-progress exclusion).
- `CvyManPackStationToteQueue`, `CvySemiAutoPackStationToteQueue`, `CvyAutoPackStationVialQueue` (the three pack queues, one per branch).

**Indexes used (predicted from the v18 body, to be confirmed against the captured plan):**

| Table | Index | Where used |
|---|---|---|
| `OeOrder` | `ByOrderStatus` (NC, key `OrderStatus`) | the bin scan, the pack-queue scans |
| `OeOrder` | `PK_OeOrder` (clustered, key `OrderId, HistoryDtTm`) | joins via `OrderId, HistoryDtTm` |
| `OeOrderShipmentAssoc` | clustered or NC on `OrderId, HistoryDtTm` | join into shipment association |
| `OeOrderShipmentAssoc` | NC on `ShipmentId` (assumed) | non-canceled count lookup |
| `OeOrderHistory` | clustered on `OrderId, HistoryDtTm` | left join in non-canceled count |
| `OrdWillCallBin` | clustered on `OrderId` (assumed) | bin scan |
| `ShpStationWillCallBinAssoc` | clustered or NC on `StationId, BinIdPrefix` | bin-prefix LIKE join |
| `PwkPrinterTray` | clustered or NC on `ShipmentId` (assumed) | exclusion |
| `OeWorkflowStepInProgress` | clustered or NC on `ObjectKey` | in-progress exclusion (typed via Try_Cast in v18) |

**Index DDL gap.** Most of the tables here are not in the pilot index extract. Confirm the indexes against Tolleson before final sign-off.

**Callers:** the paperwork printer service at each workstation. Called on a regular polling cadence to decide which shipment's paperwork to print next. The 19,839x worst-case-to-average ratio in the cross-list capture reflects the polling cadence interacting with the heavy parameter sniffing on `@StationId`.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on all four expense lists (Volume rank 9, Duration rank 8, Expense rank 20, Plan rank 9) with a 19,839x worst-case-to-average ratio, the largest plan-volatility signal in the rows 22-25 cohort. The volatility is driven by two factors: parameter sniffing on `@StationId`, where the per-station candidate population varies by orders of magnitude across workstations within a single MFC; and the procedure's structural complexity, which materializes nine temp tables in sequence and produces an execution plan whose shape is sensitive to the cardinality of every intermediate set.

The dominant cost drivers in v17 are five. The first is parameter sniffing on `@StationId`, which compiles plans against the first call's selectivity and reuses them for subsequent calls with different selectivities. The second is the doubly-evaluated correlated scalar subquery against `OeOrderShipmentAssoc` joined to `OeOrderHistory`, used in two places (the in-bin completeness check at line 237 and the top-shipment HAVING clause at line 278) to count non-canceled Rxs per shipment. The third is the `STR(ShipmentId)` join key against `OeWorkflowStepInProgress.ObjectKey`, used in two places with no supporting index because the cast is performed per row of the outer table. The fourth is the absence of indexes on any of the nine intermediate temp tables, despite several of them being joined to in downstream queries. The fifth is the nine separate `If Object_Id ... Drop Table` blocks at the top of the procedure, which is a cosmetic issue but is ubiquitous in the procedure library and worth fixing on every refactor.

The MaxDop 1 hint applied to every statement in the procedure is documented in the v17 header as an operational workaround for parallel-thread starvation errors observed in at least one fielded system. v18 preserves the hint on every statement to avoid changing the runtime characteristic the workaround addresses, and Section 11 carries a follow-up to re-evaluate the hint on current SQL Server versions where thread availability is generally less constrained.

v18 addresses the four code-level issues directly and notes the MaxDop 1 question as an open item rather than a same-PR change.

---

## 3. Evidence of Original (v17)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Max-vs-avg ratio |
|-----|---------------|------------------|-------------|----------------|--------------------|------------------|
| (paste from capture) | | | | | | |

**Roll-up across reporting sites:** to be filled in from the May 7 capture. The cross-list capture reports the procedure on all four expense lists with the 19,839x worst-case-to-average ratio as the dominant signal.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v17)

Captured against Tolleson. Warm cache. Two captures at different `@StationId` values are needed to expose the plan-volatility signature.

```
(paste STATS IO / STATS TIME for v17 against the same data state used for v18 here)
```

The expected v17 signature is two passes over `OeOrderShipmentAssoc + OeOrderHistory` (once per occurrence of the correlated scalar subquery), nine sequential `Object_Id` lookups against tempdb during the cleanup block, and multiple `STR(ShipmentId)` conversions against `OeWorkflowStepInProgress.ObjectKey`.

---

## 4. Issue Identification

The v17 body has five distinct issues. Each one corresponds to a fix in v18.

**Issue 1: Parameter sniffing on `@StationId` (every statement in v17).** The parameter is referenced directly in every WHERE clause that filters by station. The per-station candidate population varies dramatically, and the plan compiled for a low-volume station performs poorly when reused for a high-volume station and vice versa. The 19,839x worst-case-to-average ratio is the direct signature.

**Issue 2: Correlated scalar subquery for non-canceled Rx count, executed twice (lines 237-238 and 278-279 in v17).** The same correlated subquery shape appears in two distinct WHERE/HAVING clauses, each evaluating the count per outer row. The cost scales with the candidate set at each occurrence.

**Issue 3: `STR(ShipmentId)` join key against `OeWorkflowStepInProgress.ObjectKey` (lines 235, 255 in v17).** The conversion is performed per outer row, defeating any seek into a `ObjectKey` index. The right approach is to pre-materialize the in-progress shipment IDs once with a clean cast and join on a typed key.

**Issue 4: Legacy `If Object_Id ... Drop Table` block, nine separate statements (lines 38-46 in v17).** The modern equivalent is a single `Drop Table If Exists` statement listing all the temp tables. Small in absolute terms, but ubiquitous in the procedure library.

**Issue 5: No indexes on intermediate temp tables.** Six of the nine temp tables participate in downstream joins and have no indexes. The cost of the missing indexes is hidden during the build phase and becomes visible during the join phase.

A sixth observation that v18 does not address but Section 11 flags is the per-statement `Option (MaxDop 1)` hint. The hint is a documented workaround for an operational issue and is preserved in v18, but it is worth re-evaluating against current server configurations.

---

## 5. First Principles

The refactor draws on four masterclass entries.

**[[Parameter Sniffing]] with the local-variable form.** `@LocalStationId` is assigned from `@StationId` at the top of the procedure. Every downstream query references the local. The optimizer caches plans against the local-variable-typed plan, which gives a stable plan across calls with different station selectivities.

**[[Correlated Subqueries to CTEs]] with the pre-materialize-once fix.** The non-canceled Rx count per ShipmentId is pre-materialized into `#ShipmentNonCanceledCount` once, restricted to the candidate ShipmentIds that survive the upstream filters. Both downstream consumers (the in-bin completeness check and the top-shipment HAVING clause) reference the materialized count via a clean join. The cost moves from per-row to per-set.

**[[Non-SARGable Predicates]] applied to the STR conversion.** `WFS.ObjectKey = STR(t.ShipmentId)` forces the optimizer to materialize the cast per outer row. v18 pre-materializes the in-progress shipment IDs once via `Try_Cast(ObjectKey As Int)` into an indexed temp table, and downstream queries left-join to that on a typed key. The principle is that conversions on the join key should happen once per side, not once per pairing.

**[[Index Key Columns vs Included Columns]] applied to temp tables.** Indexing intermediate temp tables that participate in downstream joins is one of the cheapest wins available. v18 adds an explicit index after the population of every temp table that gets joined to.

A pattern that recurs across this procedure and several others in the library is the "build N similar candidate sets, UNION them, then pick the top one." It is a reasonable structural choice when the source tables genuinely differ in shape, but it produces N intermediate temp tables that could in principle be a single UNION ALL view. The three pack-queue scans (ManPack, SemiPack, AutoPack) are this pattern. v18 does not consolidate them because they are structurally distinct enough that the consolidation could pessimize the optimizer's plan choice for the inner branches; the cost of three temp tables is bounded and well understood, while the cost of a consolidated UNION ALL plan is not. Section 11 carries the recommendation to evaluate the consolidation after v18 stabilizes.

---

## 6. Refactor (commented)

The v18 body is reproduced in `Refactored.sql`. The salient blocks below.

**Local variable and modern temp-table cleanup.**

```sql
Declare @LocalStationId VarChar(8) = @StationId

Drop Table If Exists
      #AllRxsArrivedForShipmentId, #AllItemsStoredinBins, #TopShipmentToPrint
    , #ShipmentIdInBin, #TopShipmentIdInBin
    , #TopShipmentIdInManPack, #TopShipmentIdInSemiPack, #TopShipmentIdInAutoPack
    , #CvyRxsArrivedForShipmentId, #InProgressShipments, #ShipmentNonCanceledCount
```

**Pre-materialized in-progress shipment IDs with a typed key.**

```sql
Select Distinct
      [ShipmentIdAsKey] = Try_Cast(ObjectKey As Int)
Into #InProgressShipments
From OeWorkflowStepInProgress WFS With (NoLock)
Where Try_Cast(WFS.ObjectKey As Int) Is Not Null
Option (MaxDop 1, Recompile);

Create Index IX_InProg_ShipmentId On #InProgressShipments(ShipmentIdAsKey);
```

The `Try_Cast` is performed once per row of `OeWorkflowStepInProgress` rather than once per row of every downstream consumer. Rows whose ObjectKey is not a shipment-style integer are filtered out by the IS NOT NULL check.

**Pre-materialized non-canceled count.**

```sql
Select
      OSA.ShipmentId
    , [NonCanceledCount] = Count(OSA.OrderId)
Into #ShipmentNonCanceledCount
From OeOrderShipmentAssoc OSA With (NoLock)
Left Join OeOrderHistory OH With (NoLock)
    On OSA.OrderId = OH.OrderId And OSA.HistoryDtTm = OH.HistoryDtTm
Where OSA.ShipmentId In
(
    Select ShipmentId From #TopShipmentIdInBin
    Union
    Select ShipmentId From #CvyRxsArrivedForShipmentId
)
  And IsNull(OH.OrderStatus, '') <> 'Canceled'
Group By OSA.ShipmentId
Option (MaxDop 1);

Create Index IX_SNC_ShipmentId On #ShipmentNonCanceledCount(ShipmentId);
```

The count is computed once over the candidate ShipmentIds. Both downstream consumers read from `#ShipmentNonCanceledCount` via a clean join.

**In-bin completeness branch and final shipment selection use the pre-materialized count.**

```sql
-- In-bin completeness branch
From #TopShipmentIdInBin t
Left Join #InProgressShipments IPS  On IPS.ShipmentIdAsKey = t.ShipmentId
Left Join #ShipmentNonCanceledCount SNC On SNC.ShipmentId = t.ShipmentId
Where IPS.ShipmentIdAsKey Is Null
  And SNC.NonCanceledCount = t.RxCount

-- Final top-1 selection
From (...) tt
Inner Join #ShipmentNonCanceledCount SNC On SNC.ShipmentId = tt.ShipmentId
Group By ...
Having SNC.NonCanceledCount = Sum(tt.SumOfAllRxsArrivedForShipmentId)
```

The correlated scalar subqueries are gone. The in-progress check is a clean left-join with NULL probe against the typed key.

**Final order-list query uses a local variable read once.**

```sql
Declare @TopShipmentId Int
Select Top 1 @TopShipmentId = ShipmentId From #TopShipmentToPrint

Select ... From OeOrder O With (NoLock)
... Where OSA.ShipmentId = @TopShipmentId
Order By O.PriInternal Asc
Option (MaxDop 1);
```

---

## 7. Risk & Rollback

**What could go wrong.** The pre-materialization of `OeWorkflowStepInProgress` filtered by `Try_Cast(ObjectKey As Int) Is Not Null` produces a temp table whose size is bounded by the number of shipment-style entries in the in-progress table. This is small in practice. The Try_Cast itself is a non-throwing variant of Cast, so a malformed ObjectKey returns NULL rather than raising an error.

The non-canceled count materialization filters to the candidate ShipmentIds (the union of `#TopShipmentIdInBin` and `#CvyRxsArrivedForShipmentId`). If either of those temp tables is empty for a given call (a station with no bin shipments and no pack-queue shipments), the materialization scans an empty set, which is the correct behavior.

The local variable read for `@TopShipmentId` selects from `#TopShipmentToPrint` with Top 1. The original procedure used a sub-select in the WHERE clause; both forms select the same row given the same `#TopShipmentToPrint` state. If `#TopShipmentToPrint` is empty (no qualifying shipment for the station), `@TopShipmentId` is NULL, and the final SELECT returns no rows, which matches the original semantics.

The MaxDop 1 hint is preserved on every statement. The Recompile hint is added to the parameter-sniffing-prone candidate-building queries; the queries that operate purely on temp tables do not need Recompile because the temp-table state is freshly materialized per call.

**What to look for in the first 24 hours.** Plan variant count per site, max-vs-avg ratio per site, and the count of `OeOrderShipmentAssoc + OeOrderHistory` reads per call. The headline expected delta is the OeOrderHistory join cost dropping from "twice the per-row count" to "once the per-set count."

**Rollback path.** The v17 body is in `Original.sql` in this folder. Redeploying v17 reverts cleanly. No schema change, no index change.

---

## 8. Evidence of Refactor (v18)

Captured against Tolleson. Warm cache. Same data state as Section 3.2. Two captures at different `@StationId` values.

```
(paste STATS IO / STATS TIME for v18 against the same data state used for v17 here)
```

The expected post-refactor signature is one read of `OeOrderShipmentAssoc + OeOrderHistory` for the count materialization, one read of `OeWorkflowStepInProgress` for the in-progress materialization, indexed temp-table joins everywhere downstream, and a stable plan across the two captures.

---

## 9. Comparison & Improvement

| Metric | v17 (paste) | v18 (paste) | Delta |
|---|---|---|---|
| Logical reads (total, median run) | | | |
| Logical reads on `OeOrderShipmentAssoc` | | | |
| Logical reads on `OeOrderHistory` | | | |
| Logical reads on `OeWorkflowStepInProgress` | | | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan operator count | | | |
| Plan variant count across two captures | (expected: 2+) | (expected: 1) | |
| Max-vs-avg ratio (cross-MFC, 30d) | 19,839x | (expected: <50x) | |
| Result row count | | | |

**Verdict (to be entered once the captures land):** the headline expected delta is the plan stability improvement plus the one-time materialization of the non-canceled count and the in-progress shipment IDs. Reads on `OeOrderHistory` should drop substantially because the correlated subquery is gone. Plan variance should drop from many to one or two.

---

## 10. Validation Checklist

Marked once the captures and the plans are in hand.

- Same data state. Captures taken back to back, ideally with a data freeze. (?)
- Warm cache only. Two runs, cold-cache numbers discarded. (?)
- Non-zero result set. Both runs returned at least one row. (?)
- Identical result set. Same ShipmentId chosen, same order list returned in the final SELECT. (?)
- Plan shape matches prediction. Indexed temp-table joins, one count materialization, stable plan across two captures. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles. Per-MFC post-deployment monitoring is required given the 19,839x baseline ratio.

---

## 11. Open Items / Future Improvements

### 11.1 Re-evaluate the per-statement `Option (MaxDop 1)` hint

The v17 header documents the hint as a workaround for parallel-thread starvation errors observed in at least one fielded system. On SQL Server 2019+ with appropriate server-level MAXDOP configuration and Cost Threshold for Parallelism set above the default, the per-statement override is often unnecessary and can prevent the optimizer from choosing a parallel plan for cases where parallelism would help. The follow-up is to drop the hint on a non-production environment, observe the plan choices and elapsed times, and decide whether the hint can come off in production. v18 preserves the hint to keep the runtime characteristic unchanged.

### 11.2 Evaluate consolidating the three pack-queue scans

The ManPack, SemiPack, and AutoPack scans (lines 95-197 in v17, preserved as separate temp tables in v18) are structurally similar enough that a consolidated UNION ALL inside a single CTE is structurally possible. The trade-off is that the optimizer's plan choice for the three branches is harder to reason about under consolidation. v18 keeps them separate. The follow-up is to prototype the consolidated form and compare plan shape and elapsed time.

### 11.3 Persisted typed column on `OeWorkflowStepInProgress`

If `OeWorkflowStepInProgress.ObjectKey` is consistently shipment-style integers when used by this procedure, a persisted typed column (a computed column `ShipmentIdAsInt As Try_Cast(ObjectKey As Int) Persisted`) with a filtered index would let the in-progress join seek directly without the per-call materialization. This is a schema change, not a same-PR fix, and is documented for the architectural backlog.

### 11.4 Index recommendation on `OeOrderShipmentAssoc(ShipmentId)`

The non-canceled count materialization scans `OeOrderShipmentAssoc` filtered by `ShipmentId In (candidate set)`. If the table is keyed only on `(OrderId, HistoryDtTm)`, the scan has to read the full table and filter. A nonclustered index on `(ShipmentId)` with `OrderId, HistoryDtTm` included would let the scan seek directly into the candidate ShipmentIds. The recommended DDL:

```sql
Create NonClustered Index IX_OeOrderShipmentAssoc_ShipmentId
   On OeOrderShipmentAssoc(ShipmentId)
   Include (OrderId, HistoryDtTm);
```

Confirm against Tolleson before fleet deployment. The index may already exist; if so, no action.

### 11.5 Per-MFC post-deployment monitoring is the gating step

This procedure carries the largest plan-volatility ratio in the cohort. Per-MFC post-deployment monitoring is the only way to confirm that the local-variable plus Recompile approach is producing stable plans at every site, and that no single MFC is producing a regression that the median-case improvement is masking.
