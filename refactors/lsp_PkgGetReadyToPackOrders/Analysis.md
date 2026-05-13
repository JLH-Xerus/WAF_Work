# lsp_PkgGetReadyToPackOrders: Refactor Analysis (v43 to v44)

**Date:** 2026-05-08
**Tracking sheet row:** 26 (Priority P1, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v44 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_PkgGetReadyToPackOrders`

**Purpose in one line:** Returns the list of orders to display on the Package/Ship Orders form for a given pack station, aggregated by group and shipment, with optional filters for complete-only, override PoNum, storage-location inclusion, and pending/problem Rx handling.

**Tables touched:**

- `OeOrder` (the workhorse; queried in every stage of the candidate-set build and in the per-Rx data assembly).
- `OeOrderPoNumAssoc` (joined for PoNum throughout, and as the entry point for the override path).
- `OeGroup` (joined for GroupingCriteriaVal).
- `OePatientCust` (joined for patient name).
- `OePriority` (joined for the priority description).
- `OeOrderShipmentAssoc` (left-joined for ShipmentId).
- `OrderToteAssoc` (left-joined for ToteId).
- `OrdWillCallBin` (left-joined for storage location BinId).
- `ShpStationWillCallBinAssoc` (joined for the station's bin-prefix filtering).
- `CvyAutoPackStationVialQueue`, `CvySemiAutoPackStationToteQueue`, `CvyManPackStationToteQueue` (one each per station-type branch).
- `OeWorkflowStepInProgress` (left-joined for the in-progress check).
- `OeRxPrefLangData` (left-joined for the preferred-language patient-name override).
- `InvFlaggedProducts` (queried three times in v43 for the hazardous flags; consolidated into one scan in v44).
- `vInvMasterWithUserOverrides` (joined for cross-contamination categories).
- `PwkPrinterTray` (left-joined in the final SELECT for paperwork-printed indicator).

**Indexes used:** the procedure touches a wide range of OeOrder family, station-queue, and shipment-association indexes. The pilot index extract covers the OeOrder family; the station-queue and shipment-association indexes should be confirmed against Tolleson.

**Callers:** the Package/Ship Orders form at every pack station. Called when the form is opened or refreshed.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on three of four expense lists (Duration rank 5, Expense rank 6, Plan rank 2) with "severe parameter sniffing" identified as the dominant heuristic. The procedure carries seven input parameters, several of which fundamentally change the shape of the WHERE clause in the final SELECT, and v43 caches a single plan against the first call's parameter values.

The dominant cost drivers in v43 are five.

The first is parameter sniffing on the seven input parameters. The plan-stability ratio in the cross-list capture reflects the optimizer's inability to choose a single good plan for the variety of parameter shapes the procedure receives.

The second is the three independent CTEs for the hazardous flags (HazardousToHandle, HazardousToDispose, HazardousToShip). Each one scans `InvFlaggedProducts` independently and is left-joined to the main #Rxs SELECT three times. The work is structurally identical: a single set-based aggregate over `InvFlaggedProducts` with three conditional MAX columns is the consolidated form.

The third is the correlated scalar Count subquery in the #GrpData02 SELECT (line 464 in v43) that computes `NumOfPendingOrProblemRxs` per output row. The subquery scans `#Rxs` once per row of #GrpData01, evaluating four equality predicates per scan.

The fourth is the scalar UDF `dbo.fOrdPatNameFmtd` called once per row of #Rxs. The UDF is a string formatter with input from six columns; it carries the [[Scalar UDF Parallelism Barrier]] semantics. v44 retains the UDF call to keep the diff focused; v45 will inline it.

The fifth is the catch-all WHERE clause in the final SELECT, which combines five parameter-controlled conditions with disjunctions. The WHERE shape is not unsound but it benefits from `Option (Recompile)` so the optimizer can simplify the dead branches at compile time given the specific parameter values for the call.

v44 addresses the first three drivers directly, adds Recompile on the parameter-sniffing-sensitive SELECTs, and defers the scalar UDF inlining.

---

## 3. Evidence of Original (v43)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

**Roll-up across reporting sites:** to be filled in from the May 7 capture. The cross-list capture ranks the procedure Duration #5 and Plan #2.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v43)

Captured against Tolleson. Warm cache. Two captures at different `@RouteStop` values are needed to expose the plan-stability behavior.

```
(paste STATS IO / STATS TIME for v43 against the same data state used for v44 here)
```

The expected v43 signature is three independent scans of `InvFlaggedProducts`, N reads of `#Rxs` for the correlated NumOfPendingOrProblemRxs subquery, and a single plan cached across the two captures whose cost differs by an order of magnitude.

---

## 4. Issue Identification

**Issue 1: Parameter sniffing on all seven input parameters.** No local-variable indirection. The cross-list capture names this as the dominant heuristic.

**Issue 2: Three independent CTEs for hazardous flags (lines 289-306 in v43).** Three scans of `InvFlaggedProducts`, three left-joins into the main SELECT. One aggregate produces all three flags per ProductId.

**Issue 3: Correlated scalar Count subquery for NumOfPendingOrProblemRxs (line 464 in v43).** Per-row scan of `#Rxs` with four equality predicates.

**Issue 4: Scalar UDF `dbo.fOrdPatNameFmtd` per row (line 313 in v43).** Parallelism barrier. v44 retains; v45 will inline.

**Issue 5: Five scattered `If Object_Id ... Drop Table` blocks (lines 85, 130, 280, 407, 453 in v43).** Modern equivalent is one consolidated `Drop Table If Exists` per side.

**Issue 6: Catch-all WHERE clause in the final SELECT (lines 547-610 in v43).** Five parameter-controlled disjunctions. Benefits from Recompile.

---

## 5. First Principles

**[[Parameter Sniffing]] with the local-variable form** for all seven input parameters.

**[[Conditional Aggregation Consolidation]]** for the three hazardous-flag CTEs. One set-based aggregate over `InvFlaggedProducts` produces all three flag columns in one pass.

**[[Correlated Subqueries to CTEs]]** for the NumOfPendingOrProblemRxs aggregate. One-pass aggregation into `#PendingProblemCounts` indexed by `(GroupNum, ShipmentId, ToteId)` replaces N row-by-row reads.

**[[Catch-All Query Anti-Pattern]]** addressed via `Option (Recompile)` on the final SELECT. The full structural alternative (separate top-level SELECTs per parameter combination) would multiply the procedure body for marginal benefit; Recompile gives the optimizer the actual parameter values at compile time, which is enough to simplify the dead disjunction branches.

---

## 6. Refactor (commented)

The v44 body is reproduced in `Refactored.sql`. Salient blocks below.

**Local variables and consolidated cleanup.**

```sql
Declare @LocalRouteStop VarChar(8) = @RouteStop
-- (six more locals for the other parameters)

Drop Table If Exists
      #RxOrderIds, #AtStationGrps, #Rxs, #GrpData01, #GrpData02
    , #HazFlags, #PendingProblemCounts
```

**Consolidated hazardous-flag aggregate.**

```sql
Select
      IFP.ProductId
    , [IsHazHandle]  = Max(Case When IFP.IsFlaggedFor = 'HAZMAT' Then 1 Else 0 End)
    , [IsHazDispose] = Max(Case When IFP.IsFlaggedFor = 'HAZDSP' Then 1 Else 0 End)
    , [IsHazShip]    = Max(Case When IFP.IsFlaggedFor = 'HAZSHP' Then 1 Else 0 End)
Into #HazFlags
From InvFlaggedProducts IFP With (NoLock)
Where IFP.ProductId In (Select Distinct O.ProductId From #RxOrderIds R Inner Join OeOrder O On O.OrderId = R.OrderId)
  And IFP.IsFlaggedFor In ('HAZMAT', 'HAZDSP', 'HAZSHP')
Group By IFP.ProductId
```

The main `#Rxs` SELECT consumes this via a single left-join to `#HazFlags` instead of three.

**Pre-materialized pending/problem aggregate.**

```sql
Select GroupNum, ShipmentId, ToteId
     , [NumOfPendingOrProblemRxs] = Sum(Case When OrderStatus In ('Pending', 'Problem') And QueueStationId Is Not Null Then 1 Else 0 End)
Into #PendingProblemCounts
From #Rxs
Group By GroupNum, ShipmentId, ToteId;
```

The `#GrpData02` SELECT then left-joins to `#PendingProblemCounts` on the three keys.

**Recompile hints on long-running SELECTs.**

```sql
Select ... Into #Rxs From ... Option (Recompile);

Select ... From #GrpData02 G ...
Where ( ... catch-all disjunction ... )
Order By ...
Option (Recompile);
```

---

## 7. Risk & Rollback

**What could go wrong.** The hazardous-flag consolidation produces one row per ProductId regardless of how many flag rows existed in the source. The Max() aggregation is idempotent for boolean flags, and the `In ('HAZMAT', 'HAZDSP', 'HAZSHP')` filter matches the v43 set. Identical row identities for deterministic queries are expected.

The pending/problem pre-aggregation captures the per-(Group, Shipment, Tote) count once, joined back via three keys. The original subquery added `R.QueueStationId Is Not Null` as a predicate; v44 includes the same condition inside the Case expression of the aggregate, preserving the semantic.

The Recompile hint on the final SELECT takes the per-call compile cost. The catch-all WHERE has roughly seven distinct shapes across the parameter combinations, so the compile cost is bounded by the optimizer's per-shape work.

**What to look for in the first 24 hours.** Plan variant count per site, scans of `InvFlaggedProducts` per call (expected to drop from 3 to 1), and the disappearance of the per-row Count subquery against `#Rxs` in the execution plan.

**Rollback path.** The v43 body is in `Original.sql`. Redeploying v43 reverts cleanly.

---

## 8. Evidence of Refactor (v44)

```
(paste STATS IO / STATS TIME for v44 against the same data state used for v43 here)
```

---

## 9. Comparison & Improvement

| Metric | v43 (paste) | v44 (paste) | Delta |
|---|---|---|---|
| Logical reads (total, median run) | | | |
| Logical reads on `InvFlaggedProducts` | | (expected: ~1/3 of v43) | |
| Logical reads on `#Rxs` | | (expected: lower; correlated subquery gone) | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan variant count across two captures | (expected: 2+) | (expected: 1) | |
| Result row count | | | |

**Verdict (to be entered once the captures land):** the headline expected delta is the consolidated hazardous-flag scan, the eliminated correlated Count subquery, and the plan-stability improvement from local variables plus Recompile.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Non-zero result set. (?)
- Identical result set. (?)
- Plan shape matches prediction. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles. Per-MFC monitoring is required given the "severe parameter sniffing" classification.

---

## 11. Open Items / Future Improvements

### 11.1 v45: Inline `dbo.fOrdPatNameFmtd`

The scalar UDF is called once per row of `#Rxs` and is the dominant remaining parallelism barrier. The UDF body is a name-formatter that switches between standard and preferred-language inputs based on the bit parameters. The inlined form is a CASE expression. v45 will replace the call with the inline CASE once the UDF body is confirmed against the production deployment.

### 11.2 Index recommendation on `InvFlaggedProducts(ProductId, IsFlaggedFor)`

The consolidated hazardous-flag aggregate filters by ProductId IN (...) and IsFlaggedFor IN (...). A nonclustered index on `(ProductId, IsFlaggedFor)` would support the consolidated scan as a seek. Confirm against Tolleson before deployment.

### 11.3 Per-MFC post-deployment monitoring

The cross-list capture flags "severe parameter sniffing" on this procedure. Per-MFC monitoring with two captures at different `@RouteStop` shapes is required to confirm plan stability across sites.

### 11.4 Consider whether the catch-all WHERE can be split

The five-disjunction WHERE clause in the final SELECT could be split into separate top-level SELECTs per parameter shape and combined with UNION ALL. v44 holds back on this restructuring because the Recompile hint addresses the parameter-sniffing piece directly without the structural disruption. If the post-deployment evidence shows that Recompile alone is not enough, the explicit-branch form is the next step.
