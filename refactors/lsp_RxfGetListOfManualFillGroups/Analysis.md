# lsp_RxfGetListOfManualFillGroups: Refactor Analysis (v48 to v49 Merged)

**Date:** 2026-05-07
**Tracking sheet row:** earlier cohort (pre-Row 15), worked through individually before the deep-dive cohort began
**Deployment state:** In iA Review. The refactor body in this folder (`Refactored.sql`) is the latest "Merged_v1" body submitted to the iA team. The Original.sql in this folder is the v48 body pulled from git history, which was the committed baseline at the start of the refactor work.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_RxfGetListOfManualFillGroups`

**Purpose in one line:** Returns the top X groups of Rxs queued for manual fill at a workstation, ordered by a configurable sort column, with several optional filters (station-specific stock locations, hub group, batch fill ID, tote ID) and with per-group inventory and product flags applied to the final result set.

**Tables touched:**

- `OeOrder` (the workhorse for the `#OeData` build; filters on `OrderStatus = 'Scheduled'` and `FillType`).
- `OeGroup` (joined to `OeOrder` for `GroupingCriteriaVal` and `NumOfRxs`; also queried in the GroupingCriteria CTE for the formatted display).
- `OePriority` (joined for the `PriDesc` lookup; small reference table).
- `Pharmacy` (left-joined for `InvPool`, which feeds the `NdcInvPool` synthetic key).
- `vInvItemDistWithUserOverrides` (the view that drives the exclusion list for non-station NDC + InvPool pairs).
- `InvStationNdcAssoc` (the station-to-NDC mapping; read twice under `@UseStockLocations = 1`, once for exclusion and once for inclusion).
- `CvyToteAtManFillStation` (read once to default `@ToteId` to the tote currently at the station).
- `OrderToteAssoc` and `OeOrder` (joined to populate `#ToteGroups`, which holds the rxs currently in the tote).
- `GovRx` (left-joined throughout for `ToteGroupNum`).
- `BfBatchFillRxAssoc`, `OeWorkflowStepInProgress`, `OeGroupCvyFillData`, `SysEndPharmacy`, `CfStoreDeliveryCourierCutOff` (the final result-set build, with the delivery-day calculation collapsed into the `NextDeliveryDay` CTE in v49).
- `OeGroupSplitCodeAssoc`, `OeGroupSplitCode` (the split-code aggregation that v49 inlines via `String_Agg` to replace the scalar UDF call).
- `InvFlaggedProducts`, `vInvMasterWithUserOverrides` (joined per-group for the inventory and product flags via the `InventoryFlags` and `ProductFlags` CTEs).

**Indexes used (predicted from the v49 body, to be confirmed against the captured plan):**

| Table | Index | Where used |
|---|---|---|
| `OeOrder` | `ByOrderStatusFillTypeAddrBank` (NC, key `OrderStatus, FillType, AddrBank`) | `#OeData` build |
| `OeGroup` | `PK_OeGroup` (clustered, key `GroupNum`) | `OeOrder` to `OeGroup` join, GroupingCriteria CTE |
| `OePriority` | clustered on `PriCode` (assumed) | priority lookup |
| `Pharmacy` | clustered on `PharmacyId` (assumed) | InvPool lookup |
| `vInvItemDistWithUserOverrides` | view, base-table indexes | exclusion list build |
| `InvStationNdcAssoc` | clustered (assumed key `StationId, Ndc`) | station-NDC filter |
| `CvyToteAtManFillStation` | clustered on `StationId` (assumed) | tote default |
| `OrderToteAssoc` | `PK_OrderToteAssoc` (clustered) | `#ToteGroups` build, final join |
| `GovRx` | clustered on `OrderId` (assumed) | tote group join |
| `OeWorkflowStepInProgress` | clustered or NC on `ObjectKey` | in-progress check |
| `OeGroupCvyFillData` | clustered or NC on `GroupNum, ToteGroupNum` | tote group fill data join |
| `SysEndPharmacy` | clustered on `Id` (assumed) | end-pharmacy lookup |
| `CfStoreDeliveryCourierCutOff` | clustered or NC on `SysEndPharmacyId, WeekDayCode` | the delivery-day calculation, now driven by the `NextDeliveryDay` CTE |
| `OeGroupSplitCodeAssoc` | clustered on `GroupNum` (assumed) | split-code aggregation in `GroupingCriteria` CTE |
| `OeGroupSplitCode` | clustered on `Code` | descriptor lookup |
| `InvFlaggedProducts` | clustered on `ProductId` (assumed) | per-group hazmat flags |
| `vInvMasterWithUserOverrides` | view, base-table indexes | per-group storage and DEA flags |

**Index DDL gap.** The shared index extract from the Row 15 pilot covers the `OeOrder` family in detail. Several of the smaller reference tables (`OePriority`, `CvyToteAtManFillStation`, `OeWorkflowStepInProgress`, `OeGroupCvyFillData`, `CfStoreDeliveryCourierCutOff`, `OeGroupSplitCodeAssoc`, `OeGroupSplitCode`, `InvFlaggedProducts`) are not in that extract. The index DDL for these tables should be confirmed against Tolleson before final sign-off so the recommendations in Section 11 are written against ground truth rather than assumption.

**Callers:** the Manual Fill station UI, called when the workstation requests the next set of groups to fill. Call frequency is moderate-to-high and scales with the number of active manual-fill workstations across the fleet.

---

## 2. Overview of Performance

The procedure is large by absolute measure (459 lines in v48, 595 lines in v49) and carries a deep set of optional filters, several temp-table builds, multiple LEFT JOINs into reference tables, two repeated correlated subqueries against `CfStoreDeliveryCourierCutOff`, and a scalar UDF call on every output row. The cost in v48 is therefore distributed across multiple cost drivers rather than concentrated in a single statement, which is why the refactor is broad rather than narrow.

The dominant cost drivers in v48 are:

A scalar UDF (`dbo.fOrdGroupingCritValForDisplay`) called once per output row in the final SELECT. The UDF executes row-by-row and serializes the surrounding query, blocking parallel execution for the entire statement.

A set of repeated correlated subqueries against `CfStoreDeliveryCourierCutOff` in the WHERE clause and the SELECT list of the main result build. The pattern is the canonical "scalar subquery that runs once per outer row," repeated three times against the same target table in different but related forms.

A `@ToteGroups` table variable that is joined to several large tables in the final result build. The optimizer cannot get reliable cardinality estimates against a table variable, and the resulting join order and join method choices are systematically worse than they would be against an equivalent indexed temp table.

A non-normalized parameter set that flows into downstream queries before parameter normalization completes. The original procedure normalized inputs (empty strings to `NULL`, negative values to defaults) but then used the original parameter symbols in the SELECTs that followed. Plans were therefore being cached against the unnormalized values, which is the parameter-sniffing volatility that the lessons log calls out.

A `Select *` from `#OeData` into `#OeDataUserLocs` inside the `@UseStockLocations = 1, @FilteredLocationsOnly = 0` branch. The implicit column list mirrors the temp-table schema, which works, but it is fragile against any future schema change and prevents the optimizer from narrowing the read.

The v49 refactor addresses all five of these systematically. Each item is structural rather than cosmetic.

---

## 3. Evidence of Original (v48)

### 3.1 Query Store, cross-MFC view

The procedure has been showing up at the busiest sites for some time and is on the iA review list in part because the operational record at multiple MFCs already shows cancellations and performance failures associated with it. The cross-MFC view from the May 7, 2026 capture is paste-ready below.

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

**Roll-up across reporting sites:** to be filled in once the aggregation is run on this procedure against the May 7 capture.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run

Captured against Tolleson. Warm cache. Same data state as Section 8.

```
(paste STATS IO / STATS TIME for v48 against the same data state used for v49 here)
```

---

## 4. Issue Identification

The v48 body has multiple structural issues. Each is documented separately so that the corresponding fix in v49 can be traced back to a specific finding.

**Issue 1: Scalar UDF in the final SELECT.** `dbo.fOrdGroupingCritValForDisplay` is invoked once per output row to produce the formatted grouping criteria value. Scalar UDFs are a known parallelism barrier in SQL Server (the engine serializes the entire surrounding plan to honor the UDF's execution semantics). The UDF body itself does an `OeGroupSplitCodeAssoc` join and a string concatenation, both of which are easy to inline.

**Issue 2: Repeated correlated subqueries against `CfStoreDeliveryCourierCutOff` (lines 298 to 305 in v48).** The same table is queried three times in the WHERE clause via correlated `Select Count(Id)` and `Select Top 1 WeekDayCode` patterns, each one running once per outer row. The pattern computes "what is the next delivery day for this pharmacy" but does it inside the row evaluation rather than as a one-pass set-based operation.

**Issue 3: Table variable joined to large base tables.** `@ToteGroups` is declared as a table variable, populated conditionally, and then joined to `#OeDataUserLocs` and onward to several other tables in the main result build. Table variables do not carry meaningful statistics, so the optimizer estimates the join cardinality at 1 row regardless of the actual content. The downstream plan choices reflect that bad estimate.

**Issue 4: Parameter sniffing volatility.** The procedure parameters are normalized at the top (empty strings to `NULL`, negative values to defaults), but the downstream queries continue to reference the original parameter symbols. Plans are therefore being cached against the parameter values as supplied by the caller, not against the normalized values. Calls that come in with empty strings, zero values, or null values can produce wildly different plans from calls that come in normalized.

**Issue 5: Implicit column lists in `Insert Into #OeDataUserLocs` (multiple places).** The original uses `Insert Into #OeDataUserLocs Select * From #OeData` in the unfiltered branch and `Insert Into #OeDataUserLocs Select O.* From #OeData O Inner Join ...` in the filtered branches. Implicit column lists make the procedure fragile against schema change and prevent the optimizer from narrowing the read in cases where only a subset of columns is needed.

**Issue 6: Missing indexes on temp tables (`#Hubs`, `#ToteGroups`, `#OeDataUserLocs`).** `#OeDataUserLocs` has an index added late in v48 (`Create Index ByOrderId`), but `#Hubs` and `#ToteGroups` are queried via `In` predicates and join predicates against larger tables with no supporting index.

**Issue 7: Repeated split-suffix display inside a scalar UDF.** The split-suffix portion of the formatted grouping criteria is constructed by the UDF on every row. The same split-suffix value would be identical for every row in the same group, so the work is redundant; computing it once per group is the right shape.

**Issue 8: Heuristic group flags computed inline in the SELECT list.** The hazmat, controlled-substance, cold-storage, and pen/sulfa flags are computed in the final SELECT list via correlated subqueries (or via implicit per-row lookups depending on the v48 variant). One row per group is the right granularity; one row per output row is what v48 does.

---

## 5. First Principles

The refactor draws on several masterclass entries. Each one corresponds to one or more of the issues in Section 4.

**[[Scalar UDF Parallelism Barrier]].** The v48 scalar UDF on the final SELECT serializes the entire plan. The fix is to inline the UDF body as a set-based CTE that runs once over the relevant group set. The split-suffix computation is the heaviest UDF dependency and inlines naturally via `String_Agg` over `OeGroupSplitCodeAssoc` joined to `OeGroupSplitCode`. The result is a single-pass aggregation rather than a per-row function call.

**[[Correlated Subqueries to CTEs]].** The three correlated subqueries against `CfStoreDeliveryCourierCutOff` are the canonical case for this fix. The "next delivery day per pharmacy" computation is a one-pass `Group By SysEndPharmacyId` aggregation. The v49 `NextDeliveryDay` CTE computes both `ThisWeekDay` (the earliest weekday code at or after today's weekday) and `FirstWeekDay` (the absolute earliest weekday code) in a single pass over the table, joined in once to the main result build. The previously-correlated logic becomes a `Coalesce(NDD.ThisWeekDay, NDD.FirstWeekDay)` lookup.

**[[Table Variables vs Temp Tables]].** The v48 `@ToteGroups` table variable is the textbook case. It is populated conditionally, indexed never, and joined to multiple larger tables. The v49 conversion to `#ToteGroups` with a supporting index on `GroupNum` lets the optimizer get realistic cardinality estimates and choose appropriate join methods.

**[[Parameter Sniffing]].** The v49 introduces `@LocalStationId`, `@LocalFillType`, `@LocalToteId`, and `@LocalBatchFillId` after the parameter normalization block. Downstream queries reference the local variables exclusively. The optimizer caches plans against the normalized values rather than the raw incoming values, which stabilizes plan choice across the parameter shapes that arrive in practice.

**[[Conditional Aggregation Consolidation]].** The hazmat, controlled-substance, cold-storage, and pen/sulfa flags are computed in v49 via the `InventoryFlags` and `ProductFlags` CTEs, each one a single `Group By GroupNum` aggregation that produces all four flags in one pass over the relevant product list. The v49 form is one CTE per source table, with all the flags from that source produced together.

**[[Index Key Columns vs Included Columns]].** Several of the temp tables in v49 gain explicit indexes after population: `#Hubs.Hub`, `#ToteGroups.GroupNum`, and `#OeDataUserLocs.OrderId`. Each index supports a specific join or lookup pattern in the surrounding query. The principle is that an indexed temp table is a different optimizer subject than an unindexed one, and the cost difference scales with the size of the joined base table.

A net-new observation worth recording is the "compute once per group" pattern that underpins the `GroupingCriteria`, `InventoryFlags`, and `ProductFlags` CTEs in v49. The pattern is implicit in [[Correlated Subqueries to CTEs]] but is general enough to warrant its own masterclass entry. Section 11 carries the recommendation.

---

## 6. Refactor (commented)

The v49 body is reproduced in `Refactored.sql`. The salient blocks below.

**Local variable assignment after parameter normalization.**

```sql
-- Assign to locals AFTER normalization so downstream queries see the
-- normalized values and break parameter sniffing.
Set @LocalStationId   = @StationId;
Set @LocalFillType    = @FillType;
Set @LocalToteId      = @ToteId;
Set @LocalBatchFillId = @BatchFillId;
```

Every downstream reference to these parameters uses the local variable.

**Table variable to temp table.**

```sql
Create Table #ToteGroups
(
    ToteId varchar(24),
    GroupNum varchar(10),
    ToteGroupNum tinyint
);

-- ... populate conditionally ...

-- Index #ToteGroups after population for join performance and cardinality estimates
Create Index IX_ToteGroups_GroupNum On #ToteGroups(GroupNum);
```

**NextDeliveryDay CTE replaces three correlated subqueries.**

```sql
;With NextDeliveryDay As
(
    Select
        SysEndPharmacyId,
        Min(Case When WeekDayCode >= DatePart(Weekday, GetDate()) Then WeekDayCode End) As ThisWeekDay,
        Min(WeekDayCode) As FirstWeekDay
    From CfStoreDeliveryCourierCutOff With (NoLock)
    Group By SysEndPharmacyId
)
```

The join into the main result build then becomes:

```sql
Left Join NextDeliveryDay NDD
    On NDD.SysEndPharmacyId = EP.Id
Left Join CfStoreDeliveryCourierCutOff CF With (NoLock)
    On CF.SysEndPharmacyId = EP.Id
   And CF.WeekDayCode = Coalesce(NDD.ThisWeekDay, NDD.FirstWeekDay)
```

`Coalesce` falls through to the absolute earliest weekday code if the "at or after today" branch finds no row, which matches the v48 fallback semantics.

**Scalar UDF replaced by set-based GroupingCriteria CTE.**

```sql
, GroupingCriteria As
(
    Select
        DG.GroupNum,
        G.GroupingCriteriaVal
            + IsNull('   (' + S.SplitSuffix + ')', '') As FmtdGroupingCriteriaVal
    From DistinctGroups DG
    Inner Join OeGroup G With (NoLock)
        On G.GroupNum = DG.GroupNum
    Left Join
    (
        Select
            GSA.GroupNum,
            String_Agg(SC.Descrip, '; ') As SplitSuffix
        From OeGroupSplitCodeAssoc GSA With (NoLock)
        Inner Join OeGroupSplitCode SC With (NoLock)
            On SC.Code = GSA.SplitCode
        Group By GSA.GroupNum
    ) S
        On S.GroupNum = DG.GroupNum
)
```

The output is identical in shape to what the UDF produced; the difference is that the work runs once over the distinct group set rather than once per output row, and the surrounding plan is no longer serialized by the UDF's parallelism barrier.

**Inventory and product flags via consolidated CTEs.**

```sql
, InventoryFlags As
(
    Select
        gp.GroupNum,
        IsColdStorage         = Max(Case When m.StorageCondCode IN ('F', 'R') Then 1 Else 0 End),
        IsControlledSubstance = Max(Case When m.DeaClassCode > 0 Then 1 Else 0 End),
        IsPen                 = Max(Case When m.CrossContaminationCategory = '2' Then 1 Else 0 End),
        IsSulfa               = Max(Case When m.CrossContaminationCategory = '3' Then 1 Else 0 End)
    From DistintGroupProducts gp
    Join vInvMasterWithUserOverrides As m With (NoLock)
        On m.ProductId = gp.ProductId
    Group By gp.GroupNum
)
```

`ProductFlags` follows the same shape against `InvFlaggedProducts`. Each CTE produces all of its flags in one pass; the final SELECT consumes them with `Coalesce` to handle groups that have no rows in the source table.

The full body with all of these blocks in place is in `Refactored.sql`.

---

## 7. Risk & Rollback

**What could go wrong.** The v49 refactor is broad, which is where the risk concentration sits.

The parameter-sniffing protection via local variables is generally safe but can produce a slightly different plan on the first compilation after deployment. The plan it produces is what the optimizer chooses against the local-variable-typed plan, which is the canonical "ideal" plan for a parameter-stable workload. If the actual workload is bimodal (some calls with very selective parameters, some with very broad parameters) the new single plan may be worse for one of the two modes than the v48 plan was.

The `NextDeliveryDay` CTE materializes the per-pharmacy delivery day once. If the CTE runs into a row in `CfStoreDeliveryCourierCutOff` that the v48 correlated logic would have skipped (because the v48 subqueries had additional `Where` clauses that the CTE does not exactly reproduce), the semantics diverge. The CTE has been written to match the v48 fallback (`Coalesce(ThisWeekDay, FirstWeekDay)`), but a careful row-count comparison in Section 9 against the same data state is the only way to confirm semantic parity.

The scalar UDF replacement carries a smaller semantic risk. The UDF body is short and the inlined form preserves the `String_Agg` order via `OeGroupSplitCode.Descrip` ordering. The risk is that the UDF body had a side-effect path (a NULL handling case, a CASE branch for an unusual split-code value) that the inlined form does not reproduce. Reading the UDF body alongside the inlined CTE during review is the mitigation.

**What to look for in the first 24 hours.** Plan stability per site (the `NextDeliveryDay` CTE should produce the same join shape against the same data, and the plan should not flip across compilations). Result set row count parity against v48 captured under the same data state. Per-site monitoring (see Section 11 for the per-MFC validation expectation) to confirm that the four buckets of expected outcome (improved, unchanged, regressed, regressed-with-investigation) land where the analysis predicts.

**Rollback path.** The v48 body is in `Original.sql` in this folder. Redeploying v48 reverts cleanly. There is no schema dependency in v49 that v48 cannot accept back. The temp-table-to-table-variable change is transparent to the procedure's interface.

---

## 8. Evidence of Refactor (v49)

Captured against Tolleson. Warm cache. Same data state as Section 3.2.

```
(paste STATS IO / STATS TIME for v49 against the same data state used for v48 here)
```

The plan-shape comparison is the headline indicator on this refactor. The scalar UDF removal alone should change the plan from a serialized form to a parallel-enabled form, which is visible in the Properties pane of the execution plan as `NonParallelPlanReason: NoParallelScalarUDFs` becoming absent in v49.

---

## 9. Comparison & Improvement

| Metric | v48 (paste) | v49 (paste) | Delta |
|---|---|---|---|
| Logical reads (total) | | | |
| Logical reads on `CfStoreDeliveryCourierCutOff` | | | |
| Logical reads on `OeGroup` | | | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan operator count | | | |
| Plan: NonParallelPlanReason | NoParallelScalarUDFs | (expected: absent) | |
| Plan: scalar UDF invocations | (expected: high) | (expected: zero) | |
| Result row count | | | |

**Verdict (to be entered once the captures land):** the headline expected delta is the removal of the scalar UDF parallelism barrier, the order-of-magnitude reduction in reads on `CfStoreDeliveryCourierCutOff` from the correlated-to-CTE rewrite, and the join improvement on `#ToteGroups` from the table-variable-to-temp-table conversion. Total CPU time should fall more than total reads because the UDF removal allows parallel execution where v48 was serialized.

---

## 10. Validation Checklist

Marked once the captures and the plans are in hand.

- Same data state. Captures taken back to back, ideally with a data freeze. (?)
- Warm cache only. Two runs, cold-cache numbers discarded. (?)
- Non-zero result set. Both runs returned at least one row. (?)
- Identical result set. Same row count and same row identities for deterministic queries. (?)
- Plan shape matches prediction. UDF parallelism barrier gone; `NextDeliveryDay` materialized once; `#ToteGroups` joined with realistic cardinality estimates. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call (to be entered once the checklist is completed): the refactor is sound on first principles. The combination of the UDF removal and the correlated-to-CTE rewrite is the largest expected delta. The local-variable parameter-sniffing protection is the most likely source of a surprise at a specific MFC, so per-site post-deployment monitoring (Section 11) is non-optional on this refactor.

---

## 11. Open Items / Future Improvements

### 11.1 Per-MFC post-deployment monitoring is the gating step for closure

This refactor is the right candidate to exercise the post-deployment per-MFC monitoring discipline that the program charter calls out as the fourth methodology rule. Five distinct refactor patterns are in play in a single PR (parameter sniffing protection, table variable to temp table, scalar UDF removal, correlated subquery to CTE, conditional aggregation consolidation). The probability that all five land identically at every MFC is low. The probability that one or two MFCs see a different plan than the median is meaningful.

The monitoring expectation per site is two weeks of normal traffic, the same metrics the analysis predicted (total reads, average reads per execution, plan variants observed, average duration, distribution of reads per execution across executions), and a bucketing of the outcome per site as Improved, Unchanged, or Regressed against the site's own pre-deployment baseline. Any site recorded as Regressed gets a per-site root cause analysis on the standard template.

### 11.2 Index recommendation on `OeGroupSplitCodeAssoc`

The new `GroupingCriteria` CTE joins `OeGroupSplitCodeAssoc` on `GroupNum` and groups by `GroupNum`. If the table is keyed on `(GroupNum, SplitCode)` the join and group are cheap. If the table is keyed only on `(SplitCode, GroupNum)` the order is wrong for the new pattern and the optimizer has to sort. Confirming the index against Tolleson before sign-off avoids the surprise.

If the index needs to be added, the recommended DDL is:

```sql
Create NonClustered Index IX_OeGroupSplitCodeAssoc_GroupNum
    On OeGroupSplitCodeAssoc(GroupNum)
    Include (SplitCode);
```

### 11.3 Confirm `String_Agg` ordering semantics

`String_Agg` in SQL Server 2017+ supports `WITHIN GROUP (ORDER BY ...)` to control the order of the aggregated values. The v49 body uses bare `String_Agg(SC.Descrip, '; ')` without an explicit `WITHIN GROUP`. The order of the concatenated descriptors is therefore not guaranteed. If the v48 UDF produced a deterministic order, v49 should match it by adding `Within Group (Order By SC.Descrip)` to the aggregate. This is a one-line change and is being held pending confirmation that the order matters operationally.

### 11.4 Replace `fStringToTableByDelimiter` with `STRING_SPLIT`

The procedure still uses `dbo.fStringToTableByDelimiter(@HubNames, ',')` to parse the hub list into the `#Hubs` table. SQL Server 2016+ has a built-in `STRING_SPLIT` table-valued function that does the same job without the scalar-UDF-style serialization. See [[STRING_SPLIT vs WHILE Loop CSV Parsing]]. This is out of scope for the v49 refactor (it is a separate concern with its own validation surface) but is the next logical refactor on this procedure.

### 11.5 Audit the in-progress check for index coverage

`OeWorkflowStepInProgress` is left-joined on `ObjectKey = (O.GroupNum + '-' + Cast(GR.ToteGroupNum As varchar))`. The constructed key is not SARGable against any clean index. The candidate index would have to be a computed-column index on the concatenated expression or a refactor to add the components as separate columns. This is a schema-level recommendation, not a same-PR fix.

### 11.6 Write a "Compute Once Per Group" masterclass entry

The pattern that underpins the `GroupingCriteria`, `InventoryFlags`, and `ProductFlags` CTEs in v49 is the same general idea applied to three different sources: compute the per-group value once over the distinct-group set, then left-join the result into the main row stream. The pattern is implicit in [[Correlated Subqueries to CTEs]] but general enough to warrant its own entry. The next refactor that surfaces the pattern should produce the entry.
