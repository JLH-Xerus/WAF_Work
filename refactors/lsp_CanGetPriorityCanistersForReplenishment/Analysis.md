# lsp_CanGetPriorityCanistersForReplenishment: Refactor Analysis (v73 to v74)

**Date:** 2026-05-08
**Tracking sheet row:** 25 (Priority P0, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v74 designed by the MFC DBA team and me, awaiting iA review. v74 is the first of an expected multi-version refactor sequence on this procedure; v75 through v77 are scoped in Section 11.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_CanGetPriorityCanistersForReplenishment`

**Purpose in one line:** Returns a prioritized list of M4C canisters that need replenishment, by computing priority based on the needs of dispensers with matching products and applying optional filters on station, low-priority status, lost status, and out-of-stock products.

**Tables touched (selected; the procedure is 1,347 lines and touches roughly twenty tables):**

- `CanCanister` (the workhorse canister state table; queried in the candidate-set build, the priority-tcd join, and the two #CanisterList INSERT blocks).
- `TcdStatus` (the dispenser state table; queried in every priority branch of the UNION-driven `#PriorityTcds` build).
- `TcdDryDispensers` (left-joined throughout the priority build for the dry-dispenser exclusion).
- `TcdHoldReplen` (left-joined throughout for the hold-replenishment exclusion).
- `vCfgSystemParamVal` (read three times for the hopper-volume configuration values).
- `vInvMasterWithUserOverrides` (joined in both #CanisterList INSERT blocks for brand and strength).
- `vInvItemDistWithUserOverrides` (joined for QtyPerSku).
- `InvReplenProductOverrides` (referenced six times via correlated subqueries inside CASE expressions for per-canister-model max-quantity calculations).
- `InvNdcLocAssoc` (queried twice via `Select Top 1` correlated subqueries for stock-location display).
- `InvOutOfStockProducts` (queried for the out-of-stock filter).
- `TcdFlaggedTcds` (queried for the count-dry exclusion).
- `OeOrder`, `OeGroup` (queried inside the cursor loops for product-allocated Rx counts).
- `ToteRouteLoc` (joined for the canister current location description).
- `TcdSecondaryData` and several other Tcd-family tables in the cursor loops.

**Indexes used:** the procedure touches enough tables that a full index inventory is its own deliverable. The pilot index extract does not cover the M4C dispenser family at all. Section 11 includes a request for the index inventory against Tolleson; the index recommendations in v74 are written from query structure alone pending that inventory.

**Callers:** the M4C replenishment screen. Called when an operator opens the replenishment list at a station, and refreshed on a regular cadence while the screen is open.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on all four expense lists (Volume rank 1, Duration rank 18, Expense rank 24, Plan rank 1) with a 249,457x worst-case-to-average ratio. This is the largest plan-volatility ratio anywhere in the rows 22 to 39 cohort and by a substantial margin the strongest parameter-sniffing signature in the program's evidence base. The cross-list summary explicitly names "severe parameter sniffing" as the dominant heuristic.

The procedure is also among the longest in the procedure library at 1,347 lines, with six cursor loops, nine intermediate temp tables, the scalar UDF `dbo.fFormatNdcForDisplay` called per output row, and a multi-branch UNION over `TcdStatus` that builds the `#PriorityTcds` set from roughly fifteen near-identical SELECT branches (one per priority bucket and canister model combination). The structural complexity is the second-largest cost driver after parameter sniffing.

v74 is a phased refactor: it addresses the parameter-sniffing dominant cost driver and the lowest-cost cleanup items, and explicitly defers the cursor elimination, the scalar UDF inlining, the correlated-subquery consolidation, and the priority-bucket UNION restructuring to follow-on versions. The phased approach is deliberate. A 1,347-line procedure rewritten in one PR is harder to review than a sequence of focused changes. The parameter-sniffing fix in v74 is the change that addresses the 249,457x ratio directly; the structural changes in v75 and beyond compound the win incrementally.

The dominant cost drivers in v73 are five, listed in order of impact:

The first is parameter sniffing on the six input parameters. The 249,457x ratio is consistent with a single cached plan being reused across calls whose parameter selectivities differ by many orders of magnitude. The `@StationId` parameter is the most consequential because it gates the stock-location filtering in the cursor branches, but the sort parameters (`@SortColumn`, `@SortDirection`) also drive plan instability because the final ORDER BY is a long Case ladder over both.

The second is the scalar UDF `dbo.fFormatNdcForDisplay`. Called once per output row in the final SELECT and in the two #CanisterList INSERT blocks. The scalar UDF carries the [[Scalar UDF Parallelism Barrier]] semantics and serializes the surrounding plan.

The third is the six cursor loops. Two of them iterate over `#PriorityTcds` and `#PriorityCans` to build canister-to-tcd assignments row-by-row. Four of them iterate over the forward-stock and back-stock location regex patterns to build `#NdcForwardStockLocs` and `#NdcBackStockLocs` (twice each: once for the @StationId-specified branch and once for the no-station branch). Cursor work in T-SQL is the canonical row-by-row anti-pattern; converting each cursor to a set-based operation is what v76 is scoped to do.

The fourth is the multi-branch UNION over `TcdStatus` that builds `#PriorityTcds`. Approximately fifteen near-identical SELECT branches, each one a join chain (`TcdStatus` left-joined to `TcdDryDispensers` and `TcdHoldReplen`, and in several branches also to `CanCanister`), filtered by a slightly different combination of `TcdModel`, `LocCanModel`, `ReplenState`, and `Status` predicates. Several of the branches share the exact same `TcdSn Not In (Select c.TcdSn From CanCanister c With (NoLock) Where c.Status In (...))` subquery. Consolidating the branches is scoped to v77.

The fifth is the repeated correlated subqueries against `InvReplenProductOverrides` and `InvNdcLocAssoc`. Each of the two `#CanisterList` INSERT blocks contains six correlated `(Select ... From InvReplenProductOverrides Where ProductId = c.ProductId)` references inside a nested Case expression that computes per-model max replenishment quantities. The same pattern appears for the StockLocation lookup against `InvNdcLocAssoc`. Pre-materializing these into temp tables once is scoped to v75.

---

## 3. Evidence of Original (v73)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Max-vs-avg ratio |
|-----|---------------|------------------|-------------|----------------|--------------------|------------------|
| (paste from capture) | | | | | | |

**Roll-up across reporting sites:** to be filled in from the May 7 capture. The cross-list capture reports the procedure on all four expense lists with the 249,457x worst-case-to-average ratio. The expected total-reads value is in the billions per month per site for the busy MFCs.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v73)

Captured against Tolleson. Warm cache. The reads-per-execution variance is so large that meaningful capture requires three calls at different parameter shapes: a default-sort first call to establish the cached plan, then a different-sort call to expose the volatility, then a different-station call to add the station-selectivity dimension.

```
(paste STATS IO / STATS TIME for v73 against the same data state used for v74 here)
```

The expected v73 signature is a small number of clean operator chains for the candidate-set builds, then a large number of scalar-UDF invocations through the final SELECT, then a per-row execution of the InvReplenProductOverrides correlated subqueries inside the CASE expressions. Cursor iteration cost dominates when the candidate set is large.

---

## 4. Issue Identification

The v73 body has many distinct issues. The five highest-impact, ordered for staged refactor application, are enumerated below. The remaining issues are documented in Section 11 as queued for v75 through v77.

**Issue 1: Parameter sniffing on all six input parameters (every statement that references a parameter in v73).** No local-variable indirection. The 249,457x worst-case-to-average ratio is the direct signature. v74 addresses this.

**Issue 2: Scalar UDF `dbo.fFormatNdcForDisplay` in the final SELECT and the #CanisterList INSERT blocks.** The UDF carries the [[Scalar UDF Parallelism Barrier]] cost semantics. v75 will inline the UDF body via a CASE expression.

**Issue 3: Six cursor loops.** Two iterate over `#PriorityTcds` and `#PriorityCans` for canister-to-tcd assignment; four iterate over the forward-stock and back-stock location regex patterns. v76 will rewrite the cursor logic into set-based equivalents.

**Issue 4: Multi-branch UNION over `TcdStatus` that builds `#PriorityTcds`.** Approximately fifteen near-identical branches. v77 will consolidate the structurally identical join chains and parameterize the filters.

**Issue 5: Correlated subqueries against `InvReplenProductOverrides` and `InvNdcLocAssoc` in the two `#CanisterList` INSERT blocks.** Six occurrences of the override lookup per insert, two occurrences of the stock-location lookup per insert. v75 will pre-materialize these into temp tables.

**Issue 6 (cleanup, addressed in v74): Ten scattered `Drop Table If Exists` statements at the procedure tail.** Consolidated into a single statement in v74.

---

## 5. First Principles

The v74 refactor draws on two masterclass entries directly. The longer roadmap (v75 through v77) draws on five more.

**v74 references:**

**[[Parameter Sniffing]] with the local-variable form.** The six local variables `@LocalStationId` through `@LocalSortDirection` are assigned from the parameters at the top of the procedure. Every reference to the input parameters in the body resolves to a local. The optimizer caches plans against the local-variable-typed plan, which is what stabilizes the worst-case behavior. The `Option (Recompile)` hint on the final ORDER BY SELECT gives the optimizer the actual sort target at compile time, which is the next-most-important plan-stabilization lever for this procedure given the Case-ladder shape of the ORDER BY.

**[[TOP with ORDER BY Semantics]] applied to the final SELECT.** The final ORDER BY is a Case ladder over `@SortColumn` and `@SortDirection`. With `Option (Recompile)`, the optimizer can simplify the Case ladder at compile time to the single branch that applies for the current call, which avoids the cost of evaluating every branch of the Case at run time.

**v75-v77 references (deferred to follow-on versions):**

- [[Scalar UDF Parallelism Barrier]] for the v75 inlining of `dbo.fFormatNdcForDisplay`.
- [[Correlated Subqueries to CTEs]] for the v75 consolidation of the `InvReplenProductOverrides` and `InvNdcLocAssoc` subqueries.
- [[Table Variables vs Temp Tables]] is already followed in v73 (temp tables throughout); no v75-v77 work needed on this principle.
- [[UNION ALL Views]] for the v77 consolidation of the `#PriorityTcds` build.
- Cursor elimination does not yet have a dedicated masterclass entry. Section 11 carries the recommendation to write one as part of the v76 work.

---

## 6. Refactor (commented)

The v74 body is reproduced in `Refactored.sql`. The salient blocks below.

**Local-variable copies of all six input parameters.**

```sql
Set NoCount On;

Declare @LocalStationId                VarChar(30) = @StationId
Declare @LocalHideLowPriorityCanisters Bit         = @HideLowPriorityCanisters
Declare @LocalHideLostCanisters        Bit         = @HideLostCanisters
Declare @LocalShowOutOfStockCanisters  Bit         = @ShowOutOfStockCanisters
Declare @LocalSortColumn               Int         = @SortColumn
Declare @LocalSortDirection            Int         = @SortDirection
```

Every reference in the body below this point resolves to the local. The substitution was applied programmatically and verified by count: `@LocalStationId` appears 3 times in the body, the bit parameters 2 each, `@LocalSortColumn` and `@LocalSortDirection` 41 each (the ORDER BY Case ladder accounts for most of these). The parameter signature is preserved unchanged for backward compatibility with callers.

**Option (Recompile) on the final ORDER BY SELECT.**

```sql
Order By
   Case When @LocalSortColumn = 0 And @LocalSortDirection = 0 then Status End Asc,
   Case When @LocalSortColumn = 0 And @LocalSortDirection = 1 then Status End Desc,
   ...
   SortPriority Asc, NumAllocatedRxs Desc
Option (Recompile);
```

Recompile lets the optimizer simplify the Case ladder at compile time to the single branch that applies for the current call.

**Consolidated tail cleanup.**

```sql
Drop Table If Exists
      #CanisterList
    , #AllocatedRxCountsPerProductId
    , #PriorityTcds
    , #PriorityCans
    , #CanisterListFiltered
    , #NdcForwardStockLocs
    , #NdcBackStockLocs
    , #CanistersReadyOrInProcess
    , #CanistersForSubmitQueue
    , #ConfiguredStockLocDefs
```

The corresponding cleanup at the procedure entry is already in the modern `Drop Table If Exists` form in v73 and is preserved in place.

**Everything else preserved.**

The cursor loops, the scalar UDF call, the multi-branch UNION priority bucketing, and the correlated subqueries against `InvReplenProductOverrides` and `InvNdcLocAssoc` are preserved unchanged in v74. The parameter-sniffing fix alone is the v74 deliverable. The structural changes are queued for v75 through v77 per Section 11.

---

## 7. Risk & Rollback

**What could go wrong with v74.** The local-variable substitution is mechanical and the risk surface is narrow. The two concerns worth noting:

The Recompile hint on the final ORDER BY SELECT takes a per-call compile cost. The Case ladder is long (20 branches) but the compile cost should still be small in absolute terms; the optimizer-simplification benefit is large because Recompile collapses the Case to the single applicable branch.

The local-variable form removes the optimizer's ability to sniff a high-selectivity parameter value and produce a tight plan for that specific call. The trade-off is a stable plan compiled against the local-variable type (the optimizer's "default density" estimate). For the worst-case-to-average ratio this procedure exhibits, the stable plan is the better deal even when it is slightly slower than v73's best case.

**What to look for in the first 24 hours.** Plan variant count per site (expected to drop from many to one), max-vs-avg ratio per site (expected to drop from 249,457x to something measured in single or double digits), average duration per site (expected to fall materially for the previously-worst-case calls and rise slightly for the previously-best-case calls).

**Rollback path.** The v73 body is in `Original.sql` in this folder. Redeploying v73 reverts cleanly. No schema change, no index change.

---

## 8. Evidence of Refactor (v74)

Captured against Tolleson. Warm cache. Same data state as Section 3.2. Three captures matching the three v73 capture conditions.

```
(paste STATS IO / STATS TIME for v74 against the same data state used for v73 here)
```

The expected post-v74 signature is the same plan across the three captures and a meaningful drop in CPU time for the previously-worst-case calls. Reads on individual tables should be similar to v73 because v74 does not change the query structure beyond the local-variable substitution and the Recompile hint.

---

## 9. Comparison & Improvement

| Metric | v73 (paste, worst case) | v73 (paste, best case) | v74 (paste, stable) | Delta |
|---|---|---|---|---|
| Logical reads (total) | | | | |
| CPU time (ms) | | | | |
| Elapsed time (ms) | | | | |
| Plan operator count | | | | |
| Plan variant count across three captures | (expected: 3) | n/a | (expected: 1) | |
| Max-vs-avg ratio (cross-MFC, 30d) | 249,457x | n/a | (expected: <20x) | |
| Result row count | | | | |

**Verdict (to be entered once the captures land):** v74 delivers plan stability and addresses the dominant cost driver. The absolute read-cost numbers do not change much for the average call because the structural cost drivers (cursor loops, scalar UDF, correlated subqueries) are preserved. The plan-stability improvement is the headline expected delta, and it is what the cross-list capture identified as the primary issue.

---

## 10. Validation Checklist

Marked once the captures and the plans are in hand.

- Same data state. Captures taken back to back, ideally with a data freeze. (?)
- Warm cache only. Two runs per capture, cold-cache numbers discarded. (?)
- Non-zero result set. Three captures across different parameter shapes; each returned at least one row. (?)
- Identical result set. The local-variable substitution should not change the result set; same row count and same row identities at each parameter shape. (?)
- Plan shape matches prediction. One plan across the three captures. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original for the previously-worst-case calls; within tolerance for the previously-best-case calls. (?)

Net call: v74 is sound on first principles and addresses the dominant cost driver. Per-MFC post-deployment monitoring is required given the 249,457x baseline ratio.

---

## 11. Open Items / Future Improvements

The procedure carries enough structural complexity that v74 is the first of an expected four-version refactor. The remaining versions are scoped below.

### 11.1 v75: Scalar UDF inlining and the #CanisterList correlated subquery consolidation

**Scope:** inline `dbo.fFormatNdcForDisplay` with the equivalent inline Case expression, and pre-materialize two correlated-subquery patterns that appear identically in both `#CanisterList` INSERT blocks.

**Inline UDF:** the body of `dbo.fFormatNdcForDisplay` is a string formatter that inserts dashes at known positions. The equivalent expression is:

```sql
Case When IsNull(c.LastNdcAssigned, '') = ''
     Then ''
     Else Left(c.LastNdcAssigned, 5) + '-' + SubString(c.LastNdcAssigned, 6, 4) + '-' + SubString(c.LastNdcAssigned, 10, 2)
End
```

The exact format string should be confirmed against the UDF body before deployment. The inlined expression removes the parallelism barrier and removes the UDF invocation cost per row.

**Pre-materialize `InvReplenProductOverrides`:** the six correlated `(Select ... From InvReplenProductOverrides Where ProductId = c.ProductId)` references inside the per-model Case expressions can be replaced with a single one-time scan into `#InvReplenProductOverrides_Mat` keyed on ProductId, joined into both #CanisterList INSERT blocks.

**Pre-materialize `InvNdcLocAssoc`:** the `(Select Top 1 invla.loc From InvNdcLocAssoc invla With (NoLock) Where Left(invla.Ndc, 9) = c.ProductId)` references in both #CanisterList INSERT blocks can be replaced with a one-time scan that groups by `Left(Ndc, 9)` and picks the Min(loc) per group, joined into both blocks.

**Expected impact:** the per-row scalar UDF cost goes to zero, and the correlated-subquery cost per row drops from N reads of the small lookup tables to one indexed lookup per row.

### 11.2 v76: Cursor elimination

**Scope:** rewrite the six cursor loops as set-based equivalents.

The two cursors that iterate `#PriorityTcds` and `#PriorityCans` to build canister-to-tcd assignments are the highest-impact. The matching logic is "for each priority Tcd, find the matching priority canister by ProductId and Model, preferring closer current locations." This is a SET-based JOIN with priority-ordered tie-breaking, expressible as a `Row_Number() Over (Partition By TcdSn Order By ...)` window function over a `#PriorityTcds * #PriorityCans` join.

The four cursors that iterate over the forward-stock and back-stock location regex patterns are simpler: they build `#NdcForwardStockLocs` and `#NdcBackStockLocs` row-by-row by applying the regex against `InvNdcLocAssoc`. The set-based form is a single `Cross Apply` against a function that maps the regex to a SARGable predicate, or a `LIKE` pattern construction in T-SQL if the regex form is bounded enough.

**Expected impact:** elimination of the row-by-row cursor cost, which scales with `#PriorityTcds * average regex pattern count` in the worst case.

### 11.3 v77: Consolidate the #PriorityTcds multi-branch UNION

**Scope:** the ~15 UNION branches that build `#PriorityTcds` share a common join chain (`TcdStatus` left-joined to `TcdDryDispensers` and `TcdHoldReplen`) and differ only in the WHERE predicates. The consolidation is to build a single intermediate temp table from the common join chain and then derive the priority branches with explicit Case-driven SortPriority values in a single subsequent INSERT.

**Expected impact:** roughly 15x reduction in the per-row evaluation count for the common join chain. The optimizer benefit is uncertain pending plan inspection; the consolidation may produce a worse plan if the optimizer was effectively pruning each branch independently. v77 will be the version where the plan-shape comparison matters most.

### 11.4 Index inventory request

The pilot index extract does not cover the M4C dispenser family (`TcdStatus`, `TcdDryDispensers`, `TcdHoldReplen`, `CanCanister`, `TcdSecondaryData`, `TcdFlaggedTcds`, `InvReplenProductOverrides`, `InvOutOfStockProducts`, `InvNdcLocAssoc`, `ToteRouteLoc`). The index recommendations in v74 are written from query structure alone. v75 work should be preceded by an index extract against Tolleson for the full set of tables this procedure touches.

### 11.5 Per-MFC post-deployment monitoring

This procedure carries the largest plan-volatility ratio in the entire cohort. Per-MFC post-deployment monitoring is non-optional for v74 and for every subsequent version. The two-week monitoring window per site, the standard Improved/Unchanged/Regressed bucketing, and a per-site root cause analysis for any Regressed site follow the methodology in the Query Store Triage plan.

### 11.6 Consider whether the procedure surface area can shrink

This procedure carries six optional parameters and a sort selector that resolves at the database. Several of those concerns could move to the application layer. In particular, the sorting decision is something the operator chooses at the screen; sorting at the database (with a 20-branch Case ladder in the ORDER BY) is more expensive than returning the unsorted result and sorting at the application. The split-between-database-and-application conversation is an architectural question and belongs in the longer-arc backlog rather than in the per-procedure refactor sequence.
