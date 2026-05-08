# lsp_RxvGetRxsQueuedForVerification: Refactor Analysis (v7 to v8)

**Date:** 2026-05-07
**Tracking sheet row:** 19 (Priority P3, status Not Started)

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_RxvGetRxsQueuedForVerification`

**Purpose in one line:** Returns the list of Rxs that are filled, not yet verified, not currently being processed at a station, and that require visual verification under the active configuration, optionally filtered by fill type and stock-location prefix.

**Tables touched:**

- `OeOrder` (the workhorse for the #RxList build; filters on OrderStatus, DateFilled, DateVerified, FillType, AddrBank).
- `OeWorkflowStepInProgress` (LEFT JOIN with IS NULL probe, an in-flight "is this Rx at a station right now" check).
- `vCfgSystemParamVal` (the system-parameter view; read twice in v7, once in v8).
- `BnkConfiguredBanks`, `BnkAllBanks`, `BnkAutoFillType` (build the small `#SemiAutoFillBanks` lookup).
- `CaAuditRx`, `CaAuditRule` (build the small `#AuditRxs` lookup; the IsVisualOnly flag drives the audit branch of the WHERE clause).
- `ImgRxImgAssoc` (joined to #RxList; correlates Rxs to images via OrderId and HistoryDtTm).
- `ImgImage` and `ImgImage_IntId` (the two image stores; v7 reads them via two near-duplicate INSERT statements with different join shapes).
- `InvNdcLocAssoc` (the NDC-to-location map, joined when `@LocationFilter` is supplied).

**Indexes used:** to be confirmed against the v8 plan once captured. From the surface-area inspection, the relevant indexes against the OeOrder family are likely `ByOrderStatusFillTypeAddrBank` (matches the #RxList filter exactly when the OR/AND nest is flattened) and `PK_OeOrder` for the join into `ImgRxImgAssoc`. The image and association tables, the audit tables, the bank tables, and `InvNdcLocAssoc` are not in the index extract on hand. See Section 11 for the index DDL gap.

**Index DDL gap.** `IndexExtract.xlsx` produced for the Row 15 pilot covers the OeOrder family only. `ImgImage`, `ImgImage_IntId`, `ImgRxImgAssoc`, `OeWorkflowStepInProgress`, `CaAuditRx`, `CaAuditRule`, `BnkConfiguredBanks`, `BnkAllBanks`, `BnkAutoFillType`, `InvNdcLocAssoc`, and `vCfgSystemParamVal` are not in scope. Run `extract_index_ddl.sql` against Tolleson with the `@Tables` list set to those names to fill the picture in. Index recommendations in Section 11 are written against the surface area visible from the DDL alone.

**Callers:** the Verification application UI, called when the verification queue is loaded or refreshed. Call frequency is moderate-to-high (199K monthly per the tracking-sheet single-site number, 19.2M monthly across 7 reporting sites in the May 7 capture; see Section 3.1).

---

## 2. Overview of Performance

The tracking sheet flagged this proc on March single-site numbers (787M total reads, 199K execs, 4.0K avg reads/exec). The cross-MFC view from the 2026-05-07 Query Store capture shows the systemic picture: **roughly 772 billion logical reads per month across 19.2 million executions, distributed across 7 of 14 reporting sites**. That is more than three orders of magnitude above the single-site March headline once you roll up the fleet, and it puts the proc squarely in the upper-middle tier of I/O contributors. The detailed per-site breakdown is in Section 3.1.

Plan instability is present at three sites and worth flagging on its own. Indy, Liberty, and Memphis each return three plan variants for the same `query_id`, and the avg-duration spread between variants is large (Indy: 238 to 540 ms, Liberty: 142 to 394 ms, Memphis: 67 to 253 ms, all on the same statement). That is the parameter-sniffing signature, and it is consistent with the proc's two TVP parameters whose presence/absence flips the WHERE-clause shape on `@FillTypeFilter` and the IF/ELSE branch on `@LocationFilter`. Per-site avg reads per execution ranges from 10K (Mechanicsville) to 64K (Liberty), a 6x spread that confirms data-shape variance across MFCs.

The dominant cost drivers in v7 are structural:

1. The `#QueuedRxs` table is populated by two near-identical INSERT statements that differ only in which image table they join to (`ImgImage` via LEFT JOIN, `ImgImage_IntId` via INNER JOIN) and in their WHERE clause. The first INSERT is a textbook [[LEFT JOIN OR Anti-Pattern]] in disguise: `WHERE ContentCode In (2,3) Or @CfgRxDigitalVerificationMode = 'Local'` survives the LEFT JOIN even when no image matched, which is the same shape as "match path A or accept the row regardless." The optimizer cannot push the ContentCode predicate into the LEFT JOIN, and #RxList is scanned in full plus a key-lookup loop into ImgImage for each row.
2. The `#RxList` build has a 4-level nested OR/AND in its WHERE clause for the canister-verification autofill exclusion logic. The nest defeats clean index usage on the OeOrder side and forces the optimizer onto a wider index than the data shape needs.
3. `vCfgSystemParamVal` is read twice on every execution, once for `RxInheritsCanisterVerification` and once for `DigitalVerificationMode`. Two scans of the same view per call across 19 million calls per month is meaningful in absolute reads and in compile cost.
4. Five temp tables are created in cascade (`#QueuedRxs`, `#LocationNdcs`, `#AuditRxs`, `#RxList`, `#SemiAutoFillBanks`). None of them are indexed, so every join into them is a scan.

v8 consolidates the two INSERTs into one `INSERT...SELECT DISTINCT` over a `UNION ALL` of three explicit branches (ImgImage path, ImgImage_IntId path, Local-mode pass-through), all using INNER JOINs. It folds the two config reads into one. It pre-computes the autofill exclusion as a flat flag and rewrites the nested OR/AND as a flat AND list with one EXISTS probe. It adds clustered indexes to `#RxList`, `#AuditRxs`, and `#SemiAutoFillBanks` so the joins into them are seeks. The expected reduction is a 30 to 50 percent drop in per-execution reads, which at cross-MFC scale would save roughly 230 to 385 billion logical reads per month fleet-wide. The biggest absolute wins should land at the top three sites by reads (Liberty, Indy, Memphis), which together account for 65 percent of the captured load and which also carry the unstable plans that the structural simplification should help stabilize.

---

## 3. Evidence of Original (v7)

### 3.1 Query Store, cross-MFC view

**Source:** `diagnostics/querystore_outputs/QueryStore_20260507.xlsx`. Top 50 offenders per site, 30-day lookback, captured 2026-05-07. One tab per site.

**Aggregation method:** for each site, all rows whose `objectname` matches `lsp_RxvGetRxsQueuedForVerification` are aggregated. Multiple rows with the same `query_id` indicate plan instability. Multiple distinct `query_ids` indicate that more than one statement inside the proc is appearing in the top 50.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------------|----------------|
| Liberty | 3 | 1 | 3,531,149 | 225,885,759,789 | 63,969 | 142.5 to 394.5 | 517,345,558 |
| Indy | 3 | 1 | 3,165,216 | 147,912,641,150 | 46,731 | 238.6 to 540.5 | 552,404,637 |
| Memphis | 3 | 1 | 3,654,814 | 132,824,844,116 | 36,342 | 67.8 to 253.6 | 269,653,775 |
| Bolingbrook | 2 | 1 | 2,537,660 | 107,073,119,261 | 42,194 | 90.0 to 168.0 | 206,656,449 |
| Orlando | 2 | 1 | 1,999,834 | 105,419,241,187 | 52,714 | 137.3 to 263.4 | 179,099,361 |
| Mechanicsville | 1 | 1 | 2,969,898 | 30,597,929,851 | 10,303 | 113.5 | 53,980,893 |
| BrooklynPark | 1 | 1 | 1,341,404 | 21,916,505,446 | 16,338 | 16.9 | 20,437,556 |
| Canal | | | | | | | |
| Denver | | | | | | | |
| Kent | | | | | | | |
| Mansfield | | | | | | | |
| NorthLake | | | | | | | |
| Tolleson | | | | | | | |
| West Jordan | | | | | | | |

**Roll-up across the 7 reporting sites:** 19,199,975 executions, **771,630,040,800 total logical reads** (40,189 avg reads/exec). Seven sites did not surface the proc in their top 50 (Canal, Denver, Kent, Mansfield, NorthLake, Tolleson, West Jordan). The most likely reason is lower call volume at those sites, since the proc is tied to verification UI activity and that activity scales with the fill-station count and shift coverage. Worth confirming whether any of the seven are below the top-50 cut for unrelated reasons (workload mix, not call volume).

**Observations:**

- **Plan instability is concentrated at three sites and is severe.** Indy, Liberty, and Memphis each show three plan variants on the same statement, and the avg-duration ratio between fastest and slowest variants ranges from 2.3x at Liberty to 3.8x at Memphis. Indy's slowest variant runs at 540 ms avg, more than 2x its fastest at 238 ms. The 4-level nested OR/AND in the WHERE clause of the #RxList build is one likely structural contributor: it gives the optimizer multiple plausible plan shapes depending on cardinality estimation for each branch. Flattening the nest in v8 should reduce the number of plan shapes the optimizer can land on.
- **Per-site avg reads/exec varies 6x.** Liberty at 64K and Mechanicsville at 10K. Same proc, same code, very different cost. Consistent with the v7 cost being dominated by the #RxList scan and the duplicate INSERT into #QueuedRxs, both of which scale with the count of "Filled, not verified" Rxs at each site.
- **Top three sites contribute 65 percent of total reads.** Liberty (29%), Indy (19%), and Memphis (17%) together account for 506 billion of the 772 billion captured reads. Deploying v8 to those three first and capturing a clean before/after will surface the bulk of the systemic improvement.
- **No forced plans, no plan failures.** The `forcedplan` and `planfailures` columns are NULL for every row. Clean ground to deploy on.
- **Indy's CPU is disproportionate.** Indy spends 552 million ms of CPU per month on this proc, more than Liberty (517M) despite Liberty doing 53 percent more reads. The slow Indy plan is CPU-bound, which is consistent with the catch-all-style nested OR forcing repeated predicate evaluation on a scan.

### 3.2 SET STATISTICS IO, TIME on a representative MFC

I capture STATS IO/TIME from a representative MFC running both v7 and v8 against the same data state, with several common parameter combinations:

- `Exec lsp_RxvGetRxsQueuedForVerification @FillTypeFilter = <empty>, @LocationFilter = <empty>` (no filters, baseline)
- `Exec lsp_RxvGetRxsQueuedForVerification @FillTypeFilter = <typical fill types>, @LocationFilter = <empty>` (fill-type filter only)
- `Exec lsp_RxvGetRxsQueuedForVerification @FillTypeFilter = <empty>, @LocationFilter = <typical prefixes>` (location filter only)
- `Exec lsp_RxvGetRxsQueuedForVerification @FillTypeFilter = <typical>, @LocationFilter = <typical>` (both filters)

```
(STATS IO + STATS TIME output for v7 goes here, ideally one block per parameter combination.
Capture with `Set NoCount Off` so result-set row counts are visible. Capture the .sqlplan file
alongside.)
```

Per-table aggregated logical reads, scan counts, plan-shape summary, and per-statement timings will be filled in once the captures are pasted.

---

## 4. Issue Identification (v7)

The line numbers below reference `Original.sql` (v7).

### 4.1 Two near-duplicate INSERT statements into #QueuedRxs (lines 143 to 180)

The two blocks are textually almost identical. They differ in three places: the join type (LEFT vs INNER), the image table (`ImgImage` vs `ImgImage_IntId`), and the trailing `Or @CfgRxDigitalVerificationMode = 'Local'` predicate on the first block's WHERE clause.

Two structural issues compound here:

- **The first INSERT is the [[LEFT JOIN OR Anti-Pattern]] in disguise.** The `LEFT JOIN ImgImage` followed by `WHERE ContentCode In (2,3) Or @CfgRxDigitalVerificationMode = 'Local'` means: in non-Local mode the WHERE acts on the joined ImgImage row and forces the LEFT JOIN to behave as INNER (which the optimizer can sometimes recognize, but not always cleanly), and in Local mode the WHERE survives even when the LEFT JOIN produced no match. The optimizer cannot push the predicate into the join, and the end result is a scan of #RxList with a key-lookup loop into ImgImage. This is exactly the scan-with-residual-filter shape that the masterclass note describes.
- **Each INSERT carries its own SELECT DISTINCT.** Two separate dedup operations are performed, one per INSERT. A single dedup at the outer level after a UNION ALL collapses these into one operation.

### 4.2 4-level nested OR/AND filter in the #RxList build (lines 130 to 139)

```sql
And (A.IsVisualOnly = 1
     Or (A.IsVisualOnly Is Null
         And (@CfgRxInheritsCanisterVerification = 0
              Or ((O.FillType <> 'A')
                  Or
                  (O.FillType = 'A' And O.AddrBank In (Select Bank From #SemiAutoFillBanks))
                 )
             )
        )
    )
```

Three issues stack up:

- **The nesting prevents clean index usage.** The optimizer has to consider four logical paths into the row, each with its own filter predicate, and cannot push the filter cleanly into the `ByOrderStatusFillTypeAddrBank` index that exactly matches the leaf-level conditions (`OrderStatus = 'Filled'` AND `FillType <> 'A'` OR `FillType = 'A' AND AddrBank IN (...)`).
- **The IN-subquery against `#SemiAutoFillBanks` is evaluated row-by-row.** `#SemiAutoFillBanks` has no index in v7, so each evaluation is a scan of that small table. With #SemiAutoFillBanks indexed (v8) and the predicate rewritten as `EXISTS`, the evaluation is a clustered-index seek per row.
- **The condition is hard to read and easy to break.** The four logical paths are inter-related and the nesting makes the intent opaque. v8 collapses this into a flat list of OR'd cases keyed off `@ExcludeAutoFill`.

### 4.3 Two separate config reads against vCfgSystemParamVal (lines 50 to 70)

The proc issues two separate SELECTs against `vCfgSystemParamVal`, one for `RxInheritsCanisterVerification` and one for `DigitalVerificationMode`. The view is read twice per execution. Both rows are in the same `Section = 'RxVerification'` slice, so a single SELECT with a CASE-aggregated pivot reads the view once and assigns both variables.

This is a cosmetic-leaning issue (the per-call cost is small in absolute terms) but at 19 million monthly executions across the fleet, the doubled compile and scan cost is meaningful. More importantly, the consolidation is a net simplification: two statements become one, and the eight-line block becomes a four-line block.

### 4.4 Five temp tables, none of them indexed (lines 37 to 222)

Five temp tables are created in cascade, each populated by its own SELECT INTO. None of them carries a clustered index, which means every join into them in subsequent statements is a scan. The cost is largest on:

- `#RxList` joined to `ImgRxImgAssoc` on `(OrderId, HistoryDtTm)` from inside the duplicate INSERT statements.
- `#AuditRxs` joined to `OeOrder` on `OrderId` from inside the #RxList build.
- `#SemiAutoFillBanks` referenced by the IN-subquery from inside the nested OR/AND.

A clustered index on each temp table at the natural lookup column is a one-line change with predictable benefit, and is exactly the [[Table Variables vs Temp Tables]] pattern's "insert then index" recommendation.

### 4.5 The #AuditRxs build can multiply rows (lines 91 to 99)

If a single `OrderId` has multiple rows in `CaAuditRx` (one per audit rule), the LEFT JOIN from `OeOrder` to `#AuditRxs` in the #RxList build will multiply the OeOrder rows by the audit-rule count. The downstream SELECT INTO uses no DISTINCT, so a row multiplication produces duplicate rows in #RxList. v8 aggregates the build by OrderId with `Max(IsVisualOnly)` so #AuditRxs has one row per OrderId. The semantics are preserved (any visual-only audit rule on the OrderId qualifies the Rx for the visual-only branch).

### 4.6 Inconsistent capitalization on `With (NoLock)` and `nolock` (multiple lines)

A purely cosmetic issue. The proc mixes `With (NoLock)`, `with (nolock)`, and `With (NOLOCK)` across statements. v8 standardizes on `With (NoLock)`.

---

## 5. First Principles

### 5.1 LEFT JOIN OR Anti-Pattern

The first INSERT into `#QueuedRxs` matches the [[LEFT JOIN OR Anti-Pattern]] template precisely once you recognize that `Or @CfgRxDigitalVerificationMode = 'Local'` is a path-2 condition that survives the LEFT JOIN regardless of whether path 1 matched. The LEFT JOIN to `ImgImage` is path 1 (the image-match path), and the Local-mode pass-through is path 2 (the no-image-match path).

Two fixes are available:

- **Fix #1 (UNION branches):** split into one INNER JOIN branch per path, combined with UNION (or UNION ALL when overlap is impossible).
- **Fix #2 (materialize-once + EXISTS):** scan the base table once, then probe each path with EXISTS.

In this case the base table is `#RxList`, which is already a temp table built once at the top of the proc, so Fix #2 is functionally already in place at the top. The work needed is at the INSERT into #QueuedRxs: split the LEFT JOIN OR into branches with INNER JOINs. The branches are mutually exclusive enough that UNION ALL plus a single outer DISTINCT is correct (an Rx can match both ImgImage and ImgImage_IntId paths in principle, but the DISTINCT collapses the duplicates; the Local-mode branch contributes additional rows only when in Local mode and is mutually exclusive with the other two branches in the sense that DISTINCT swallows any overlap).

This is the deeper principle behind the duplicate-INSERT consolidation. The tracking sheet describes it as a UNION ALL consolidation, and that is correct as far as it goes, but the underlying reason the consolidation works is that the original was a LEFT JOIN OR shape that the engine could not optimize cleanly.

### 5.2 UNION ALL Consolidation Is Materialize-Once Reapplied

The same materialize-once principle from [[FOR XML PATH Consolidation]] applies here in compressed form. v7 evaluates the same INSERT shape twice with different image tables and a slightly different WHERE clause. v8 evaluates it once over a UNION ALL of the three branches. The structure of the INSERT is run once; the two image tables and the Local-mode pass-through are alternatives within that single structure. The optimizer plans the whole UNION ALL in one pass with one cardinality view, and the SELECT DISTINCT at the outer level dedups across branches in a single sort instead of two per-INSERT dedups.

The principle: when the same operation is being performed against multiple alternative sources with the same column shape, consolidate the alternatives into a UNION ALL and run the operation once over the consolidated source. Whether the operation is a FOR XML PATH expression, an INSERT, or a SELECT, the win is the same. One plan operator instead of N, one cardinality estimate instead of N independent ones, one dedup instead of N.

### 5.3 Pre-Compute Configuration Flags Outside the Set-Based Statement

The 4-level nested OR/AND in the #RxList build mixes configuration logic (does the system inherit canister verification?) with row-level logic (is this Rx's FillType 'A'?). The optimizer cannot tell that `@CfgRxInheritsCanisterVerification = 0` is constant for the duration of the call. It plans for both branches and produces a wider plan than necessary.

Pre-computing the configuration logic into a single flag (`@ExcludeAutoFill = @CfgRxInheritsCanisterVerification`) and reading the flag in a flat predicate gives the optimizer a constant to fold at runtime, and gives the human reading the SQL a clearer split between the configuration-driven cases and the row-level filters. The masterclass equivalent is in the [[Catch-All Query Anti-Pattern]] discussion of "use local variables to make the optimizer's job easier" but the principle generalizes: anything that is constant for the duration of a call should be a local variable, not a column in a nested condition.

### 5.4 Index Temp Tables That Are Joined To

`#RxList`, `#AuditRxs`, and `#SemiAutoFillBanks` all participate in joins in subsequent statements. v7 has no clustered index on any of them. The cost is a series of scans that each look small in isolation but compound across the call rate. See [[Table Variables vs Temp Tables]] for the underlying principle: a temp table without an index gives the optimizer a heap; with a clustered index on the natural join key, the optimizer can choose seek-based joins with proper cardinality estimation.

The "insert then index" pattern is the operational form: insert all rows first, then `Create Clustered Index` on the natural join key. Building the index on a complete dataset is a single sort, which is faster than maintaining the b-tree during inserts. v8 applies this pattern to all three temp tables.

### 5.5 Aggregate Lookup Tables to Their Natural Grain

The v7 `#AuditRxs` build can produce multiple rows per `OrderId` if an Rx has multiple audit-rule mappings. The downstream LEFT JOIN to `#AuditRxs` in the #RxList build will multiply OeOrder rows by the audit-rule count, which then feeds into the duplicate-INSERT into #QueuedRxs and ultimately into the final SELECT DISTINCT. The DISTINCT at the end masks the multiplication, but the cost of the multiplied rows is paid through every intermediate join.

Building lookup temp tables at their natural grain (one row per `OrderId`, with `IsVisualOnly` aggregated as `Max`) avoids the row multiplication entirely. The semantics are preserved: any visual-only audit rule on the OrderId qualifies the Rx for the visual-only branch, which is what `Max(Cast(IsVisualOnly As Int))` expresses. This is a structural correction, not a performance optimization, but it has performance consequences when the multiplication factor is meaningful.

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v8. The narrative below walks the changes block by block. Line references are to `Refactored.sql`.

### 6.1 Single config-table read (lines 53 to 67)

```sql
Select
     @CfgRxInheritsCanisterVerification =
        Max(Case When SW.Parameter = 'RxInheritsCanisterVerification'
                 Then IIF(SW.Value = 'True', 1, 0) End)
   , @CfgRxDigitalVerificationMode =
        Max(Case When SW.Parameter = 'DigitalVerificationMode'
                 Then SW.Value End)
From vCfgSystemParamVal As SW With (NoLock)
Where SW.Section = 'RxVerification'
  And SW.Parameter In ('RxInheritsCanisterVerification', 'DigitalVerificationMode')
```

One scan of `vCfgSystemParamVal` instead of two. The `MAX(CASE WHEN ...)` pivot returns one scalar per parameter into the matching variable. The `Section = 'RxVerification' AND Parameter IN (...)` predicate is selective and seek-friendly against any composite index on `(Section, Parameter)` (which the view should have, given that it is a config view; verify against the index DDL when it is captured).

### 6.2 Pre-computed @ExcludeAutoFill flag (line 75)

A single flat flag that drives the autofill exclusion logic in the #RxList build. v7 expressed this as a 4-level nested OR/AND that the optimizer had to plan for at compile time. v8 resolves it at runtime to a single bit, then reads the bit in a flat predicate.

### 6.3 Indexed #SemiAutoFillBanks and #AuditRxs (lines 87 to 105)

`#SemiAutoFillBanks` is probed by the EXISTS clause in the #RxList build. The clustered index on `[Bank]` makes that probe a seek instead of a scan. `#AuditRxs` is LEFT JOINed to `OeOrder.OrderId` in the #RxList build, and the clustered index on `OrderId` makes that join a seek-based probe.

`#AuditRxs` is also aggregated to one row per `OrderId` via `Max(Cast(IsVisualOnly As Int))`. v7 carried the raw join output, which can multiply rows. v8 collapses to natural grain.

### 6.4 Flattened #RxList WHERE clause (lines 116 to 132)

Two changes from v7:

- The 4-level nest is reshaped into a flat OR list at the second level. The four logical paths (`@ExcludeAutoFill = 0`, `FillType <> 'A'`, `FillType = 'A' AND in SemiAutoFillBanks`, plus the audit-visual-only short-circuit on the first level) are now each one OR'd predicate at the appropriate level. The optimizer can recognize the FillType conditions as a set of mutually exclusive cases and choose `ByOrderStatusFillTypeAddrBank` for the leaf-level seek.
- The `O.AddrBank In (Select Bank From #SemiAutoFillBanks)` is rewritten as `Exists (...)`. EXISTS short-circuits on the first match and is the canonical form for "does at least one matching row exist." Combined with the clustered index on `#SemiAutoFillBanks.[Bank]`, the probe is a single seek.

### 6.5 #RxList clustered index (line 134)

```sql
Create Clustered Index IX_RxL On #RxList (OrderId, HistoryDtTm)
```

Supports the join into `ImgRxImgAssoc` from the consolidated INSERT into #QueuedRxs. The composite key matches the join condition exactly.

### 6.6 Single INSERT into #QueuedRxs with UNION ALL (lines 152 to 192)

Three v7 changes are folded together here:

- The two separate INSERT statements collapse into one `INSERT...SELECT DISTINCT` over a UNION ALL of three branches. Two dedup operations (one per INSERT) become one.
- The LEFT JOIN OR pattern from v7's first INSERT is split into two explicit INNER JOIN branches plus a Local-mode pass-through branch. Each INNER JOIN branch can be planned with a clean predicate-pushdown and the `ByOrderStatusFillTypeAddrBank` (or equivalent) index choice. The Local-mode branch returns rows only when the config flag is set; in non-Local mode the optimizer prunes it as a constant-false branch.
- UNION ALL is correct because the outer SELECT DISTINCT handles dedup across branches. UNION (with implicit dedup per pair of branches) would be wasted work given that the outer SELECT already has DISTINCT.

### 6.7 Final result (lines 196 to 217)

The IF/ELSE on `@LocationFilter` is preserved unchanged from v7. Each branch produces a clean cached plan, and the parameter is genuinely optional. The catch-all anti-pattern does not apply here because the IF/ELSE shape avoids it. No change needed.

### 6.8 Cleanup (lines 219 to 225)

Explicit drops at the end of the proc, preserved from v7. SQL Server cleans up session-scoped temp tables automatically when the session ends, but explicit drops free tempdb pages immediately and avoid carrying state across rapid back-to-back invocations of the proc. Given the call rate of 19 million per month fleet-wide, explicit cleanup is worth keeping.

---

## 7. Risk & Rollback

### Risks

- **Semantic drift on the LEFT JOIN OR split.** The v7 first INSERT used `LEFT JOIN ImgImage WHERE ContentCode In (2,3) Or @CfgRxDigitalVerificationMode = 'Local'`. The v8 split is into one INNER JOIN to ImgImage with `ContentCode In (2,3)` and a separate Local-mode pass-through branch. The two should be semantically equivalent for the produced row set: in non-Local mode v7 required an ImgImage match (the LEFT JOIN result with WHERE on a column from the joined table behaves as INNER), and v8 enforces that with the INNER JOIN. In Local mode v7 included rows regardless of ImgImage match, and v8 includes rows via the pass-through branch. The risk is that there is some edge case (e.g., Local mode AND a matching ImgImage row) where v7 would have returned the row only once via the WHERE survival path while v8 returns it twice (once from the ImgImage branch, once from the Local-mode branch). The outer SELECT DISTINCT handles that case. Verification should confirm row-count parity across at least the four parameter combinations listed in Section 3.2.
- **Semantic drift on the #AuditRxs aggregation.** v8 aggregates `IsVisualOnly` to one row per OrderId via `Max(Cast(IsVisualOnly As Int))`. If multiple audit rules on the same OrderId have inconsistent `IsVisualOnly` values, v7 would have evaluated them in some non-deterministic order through the LEFT JOIN; v8 collapses them with MAX (returning 1 if any rule is visual-only). The intended semantics, judging from the surrounding code, are "visual-only audit rule wins" since the visual-only branch is a short-circuit at the top of the OR. The MAX preserves that. Confirm with the application owners that this matches the business rule.
- **Plan choice on the consolidated INSERT.** The v8 consolidated INSERT will be planned with a single combined cardinality estimate over the three branches. If branch 3 (Local-mode pass-through) is a large fraction of #RxList in Local-mode environments, the optimizer may choose a hash-based dedup at the outer SELECT instead of a sort-based dedup. That is the correct call given the cardinality, but it represents a different memory grant shape than v7's two separate sort-based dedups, and it is worth confirming via the .sqlplan that no excessive memory grant warnings appear.
- **The TVP catch-all on `@FillTypeFilterExists`.** The predicate `(@FillTypeFilterExists = 0 Or F.[Value] Is Not Null)` is preserved from v7. It is a mini catch-all-style predicate, but on a TVP rather than a scalar parameter, and the LEFT JOIN to the TVP is reasonable here. Out of scope for v8. If the parameter-sniffing volatility seen at Indy/Liberty/Memphis persists after deployment, this is the next place to look.

### Rollback

The proc is a read-only `SELECT` returning a result set. Rollback is a pure DDL revert: restore the v7 body via `ALTER PROCEDURE`. No data state is mutated, so there is no compensating action required.

If a regression appears, run the v7 form (committed at git revision `2ecd7f1`) against the same data state to confirm. Rollback path:

```sql
-- pull v7 from git
git show 2ecd7f1:stored_procedures/lsp_RxvGetRxsQueuedForVerification.sql

-- adjust CREATE -> ALTER, deploy
```

### Monitoring Window

For the first 24 hours after deployment, watch:

- Verification UI for unexpectedly empty result sets or duplicate rows. The application is interactive, so a row-count anomaly will surface as user-visible behavior.
- Query Store for `lsp_RxvGetRxsQueuedForVerification` average duration, average reads, and plan count. Plan count should stabilize at 1 to 2 once the new shape is cached. The unstable-plan sites (Indy, Liberty, Memphis) should converge.
- Tempdb allocation. Five temp tables and three new clustered indexes are small in absolute terms, but at 19 million calls per month fleet-wide the churn is non-trivial. Watch the "unallocated extents" counter for the first hour after deployment to a high-volume site.
- The #AuditRxs aggregation. If the aggregation reduces the row count meaningfully, #RxList should shrink as well, which is the expected outcome. If #RxList grows or holds steady, the aggregation either had no effect (no multi-rule OrderIds in the data) or v7 was masking a duplication bug I have not yet surfaced.

---

## 8. Evidence of Refactor (v8)

Source: same results file as Section 3.2, second half. Captured back to back on the same MFC and same data state.

```
(STATS IO + STATS TIME output for v8 goes here, one block per parameter combination.
Capture with `Set NoCount Off` so result-set row counts are visible. Capture the .sqlplan
file alongside.)
```

Per-table aggregated logical reads, scan counts, plan-shape summary, and per-statement timings will be filled in once the captures are pasted. The post-refactor plan should show:

- One INSERT operator into `#QueuedRxs` instead of two.
- One sort/hash dedup at the outer SELECT instead of two per-INSERT dedups.
- Index Seeks on `#RxList`, `#AuditRxs`, and `#SemiAutoFillBanks` where v7 had Table Scans.
- A constant-folded branch on `@CfgRxDigitalVerificationMode = 'Local'` that prunes branch 3 of the UNION ALL when the mode is not Local.
- Predicate pushdown into `ByOrderStatusFillTypeAddrBank` (or equivalent OeOrder index) on the #RxList build, where v7 was forced to a wider seek with residual filter.

---

## 9. Comparison & Improvement

*Filled in once both v7 and v8 STATS IO/TIME outputs are pasted above.*

| Metric | v7 (5 temp tables, dup INSERT) | v8 (consolidated INSERT, indexed temps) | Delta | % Change |
|--------|--------------------------------|------------------------------------------|-------|----------|
| Logical reads, no-filter call | | | | |
| Logical reads, fill-type filter | | | | |
| Logical reads, location filter | | | | |
| Logical reads, both filters | | | | |
| CPU time per call (ms) | | | | |
| Elapsed time per call (ms) | | | | |
| Plan operator count | | | | |
| Result row count (must match) | | | | |

**Plan-shape verification:**

- v7 plan should show two INSERT operators into `#QueuedRxs`, two SELECT DISTINCT sorts, a LEFT JOIN with residual filter on the first INSERT, and Table Scans on `#RxList`, `#AuditRxs`, `#SemiAutoFillBanks`.
- v8 plan should show one INSERT into `#QueuedRxs`, one outer SELECT DISTINCT (sort or hash), three INNER JOIN sub-plans inside the UNION ALL (one of which is constant-folded out in non-Local mode), and Index Seeks on `#RxList`, `#AuditRxs`, `#SemiAutoFillBanks`.

---

## 10. Validation Checklist

Per `tasks/lessons.md`. Mark each pass or fail before declaring the refactor done.

- [ ] **Same data state.** Both v7 and v8 captures taken back to back, with no intervening writes to the touched tables.
- [ ] **Warm cache only.** Both captures are the second of two consecutive runs.
- [ ] **Non-zero result set.** Each parameter combination returned at least one row. The verification queue is rarely empty during normal operating hours; capture during a busy window.
- [ ] **Identical result set.** v7 and v8 returned the same rows (same OrderIds, same DateFilled values, same PriInternalShort) for each parameter combination.
- [ ] **Plan shape matches prediction.** v8 shows one INSERT operator into `#QueuedRxs`, Index Seeks on the indexed temp tables, and a constant-folded Local-mode branch in non-Local environments.
- [ ] **No new error or warning messages.** Neither run produced cardinality errors, conversion warnings, or excessive memory grant warnings.
- [ ] **Warm-cache elapsed time at or below v7.** v8 is faster on every parameter combination, with the largest improvement on calls that produce non-trivial #RxList row counts.

End the section with a one-sentence net call once the checklist is filled in.

---

## 11. Open Items / Future Improvements

These are observations made during the deep dive that are not in v8. Sections 11.1 and 11.2 are concrete and ready to execute. Section 11.3 is longer-horizon. Estimated impact assumes Indy-shape data and 19 million monthly executions across the captured sites.

### 11.1 Index Recommendations (high-value, no proc change required)

#### A. Pull index DDL for the missing surface area

Run `extract_index_ddl.sql` against Tolleson with the `@Tables` list set to:

```sql
@Tables = ('ImgImage', 'ImgImage_IntId', 'ImgRxImgAssoc',
           'OeWorkflowStepInProgress', 'CaAuditRx', 'CaAuditRule',
           'BnkConfiguredBanks', 'BnkAllBanks', 'BnkAutoFillType',
           'InvNdcLocAssoc', 'CfgSystemParamVal', 'CfgSystemParamDef')
```

This is a prerequisite for the rest of Section 11. Several of the recommendations below assume specific indexes exist or do not exist; confirm before acting.

#### B. Confirm the `ByOrderStatusFillTypeAddrBank` index on OeOrder is being used

The index extract for the Row 15 pilot shows `ByOrderStatusFillTypeAddrBank On dbo.OeOrder (OrderStatus, FillType, AddrBank)`. With the v8 flat WHERE clause, this index is the natural seek path for the #RxList build. If the v8 plan shows the optimizer choosing a different index (e.g., `ByOrderStatus` plus residual filter), consider an index hint or update statistics on the index. The index is small (8.49 MB per the Row 15 extract) so it should always be in cache.

#### C. Composite key on `(Section, Parameter)` for `vCfgSystemParamVal`

The v8 single-read against the config view uses `Where Section = 'RxVerification' And Parameter In ('RxInheritsCanisterVerification', 'DigitalVerificationMode')`. If the underlying tables behind the view do not have a composite index on `(Section, Parameter)`, the seek will fall back to a scan. The view is small (a few thousand rows at most in typical deployments), so the absolute cost is small, but at 19 million monthly executions any avoidable scan adds up. Confirm against the index DDL extract from 11.1.A and create the composite index if needed.

#### D. Index on `ImgRxImgAssoc(OrderId, HistoryDtTm) Include (ImgId)`

The consolidated INSERT into #QueuedRxs joins #RxList to ImgRxImgAssoc on `(OrderId, HistoryDtTm)` and reads `ImgId` to join into the image tables. A composite key on `(OrderId, HistoryDtTm)` with `ImgId` as an INCLUDE column would give the optimizer a covering index for this join. Confirm against 11.1.A whether this index already exists; if not, add it. Estimated impact is meaningful because the #RxList-to-ImgRxImgAssoc join runs across both UNION ALL branches.

### 11.2 Proc-Level Changes (out of scope for v8 but ready to apply next)

#### A. Re-evaluate the `@FillTypeFilterExists = 0 Or F.[Value] Is Not Null` predicate

This is a mini catch-all-style predicate on the LEFT JOIN to `@FillTypeFilter`. The TVP is small so the absolute cost is small, but if the parameter-sniffing volatility at Indy, Liberty, and Memphis persists after the v8 deployment, this is the next place to look. The structural fix is to branch on `@FillTypeFilterExists` at the proc level (similar to the IF/ELSE on `@LocationFilter`):

```sql
If @FillTypeFilterExists = 1
   Begin
      -- #RxList build with INNER JOIN to @FillTypeFilter
   End
Else
   Begin
      -- #RxList build with no FillType filter
   End
```

This produces two cached plans, each tuned to its parameter shape, and removes the OR-IS-NULL pattern entirely. Estimated impact depends on how often the parameter is supplied.

#### B. Remove the redundant top-of-proc `Drop Table If Exists` block

The proc has two `Drop Table If Exists ...` blocks for the same five temp tables (one at the top, one at the end). The top block is defensive (in case a previous call left state behind) and the end block is hygiene. Both are correct, but if connection-pooling guarantees a clean session start, the top block can be removed. Out of scope for v8 since the cost is negligible.

#### C. Surface a `@TopN` parameter for verification-queue UI pagination

The Verification UI reads from this proc and likely paginates its display. If the UI consumes only the first N rows, surfacing `@TopN` with `TOP (@TopN)` on the outer SELECT lets the optimizer apply rowgoal across the full pipeline. Requires checking the caller contract. Same pattern as the implicit-TOP recommendation in the Row 15 pilot's Section 11.3.

### 11.3 Schema-Level Changes (longer horizon)

These require schema changes and are noted for future consideration:

- **Filtered index on `OeOrder` for `OrderStatus = 'Filled' AND DateVerified Is Null`.** The combination of these two predicates is the "queued for verification" universe; a filtered index keyed on `(OrderId, HistoryDtTm)` with `FillType, AddrBank, DateFilled, PriInternal, Ndc` as INCLUDE columns would cover the entire #RxList build. The filter is selective (only Rxs in the queue, not the full OeOrder table) so the index would be small. Requires DBA blessing because filtered indexes have query-shape compatibility constraints that need monitoring.
- **Re-evaluate the `ImgImage` vs `ImgImage_IntId` split.** If `ImgImage_IntId` is a migration target that has been completed (or abandoned), one of the two image stores is empty and the UNION ALL branch can be dropped. See [[UNION ALL Views]] for the operational considerations. Worth a brief audit of row counts on both tables across the fleet.
- **Persisted computed column `IsVisualOnlyAudit` on `OeOrder`.** Eliminates the entire `#AuditRxs` build by promoting the audit-visual-only flag onto OeOrder directly. Requires DBA blessing and an application change to maintain the column on audit-rule changes; longer-term move.
