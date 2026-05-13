# lsp_TcdGetNextCountLauncherRx: Refactor Analysis (v9 to v10)

**Date:** 2026-05-08
**Tracking sheet row:** 22 (Priority P0, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v10 designed by the MFC DBA team and me, awaiting iA review.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_TcdGetNextCountLauncherRx`

**Purpose in one line:** Returns the next candidate Scheduled, Auto-Fill Rx for the TCD Controller Count Launcher to attempt to count within a given cabinet bank, choosing the candidate by sibling status, priority, and date.

**Tables touched:**

- `OeOrder` (the workhorse; queried three times in v9, once as the main candidate scan and twice inline for sibling lookups).
- `TcdCountLauncherAlreadyConsideredRxs` (the per-pass exclusion table; left-joined with NULL probe).
- `vCfgSystemParamVal` (the system-parameter view; read once at the top to retrieve `LaunchCountingOfRxGroupsTogether`).

**Indexes used (predicted from the v10 body, to be confirmed against the captured plan):**

| Table | Index | Where used |
|---|---|---|
| `OeOrder` | `ByOrderStatusFillTypeAddrBank` (NC, key `OrderStatus, FillType, AddrBank`) | the main candidate scan; v9 was forcing this hint, v10 lets the optimizer choose it naturally |
| `OeOrder` | `ByOrderStatus` (NC, key `OrderStatus`) | the CountedSiblings CTE and the group-together inner aggregate |
| `OeOrder` | `PK_OeOrder` (clustered, key `OrderId, HistoryDtTm`) | the sibling-OrderId equality join |
| `TcdCountLauncherAlreadyConsideredRxs` | clustered on `OrderId` (assumed) | per-call already-considered exclusion |
| `vCfgSystemParamVal` | base-table indexes on `Section, Parameter` | one-time config read |

**Index DDL gap.** The pilot index extract covers the `OeOrder` family in detail. `TcdCountLauncherAlreadyConsideredRxs` is not in the extract. Confirm the clustered key on Tolleson before final sign-off so the recommendations below are written against ground truth.

**Callers:** the TCD Controller Count Launcher. Called once per "what should I count next" question, in a tight loop, until the cabinet bank exhausts its candidate population for the current pass.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture shows it appearing on all four expense lists (Volume rank 3, Duration rank 1, Expense rank 4, Plan rank 24) and because its worst-case-to-average ratio is 6,024x. A 6,024x spread on the same statement is not a query-shape problem; it is a plan-volatility problem. The optimizer is producing wildly different plans for the same statement depending on what parameter values it sniffs at compile time, and the worst-case plan is several orders of magnitude more expensive than the best-case plan. On a tight-loop caller like the Count Launcher, the worst-case plan is what the operator sees during the bad windows.

The dominant cost drivers in v9 are three. The first is the non-SARGable `PriCodeSys <> Case @InclFillByNotReachedPriRxs ...` predicate in the main candidate scan. Putting a Case inside the WHERE clause prevents the optimizer from choosing an index seek on `PriCodeSys` and from caching a tight plan per branch. The procedure has two distinct operational modes (include all priorities, exclude PriCodeSys = 60), and a single cached plan serves both modes badly.

The second is parameter sniffing on `@AddrBank`. The cabinet banks at any one site vary significantly in their candidate population. A plan compiled against a low-volume bank will scan-then-filter when it should seek; a plan compiled against a high-volume bank will produce excessive logical reads when seeking would have sufficed. The worst-case-to-average ratio reflects this directly.

The third is the duplicated sibling-OrderId derivation. The expression `Left(OrderId, Len(OrderId) - 2) + Cast(... As Char(1)) + Right(OrderId, 1)` appears twice in v9 (once in the group-together path, once in the main path) inside inline derived tables. The construction is non-SARGable against any natural index, so the inner query scans `OeOrder` filtered by `OrderStatus In ('Counting','Counted','Suspended')` and then materializes the constructed sibling key. Doing it once in a CTE is a structural cleanup; doing it once total per call across the two paths is a follow-up that requires more invasive restructuring and is documented in Section 11.

The v10 refactor addresses all three of these systematically, plus four smaller issues catalogued in Section 4.

---

## 3. Evidence of Original (v9)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Max-vs-avg ratio |
|-----|---------------|------------------|-------------|----------------|--------------------|------------------|
| (paste from capture) | | | | | | |

**Roll-up across reporting sites:** to be filled in from the May 7 capture. The cross-list capture reports the procedure on all four expense lists, with a 6,024x worst-case-to-average ratio that is the program's strongest plan-volatility signal among the rows 22 to 39 cohort.

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v9)

Captured against Tolleson. Warm cache. Two runs at different `@AddrBank` values are required to expose the plan-volatility signature; the median run is the baseline for Section 9.

```
(paste STATS IO / STATS TIME for v9 against the same data state used for v10 here)
```

The expected v9 signature is two `OeOrder` scans on every call (the main candidate scan plus the inline sibling-OrderId derived table) plus a third in the group-together path when that branch is taken. Read counts vary widely with the chosen plan; capturing two different parameter values is what makes the volatility visible.

---

## 4. Issue Identification

The v9 body has seven distinct issues. Each one corresponds to a fix in v10.

**Issue 1: Non-SARGable Case predicate in the WHERE clause (line 180 in v9).** `O.PriCodeSys <> Case @InclFillByNotReachedPriRxs When 1 Then -1 Else 60 End` defeats index seek on `PriCodeSys` and forces the optimizer to cache a single plan for two operational modes.

**Issue 2: Parameter sniffing on `@AddrBank` and `@InclFillByNotReachedPriRxs`.** Both parameters flow into the candidate scan WHERE clause without a local-variable indirection. The cabinet bank population varies dramatically across `@AddrBank` values, and the bimodal `@InclFillByNotReachedPriRxs` is sniffed once and cached. The 6,024x worst-case ratio reflects this.

**Issue 3: Forced `Index(ByOrderStatusFillTypeAddrBank)` hint on the main candidate scan (line 153 in v9).** The hint is a vestige of the original query's ambiguity. Hints of this kind freeze the plan choice and prevent the optimizer from adapting when statistics change.

**Issue 4: Legacy `Object_Id` temp table check (lines 69-70, 125-126, 131-132 in v9).** Three separate `If OBJECT_ID('TempDb..#temp') Is Not Null Drop Table #temp` blocks. The modern equivalent is `Drop Table If Exists #temp`.

**Issue 5: `If Exists (Select * From #temp)` after the INSERT that just populated #temp (line 114 in v9).** The Exists scan re-reads the temp table that was just populated. `@@ROWCOUNT` immediately after the INSERT carries the same information without the re-read.

**Issue 6: Duplicated sibling-OrderId inline derived table.** Lines 89-103 and 154-168 carry nearly identical inline derived tables. The derivation is structurally awkward (concatenating a constructed character into the middle of the OrderId) and is harder to reason about embedded in the main FROM clause than as a CTE.

**Issue 7: The inner `Select GroupNum, DateFilled, LastStatusChgDtTm From OeOrder Where OrderStatus In (...) Or DateFilled Is Not Null` in the group-together path returns one row per qualifying Rx, not one row per group.** Joining that to the outer `OeOrder` on GroupNum can produce a cardinality explosion in cases where a group has many counted Rxs. A pre-aggregation to one row per GroupNum is cheaper.

---

## 5. First Principles

The refactor draws on four masterclass entries.

**[[Parameter Sniffing]] with the local-variable form.** The local variables `@LocalAddrBank` and `@LocalInclFillByNotReachedPriRxs` are assigned from the parameters at the top of the procedure. Downstream queries reference the locals. The optimizer caches plans against the local-variable-typed plan, which is the canonical "ideal" plan for a parameter-stable workload.

**[[Catch-All Query Anti-Pattern]] with the explicit-branch fix.** The non-SARGable `<> Case ...` predicate is the catch-all signature. The fix is to split the query into two explicit branches selected by an IF on the parameter, so each branch has a tight WHERE clause that the optimizer can plan independently. This is what v10 does for the main candidate scan.

**[[Correlated Subqueries to CTEs]] with the duplicate-derived-table fix.** The duplicated sibling-OrderId derivation is structurally cleaner as a CTE referenced from each branch. The CTE form expresses the derivation once per branch and makes the join condition `O.OrderId = CS.SiblingOrderId` the visible join predicate rather than burying it inside an inline derived table.

**[[TOP with ORDER BY Semantics]] applied to the @@ROWCOUNT fix.** Replacing `If Exists (Select * From #temp)` with `@@ROWCOUNT > 0` after the INSERT removes a second read of a just-populated temp table. The rowgoal benefit is incidental; the larger win is eliminating the redundant scan.

A net-new pattern that does not yet have its own masterclass entry but recurs across the procedure library is the "constructed-key self-join," where a procedure derives a key from columns of one row and joins back to the same table on the derived key. The pattern appears here as the sibling-OrderId derivation and shows up in at least two other procedures in the optimization list. Section 11 carries the recommendation to write a "Constructed-Key Self-Joins" entry the next time the pattern surfaces in a deep-dive.

---

## 6. Refactor (commented)

The v10 body is reproduced in `Refactored.sql`. The salient blocks below.

**Local variables after the input parameters.**

```sql
-- Local variable copies break parameter sniffing on the downstream queries.
Declare @LocalAddrBank VarChar(2) = @AddrBank
Declare @LocalInclFillByNotReachedPriRxs TinyInt = @InclFillByNotReachedPriRxs
```

**Group-together path with a pre-aggregated inner subquery and a CTE for the sibling-OrderId.**

```sql
;With CountedSiblings As
(
    Select
        [SiblingOrderId] = Left(OrderId, Len(OrderId) - 2)
                         + Cast((Cast(SubString(OrderId, Len(OrderId) - 1, 1) As SmallInt) % 2) + 1 As Char(1))
                         + Right(OrderId, 1)
    From OeOrder With (NoLock)
    Where OrderStatus In ('Counting', 'Counted', 'Suspended')
      And AddrBank = @LocalAddrBank
      And OrderId Like '%<[1-2]>'
)
Select ...
From OeOrder As O With (NoLock)
Inner Join
(
    -- One row per group, not one row per qualifying Rx.
    Select GroupNum, DateFilled = Max(DateFilled), LastStatusChgDtTm = Min(LastStatusChgDtTm)
    From OeOrder With (NoLock)
    Where OrderStatus In ('Counting', 'Counted', 'Suspended')
       Or DateFilled Is Not Null
    Group By GroupNum
) As CFG
    On O.GroupNum = CFG.GroupNum
Left Join CountedSiblings As CS
    On O.OrderId = CS.SiblingOrderId
Left Join TcdCountLauncherAlreadyConsideredRxs As C With (NoLock)
    On O.OrderId = C.OrderId
Where ...
Option (Recompile)

If @@ROWCOUNT > 0
Begin
    Set NoCount Off
    Select Top 1 * From #temp ...
    Return
End
```

**Main path split into two explicit branches.**

```sql
If @LocalInclFillByNotReachedPriRxs = 1
Begin
    -- Include all priorities; no PriCodeSys filter.
    Select Top 1 ... From OeOrder ... Where ...
    Option (Recompile)
End
Else
Begin
    -- Exclude PriCodeSys = 60 (Fill By Not Reached).
    Select Top 1 ... From OeOrder ... Where O.PriCodeSys <> 60 ...
    Option (Recompile)
End
```

The optimizer can cache a tight plan per branch. The Recompile hint accepts the per-call compile cost in exchange for accurate selectivity estimates on the local-variable parameters; for a tight-loop caller this is a favorable trade.

---

## 7. Risk & Rollback

**What could go wrong.** The local-variable plan-stabilization is generally safe but can produce a slightly worse plan than v9's sniffed-and-lucky case at a specific MFC. Per-MFC monitoring is therefore non-optional on this refactor.

The Recompile hint accepts a per-call compile cost. On a tight-loop caller the compile cost can be observable. If the post-deployment monitoring shows the compile time becoming the dominant cost, the alternative is to drop Recompile and rely on the local-variable form's stable plan; the plan-stability ratio should still improve from the v9 baseline.

The pre-aggregation in the group-together path's inner subquery changes the cardinality estimate the optimizer sees for the inner join. Where v9 was joining one row per qualifying Rx, v10 joins one row per qualifying group. If the v9 plan was incidentally benefiting from the over-counted cardinality (for example, a hash join that was sized larger than necessary but ran fast), the v10 plan may be slightly different shape. The semantic output is identical because the outer query is over `OeOrder` filtered by `OrderStatus = 'Scheduled'`; the inner join is establishing group membership only.

**What to look for in the first 24 hours.** Plan variant count per site, max-vs-avg ratio per site, average duration per site. The headline expected delta is the variant count dropping from many to one or two, and the max-vs-avg ratio dropping from 6,024x to something like 5x or 10x.

**Rollback path.** The v9 body is in `Original.sql` in this folder. Redeploying v9 reverts cleanly. No schema change, no index change.

---

## 8. Evidence of Refactor (v10)

Captured against Tolleson. Warm cache. Same data state as Section 3.2. Two captures at different `@AddrBank` values to confirm plan stability.

```
(paste STATS IO / STATS TIME for v10 against the same data state used for v9 here)
```

The expected post-refactor signature is the same plan across the two captures, the OeOrder reads in the candidate scan dominated by the `ByOrderStatusFillTypeAddrBank` index seek, the sibling-OrderId derivation cost expressed as a single CTE materialization per call, and no `If Exists` re-read in the group-together path.

---

## 9. Comparison & Improvement

| Metric | v9 (paste) | v10 (paste) | Delta |
|---|---|---|---|
| Logical reads (total, median run) | | | |
| Logical reads on `OeOrder` | | | |
| Logical reads on `TcdCountLauncherAlreadyConsideredRxs` | | | |
| CPU time (ms) | | | |
| Elapsed time (ms) | | | |
| Plan operator count | | | |
| Plan variant count across two captures | (expected: 2+) | (expected: 1) | |
| Max-vs-avg ratio (cross-MFC, 30d) | 6,024x | (expected: <50x) | |
| Result row count | | | |

**Verdict (to be entered once the captures land):** the headline expected delta is the plan stability improvement, which the cross-list capture identifies as the primary issue on this procedure. Total reads should drop materially in the cases where v9 was running the wrong plan; total reads may rise slightly in the cases where v9 happened to be running a good plan. The cross-MFC average is what matters here, not the best case.

---

## 10. Validation Checklist

Marked once the captures and the plans are in hand.

- Same data state. Captures taken back to back, ideally with a data freeze. (?)
- Warm cache only. Two runs, cold-cache numbers discarded. (?)
- Non-zero result set. Both runs returned at least one row. (?)
- Identical result set. Same row count and same row identities for deterministic queries. (?)
- Plan shape matches prediction. Stable plan across the two captures; ByOrderStatusFillTypeAddrBank seek on the candidate scan. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)

Net call: the refactor is sound on first principles and the dominant cost driver (plan volatility) is what v10 directly addresses. Per-MFC post-deployment monitoring is required on this procedure given the 6,024x worst-case ratio in the baseline.

---

## 11. Open Items / Future Improvements

### 11.1 Confirm Recompile is the right hint at scale

`Option (Recompile)` accepts a per-call compile cost. On a tight-loop caller this is observable in CPU time. If post-deployment monitoring shows the compile cost becoming dominant, the alternative is to drop Recompile and rely on the local-variable form's stable plan. The local-variable form alone produces a plan compiled against the local-variable type, which is the "fair" plan for an arbitrary parameter value; the trade-off is that the local-variable plan can be worse than a sniffed plan for a specific high-selectivity parameter value. Recompile gives the optimizer the per-call selectivity at the cost of the compile.

### 11.2 Filtered index on `OeOrder` for the sibling-OrderId source

The CountedSiblings CTE filters `OeOrder` on `OrderStatus In ('Counting', 'Counted', 'Suspended') And OrderId Like '%<[1-2]>'`. A filtered index covering these conditions would let the CTE seek directly into a much smaller subset of rows. The recommended DDL:

```sql
Create NonClustered Index IX_OeOrder_DualDispenserPortions_InFlight
   On OeOrder(OrderId)
   Include (OrderStatus, AddrBank)
   Where OrderStatus In ('Counting', 'Counted', 'Suspended')
     And OrderId Like '%<[1-2]>';
```

The filter predicate has to match the query predicate exactly for the index to be chosen. The two clauses are reproduced verbatim above. This is a Tolleson-first recommendation that should be evaluated before deployment to the rest of the fleet.

### 11.3 Build the CountedSiblings set once across both paths

The CountedSiblings CTE is currently expressed once per branch (three times in the procedure, when including the group-together path). For a tight-loop caller, building the set once into a temp table at the top of the procedure and referencing it from each branch is a straightforward optimization. v10 holds back from this restructuring to keep the diff small and the validation surface narrow; v11 should make the change once v10 has been observed in production.

### 11.4 Write a "Constructed-Key Self-Joins" masterclass entry

The pattern of deriving a key from columns of one row and joining back to the same table on the derived key recurs across the procedure library. The note should cover the cost mechanism (the derivation is non-SARGable against any natural index, so the inner query scans), the recommended fix when the derivation is stable (a persisted computed column, indexed), and the alternative when the derivation is volatile or rare (a CTE that constructs the key once per call, as v10 does here). The next refactor that surfaces the pattern should produce the note.

### 11.5 Audit the broader Count Launcher loop for the same shape

The procedure is one statement in a larger loop driven by the TCD Controller Count Launcher. Several adjacent procedures in the Count Launcher path (in particular the `TcdCountLauncherAlreadyConsideredRxs` writers and readers) may share the parameter-sniffing pattern. An audit of the surrounding loop is worth scoping after this refactor lands, separately, to confirm the plan stability benefits do not regress when the broader workload is exercised.
