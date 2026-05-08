# lsp_RbtDetermineNextVialToProcess: Refactor Analysis (v10 to v11)

**Date:** 2026-05-07
**Tracking sheet row:** 21 (Priority P3, status Not Started)

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_RbtDetermineNextVialToProcess`

**Purpose in one line:** Decides which type of vial the robot should fill next (dispenser reset, count-dry non-Rx, or standard Rx) for a given bank, then returns a header recordset describing the decision plus a detail recordset of the work items to act on.

**Tables touched:**

- `OeOrder` (the workhorse Rx table; read once at the top to count counted Auto-Fill Rxs in the bank).
- `TcdStatus` (per-dispenser status; read in the non-Rx count, the dispenser-reset count, the priority-1 detail return, and the priority-2 detail return).
- `RvoNonRxProductVial` (non-Rx product vial state; joined to `TcdStatus` for the count-dry path in counts and detail).
- `RbtQueuedForResetTcds` (queue of dispensers awaiting reset; joined to `TcdStatus` in the dispenser-reset count and detail).
- `RbtVialSupply` (vial supply inventory in the bank; read twice in v10 for the largest-vial-availability check).
- `TcdSecondaryData` (joined to `TcdStatus` in the priority-1 detail return for `CostPerPill`).
- `vInvMasterWithUserOverrides` (inventory master view with user overrides; joined in priority-1 and priority-2 detail returns for `FirstNdc`, `QtyPer100Cc`, and similar columns).
- `#temp_table2` (heap temp table populated by `lsp_OrdGetReadyForDispenseRxsInBank` in the priority-3 path).

**Called procedure:** `lsp_OrdGetReadyForDispenseRxsInBank @AddrBank, 0`. The output of this proc is materialized into `#temp_table2` and then streamed to the caller in the priority-3 path. Per the cross-MFC Query Store, the `INSERT INTO #temp_table2 EXEC lsp_OrdGetReadyForDispenseRxsInBank` line is the only statement from this proc that appears in the top 50 anywhere in the fleet (see Section 2).

**Indexes used (predicted from the v10 body, to be confirmed against the v11 plan once captured):**

| Table | Index | Where used |
|---|---|---|
| `OeOrder` | `ByOrderStatusFillTypeAddrBank` (NC, key `OrderStatus, FillType, AddrBank`) | counted-Rx count predicate |
| `OeOrder` | `ByOrderStatus` (NC, key `OrderStatus`) | counted-Rx count fallback |
| `TcdStatus` | unknown (TcdStatus is not in IndexExtract; flagged in Section 11) | non-Rx count, reset count, both detail returns |
| `RvoNonRxProductVial` | unknown | non-Rx count and priority-2 detail return |
| `RbtQueuedForResetTcds` | unknown (not in IndexExtract; flagged in Section 11) | reset count and priority-1 detail return |
| `RbtVialSupply` | unknown | large-vial-availability check |
| `TcdSecondaryData` | clustered on `TcdSn` (assumed) | priority-1 detail return |
| `vInvMasterWithUserOverrides` | base-table indexes on `ProductId` (view) | priority-1 and priority-2 detail returns |

**Index DDL gap.** The shared `IndexExtract.xlsx` from the Row 15 pilot covers the `OeOrder` family. `TcdStatus`, `RbtQueuedForResetTcds`, `RbtVialSupply`, `RvoNonRxProductVial`, and `TcdSecondaryData` are not in that extract. Run `extract_index_ddl.sql` against Tolleson with the table list set to those five before deploying v11. Section 11.1 stages the extraction request as the first open item.

**Callers:** Robot dispense controller. The proc is the decision step in the per-vial loop, called once per "what should I fill next" question. Across the 14 reporting MFCs, this lands at roughly 43.4M executions per month (see Section 3.1).

---

## 2. Overview of Performance

The tracking sheet flagged this proc on March single-site numbers: 189K executions, 1.4B total reads, 7.4K avg reads/exec. The current cross-MFC view from the 2026-05-07 Query Store capture shows the systemic picture: **roughly 705 billion logical reads per month across 43.4 million executions, present in 14 of 14 reporting sites**. The proc is the highest-call-volume member of the deep-dive cohort so far. Per-site avg reads/exec ranges from 6,495 (Denver) to 28,220 (Bolingbrook), a 4.3x spread that is narrower than the spreads observed on Row 15 and Row 16, suggesting the dominant cost driver is similar in shape across sites. The detailed per-site breakdown is in Section 3.1.

A surprising finding from the cross-MFC capture: **only one statement inside the proc appears in the top 50 at any site, and it is the same statement at every site**: `INSERT INTO #temp_table2 EXEC lsp_OrdGetReadyForDispenseRxsInBank @AddrBank, 0`. The four `Count(*)` queries called out in the tracking-sheet narrative do not appear in the top 50 at any site. They are real, they are repeated, and they are worth fixing for code-organization and plan-cache reasons, but they are not the dominant I/O contributor. The dominant cost is in the called sub-proc `lsp_OrdGetReadyForDispenseRxsInBank`. That proc is out of scope for this refactor, but it is where the next refactor pass should focus: see Section 11.2.

The dominant cost driver at the proc-level statements that v11 actually changes is structural rather than scan-heavy: three separate `Count(*)` variable assignments are followed by an `If Exists` check whose predicate contains a correlated `Top 1 / Order By` subquery against the same table, then a heap temp table populated by the called proc and read back with a sort. v11 collapses the three counts into one statement via [[Conditional Aggregation Consolidation]] (Flavor B, since the three sources hit different tables), replaces the two-pass vial-supply check with a single-pass conditional-aggregation form, swaps the heap temp table for one with a clustered key on the `Order By` columns, and replaces `Select * From #temp_table2` with an explicit column list. These are honest code-quality and small per-call wins, not a 50% read reduction. Setting expectations correctly: the read-cost win on the proc-level body is small in absolute terms because the four counts being collapsed are each cheap. The big lever for this proc's overall cost lives inside `lsp_OrdGetReadyForDispenseRxsInBank` and is reachable as a follow-on refactor.

---

## 3. Evidence of Original (v10)

### 3.1 Query Store, cross-MFC view

**Source:** `diagnostics/querystore_outputs/QueryStore_20260507.xlsx`. Top 50 offenders per site, 30-day lookback, captured 2026-05-07. One tab per site.

**Aggregation method:** for each site, all rows whose `objectname` matches `lsp_RbtDetermineNextVialToProcess` are aggregated. Each row in the source file is the `INSERT INTO #temp_table2 EXEC lsp_OrdGetReadyForDispenseRxsInBank` statement. Multiple distinct `query_id`s would indicate that more than one statement inside the proc is appearing in the top 50; in this capture, every site has exactly one row.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------|----------------|
| Bolingbrook | 1 | 1 | 5,098,765 | 143,887,649,000 | 28,220 | 143.8 | 525,641,100 |
| Orlando | 1 | 1 | 6,109,653 | 118,224,418,931 | 19,350 | 98.7 | 418,637,609 |
| NorthLake | 1 | 1 | 3,437,915 | 85,193,343,427 | 24,780 | 117.0 | 279,317,667 |
| Mechanicsville | 1 | 1 | 4,375,766 | 63,033,219,154 | 14,405 | 104.9 | 211,723,128 |
| Indy | 1 | 1 | 4,227,212 | 60,180,320,785 | 14,236 | 61.9 | 206,629,067 |
| Memphis | 1 | 1 | 2,876,323 | 48,064,601,899 | 16,710 | 48.3 | 118,540,861 |
| Tolleson | 1 | 1 | 4,884,472 | 42,756,104,849 | 8,753 | 72.9 | 242,119,420 |
| Mansfield | 1 | 1 | 2,664,455 | 37,045,935,384 | 13,903 | 77.4 | 139,616,882 |
| Canal | 1 | 1 | 3,353,835 | 33,778,649,226 | 10,071 | 37.1 | 110,384,744 |
| Liberty | 1 | 1 | 1,750,198 | 32,826,277,507 | 18,755 | 54.7 | 75,544,885 |
| BrooklynPark | 1 | 1 | 1,060,271 | 15,839,871,357 | 14,939 | 58.5 | 56,745,430 |
| Denver | 1 | 1 | 1,701,783 | 11,054,245,653 | 6,495 | 22.1 | 32,860,162 |
| Kent | 1 | 1 | 1,405,482 | 10,276,448,870 | 7,311 | 25.2 | 33,438,569 |
| West Jordan | 1 | 1 | 428,605 | 2,784,952,982 | 6,497 | 22.0 | 8,889,680 |

**Roll-up across all 14 reporting sites:** 43,374,735 executions, **704,946,039,024 total logical reads** (16,252 avg reads/exec), 2,460,089,204 ms total CPU. Every site reports.

**Observations:**

- **No plan instability.** Every site shows one plan variant for one query_id. No forced plans, no plan failures. This is unusual for a proc this hot; it suggests that the parameter shape (`@AddrBank` plus three small integer/tinyint controls) does not vary widely enough to trigger plan-choice volatility, and the called proc's plan is stable across the parameter space it actually sees in production.
- **Only one statement is visible at the top-50 level.** The `INSERT ... EXEC lsp_OrdGetReadyForDispenseRxsInBank` line is the only thing that surfaces. The four `Count(*)` queries that the tracking sheet flagged are below the top-50 threshold at every site. They are real cost contributors when summed across 43M executions, but each individual statement is cheap. The takeaway is that the proc-body wins from the v11 refactor are real but small per-call, while the actual dominant cost driver is one level deeper, inside the called sub-proc.
- **Per-site avg reads scales with call volume more than with data shape.** Bolingbrook at 5.1M execs averages 28K reads, while Denver at 1.7M execs averages 6.5K. The 4.3x avg-reads spread is narrower than the spreads observed on Row 15 (37x) and Row 16 (14x). This is consistent with one stable plan returning a stable amount of work per call.
- **The CPU bill is large in absolute terms.** 2.46 billion CPU ms per month across the fleet, roughly 28.5 days of CPU. Bolingbrook alone burns 526M CPU ms (146 hours) per month on this single proc.
- **Top 5 sites by total reads contribute 67% of the load.** Bolingbrook, Orlando, NorthLake, Mechanicsville, and Indy together account for 470B of the 705B reads. v11 will deliver its impact at every site, but the absolute savings concentrate at the top 5.

### 3.2 SET STATISTICS IO, TIME on a representative MFC

I capture STATS IO/TIME from Tolleson (or another representative MFC) running both v10 and v11 against the same data state. Recommended parameter combinations (one of each for breadth of priority-path coverage):

- `Exec lsp_RbtDetermineNextVialToProcess @AddrBank = 'B', @InProcessTcdResets = 0, @InProcessCountDryRxsBeingFilled = 0` (typical priority-3 path: counted Rxs to process, drives the `INSERT ... EXEC` cost)
- Same parameters during a window when `RbtQueuedForResetTcds` has rows (priority-1 path)
- Same parameters during a window when count-dry non-Rx vials are queued (priority-2 path)

```
(STATS IO + STATS TIME output for v10 goes here, paired with .sqlplan)
```

Per-table aggregated logical reads, scan counts, and per-statement timing should be captured in a single text block paired with the `.sqlplan` for both versions.

---

## 4. Issue Identification (v10)

The line numbers below reference `Original.sql` (v10).

### 4.1 Three separate `Count(*)` variable-assignment statements (lines 57 to 103)

```sql
Select @TotalCountedRxs = Count(*) From OeOrder ... Where AddrBank = @AddrBank ...
Select @TotalCountedNONRxs = Count(*) From TcdStatus T Inner Join RvoNonRxProductVial RNPV ... Where AddrBank = @AddrBank ...
Select @TotalDispenserResetsRequired = Count(*) From RbtQueuedForResetTcds RT Inner Join TcdStatus T ... Where RT.AddrBank = @AddrBank ...
```

The three counts run back to back at the top of the proc. Each is a standalone statement that compiles, executes, and returns a scalar. Because the three sources are different tables (`OeOrder`, `TcdStatus + RvoNonRxProductVial`, `RbtQueuedForResetTcds + TcdStatus`), the leaf-level reads cannot be reduced by consolidation alone. What can be reduced is the per-statement overhead: three compiles, three plan-cache slots, three round trips become one of each. See Section 5.1 for the principle.

### 4.2 Two-pass `If Exists` check with correlated `Top 1 / Order By` subquery (line 115)

```sql
If Exists (
    Select SupplyNum From RbtVialSupply With (NOLOCK)
    Where StatusId In (0, 1) And AddrBank = @AddrBank
      And SupplyNum = (
            Select Top 1 SupplyNum From RbtVialSupply With (NOLOCK)
            Where AddrBank = @AddrBank
            Order By StoredVialSizeDrams Desc))
```

The intent is "is the largest-size vial currently available in this bank?" The implementation makes two passes against `RbtVialSupply`:

- The inner `Top 1 ... Order By StoredVialSizeDrams Desc` pulls the `SupplyNum` of the largest vial in the bank (one pass, with a sort if no covering index exists on `StoredVialSizeDrams`).
- The outer `If Exists` then checks whether that `SupplyNum` is in available status `(0, 1)` (a second pass).

The redundancy is the pattern the tracking sheet calls out as "redundant subquery in EXISTS check." The same answer is computable in one pass via conditional aggregation: compare `Max(StoredVialSizeDrams)` against `Max(StoredVialSizeDrams)` filtered to available status, and check whether the two are equal.

### 4.3 Heap temp table with `Order By` on uncovered columns (lines 187 to 206)

```sql
Drop Table If Exists #temp_table2
Create Table #temp_table2 (
   OrderId VarChar(30), ProductId VarChar(20), ...
   ActOnAsPriInternal VarChar(40), PriInternal VarChar(40)
)
Insert Into #temp_table2 Exec lsp_OrdGetReadyForDispenseRxsInBank @AddrBank, 0

Select * From #temp_table2 With (NOLOCK) Order By ActOnAsPriInternal, PriInternal
```

Two issues compound here:

- The temp table is a heap. Reading it with `Order By ActOnAsPriInternal, PriInternal` requires a Sort operator at read time. For small inputs the sort is cheap; for inputs in the hundreds or low thousands the sort starts to register in CPU and tempdb memory grant.
- Without a clustered index, the optimizer has only a single auto-stat per queried column at most, and the Sort spills risk grows with input size. There is no cardinality leverage available from a clustered key.

The fix is to declare the temp table with a clustered primary key on `(ActOnAsPriInternal, PriInternal)`. That maintains insertion order to match the read-time `Order By` (eliminating the sort), gives the optimizer auto-statistics on the keyed columns, and makes the read a no-cost ordered scan of the clustered structure. See [[Table Variables vs Temp Tables]] for the underlying principle.

### 4.4 `Select * From #temp_table2` (line 206)

```sql
Select * From #temp_table2 With (NOLOCK) Order By ActOnAsPriInternal, PriInternal
```

The `Select *` expands at bind time to the column list of `#temp_table2`. Because `#temp_table2` is declared inside the proc, the binding is stable across executions, so the runtime cost is unchanged. The issues are upstream of runtime:

- If `lsp_OrdGetReadyForDispenseRxsInBank` ever returns a different column set, this proc's caller contract drifts silently. The explicit column list in v11 documents and pins the contract.
- `Select *` against a temp table populated by `Insert ... Exec` is only safe when the temp-table column shape matches the called proc's `Select` column shape exactly. Today it does. Pinning the column list in both the `Insert` and the `Select` makes the binding explicit on both ends.

### 4.5 Inconsistent capitalization on `WITH (NOLOCK)` and inline whitespace (multiple lines)

A purely cosmetic issue. The proc mixes `With (NOLOCK)`, `WITH (NOLOCK)`, and `With (NoLock)`. Fixing this is below-the-fold cleanup; v11 standardizes on `With (NOLOCK)` to match the dominant style in the file.

---

## 5. First Principles

### 5.1 Conditional Aggregation Consolidation (Flavor B)

When a proc runs several `Count(*)` (or similar) statements back to back and each lands in its own scalar variable, the per-statement overhead (compile, plan-cache slot, dispatch, result return) is paid N times even when the underlying work is logically one composite question. The fix is a single statement with a `Sum(Case When ...)` per output bucket. When all the counts hit the same table, this is also a leaf-I/O win because the table is read once instead of N times (Flavor A). When the counts hit different tables that share a parameter, the leaf-I/O is unchanged but the per-statement overhead and plan-cache pressure drop (Flavor B).

This proc's three counts hit three different table sources, so v11's consolidation is Flavor B. The honest framing: the savings are in dispatch, compile, and plan-cache slots, not in scan counts. See [[Conditional Aggregation Consolidation]] for the full pattern, the criteria for choosing between Flavor A and Flavor B, and the cases where consolidation is not appropriate.

### 5.2 Single-Pass Existence-Plus-Filter (eliminating the redundant inner subquery)

When an `If Exists` predicate carries a correlated `Top 1 / Order By` subquery against the same table, the engine reads the table twice: once to compute the inner ordering, once to evaluate the outer existence check. Most of these patterns can be rewritten as a single-pass aggregation that answers both questions at once.

For "is the largest-X record in available status?", the pattern is:

```sql
Select @Available =
    Case
        When Max(X) Is Null Then 0
        When Max(Case When Status In (Available) Then X End) = Max(X) Then 1
        Else 0
    End
From Table Where (filter)
```

The largest available value matches the global maximum if and only if at least one available row holds the global maximum. One scan, two `Max`es, one comparison. This is the same idea as Flavor A above, applied to a boolean output instead of a count output. It is the pattern that turns the v10 two-pass `If Exists` into a one-pass `Select`.

### 5.3 Indexed Temp Tables Eliminate Heap Sorts

A `#temp` table declared without an index is a heap. Reading it with `Order By` requires the engine to sort at read time. Declaring the temp table with a clustered key on the `Order By` columns moves the sort to insertion time (where it is cheaper because the engine sorts as it inserts) and turns the read-time `Order By` into a no-cost ordered scan of the clustered structure.

The clustered-key choice for v11 is `(ActOnAsPriInternal, PriInternal)`, which exactly matches the final `Order By`. The table has no other consumer in this proc, so there is no competing key to consider. A `Primary Key Clustered` constraint is used rather than a plain `Create Clustered Index` so that uniqueness is enforced; if `lsp_OrdGetReadyForDispenseRxsInBank` ever returns duplicates on that composite, the constraint will fail at insert and surface the duplicate immediately. This is a deliberate fail-closed choice. If validation surfaces duplicates as a legitimate output, downgrade to a non-unique clustered index. See [[Table Variables vs Temp Tables]] for the broader temp-table-vs-heap discussion.

### 5.4 Explicit Column Lists at Procedure Boundaries

`Select *` against a temp table populated by `Insert ... Exec` is brittle in a way that is easy to miss. The contract between the called proc and the temp-table declaration is implicit: the columns returned by `lsp_OrdGetReadyForDispenseRxsInBank` must match the temp-table column shape exactly, in the same order, every time. The `Insert ... Exec` enforces the order at insert time, but a future change to the called proc's `Select` list will silently propagate to every caller that uses `Select *`. The explicit column list in v11 makes the contract visible at both ends (the `Insert ... (col, col, col) Exec ...` and the `Select col, col, col From #temp_table2`), which is the canonical way to pin a contract that crosses procedure boundaries.

### 5.5 `Drop Table If Exists` (Modern Form)

The proc already uses `Drop Table If Exists` (line 187 in v10), so this is not a new pattern. It is mentioned here only to confirm that v11 does not regress to the legacy `If Object_Id('TempDb..#X') Is Not Null Drop Table #X` form.

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v11. The narrative below walks the changes block by block. Line references are to `Refactored.sql`.

### 6.1 Consolidated counts (lines 88 to 134)

The three `Select @Var = Count(*)` statements collapse into one statement. Each branch of the `Union All` is a projection query that emits one row per qualifying record, tagged with a `Bucket` literal identifying which counter the row contributes to. The outer `Select` aggregates the buckets via `Sum(Case When Bucket = ... Then 1 Else 0 End)` into the three local variables.

```sql
Select
    @TotalCountedRxs              = Sum(Case When Bucket = 'CountedRxs'      Then 1 Else 0 End),
    @TotalCountedNONRxs           = Sum(Case When Bucket = 'CountedNONRxs'   Then 1 Else 0 End),
    @TotalDispenserResetsRequired = Sum(Case When Bucket = 'DispenserResets' Then 1 Else 0 End)
From
(
    Select 'CountedRxs' As Bucket From OeOrder With (NOLOCK) Where ...
    Union All
    Select 'CountedNONRxs' As Bucket From TcdStatus T Inner Join RvoNonRxProductVial RNPV ... Where ...
    Union All
    Select 'DispenserResets' As Bucket From RbtQueuedForResetTcds RT Inner Join TcdStatus T ... Where ...
) As CountSources
```

The per-branch `Where` clauses are preserved verbatim from v10. The leaf-level reads on the three sources (`OeOrder`, `TcdStatus + RvoNonRxProductVial`, `RbtQueuedForResetTcds + TcdStatus`) are unchanged. The savings are one statement compile instead of three, one plan-cache slot instead of three, and one dispatch round trip instead of three. See [[Conditional Aggregation Consolidation]] Flavor B.

The three `Set @Var = IsNull(@Var, 0)` lines that follow exist because `Sum` returns `Null` when the union projects zero rows. The downstream `If` block compares the variables to zero, so coalescing protects the comparisons.

### 6.2 Single-pass large-vial-availability check (lines 144 to 152)

```sql
Select @LargeVialSizeAvailable =
    Case
        When Max(StoredVialSizeDrams) Is Null Then 0
        When Max(Case When StatusId In (0, 1) Then StoredVialSizeDrams End) = Max(StoredVialSizeDrams) Then 1
        Else 0
    End
From RbtVialSupply With (NOLOCK)
Where AddrBank = @AddrBank
```

One scan against `RbtVialSupply` filtered by `AddrBank`. The engine computes `Max(StoredVialSizeDrams)` (the global largest size in the bank) and `Max(Case When StatusId In (0, 1) Then StoredVialSizeDrams End)` (the largest size that is also available) in the same pass. The comparison answers "is the largest size currently available?" without a second read. The `Max ... Is Null` guard handles the empty-bank case (no supply rows for this bank means no large vial is available, which is the same answer v10 produced via `If Exists` returning false on empty input).

### 6.3 Indexed temp table for the priority-3 path (lines 192 to 222)

```sql
Create Table #temp_table2 (
    ...
    ActOnAsPriInternal VarChar(40) Not Null,
    PriInternal        VarChar(40) Not Null,
    Constraint PK_TempTable2_ActOnAsPriInternal_PriInternal
        Primary Key Clustered (ActOnAsPriInternal, PriInternal)
)

Insert Into #temp_table2 (col, col, ...) Exec lsp_OrdGetReadyForDispenseRxsInBank @AddrBank, 0

Select col, col, ... From #temp_table2 With (NOLOCK) Order By ActOnAsPriInternal, PriInternal
```

Three changes folded together:

- The temp table now has a clustered primary key on `(ActOnAsPriInternal, PriInternal)`, which exactly matches the final `Order By`. The sort moves from read time (where it was a Sort operator on a heap) to insert time (where it is part of the clustered-index maintenance). The read becomes a no-cost ordered scan.
- The `Insert ... Exec` now carries an explicit column list, pinning the contract between the called proc and the temp-table shape on both sides.
- The final `Select` now lists every column explicitly, replacing v10's `Select *`. The column order matches the temp-table declaration.

The `Order By ActOnAsPriInternal, PriInternal` is preserved on the final `Select` even though the clustered key already streams the rows in that order. Keeping the `Order By` documents the intended row order for the caller and is a no-cost belt and braces in case a future schema change disturbs the clustered-key shape.

### 6.4 Cosmetic cleanup throughout

- `WITH (NOLOCK)` is normalized to a single style across the proc.
- Whitespace around `Inner Join`, `Where`, and `On` is cleaned up to match the conventions established by the Row 15 and Row 16 refactors.
- The trailing safety `Set @VialTypeToProcess = -99` block is preserved exactly.

---

## 7. Risk & Rollback

### Risks

- **Sum vs Count semantic preservation.** v10's `Count(*)` returns 0 for empty input. v11's `Sum(Case ...)` returns `Null` for empty input. The three `IsNull(@Var, 0)` lines after the consolidated `Select` cover this. The risk is forgetting the coalesce on a future edit; mitigated by the comment block.
- **Primary-key constraint on the temp table.** v11 declares `(ActOnAsPriInternal, PriInternal)` as a primary key, which enforces uniqueness. v10 had no such constraint. If `lsp_OrdGetReadyForDispenseRxsInBank` ever returns duplicate `(ActOnAsPriInternal, PriInternal)` pairs, the v11 `Insert ... Exec` will fail with a primary-key violation. The validation run in Section 8 must confirm that the called proc never returns duplicates on this composite for the parameter shapes in production use. If it does, downgrade the constraint to a non-unique clustered index (`Create Clustered Index IX_TempTable2_ActOnAsPri On #temp_table2 (ActOnAsPriInternal, PriInternal)`) after the table create.
- **Plan-cache invalidation on first deploy.** The consolidated count statement and the single-pass vial-supply check are new query shapes. Both will compile fresh on first execution at each MFC, and the v10 plans will age out of cache. Expect a brief compile cost on the first call and steady state thereafter.
- **The `Union All` plan choice.** The optimizer plans the `Union All` once with the full cardinality of the three sources in view. If the three branches have wildly different row counts (one returns 50K, another returns 10), the optimizer may choose join orders inside the branches that are tuned to the average. This is unlikely to be a regression because the original three plans were already optimizing each branch independently with no cross-branch information; the consolidated form has more information, not less. If a regression appears in a single branch, the fallback is to revert that one branch to a standalone `Select @Var = Count(*) ...` and keep the other two consolidated.

### Rollback

The proc is read-only with side-effect-free outputs (two recordsets returned to the robot caller). Rollback is a pure DDL revert: restore the v10 body via `Alter Procedure`. No data state is mutated, so there is no compensating action required.

If a regression appears, run the v10 form against the same data state to confirm. Rollback path:

```sql
-- pull v10 from git
git show a43f691:stored_procedures/lsp_RbtDetermineNextVialToProcess.sql

-- adjust ALTER -> CREATE if needed, deploy
```

### Monitoring Window

For the first 24 hours after deployment, watch:

- Robot dispense controller log for any errors on the `Insert ... Exec` (specifically primary-key violations on `#temp_table2`, which would surface a duplicate `(ActOnAsPriInternal, PriInternal)` pair from the called proc).
- Query Store for `lsp_RbtDetermineNextVialToProcess` average reads/exec, average duration, and plan count. The headline metric is the `INSERT INTO #temp_table2 EXEC ...` line; v11 does not change this line, so the cross-MFC reads-per-exec on that line should be flat. Any large drift from the 8K-28K range is signal that something else changed.
- Plan cache. Should now show one plan for the consolidated count statement, one for the single-pass vial-supply check, one for the priority-3 final `Select`, and the existing plans for the priority-1 and priority-2 detail returns. Total plan count for the proc should be small and stable.

---

## 8. Evidence of Refactor (v11)

I capture STATS IO/TIME from the same MFC and same data state used for the v10 capture, running v11 with parameters that exercise at least the priority-3 path (the highest-frequency path) and ideally one of priority-1 or priority-2 for breadth.

```
(STATS IO + STATS TIME output for v11 goes here, paired with .sqlplan)
```

The post-refactor plan summary should show:

- One plan for the consolidated count statement, with three `Index Seek` (or `Index Scan`) operators feeding a `Concatenation` (Union All), then a `Stream Aggregate` for the conditional sums.
- One plan for the single-pass `RbtVialSupply` check, with a single `Index Seek` or `Clustered Index Scan` filtered to `AddrBank = @AddrBank`.
- The `INSERT INTO #temp_table2 EXEC ...` operator unchanged from v10 (because the called proc and its plan are unchanged).
- The final `Select From #temp_table2` plan should now show a `Clustered Index Scan` of `PK_TempTable2_ActOnAsPriInternal_PriInternal` with no `Sort` operator. v10 showed a `Table Scan` of the heap followed by a `Sort`.

---

## 9. Comparison & Improvement

*Filled in once both v10 and v11 STATS IO/TIME outputs are pasted above.*

| Metric | v10 (heap, three counts) | v11 (indexed, consolidated) | Delta | % Change |
|--------|--------------------------|-----------------------------|-------|----------|
| Logical reads, OeOrder | | | | |
| Logical reads, TcdStatus | | | | |
| Logical reads, RvoNonRxProductVial | | | | |
| Logical reads, RbtQueuedForResetTcds | | | | |
| Logical reads, RbtVialSupply | | | | |
| Logical reads, #temp_table2 | | | | |
| Total logical reads | | | | |
| CPU time (ms) | | | | |
| Elapsed time (ms) | | | | |
| Statements compiled (first call) | | | | |
| Plans cached for this proc | | | | |
| Result row count (must match) | | | | |

**Plan-shape verification:**

- v10 `INSERT INTO #temp_table2 EXEC ...` operator should match v11 (both inherit from the unchanged called proc).
- v10 final `Select From #temp_table2` should show a heap `Table Scan` followed by a `Sort` operator. v11 should show a clustered index scan of `PK_TempTable2_ActOnAsPriInternal_PriInternal` with no `Sort`.
- v10 large-vial-availability check should show two seeks against `RbtVialSupply`. v11 should show one.
- v10 should show three independent count plans. v11 should show one consolidated count plan with a `Concatenation` operator at the top.

---

## 10. Validation Checklist

Per `tasks/lessons.md`. Mark each pass or fail before declaring the refactor done.

- [ ] **Same data state.** Both v10 and v11 captures taken against the same data, ideally back to back without intervening writes to `OeOrder`, `TcdStatus`, `RvoNonRxProductVial`, `RbtQueuedForResetTcds`, or `RbtVialSupply`.
- [ ] **Warm cache only.** Both captures are the second of two consecutive runs.
- [ ] **Non-zero result set.** The priority-3 path should be the test target. `#temp_table2` should hold at least one row from `lsp_OrdGetReadyForDispenseRxsInBank` so the final `Select` exercises the indexed-vs-heap difference. If the test data state has zero counted Rxs, the comparison cannot speak to the temp-table indexing change.
- [ ] **Identical result set.** v10 and v11 returned the same rows in the same order from the priority-3 final `Select`. Same row count, same row identities. The `Order By` is preserved on both, so the order is deterministic.
- [ ] **Plan shape matches prediction.** v11's consolidated count plan, single-pass vial-supply check, and clustered-index scan of `#temp_table2` are visible in the `.sqlplan`. No `Sort` operator on the final `Select`.
- [ ] **No new error or warning messages.** No primary-key violations on the `Insert ... Exec`, no cardinality errors, no conversion warnings, no excessive memory grant warnings.
- [ ] **Warm-cache elapsed time at or below v10.** v11 is at least as fast as v10. The expected delta is small in absolute terms because the dominant cost (the `Insert ... Exec`) is unchanged.

---

## 11. Open Items / Future Improvements

These are observations made during the deep dive that are not in v11. Section 11.1 stages the index DDL extraction that needs to happen first. Section 11.2 stages the high-leverage follow-on refactor inside the called sub-proc. Section 11.3 is longer-horizon.

### 11.1 Index DDL extraction (must happen before deployment)

Run `extract_index_ddl.sql` against Tolleson with the `@Tables` list set to:

- `TcdStatus`
- `RbtQueuedForResetTcds`
- `RbtVialSupply`
- `RvoNonRxProductVial`
- `TcdSecondaryData`

Save to `refactors/lsp_RbtDetermineNextVialToProcess/IndexExtract.xlsx`. Confirm:

- `RbtVialSupply` has an index supporting `AddrBank` lookups (a key on `AddrBank` or a composite leading with `AddrBank`). Without one, the v11 single-pass vial-supply check still scans the table once, which is the same cost as v10's first pass; v11 still wins on the second-pass elimination.
- `TcdStatus` has an index supporting joins from `RbtQueuedForResetTcds.TcdSn` and from `RvoNonRxProductVial.VialId` (which lands as `T.OrderId` in this proc).
- `RbtQueuedForResetTcds` has an index supporting `(AddrBank, RemainingTries)` lookups, ideally with `TcdSn` and `QueuedDtTm` included for the priority-1 detail return.

Index recommendations for missing coverage will land here once the extract is in.

### 11.2 The actual lever: refactor `lsp_OrdGetReadyForDispenseRxsInBank`

The cross-MFC Query Store evidence makes it unambiguous that the dominant cost for this proc is the called sub-proc, not the proc-level statements. Across 14 sites, the only statement that surfaces in the top 50 is `INSERT INTO #temp_table2 EXEC lsp_OrdGetReadyForDispenseRxsInBank @AddrBank, 0`. v11 does not change that statement.

**Recommendation:** add `lsp_OrdGetReadyForDispenseRxsInBank` to the deep-dive cohort as its own row in the tracking sheet. The Query Store capture for that proc is one tab away from the existing capture (just filter `objectname = 'lsp_OrdGetReadyForDispenseRxsInBank'` on the same per-site tabs in `QueryStore_20260507.xlsx`). The fleet-wide reads on this single statement, summed across 14 sites and all callers (this proc plus any others), are likely well into the hundreds of billions per month.

### 11.3 Proc-level changes deferred from v11

#### A. Materialize vs scalar variable for the largest-vial check

If the index audit in 11.1 reveals that `RbtVialSupply` has no covering index on `AddrBank`, an alternative form materializes the bank's supply rows once into a small temp table and probes both questions from there:

```sql
Drop Table If Exists #BankSupply
Select SupplyNum, StoredVialSizeDrams, StatusId
Into #BankSupply
From RbtVialSupply With (NOLOCK)
Where AddrBank = @AddrBank
Create Clustered Index IX_BankSupply On #BankSupply (StoredVialSizeDrams Desc)
```

The single-pass conditional aggregation in v11 already achieves the same goal in one statement, so this materialization is only worth doing if the bank's supply row count is large enough that probing it twice from the conditional aggregation dominates. For typical bank sizes (likely tens to low hundreds of supply rows), the v11 form is sufficient.

#### B. Tighten the `OeOrder` count predicate to use the composite index

The v11 counted-Rxs branch filters on `AddrBank = @AddrBank And FillType = 'A' And OrderStatus In (...)`. The `OeOrder.ByOrderStatusFillTypeAddrBank` index has key `(OrderStatus, FillType, AddrBank)`. The predicate is fully seekable on this index for the `OrderStatus = 'Counted'` and `OrderStatus = 'Partial Counted'` branches; it is also fully seekable for the `OrderStatus = 'Split-DD'` branch. The v10 plan's index choice is unconfirmed (no plan file pulled yet). If the optimizer is choosing `ByOrderStatus` (key `(OrderStatus)`) instead of `ByOrderStatusFillTypeAddrBank`, an index hint or `Force Seek` could nudge it to the better choice. Confirm in the v11 plan capture before recommending.

#### C. Result-set row counts for verification

`Set NoCount On` is set at the top of the proc. The validation run should add `Set NoCount Off` at the session level so the `(N rows affected)` lines are visible in the STATS output, per `tasks/lessons.md`.

### 11.4 Schema-Level Changes (longer horizon)

- **Filtered index on `OeOrder` for counted Auto-Fill rows.** The counted-Rxs branch of the consolidated count is tightly filtered (`FillType = 'A'`, `OrderStatus In ('Counted', 'Partial Counted', 'Split-DD'+'Counted')`, with the parent/portion exclusions). A filtered index covering this exact predicate would reduce the count to a handful of reads. Worth modeling once the index extract for `OeOrder` is reconfirmed. The cost is index maintenance on every `OeOrder` insert/update where the row qualifies, which is high-volume; the benefit only justifies the cost if the count is hot enough at the leaf level. Cross-MFC Query Store does not currently flag the count as a top-50 statement, which suggests this index is probably not justified today.
- **Persisted computed column on `OeOrder.OrderId` for the dual-dispenser portion pattern.** The predicate `OrderId Not Like '%<[1-2]>'` is non-SARGable and forces a scan-or-filter every time. A bit column `IsDualDispenserPortion` populated on insert/update would let the optimizer use a normal equality predicate. Same cost-benefit story as the filtered index above; defer until the leaf-level read cost on this predicate justifies the schema change.
