# lsp_SrtGetSortationItems: Refactor Analysis (v2 to v3)

**Date:** 2026-05-07
**Tracking sheet row:** 16 (Priority P2, status In Progress)

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_SrtGetSortationItems`

**Purpose in one line:** Look up rows in `SrtSortationItem` by `Barcode`, `SortationCode`, both, or neither (return all). Called on every barcode scan from the Sortation system.

**Tables touched:**

- `SrtSortationItem` (the only table; 4 columns per the tracking sheet, returns Barcode and SortationCode).

**Indexes used:** to be confirmed against the v3 plan once captured. The proc assumes seekable indexes on `Barcode` and `SortationCode`. If those indexes do not exist, the IF/ELSE rewrite still allows a scan but does not deliver a seek win. The Section 11 index recommendations cover this contingency.

**Index DDL gap.** `IndexExtract.xlsx` produced for the Row 15 pilot covered the `OeOrder` family, not `SrtSortationItem`. Run `extract_index_ddl.sql` against Tolleson with the `@Tables` list set to `('SrtSortationItem')` to fill this in.

**Callers:** Sortation system, called per barcode scan. High frequency.

---

## 2. Overview of Performance

The cross-MFC view from the 2026-05-07 Query Store capture shows this proc is the largest single I/O contributor in the fleet by a wide margin. **1.84 trillion logical reads per month across 17.3 million executions**, present in 13 of 14 sites. Bolingbrook alone burns 400 billion reads. Avg reads per execution sits at 106K fleet-wide, peaking at 165K at Bolingbrook and 142K at NorthLake. The detailed per-site breakdown is in Section 3.1.

Plan stability is a tell. Every site returns exactly one plan variant for one query_id, with no parameter sniffing volatility. That is the catch-all query anti-pattern's signature: the optimizer is forced into a single scan plan that is identical across all parameter combinations, and there is no plan to vary because the plan is simply wrong by design.

The dominant cost driver is the WHERE clause:

```sql
Where (@Barcode Is Null Or Barcode = @Barcode)
  And (@SortationCode Is Null Or SortationCode = @SortationCode)
```

This catch-all pattern prevents the optimizer from compiling a seekable plan. To stay correct in the worst case (both parameters NULL, return all rows), the optimizer chooses a scan and uses it for every call, including the common case where a single barcode is supplied and a single row is the desired result. Production data confirms it: 95K reads per execution against a 4-column table whose result is typically one row.

v3 splits the proc into four IF/ELSE branches, one per parameter combination. Each branch produces its own cached plan that uses index seeks when parameters are provided. The expected reduction is from roughly 95K reads per execution to under 10. Across 17.3 million monthly executions, that is approximately **1.84 trillion logical reads saved per month, fleet-wide**, on this single change. The estimated savings are larger than the entire monthly load of `lsp_ShpGetOrdersForTopReadyToShipGroup` (Row 15) by nearly 2x.

---

## 3. Evidence of Original (v2)

### 3.1 Query Store, cross-MFC view

**Source:** `diagnostics/querystore_outputs/QueryStore_20260507.xlsx`. Top 50 offenders per site, 30-day lookback, captured 2026-05-07.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------|----------------|
| Bolingbrook | 1 | 1 | 2,423,739 | 399,885,190,666 | 164,987 | 615.5 | 4,023,798,072 |
| NorthLake | 1 | 1 | 2,346,021 | 333,458,877,811 | 142,138 | 504.6 | 3,389,188,312 |
| Orlando | 1 | 1 | 2,151,957 | 263,438,145,840 | 122,418 | 491.0 | 2,894,504,590 |
| Tolleson | 1 | 1 | 1,469,840 | 152,529,462,041 | 103,773 | 307.9 | 1,477,139,173 |
| Indy | 1 | 1 | 1,244,010 | 138,961,828,306 | 111,705 | 349.6 | 1,350,837,211 |
| Liberty | 1 | 1 | 1,397,342 | 126,031,665,958 | 90,194 | 154.3 | 1,103,434,047 |
| Memphis | 1 | 1 | 1,343,814 | 109,093,330,524 | 81,182 | 137.3 | 1,000,205,113 |
| Canal | 1 | 1 | 1,369,263 | 101,959,163,341 | 74,463 | 178.0 | 874,682,079 |
| Mechanicsville | 1 | 1 | 1,338,982 | 89,758,133,344 | 67,035 | 160.4 | 767,314,086 |
| Mansfield | 1 | 1 | 1,192,207 | 82,920,674,743 | 69,552 | 252.5 | 809,532,349 |
| Denver | 1 | 1 | 486,801 | 21,120,773,088 | 43,387 | 126.1 | 214,950,852 |
| BrooklynPark | 1 | 1 | 432,450 | 20,607,152,976 | 47,652 | 131.1 | 200,750,622 |
| West Jordan | 1 | 1 | 156,415 | 1,862,064,162 | 11,905 | 131.2 | 19,272,380 |
| Kent | | | | | | | |

**Roll-up across the 13 reporting sites:** 17,352,841 executions, **1,841,626,462,800 total logical reads** (106,128 avg reads/exec). Kent is the only site without it in top 50, suggesting either much lower call volume or a smaller `SrtSortationItem` table that scans cheaply. Worth confirming.

**Observations:**

- **No plan instability anywhere.** Every site shows one plan variant for one query_id. This is the catch-all query signature: the optimizer compiles a single scan plan that handles every parameter combination, so there is no plan-choice variability. The "stability" is consistent badness.
- **Per-site avg reads scales with table size, not call volume.** Bolingbrook averages 165K reads/exec while West Jordan averages 12K. The proc returns the same shape of data at every site, but the table size differs, which is exactly what you would expect from a scan-bound plan. Bolingbrook either has a larger `SrtSortationItem` or wider rows.
- **Avg duration is unusually high for what should be a simple lookup.** 615 ms at Bolingbrook, 504 ms at NorthLake. A single-row barcode lookup against an indexed column should run in single-digit milliseconds. Wall-clock time confirms the engine is doing far more work than the result requires.
- **CPU time is enormous in absolute terms.** Bolingbrook spends roughly 4 billion ms (about 1,118 hours, or 47 days of CPU) per month on this single proc. This is the biggest single-proc CPU drain I have surfaced in the fleet so far.

### 3.2 SET STATISTICS IO, TIME from a representative MFC

I capture STATS IO/TIME from Tolleson (or another representative MFC) running both v2 and v3 against the same data state, with several common parameter combinations:

- `Exec lsp_SrtGetSortationItems @Barcode = '<known barcode>'` (barcode only, returns 1 row)
- `Exec lsp_SrtGetSortationItems @SortationCode = '<known code>'` (sortation code only)
- `Exec lsp_SrtGetSortationItems @Barcode = '<known>', @SortationCode = '<known>'` (both)
- `Exec lsp_SrtGetSortationItems` (no params, returns all rows)

```
(STATS IO + STATS TIME output for v2 goes here, ideally one block per parameter combination)
```

---

## 4. Issue Identification (v2)

The line numbers below reference `Original.sql` (v2).

### 4.1 Catch-all WHERE clause forces a scan plan (lines 27 to 32)

```sql
Where
   (@Barcode Is Null Or Barcode = @Barcode) And
   (@SortationCode Is Null Or SortationCode = @SortationCode)
```

The optimizer compiles plans at compile time and cannot evaluate `@Param Is Null` until runtime. To stay correct in the worst case (both parameters NULL), it chooses a scan and uses it for every call. The Query Store evidence confirms this: every site shows one plan with avg reads/exec scaling to table size rather than result size.

This is the only issue in the proc and it is responsible for the entire 1.84T monthly read load.

---

## 5. First Principles

### 5.1 Catch-All Query Anti-Pattern

The `(@Param Is Null Or Col = @Param)` pattern is the canonical way to defeat the optimizer's ability to choose a seekable plan. The proc accepts optional parameters by design, but the implementation tells the optimizer "I do not know which combination will arrive at runtime, so plan for all of them." The optimizer's only safe choice is a scan. See [[Catch-All Query Anti-Pattern]] for the three canonical fixes (IF/ELSE branching, OPTION (RECOMPILE), and dynamic SQL with sp_executesql) and the criteria for choosing between them.

v3 chooses IF/ELSE branching because the proc has only two optional parameters and four parameter combinations, which is the cleanest case for branching. Each branch produces its own cached plan with index seeks where parameters are provided. The all-NULL case still scans, which is correct because there is no predicate to seek on.

### 5.2 Plan-Cache Reuse vs. Plan Adaptability

A single cached plan that is wrong for every call is worse than four cached plans, each tuned to its parameter shape. v3's branching produces four plans, one per branch, and each is reused indefinitely. There is no parameter sniffing concern because each plan only ever sees calls that match its branch (calls with both parameters never enter the "barcode only" branch). The plan cache stays small and the plans stay good. See [[Parameter Sniffing]] for the related but distinct problem of one plan that is good for some parameter values and bad for others.

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v3. The narrative below walks the structure. Line references are to `Refactored.sql`.

### 6.1 Four IF/ELSE branches, one per parameter combination (lines 41 to 84)

```sql
If @Barcode Is Not Null And @SortationCode Is Not Null
Begin
   Select Barcode, SortationCode
   From SrtSortationItem With (NoLock)
   Where Barcode = @Barcode And SortationCode = @SortationCode
End
Else If @Barcode Is Not Null
Begin
   Select Barcode, SortationCode
   From SrtSortationItem With (NoLock)
   Where Barcode = @Barcode
End
Else If @SortationCode Is Not Null
Begin
   Select Barcode, SortationCode
   From SrtSortationItem With (NoLock)
   Where SortationCode = @SortationCode
End
Else
Begin
   Select Barcode, SortationCode
   From SrtSortationItem With (NoLock)
End
```

Each branch is a standalone SELECT against `SrtSortationItem`. The optimizer compiles four separate plans, one per branch, each tuned to its specific filter. With seekable indexes on `Barcode` and `SortationCode`, the first three branches all become index seeks. The fourth branch is a scan, which is correct because it is asking for the entire table.

There is no semantic change from v2. The same rows are returned for the same parameter values. The only difference is that the engine no longer has to plan for parameter combinations that did not arrive.

---

## 7. Risk & Rollback

### Risks

- **Indexes assumed but not yet verified.** v3's win depends on `SrtSortationItem` having seekable indexes on `Barcode` and on `SortationCode`. If neither index exists, the IF/ELSE branches still execute scans, just in four different plans rather than one. The cost would be similar to v2. Pull the index DDL for `SrtSortationItem` (Section 11.1.A) before deployment.
- **Caller contract is unchanged.** The proc returns the same columns in the same order with the same parameter signature. No caller-visible change.
- **Branch count is small enough not to bloat plan cache.** Four cached plans for four branches is trivial.

### Rollback

The proc is a read-only `SELECT` returning a result set. Rollback is a pure DDL revert: restore the v2 body via `ALTER PROCEDURE`. No data state is mutated, so there is no compensating action required.

If a regression appears, run the v2 form (committed at git revision `8e93671`) against the same data state to confirm. Rollback path:

```sql
-- pull v2 from git
git show 8e93671:stored_procedures/lsp_SrtGetSortationItems.sql

-- adjust CREATE -> ALTER, deploy
```

### Monitoring Window

For the first 24 hours after deployment, watch:

- Query Store avg reads/exec for the proc. Expected drop is from roughly 95K to under 10 for single-parameter calls. If it does not drop, the index assumption is wrong; pull the index DDL and reassess.
- Avg duration. Expected drop is proportional to read drop, single-digit milliseconds for parameter-supplied calls.
- Plan cache. Should now show four plans for the proc, one per branch, each stable.
- Caller-side throughput on the Sortation system. Per-call latency drop should translate to higher tote-scan throughput.

---

## 8. Evidence of Refactor (v3)

I capture STATS IO/TIME from the same MFC and same data state used for the v2 capture, running v3 with the same parameter combinations.

```
(STATS IO + STATS TIME output for v3 goes here, one block per parameter combination)
```

The post-refactor plan summary should show four distinct plans, each with an Index Seek for its parameter-supplied filter (or a Clustered Index Scan for the no-parameter branch). Confirm against the .sqlplan file.

---

## 9. Comparison & Improvement

*Filled in once both v2 and v3 STATS IO/TIME outputs are pasted above.*

| Metric | v2 (catch-all) | v3 (IF/ELSE) | Delta | % Change |
|--------|----------------|--------------|-------|----------|
| Logical reads, barcode-only call | | | | |
| Logical reads, sortation-code-only call | | | | |
| Logical reads, both-parameters call | | | | |
| Logical reads, no-parameters call | | | | |
| CPU time per call (ms) | | | | |
| Elapsed time per call (ms) | | | | |
| Result row count (must match) | | | | |

**Plan-shape verification:**

- v2 plan should show one plan with Clustered Index Scan or Index Scan, regardless of which parameters were supplied.
- v3 plan should show four distinct plans. Three of them (single-parameter and both-parameters branches) should show Index Seeks. The fourth (no-parameters branch) should show a Clustered Index Scan, which is correct.

---

## 10. Validation Checklist

Per `tasks/lessons.md`. Mark each pass or fail before declaring the refactor done.

- [ ] **Same data state.** Both v2 and v3 captures taken against the same data, ideally back to back without intervening writes to `SrtSortationItem`.
- [ ] **Warm cache only.** Both captures are the second of two consecutive runs.
- [ ] **Non-zero result set.** Each parameter combination returned at least one row. The all-NULL case returned the full table.
- [ ] **Identical result set.** v2 and v3 returned the same rows for each parameter combination.
- [ ] **Plan shape matches prediction.** v3 shows Index Seeks for parameter-supplied branches and a Clustered Index Scan for the all-NULL branch.
- [ ] **No new error or warning messages.** Neither run produced cardinality errors, conversion warnings, or excessive memory grant warnings.
- [ ] **Warm-cache elapsed time at or below v2.** v3 is faster on every parameter combination, with the largest improvement on single-parameter calls.

---

## 11. Open Items / Future Improvements

### 11.1 Index Recommendations

These can be ordered or merged depending on what the index DDL extraction reveals.

#### A. Confirm seekable indexes on `Barcode` and `SortationCode`

Run `extract_index_ddl.sql` against Tolleson with `@Tables` set to `('SrtSortationItem')` and check whether nonclustered indexes exist on each parameter column. The expected shape is:

- `IX_SrtSortationItem_Barcode On dbo.SrtSortationItem (Barcode)`
- `IX_SrtSortationItem_SortationCode On dbo.SrtSortationItem (SortationCode)`

If either is missing, add it. The table is described as 4 columns (per the tracking sheet) so each index would be small and cheap.

```sql
Create NonClustered Index IX_SrtSortationItem_Barcode
   On dbo.SrtSortationItem (Barcode);

Create NonClustered Index IX_SrtSortationItem_SortationCode
   On dbo.SrtSortationItem (SortationCode);
```

If both columns are already in indexes, no action needed.

### 11.2 Proc-Level Changes

None pending. v3 is the recommended deployment.

### 11.3 Schema-Level Changes

None pending. The table is small (4 columns) and the lookup pattern is clean. No computed columns or schema redesign needed.
