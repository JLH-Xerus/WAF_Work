# lsp_DbDeleteOldScriptImages: Refactor Analysis (v1 to v2)

**Date:** 2026-05-12
**Tracking sheet row:** Purge-sweep, CTE delete projection
**Scope:** Single-line projection narrowing inside the batched-delete CTE.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_DbDeleteOldScriptImages`

**Purpose in one line:** Delete `OeScriptImage` rows whose `ScanDateTime` is older than `@OlderThanXDays` and whose `ArchivedDtTm` is NULL, in blocks of `@NumOfRowsBlockSize` rows, up to a total of `@MaxToDelete`.

**Tables touched:**

- `OeScriptImage`. The target. Two `image` columns (`Image`, `ImageBack`) with `TEXTIMAGE_ON [PRIMARY]`, plus seven scalar columns. Primary key clustered on `RxNum`.

**Indexes used (predicted, pending plan capture):**

| Table | Index | Key | Where used |
|---|---|---|---|
| `OeScriptImage` | `PK_OeScriptImage` (clustered) | `RxNum` | the only index referenced in v1; the row locator for the delete |
| `OeScriptImage` | (any nonclustered on `ScanDateTime` if present) | `ScanDateTime` | candidate for the v2 narrow projection if present; confirm via Section 8 plan |

The clustered key `RxNum` is not aligned with the filter column `ScanDateTime`. The v1 plan is therefore a clustered index scan with a top-N sort, materializing every column including the two `image` columns for every block. Any nonclustered index on `ScanDateTime` (filtered on `ArchivedDtTm IS NULL` or not) would become a covering candidate once the projection is narrowed to `RxNum`. Whether such an index exists must be confirmed against `IndexExtract.xlsx`; the recommendation in Section 11 covers both cases.

**Callers:** `lsp_DbPerformNightlyMaintenance` (the "Delete Old Script Images" step). Single caller, run once per nightly maintenance window.

---

## 2. Overview of Performance

This is the highest-payoff instance of the CTE delete projection pattern in the purge sweep, because `OeScriptImage` is the only target table in the sweep with off-row LOB storage. The two `image` columns and the `TEXTIMAGE_ON [PRIMARY]` clause mean that every row carries an LOB allocation pointer, and any operator that materializes the full row has to follow the pointer to the LOB pages. v1's `Select Top (@NumOfRowsBlockSize) *` projection asks for both `Image` and `ImageBack` for every row in the block, on every iteration of the purge loop. The delete operator throws those reads away.

The cost picture in plain terms: per block of N rows, v1 reads roughly N row pages plus N LOB chains. With image sizes in the hundreds of kilobytes to single megabytes, the LOB reads dominate the per-block cost by orders of magnitude. v2 reads N row pages and zero LOB pages, because `RxNum` is in the clustered index and the delete operator only needs the row locator.

The proc is not called frequently (once per nightly maintenance run), but each call processes blocks until either the qualifying set is empty or `@MaxToDelete` is hit. At the default `@NumOfRowsBlockSize = 1000` and `@MaxToDelete = 50000`, a worst-case run reads the LOB chains of 50,000 image rows in v1 and zero in v2.

---

## 3. Evidence of Original (v1)

### 3.1 Query Store, cross-MFC view

Pending capture. The proc runs inside the nightly maintenance window and may not appear in the top-N read offenders, because the per-call read cost is paid once per nightly run rather than continuously. The Query Store filter for this proc is `objectname = 'lsp_DbDeleteOldScriptImages'` against the 30-day window.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------------|----------------|
|     |               |                    |                  |             |                |                    |                |

### 3.2 SET STATISTICS IO, TIME from a representative MFC

I capture STATS IO/TIME from a representative MFC running v1 against a staged data state where at least one full block (`@NumOfRowsBlockSize` rows) qualifies for deletion. The relevant metrics are logical reads on `OeScriptImage` (row pages) and LOB logical reads on `OeScriptImage` (image pages), captured separately.

```
(STATS IO + STATS TIME output for v1 goes here)
```

The plan summary should be captured into `lsp_DbDeleteOldScriptImages_original_plan.sqlplan`. Expected shape: clustered index scan or seek on `PK_OeScriptImage`, top-N sort by `ScanDateTime`, then delete. The output column list on the index access operator should include every column of `OeScriptImage`, which is the artifact of the `Select *` projection.

---

## 4. Issue Identification (v1)

The line numbers below reference `Original.sql` (v1).

### 4.1 CTE projection is `Select *` against an LOB-heavy table (lines 80 to 93)

```sql
;With BlockOfRows As
   (
      Select
         Top (@NumOfRowsBlockSize) *
      From
         OeScriptImage
      Where
         ScanDateTime < @CutoffDtTm
         And
         ArchivedDtTm Is Null
      Order By
         ScanDateTime
   )
Delete From BlockOfRows
```

The CTE asks for every column from `OeScriptImage` to identify rows for deletion. The delete operator on a `Delete From <CteName>` consumes only the row locator (the clustered key, `RxNum`). Every other column in the projection is read and then discarded. For a table with two `image` columns and `TEXTIMAGE_ON [PRIMARY]`, "every other column" includes the LOB pages.

Three concrete consequences inherent to the pattern:

- **Plan-shape constraint.** The optimizer treats the CTE as a query that must produce every column. The clustered index is the only structure that carries the LOB pointers, so a clustered access path is forced even if a narrower nonclustered index on `ScanDateTime` exists.
- **LOB page reads.** The projection requires materializing `Image` and `ImageBack` for every row in the block. The LOB pages are touched, the bytes are read, and the delete operator never uses them.
- **Memory grant.** The per-row size estimate for the sort feeding the top-N is dominated by the LOB column estimates. The sort's memory grant request is correspondingly inflated.

The fix is to project only `RxNum`. The where-clause and order-by-clause stay exactly as they are. The CTE keeps its name (`BlockOfRows`) for minimum diff and for compatibility with whatever future changes someone may layer on top.

---

## 5. First Principles

### 5.1 CTE Delete Projection

A CTE that drives a `DELETE` is not a materialized copy of the rows. It is an inline view definition that the optimizer folds into the delete plan. The delete operator only needs the row locator (the clustered key) to find and delete the row. Every other column in the CTE's projection is read but never used. When the underlying table has off-row LOB storage, the projection's hidden cost is reading the LOB pages on every iteration of the purge loop. See [[CTE Delete Projection]] for the full pattern, the four concrete cost dimensions (plan-shape constraint, key lookups, LOB page reads, memory grant), and how this differs from the general `Select *` antipattern.

### 5.2 Index Key Columns vs Included Columns

The fix narrows the CTE projection to the primary key. The reason that helps is that the primary key is, by definition, present in every nonclustered index as the row locator. A nonclustered index on `ScanDateTime` becomes a covering index for the v2 projection automatically, without an `INCLUDE` list. The same index could not cover the v1 projection without including every column of the table. See [[Index Key Columns vs Included Columns]].

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v2. The diff against v1 is one line: the CTE projection.

```sql
;With BlockOfRows As
   (
      Select
         Top (@NumOfRowsBlockSize) RxNum    -- v2: project the PK only.
      From
         OeScriptImage                       -- LOB-heavy table; Select * would
                                             -- touch the Image and ImageBack
                                             -- LOB pages on every iteration.
      Where
         ScanDateTime < @CutoffDtTm
         And
         ArchivedDtTm Is Null
      Order By
         ScanDateTime
   )
Delete From BlockOfRows
```

The where clause and order-by clause are unchanged. The CTE name is preserved. The delete clause is unchanged. The set of rows deleted is identical to v1, because the CTE's row identity is determined by the where-clause and order-by-clause and top, not by the projection.

---

## 7. Risk & Rollback

### Risks

- **Row identity must be unchanged.** The set of rows the CTE represents is fully determined by the where-clause, the order-by-clause, and the top-N. The projection list cannot change which rows are in the CTE. v2 and v1 delete the same rows, by construction.
- **`RxNum` is the primary key.** `OeScriptImage` is keyed on `RxNum`, not `Id`. Selecting `Id` would not work because the table has no `Id` column. The fix has to follow the schema, not the codebase convention of using `Id`. Confirmed against `Tolleson_PA_tables.sql` line 6736.
- **Off-row LOB allocation.** The `TEXTIMAGE_ON [PRIMARY]` clause confirms that `Image` and `ImageBack` are stored off-row. The v1 `Select *` therefore pays for LOB page reads; the v2 `Select RxNum` does not. The behavior change is purely on the read side, not on the delete side.
- **Order-by column not in the projection.** v2 orders by `ScanDateTime` while projecting only `RxNum`. That is allowed by SQL Server: the order-by column needs to be present in the source, not in the projection. The optimizer will resolve it against `OeScriptImage` directly.

### Rollback

DDL revert. The proc is read-only with respect to data identity (the rows deleted are identical to v1). Restore v1 via `ALTER PROCEDURE`. No compensating action.

### Monitoring Window

For the first 24 to 72 hours after deployment, watch:

- The nightly maintenance log entry for the "Delete Old Script Images" step. Compare wall-clock duration against the prior week's runs.
- Tempdb peak usage during the maintenance window. The sort feeding the top-N should request a much smaller memory grant in v2; if it spilled to tempdb in v1, the spill should disappear.
- LOB logical reads on `OeScriptImage` (visible via STATS IO or Query Store metric breakdown if enabled). Should drop to zero for this proc.

---

## 8. Evidence of Refactor (v2)

I capture STATS IO/TIME from the same MFC and same staged data state used for v1.

```
(STATS IO + STATS TIME output for v2 goes here)
```

Expected plan summary, to be confirmed against `lsp_DbDeleteOldScriptImages_refactor_plan.sqlplan`:

- The output column list on the index access operator should show `RxNum` only.
- If a nonclustered index on `ScanDateTime` exists, the optimizer should now choose it (narrow covering seek) instead of the clustered access path. If no such index exists, the access path will still be against the clustered index but will not materialize the LOB columns.
- The sort operator's estimated row size should drop from the full-row estimate (dominated by the LOB pointer width plus the two `varchar` columns) to the bare `RxNum` width.

---

## 9. Comparison & Improvement

*Filled in once both v1 and v2 STATS IO/TIME outputs are pasted above.*

| Metric | v1 (`Select *`) | v2 (`Select RxNum`) | Delta | % Change |
|--------|-----------------|---------------------|-------|----------|
| Logical reads on OeScriptImage (row pages) per block | | | | |
| LOB logical reads on OeScriptImage per block | | | | |
| Sort memory grant (KB) | | | | |
| CPU time per block (ms) | | | | |
| Elapsed time per block (ms) | | | | |
| Rows deleted (must match) | | | | parity check |

**Plan-shape verification:**

- v1 plan should show every column of `OeScriptImage` in the output list of the clustered index operator, including the LOB pointers.
- v2 plan should show `RxNum` only in the output list. If a nonclustered index on `ScanDateTime` exists, the access path should switch.

The expected wins are bounded by image size. If the average `Image` size is 200 KB and a block is 1000 rows, v1 reads 200 MB of LOB content per iteration; v2 reads zero. The wall-clock effect depends on the storage subsystem.

---

## 10. Validation Checklist

- [ ] **Same data state.** Both v1 and v2 captures taken against the same data, ideally back to back with no intervening writes to `OeScriptImage`.
- [ ] **Warm cache only.** Both captures are the second of two consecutive runs.
- [ ] **Non-zero result set.** The qualifying set is non-empty for the test parameters; at least one full block was deleted.
- [ ] **Identical rows deleted.** The set of `RxNum` values deleted by v1 and v2 against the same data state is identical. Verifiable by snapshotting `OeScriptImage.RxNum` for the qualifying set before each run and confirming the two delete operations remove the same set.
- [ ] **Plan shape matches prediction.** v2's plan shows `RxNum`-only output on the index access operator, and (if applicable) a narrower index choice.
- [ ] **LOB reads drop to zero.** STATS IO for v2 reports zero LOB logical reads on `OeScriptImage`.
- [ ] **No new error or warning messages.** Neither run produced cardinality errors, conversion warnings, or memory grant warnings.
- [ ] **Warm-cache elapsed time at or below v1.** Wall clock has not regressed.

Net call: pending evidence capture.

---

## 11. Open Items / Future Improvements

### 11.1 Index Recommendations

#### A. Confirm or create a nonclustered index on `ScanDateTime`

The proc's filter is `ScanDateTime < @CutoffDtTm And ArchivedDtTm Is Null`, ordered by `ScanDateTime`. With the v2 PK-only projection, a nonclustered index on `ScanDateTime` becomes a covering index automatically (the PK `RxNum` is carried as the row locator). If such an index exists, the optimizer will pick it. If it does not, the recommended DDL is:

```sql
Create NonClustered Index IX_OeScriptImage_ScanDateTime
   On dbo.OeScriptImage (ScanDateTime)
   Where ArchivedDtTm Is Null
   With (Fillfactor = 92, Online = On);
```

The filtered predicate matches the proc's filter exactly, which means the index will be used as a seek with no residual on `ArchivedDtTm`. See [[Index Key Columns vs Included Columns]] for the broader principle.

**Expected impact:** the access path on `OeScriptImage` becomes a narrow filtered seek. Combined with the v2 projection, no LOB pages are touched and no key lookups are performed. The full win of the refactor is realized when both the projection and the index choice are aligned.

#### B. Confirm filter-predicate parity

If a filtered index on `ScanDateTime` exists but has a slightly different filter predicate (for example `Where ArchivedDtTm Is Null And ScanDateTime Is Not Null`), the optimizer will not use it. Filtered indexes require exact predicate match. Worth confirming before assuming the optimizer will pick it.

### 11.2 Proc-Level Changes

#### A. Drop the `EXISTS` on the loop condition

The `While Exists (Select * From OeScriptImage Where ScanDateTime < @CutoffDtTm And ArchivedDtTm Is Null)` on line 72 has the same `Select *` antipattern. The fix is the same: `Select 1` (or any constant projection). The exists subquery does not need to materialize columns. Out of scope for this version because it is a separate statement and not part of the CTE pattern, but trivially fixable in the next pass.

#### B. Drop the `@NumOfRowsBlockSize` increment that assumes the block was full

Line 103 increments `@NumOfRowsDeleted` by `@NumOfRowsBlockSize` unconditionally. If the last block is partial (the remaining qualifying set is smaller than the block size), the counter overshoots. Use `@@RowCount` instead. Not strictly a correctness bug because the loop terminates on the `Exists` check, but it makes the `@MaxToDelete` ceiling approximate where it could be exact. Out of scope.

### 11.3 Schema-Level Changes

#### A. Partition by `ScanDateTime`

The `ScalabilityRoadmap_PartitioningAndArchitecture.md` already calls out `OeScriptImage` as a partitioning candidate. Partition switching collapses the purge from a row-by-row delete loop into a metadata operation. The CTE projection fix is a stop-gap that buys time until partition switching ships. Listed here for completeness.

#### B. Audit other procs for the same CTE projection pattern

The CTE delete projection pattern is mechanical. A grep for `Select Top \(.*\) \*` inside the `stored_procedures` folder will find every instance. The sweep this analysis is part of covers all five remaining occurrences. After this sweep, the inventory is empty; a periodic re-scan would catch any new occurrences introduced by future work.
