# lsp_DbDeleteOldSmartShelfErrors: Refactor Analysis (v1 to v2)

**Date:** 2026-05-12
**Tracking sheet row:** Purge-sweep, CTE delete projection
**Scope:** Single-line projection narrowing inside the batched-delete CTE.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_DbDeleteOldSmartShelfErrors`

**Purpose in one line:** Delete `SmartShelf.EvtErrorLog` rows whose `DtTm` is older than `@OlderThanXDays`, in blocks of `@NumOfRowsBlockSize` rows, up to a total of `@MaxToDelete`.

**Tables touched:**

- `SmartShelf.EvtErrorLog`. The target. Ten columns including `Message varchar(6500)`, the dominant per-row width. Primary key clustered on `Id` (bigint identity).

**Indexes used (predicted, pending plan capture):**

| Table | Index | Key | Where used |
|---|---|---|---|
| `SmartShelf.EvtErrorLog` | `PK_SmartShelf_EvtErrorLog` (clustered) | `Id` | the only index referenced in v1; the row locator for the delete |
| `SmartShelf.EvtErrorLog` | (any nonclustered on `DtTm` if present) | `DtTm` | candidate for the v2 narrow projection if present; confirm via Section 8 |

The clustered key `Id` is roughly aligned with the filter column `DtTm` because both grow monotonically with time (newer error rows have larger `Id` and later `DtTm`). The v1 plan is therefore likely a clustered index scan with a top-N over the leading range, materializing the `varchar(6500) Message` column for every row in the block. A nonclustered index on `DtTm` would become a narrow covering candidate once the projection is narrowed to `Id`.

**Callers:** `lsp_DbPerformNightlyMaintenance` (the SmartShelf error retention step). Single caller, run once per nightly maintenance window.

---

## 2. Overview of Performance

`SmartShelf.EvtErrorLog` is a wide-row table because of the `Message varchar(6500)` column. A `Select *` projection inside the delete-driving CTE asks the engine to produce that 6500-byte field for every row in the block, on every iteration of the purge loop. The delete operator throws those bytes away. The per-block cost in v1 scales with the average `Message` length, not with row count.

The cost picture in plain terms: per block of N rows, v1 reads N data pages where each page holds a small number of rows because the rows are wide. v2 reads the same pages but the per-row size estimate (and therefore the memory grant for the sort feeding the top-N) is set by `bigint Id` rather than by `varchar(6500) Message`. The plan can also switch to a nonclustered seek if a `DtTm` index exists.

The proc is called once per nightly run. Wall-clock matters mostly for the maintenance window envelope, not for steady-state user latency.

---

## 3. Evidence of Original (v1)

### 3.1 Query Store, cross-MFC view

Pending capture. The proc runs inside the nightly maintenance window. Query Store filter: `objectname = 'lsp_DbDeleteOldSmartShelfErrors'` against the 30-day window.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------------|----------------|
|     |               |                    |                  |             |                |                    |                |

### 3.2 SET STATISTICS IO, TIME from a representative MFC

I capture STATS IO/TIME from a representative MFC running v1 against a staged data state where at least one full block qualifies for deletion.

```
(STATS IO + STATS TIME output for v1 goes here)
```

The plan summary should be captured into `lsp_DbDeleteOldSmartShelfErrors_original_plan.sqlplan`. Expected shape: clustered index scan or seek on `PK_SmartShelf_EvtErrorLog`, top-N sort by `Id`, then delete. The output column list on the index access operator should include every column, with `Message varchar(6500)` as the dominant width.

---

## 4. Issue Identification (v1)

The line numbers below reference `Original.sql` (v1).

### 4.1 CTE projection is `Select *` against a wide-row table (line 90)

```sql
;With Cte As
   (Select Top(@NumOfRowsBlockSize) * From SmartShelf.EvtErrorLog Where DtTm < @CutoffDtTm Order By Id)
Delete From Cte
```

The CTE asks for every column from `SmartShelf.EvtErrorLog` to identify rows for deletion. The delete operator consumes only the row locator (`Id`). Every other column in the projection is read and then discarded. For a table whose dominant column is a `varchar(6500)`, "every other column" represents most of the per-row cost.

Three concrete consequences inherent to the pattern:

- **Plan-shape constraint.** The optimizer treats the CTE as a query that must produce every column. A narrow nonclustered index on `DtTm` cannot be chosen because it does not carry the `Message` column.
- **Per-row size estimate.** The sort operator's memory grant is computed from the per-row size of the full projection, which is dominated by the `varchar(6500)`.
- **Row-page reads.** Each data page holds fewer rows because the rows are wide. The delete operator's row identification path reads more pages than it would if the engine could navigate by a narrow index leaf.

The fix is to project only `Id`. The where-clause and order-by-clause stay exactly as they are.

---

## 5. First Principles

### 5.1 CTE Delete Projection

A CTE that drives a `DELETE` is not a materialized copy of the rows. It is an inline view definition that the optimizer folds into the delete plan. The delete operator only needs the row locator to find and delete the row. Every other column in the CTE's projection is read but never used. When the underlying table has wide columns (here `varchar(6500)`), the projection's hidden cost is materializing that column on every iteration of the purge loop. See [[CTE Delete Projection]] for the full pattern and cost dimensions.

### 5.2 Index Key Columns vs Included Columns

The fix narrows the CTE projection to the primary key. The reason that helps is that the primary key is, by definition, present in every nonclustered index as the row locator. A nonclustered index on `DtTm` becomes a covering index for the v2 projection automatically. The same index could not cover the v1 projection without including the `Message varchar(6500)` column, which would bloat the index beyond justification. See [[Index Key Columns vs Included Columns]].

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v2. The diff against v1 is one line: the CTE projection.

```sql
;With Cte As
   (Select Top(@NumOfRowsBlockSize) Id From SmartShelf.EvtErrorLog Where DtTm < @CutoffDtTm Order By Id)
   -- v2: project the PK (Id) only.
   -- SmartShelf.EvtErrorLog has a varchar(6500) Message column; v1's Select *
   -- materialized that column for every row in every block. The delete
   -- operator only needs the row locator.
Delete From Cte
```

The where clause and order-by clause are unchanged. The CTE name is preserved.

---

## 7. Risk & Rollback

### Risks

- **Row identity must be unchanged.** The set of rows the CTE represents is fully determined by the where-clause, the order-by-clause, and the top-N. The projection list cannot change which rows are in the CTE. v2 and v1 delete the same rows, by construction.
- **`Id` is the primary key.** Confirmed against `Tolleson_PA_tables.sql` line 10539: `CONSTRAINT [PK_SmartShelf_EvtErrorLog] PRIMARY KEY CLUSTERED ([Id] ASC)`. `Id` is also the `Order By` column, so the optimizer can satisfy the top-N from the clustered index in `Id` order without an explicit sort.
- **No behavior change.** This is purely a read-side optimization. The deleted rows are identical.

### Rollback

DDL revert. Restore v1 via `ALTER PROCEDURE`. No compensating action.

### Monitoring Window

For the first 24 to 72 hours after deployment, watch:

- The nightly maintenance log entry for this step. Wall-clock duration against prior-week runs.
- STATS IO on `SmartShelf.EvtErrorLog`: logical reads per block should drop because fewer pages are scanned (no need to materialize the `varchar(6500)` field).

---

## 8. Evidence of Refactor (v2)

I capture STATS IO/TIME from the same MFC and same staged data state used for v1.

```
(STATS IO + STATS TIME output for v2 goes here)
```

Expected plan summary, to be confirmed against `lsp_DbDeleteOldSmartShelfErrors_refactor_plan.sqlplan`:

- The output column list on the index access operator should show `Id` only.
- The sort estimated row size should drop from the full-row width (driven by `varchar(6500) Message`) to the 8-byte `bigint`.
- If a nonclustered index on `DtTm` exists, the optimizer should choose it; if not, the clustered index path remains but with a narrower output.

---

## 9. Comparison & Improvement

*Filled in once both v1 and v2 STATS IO/TIME outputs are pasted above.*

| Metric | v1 (`Select *`) | v2 (`Select Id`) | Delta | % Change |
|--------|-----------------|------------------|-------|----------|
| Logical reads on SmartShelf.EvtErrorLog per block | | | | |
| Sort memory grant (KB) | | | | |
| CPU time per block (ms) | | | | |
| Elapsed time per block (ms) | | | | |
| Rows deleted (must match) | | | | parity check |

The expected wins scale with the average `Message` length in the deleted set and with how many `DtTm` data pages each block spans.

---

## 10. Validation Checklist

- [ ] **Same data state.** Both v1 and v2 captures taken against the same data, back to back with no intervening writes to `SmartShelf.EvtErrorLog`.
- [ ] **Warm cache only.** Both captures are the second of two consecutive runs.
- [ ] **Non-zero result set.** At least one full block was deleted in both runs.
- [ ] **Identical rows deleted.** Verifiable by snapshotting `Id` for the qualifying set before each run.
- [ ] **Plan shape matches prediction.** v2 shows `Id`-only output on the index access operator, and (if applicable) a narrower index choice on `DtTm`.
- [ ] **No new error or warning messages.**
- [ ] **Warm-cache elapsed time at or below v1.**

Net call: pending evidence capture.

---

## 11. Open Items / Future Improvements

### 11.1 Index Recommendations

#### A. Consider a nonclustered index on `DtTm`

The proc filters by `DtTm` and orders by `Id`. With the v2 PK-only projection, a nonclustered index on `DtTm` becomes a covering candidate. The optimizer can range-seek by `DtTm`, take the top-N by `Id`, and pass the row locators to the delete operator. Whether this is the right add depends on what other queries hit `SmartShelf.EvtErrorLog` and how write-heavy the table is. Index DDL:

```sql
Create NonClustered Index IX_SmartShelf_EvtErrorLog_DtTm
   On SmartShelf.EvtErrorLog (DtTm)
   With (Fillfactor = 92, Online = On);
```

The PK `Id` is carried as the row locator; no `INCLUDE` is needed for the v2 projection.

### 11.2 Proc-Level Changes

#### A. Drop the `EXISTS` projection on the loop condition

The `While Exists (Select * From SmartShelf.EvtErrorLog Where DtTm < @CutoffDtTm)` (if present in the loop header) has the same `Select *` antipattern. The fix is `Select 1`. The exists subquery only needs to know whether any qualifying row exists, not what the rows look like. Out of scope for this revision.

### 11.3 Schema-Level Changes

#### A. Partition `SmartShelf.EvtErrorLog` by `DtTm`

`ScalabilityRoadmap_PartitioningAndArchitecture.md` calls out SmartShelf tables as partitioning candidates. Partition switching collapses the purge from a row-by-row delete loop into a metadata operation. The CTE projection fix is a stop-gap.

#### B. Re-evaluate the `Message varchar(6500)` column width

A 6500-character error message column is large. If actual messages are much shorter in practice, the column could be narrowed. If actual messages are sometimes that long, the column should arguably be `varchar(max)` with off-row storage so that wide-row scans are not paying for it on every read. Out of scope; flagged for the schema review.
