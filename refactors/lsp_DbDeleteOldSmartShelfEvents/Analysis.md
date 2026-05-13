# lsp_DbDeleteOldSmartShelfEvents: Refactor Analysis (v1 to v2)

**Date:** 2026-05-12
**Tracking sheet row:** Purge-sweep, CTE delete projection
**Scope:** Single-line projection narrowing inside the batched-delete CTE.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_DbDeleteOldSmartShelfEvents`

**Purpose in one line:** Delete `SmartShelf.EvtEventLog` rows whose `DtTm` is older than `@OlderThanXDays`, in blocks of `@NumOfRowsBlockSize` rows, up to a total of `@MaxToDelete`.

**Tables touched:**

- `SmartShelf.EvtEventLog`. The target. Thirteen columns, no LOB, no single very wide column (largest is `Process varchar(256)`). Primary key clustered on `Id` (bigint identity).

**Indexes used (predicted, pending plan capture):**

| Table | Index | Key | Where used |
|---|---|---|---|
| `SmartShelf.EvtEventLog` | `PK_SmartShelf_EvtEventLog` (clustered) | `Id` | the only index referenced in v1; the row locator for the delete |
| `SmartShelf.EvtEventLog` | (any nonclustered on `DtTm` if present) | `DtTm` | candidate for the v2 narrow projection if present; confirm via Section 8 |

`Id` is the clustered key and the order-by column. The proc filters by `DtTm` and orders by `Id`. Because identity values grow monotonically with time, `Id` order and `DtTm` order are roughly aligned on this table, which means v1's scan reads the front of the clustered index. v2 reduces the per-row work the scan has to do.

**Callers:** `lsp_DbPerformNightlyMaintenance`.

---

## 2. Overview of Performance

This is the lower-payoff instance of the CTE delete projection pattern in the sweep, because `SmartShelf.EvtEventLog` has no LOB column and no single very wide column. The per-row width in v1's projection is the sum of thirteen scalar columns, the largest of which is `varchar(256)`. The cost difference between `Select *` and `Select Id` here is real but modest in absolute terms.

The payoff is still worth taking. The CTE projection pattern is mechanical, the fix is one line, and the principle is the same as the higher-payoff cases (LOB-heavy and very-wide tables). Applying the fix uniformly across the purge sweep keeps the pattern out of the codebase for future grep sweeps.

---

## 3. Evidence of Original (v1)

### 3.1 Query Store, cross-MFC view

Pending capture. Query Store filter: `objectname = 'lsp_DbDeleteOldSmartShelfEvents'` against the 30-day window.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------------|----------------|
|     |               |                    |                  |             |                |                    |                |

### 3.2 SET STATISTICS IO, TIME from a representative MFC

I capture STATS IO/TIME from a representative MFC running v1 against a staged data state where at least one full block qualifies for deletion.

```
(STATS IO + STATS TIME output for v1 goes here)
```

The plan summary should be captured into `lsp_DbDeleteOldSmartShelfEvents_original_plan.sqlplan`. Expected shape: clustered index scan or seek on `PK_SmartShelf_EvtEventLog`, top-N sort by `Id`, then delete. The output column list on the index access operator should include every column.

---

## 4. Issue Identification (v1)

The line numbers below reference `Original.sql` (v1).

### 4.1 CTE projection is `Select *` (line 87)

```sql
;With Cte As
   (Select Top(@NumOfRowsBlockSize) * From SmartShelf.EvtEventLog Where DtTm < @CutoffDtTm Order By Id)
Delete From Cte
```

The CTE asks for every column from `SmartShelf.EvtEventLog` to identify rows for deletion. The delete operator consumes only the row locator (`Id`). Every other column is read and then discarded.

Three concrete consequences inherent to the pattern:

- **Plan-shape constraint.** A narrow nonclustered index on `DtTm` cannot be chosen because it does not carry the other twelve columns.
- **Per-row size estimate.** The sort operator's memory grant is computed from the per-row size of the full projection.
- **Row-page reads.** The scan touches the same number of pages either way, but the v1 projection forces the engine to materialize the full row into the intermediate result.

The fix is to project only `Id`.

---

## 5. First Principles

### 5.1 CTE Delete Projection

A CTE that drives a `DELETE` is not a materialized copy of the rows. It is an inline view definition that the optimizer folds into the delete plan. The delete operator only needs the row locator. Every other column in the CTE's projection is read but never used. See [[CTE Delete Projection]] for the full pattern and the four cost dimensions (plan-shape constraint, key lookups, LOB page reads, memory grant).

### 5.2 Index Key Columns vs Included Columns

The fix narrows the CTE projection to the primary key. The primary key is, by definition, present in every nonclustered index as the row locator. A nonclustered index on `DtTm` becomes a covering index for the v2 projection automatically. See [[Index Key Columns vs Included Columns]].

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v2. The diff against v1 is one line: the CTE projection.

```sql
;With Cte As
   (Select Top(@NumOfRowsBlockSize) Id From SmartShelf.EvtEventLog Where DtTm < @CutoffDtTm Order By Id)
   -- v2: project the PK (Id) only. Per the CTE Delete Projection pattern,
   -- the delete operator only needs the row locator; the other twelve
   -- columns in v1's projection are read and discarded.
Delete From Cte
```

The where clause and order-by clause are unchanged. The CTE name is preserved.

---

## 7. Risk & Rollback

### Risks

- **Row identity must be unchanged.** The set of rows the CTE represents is fully determined by the where-clause, the order-by-clause, and the top-N. The projection list cannot change which rows are in the CTE. v2 and v1 delete the same rows, by construction.
- **`Id` is the primary key.** Confirmed against `Tolleson_PA_tables.sql` line 10578: `CONSTRAINT [PK_SmartShelf_EvtEventLog] PRIMARY KEY CLUSTERED ([Id] ASC)`. `Id` is also the `Order By` column.
- **No behavior change.** Purely a read-side optimization.

### Rollback

DDL revert. Restore v1 via `ALTER PROCEDURE`. No compensating action.

### Monitoring Window

For the first 24 to 72 hours after deployment, watch the nightly maintenance log entry for this step. The wall-clock change is expected to be modest given the table is not LOB-heavy, but should not regress.

---

## 8. Evidence of Refactor (v2)

I capture STATS IO/TIME from the same MFC and same staged data state used for v1.

```
(STATS IO + STATS TIME output for v2 goes here)
```

Expected plan summary, to be confirmed against `lsp_DbDeleteOldSmartShelfEvents_refactor_plan.sqlplan`:

- The output column list on the index access operator should show `Id` only.
- The sort estimated row size should drop from the full-row width to the 8-byte `bigint`.

---

## 9. Comparison & Improvement

*Filled in once both v1 and v2 STATS IO/TIME outputs are pasted above.*

| Metric | v1 (`Select *`) | v2 (`Select Id`) | Delta | % Change |
|--------|-----------------|------------------|-------|----------|
| Logical reads on SmartShelf.EvtEventLog per block | | | | |
| Sort memory grant (KB) | | | | |
| CPU time per block (ms) | | | | |
| Elapsed time per block (ms) | | | | |
| Rows deleted (must match) | | | | parity check |

The expected wins are smaller than the SmartShelf.EvtErrorLog or OeScriptImage cases. The memory grant reduction is the most measurable single effect on this table.

---

## 10. Validation Checklist

- [ ] **Same data state.** Both v1 and v2 captures taken against the same data, back to back.
- [ ] **Warm cache only.** Both captures are the second of two consecutive runs.
- [ ] **Non-zero result set.** At least one full block was deleted.
- [ ] **Identical rows deleted.** Verifiable by snapshotting `Id` for the qualifying set before each run.
- [ ] **Plan shape matches prediction.** v2 shows `Id`-only output.
- [ ] **No new error or warning messages.**
- [ ] **Warm-cache elapsed time at or below v1.**

Net call: pending evidence capture.

---

## 11. Open Items / Future Improvements

### 11.1 Index Recommendations

#### A. Consider a nonclustered index on `DtTm`

Same recommendation as the SmartShelf.EvtErrorLog analysis. With the v2 PK-only projection, a nonclustered index on `DtTm` becomes a covering candidate. Whether the add is justified depends on the table's other access patterns.

```sql
Create NonClustered Index IX_SmartShelf_EvtEventLog_DtTm
   On SmartShelf.EvtEventLog (DtTm)
   With (Fillfactor = 92, Online = On);
```

### 11.2 Proc-Level Changes

#### A. Audit the loop's `Exists` for the same projection antipattern

Same note as the SmartShelf.EvtErrorLog analysis.

### 11.3 Schema-Level Changes

#### A. Partition `SmartShelf.EvtEventLog` by `DtTm`

Per `ScalabilityRoadmap_PartitioningAndArchitecture.md`. Out of scope for this refactor.
