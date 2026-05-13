# lsp_DbDeleteOldRxData: Refactor Analysis (v1 to v2)

**Date:** 2026-05-12
**Tracking sheet row:** Purge-sweep, CTE delete projection
**Scope:** Single-line projection narrowing inside the OeOrderAcceptReject batched-delete CTE (line 1513). Other CTEs in this proc already use the PK-only form per the prior nightly maintenance refactor and are not touched in this revision.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_DbDeleteOldRxData`

**Purpose in one line:** Delete Rx-related historical rows older than the cutoff across the OeOrder family of tables, in cascading dependency order, in blocks of `@NumOfRowsBlockSize` rows.

**Tables touched (scope of this refactor):**

- `OeOrderAcceptReject`. Sixty columns. Several wide `varchar` columns including `Directions varchar(1033)`, `Address1 varchar(1024)`, `Address2 varchar(1024)`, `DurData varchar(1023)`. Primary key clustered on `Id` (bigint identity).

The proc touches many other tables across its 1550 lines. Those statements are out of scope for this revision. The change is local to the OeOrderAcceptReject CTE on line 1513.

**Indexes used (predicted, pending plan capture):**

| Table | Index | Key | Where used |
|---|---|---|---|
| `OeOrderAcceptReject` | `PK_OeOrderAcceptReject` (clustered) | `Id` | the row locator for the delete |
| `OeOrderAcceptReject` | (any nonclustered on `RejectDtTm` if present) | `RejectDtTm` | candidate for the v2 narrow projection; confirm via Section 8 |

`Id` is the clustered key and is roughly aligned with `RejectDtTm` because the identity grows monotonically with time. The clustered index is therefore a natural candidate for the top-N over the leading range, but with v1's `Select *` the optimizer must read every column.

**Callers:** `lsp_DbPerformNightlyMaintenance`.

---

## 2. Overview of Performance

This is the second of two procs in the sweep that target `OeOrderAcceptReject` (the other is `lsp_DbPurgeHistoryData`). Both use the same CTE delete projection pattern against the same wide table.

`OeOrderAcceptReject` is a wide table without LOB columns. The per-row width in v1's projection is dominated by the four `varchar(1000+)` fields. A `Select *` projection asks the engine to materialize roughly 4 KB of varchar bytes per row in the block, on every iteration of the purge loop. The delete operator throws those bytes away.

The proc itself is large (1550 lines) and the OeOrderAcceptReject CTE is one of many delete operations it performs. The wall-clock impact of this single change is bounded by how many blocks of OeOrderAcceptReject get deleted per nightly run, which varies by site.

---

## 3. Evidence of Original (v1)

### 3.1 Query Store, cross-MFC view

Pending capture. Query Store filter: `objectname = 'lsp_DbDeleteOldRxData'`. The proc executes many statements per call; the relevant query_id is the one matching the OeOrderAcceptReject CTE on line 1513. Use `query_sql_text LIKE '%OeOrderAcceptReject%'` to filter.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------------|----------------|
|     |               |                    |                  |             |                |                    |                |

### 3.2 SET STATISTICS IO, TIME from a representative MFC

I capture STATS IO/TIME for the OeOrderAcceptReject CTE specifically. The full proc generates STATS IO from every statement; for the comparison the relevant block is the rows for the `OeOrderAcceptReject` table.

```
(STATS IO + STATS TIME output for v1 goes here, scoped to OeOrderAcceptReject)
```

Plan summary should be captured for the OeOrderAcceptReject statement into `lsp_DbDeleteOldRxData_original_plan.sqlplan` (whole-proc plan; the relevant subtree is the CTE on line 1513).

---

## 4. Issue Identification (v1)

The line numbers below reference `Original.sql` (v1).

### 4.1 CTE projection is `Select *` against a wide table (line 1513)

```sql
;With Cte As
   (
      Select Top (@NumOfRowsBlockSize) * From OeOrderAcceptReject Where RejectDtTm < @CutoffDtTm Order By Id
   )
Delete From Cte
```

The CTE asks for every column from `OeOrderAcceptReject` to identify rows for deletion. The delete operator consumes only the row locator (`Id`). Every other column is read and then discarded.

The consequences are the standard set for the pattern, with the wide `varchar` columns driving the per-row cost:

- **Plan-shape constraint.** A narrow nonclustered index on `RejectDtTm` cannot be chosen because it does not carry the 59 other columns.
- **Per-row size estimate.** The sort operator's memory grant is computed from the per-row size of the full projection, dominated by the four `varchar(1000+)` columns.
- **Row-page reads.** Wide rows mean fewer rows per data page. The scan touches more pages per block than a narrower-projection plan would need.

The fix is to project only `Id`.

---

## 5. First Principles

### 5.1 CTE Delete Projection

A CTE that drives a `DELETE` is not a materialized copy of the rows. It is an inline view definition that the optimizer folds into the delete plan. The delete operator only needs the row locator. Every other column in the CTE's projection is read but never used. See [[CTE Delete Projection]] for the full pattern.

### 5.2 Index Key Columns vs Included Columns

The fix narrows the CTE projection to the primary key. The primary key is, by definition, present in every nonclustered index as the row locator. A nonclustered index on `RejectDtTm` becomes a covering index for the v2 projection automatically. See [[Index Key Columns vs Included Columns]].

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v2. The diff against v1 is one line: the CTE projection on line 1513.

```sql
;With Cte As
   (
      Select Top (@NumOfRowsBlockSize) Id From OeOrderAcceptReject Where RejectDtTm < @CutoffDtTm Order By Id
      -- v2: project the PK (Id) only. The delete operator only needs the
      -- row locator; v1's Select * materialized the full 60-column row
      -- (with four varchar(1000+) fields) for every row in every block.
   )
Delete From Cte
```

The where clause and order-by clause are unchanged. The CTE name is preserved. All other CTEs in the proc were already in PK-only form prior to this refactor and are not modified.

---

## 7. Risk & Rollback

### Risks

- **Row identity must be unchanged.** The set of rows the CTE represents is fully determined by the where-clause, the order-by-clause, and the top-N. The projection list cannot change which rows are in the CTE. v2 and v1 delete the same rows, by construction.
- **`Id` is the primary key.** Confirmed against `Tolleson_PA_tables.sql` line 5719: `CONSTRAINT [PK_OeOrderAcceptReject] PRIMARY KEY CLUSTERED ([Id] ASC)`. `Id` is also the `Order By` column.
- **Scope is one CTE.** The refactor touches only the OeOrderAcceptReject CTE on line 1513. The rest of the 1550-line proc is unchanged. There is no risk of inadvertent change to the other delete statements in this proc.
- **No behavior change.** Purely a read-side optimization for one statement.

### Rollback

DDL revert. Restore v1 via `ALTER PROCEDURE`. No compensating action.

### Monitoring Window

For the first 24 to 72 hours after deployment, watch:

- The nightly maintenance log entry for this proc. Wall-clock duration against prior-week runs.
- STATS IO on `OeOrderAcceptReject` during the proc's execution. Logical reads for the OeOrderAcceptReject CTE block should drop.

---

## 8. Evidence of Refactor (v2)

I capture STATS IO/TIME from the same MFC and same staged data state used for v1.

```
(STATS IO + STATS TIME output for v2 goes here, scoped to OeOrderAcceptReject)
```

Expected plan summary, to be confirmed against `lsp_DbDeleteOldRxData_refactor_plan.sqlplan`:

- The output column list on the index access operator for the OeOrderAcceptReject CTE should show `Id` only.
- The sort estimated row size should drop from the full-row width (driven by the four wide `varchar` columns) to the 8-byte `bigint`.
- If a nonclustered index on `RejectDtTm` exists, the optimizer should choose it.

---

## 9. Comparison & Improvement

*Filled in once both v1 and v2 STATS IO/TIME outputs are pasted above.*

| Metric | v1 (`Select *`) | v2 (`Select Id`) | Delta | % Change |
|--------|-----------------|------------------|-------|----------|
| Logical reads on OeOrderAcceptReject per block | | | | |
| Sort memory grant (KB) | | | | |
| CPU time for OeOrderAcceptReject statement (ms) | | | | |
| Elapsed time for OeOrderAcceptReject statement (ms) | | | | |
| Rows deleted from OeOrderAcceptReject (must match) | | | | parity check |

The expected wins scale with row width, which is large on this table because of the four `varchar(1000+)` columns.

---

## 10. Validation Checklist

- [ ] **Same data state.** Both v1 and v2 captures taken against the same data, back to back.
- [ ] **Warm cache only.** Both captures are the second of two consecutive runs.
- [ ] **Non-zero result set.** At least one full block of OeOrderAcceptReject was deleted in both runs.
- [ ] **Identical rows deleted from OeOrderAcceptReject.** Verifiable by snapshotting `Id` for the qualifying set before each run.
- [ ] **Plan shape matches prediction.** v2 shows `Id`-only output on the OeOrderAcceptReject index access operator.
- [ ] **No new error or warning messages.**
- [ ] **Warm-cache elapsed time at or below v1.**

Net call: pending evidence capture.

---

## 11. Open Items / Future Improvements

### 11.1 Index Recommendations

#### A. Consider a nonclustered index on `RejectDtTm`

The proc filters by `RejectDtTm < @CutoffDtTm` and orders by `Id`. With the v2 PK-only projection, a nonclustered index on `RejectDtTm` becomes a covering candidate. The optimizer can range-seek by `RejectDtTm`, take the top-N by `Id` (the `Id` order-by would require a sort after the seek, since the seek key is `RejectDtTm` not `Id`), and pass the row locators to the delete. Index DDL:

```sql
Create NonClustered Index IX_OeOrderAcceptReject_RejectDtTm
   On dbo.OeOrderAcceptReject (RejectDtTm)
   With (Fillfactor = 92, Online = On);
```

The PK `Id` is carried as the row locator; no `INCLUDE` is needed for the v2 projection.

Note: if the order-by were changed from `Order By Id` to `Order By RejectDtTm` (which is more semantically natural for "delete the oldest rows first"), this index would become a fully ordered covering seek with no sort. Out of scope for this revision because the order-by is part of the contract with however the proc was originally tuned; the change can be considered alongside the index add.

### 11.2 Proc-Level Changes

#### A. Apply the same projection fix to remaining `Select *` patterns in the proc

The other CTEs in this proc are already in PK-only form per a prior pass. A periodic grep across the proc for any newly introduced `Select Top * From <Table>` patterns inside CTE deletes is the right ongoing hygiene check. The masterclass note [[CTE Delete Projection]] documents the pattern for code review.

#### B. Re-evaluate the `Order By Id` choice

`Order By Id` is roughly aligned with `Order By RejectDtTm` because identity grows monotonically with time on a high-volume insert-only audit table. The alignment is not guaranteed and can drift if the table is ever reseeded or rebuilt. Switching to `Order By RejectDtTm` makes the "delete oldest first" intent explicit and aligns with the nonclustered index recommendation in 11.1. Out of scope for this revision.

### 11.3 Schema-Level Changes

#### A. Re-evaluate the wide `varchar` columns

`Directions varchar(1033)`, `Address1 varchar(1024)`, `Address2 varchar(1024)`, `DurData varchar(1023)` together account for most of the per-row width. If actual content is consistently much shorter than the declared width, the declared widths are over-provisioned. SQL Server does not pad `varchar`, so on-disk storage is not directly affected, but the per-row size estimate the optimizer uses for memory grants is. Out of scope; flagged for the schema review.

#### B. Audit other procs for the same CTE projection pattern

The CTE delete projection pattern is mechanical. A grep for `Select Top \(.*\) \*` inside `stored_procedures/` will find every instance. The sweep this analysis is part of covers all five remaining occurrences. After this sweep, the inventory is empty; a periodic re-scan would catch any new occurrences introduced by future work.
