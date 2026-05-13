# lsp_DbPurgeHistoryData: Refactor Analysis (v1 to v2)

**Date:** 2026-05-12
**Tracking sheet row:** Purge-sweep, CTE delete projection
**Scope:** Single-line projection narrowing inside the OeOrderAcceptReject batched-delete CTE (line 1590). Other CTEs in this proc already use the PK-only form per the prior nightly maintenance refactor and are not touched in this revision.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_DbPurgeHistoryData`

**Purpose in one line:** Purge Rx history data across the OeOrder family of tables for a configurable history window, in cascading dependency order, in blocks of `@NumOfRowsBlockSize` rows.

**Tables touched (scope of this refactor):**

- `OeOrderAcceptReject`. Sixty columns. Four wide `varchar` columns (`Directions varchar(1033)`, `Address1 varchar(1024)`, `Address2 varchar(1024)`, `DurData varchar(1023)`). Primary key clustered on `Id` (bigint identity).

The proc touches many other tables across its 1618 lines. Those statements are out of scope for this revision. The change is local to the OeOrderAcceptReject CTE on line 1590.

**Indexes used (predicted, pending plan capture):**

| Table | Index | Key | Where used |
|---|---|---|---|
| `OeOrderAcceptReject` | `PK_OeOrderAcceptReject` (clustered) | `Id` | the row locator for the delete |
| `OeOrderAcceptReject` | (any nonclustered on `RejectDtTm` if present) | `RejectDtTm` | candidate for the v2 narrow projection; confirm via Section 8 |

This proc's CTE differs from `lsp_DbDeleteOldRxData`'s CTE on the same table in one respect: the where clause is a two-sided range `RejectDtTm >= @HistoryDtTm_EffectiveFrom And RejectDtTm < @HistoryDtTm_EffectiveTo` rather than a single-sided upper bound. The optimizer's access pattern is otherwise the same.

**Callers:** `lsp_DbPerformNightlyMaintenance`.

---

## 2. Overview of Performance

This is the first of two procs in the sweep that target `OeOrderAcceptReject`, the other being `lsp_DbDeleteOldRxData`. Both apply the same fix, and the analysis is parallel.

`OeOrderAcceptReject` is wide because of four `varchar(1000+)` fields. v1's `Select *` projection materializes those fields for every row in the block, on every iteration of the purge loop. The delete operator throws them away.

The two-sided range filter (`RejectDtTm >= X And RejectDtTm < Y`) is a SARGable predicate against any index keyed on `RejectDtTm`. With the v2 PK-only projection, such an index becomes a fully covering range seek with the PK carried as the row locator.

---

## 3. Evidence of Original (v1)

### 3.1 Query Store, cross-MFC view

Pending capture. Query Store filter: `objectname = 'lsp_DbPurgeHistoryData'`. The proc executes many statements per call; the relevant query_id is the one matching the OeOrderAcceptReject CTE on line 1590.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------------|----------------|
|     |               |                    |                  |             |                |                    |                |

### 3.2 SET STATISTICS IO, TIME from a representative MFC

I capture STATS IO/TIME for the OeOrderAcceptReject CTE specifically.

```
(STATS IO + STATS TIME output for v1 goes here, scoped to OeOrderAcceptReject)
```

Plan summary captured into `lsp_DbPurgeHistoryData_original_plan.sqlplan`.

---

## 4. Issue Identification (v1)

The line numbers below reference `Original.sql` (v1).

### 4.1 CTE projection is `Select *` against a wide table (line 1590)

```sql
;With Cte As
   (
      Select Top (@NumOfRowsBlockSize) * From OeOrderAcceptReject Where RejectDtTm >= @HistoryDtTm_EffectiveFrom And RejectDtTm < @HistoryDtTm_EffectiveTo Order By Id
   )
Delete From Cte
```

The CTE asks for every column from `OeOrderAcceptReject` to identify rows for deletion. The delete operator consumes only the row locator (`Id`). Every other column is read and then discarded. Same pattern, same cost dimensions, as the OeOrderAcceptReject CTE in `lsp_DbDeleteOldRxData`.

---

## 5. First Principles

### 5.1 CTE Delete Projection

See [[CTE Delete Projection]] for the full pattern, the four cost dimensions (plan-shape constraint, key lookups, LOB page reads, memory grant), and how this differs from the general `Select *` antipattern.

### 5.2 Index Key Columns vs Included Columns

The fix narrows the CTE projection to the primary key. The primary key is, by definition, present in every nonclustered index as the row locator. A nonclustered index on `RejectDtTm` becomes a covering index for the v2 projection automatically. See [[Index Key Columns vs Included Columns]].

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v2. The diff against v1 is one line: the CTE projection on line 1590.

```sql
;With Cte As
   (
      Select Top (@NumOfRowsBlockSize) Id From OeOrderAcceptReject Where RejectDtTm >= @HistoryDtTm_EffectiveFrom And RejectDtTm < @HistoryDtTm_EffectiveTo Order By Id
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
- **Scope is one CTE.** The refactor touches only the OeOrderAcceptReject CTE on line 1590. The rest of the 1618-line proc is unchanged.
- **Cross-proc parity with `lsp_DbDeleteOldRxData`.** Both procs target the same table with the same pattern. The two refactors should be deployed together so both call sites benefit, and any post-deploy regression on the OeOrderAcceptReject access path surfaces at both procs consistently.
- **No behavior change.** Purely a read-side optimization for one statement.

### Rollback

DDL revert. Restore v1 via `ALTER PROCEDURE`. No compensating action.

### Monitoring Window

For the first 24 to 72 hours after deployment, watch:

- The nightly maintenance log entry for this proc and `lsp_DbDeleteOldRxData`. Wall-clock duration against prior-week runs.
- STATS IO on `OeOrderAcceptReject` during both procs' executions.

---

## 8. Evidence of Refactor (v2)

I capture STATS IO/TIME from the same MFC and same staged data state used for v1.

```
(STATS IO + STATS TIME output for v2 goes here, scoped to OeOrderAcceptReject)
```

Expected plan summary, to be confirmed against `lsp_DbPurgeHistoryData_refactor_plan.sqlplan`:

- The output column list on the index access operator for the OeOrderAcceptReject CTE should show `Id` only.
- The sort estimated row size should drop from the full-row width to the 8-byte `bigint`.
- If a nonclustered index on `RejectDtTm` exists, the optimizer should choose it; the two-sided range predicate is fully SARGable.

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

Same recommendation as the `lsp_DbDeleteOldRxData` analysis on this table. With the v2 PK-only projection, a nonclustered index on `RejectDtTm` becomes a covering candidate. Index DDL:

```sql
Create NonClustered Index IX_OeOrderAcceptReject_RejectDtTm
   On dbo.OeOrderAcceptReject (RejectDtTm)
   With (Fillfactor = 92, Online = On);
```

The PK `Id` is carried as the row locator; no `INCLUDE` is needed for the v2 projection. The two-sided range predicate in this proc is fully SARGable against this index.

### 11.2 Proc-Level Changes

#### A. Apply the same projection fix to remaining `Select *` patterns in the proc

Other CTEs in this proc are already in PK-only form. Periodic grep across `stored_procedures/` for any newly introduced `Select Top * From <Table>` patterns inside CTE deletes is the right ongoing hygiene check. The masterclass note [[CTE Delete Projection]] documents the pattern for code review.

#### B. Cross-proc consolidation with `lsp_DbDeleteOldRxData`

Both procs target the same table with the same pattern and the same intent (purge old OeOrderAcceptReject rows). The two could be consolidated into a single helper or factored so that the OeOrderAcceptReject purge logic exists in one place. Out of scope; flagged for the schema and proc roadmap.

### 11.3 Schema-Level Changes

#### A. Re-evaluate the wide `varchar` columns on `OeOrderAcceptReject`

Same note as the `lsp_DbDeleteOldRxData` analysis. The four `varchar(1000+)` columns drive the per-row size estimate. If actual content is consistently much shorter, the declared widths are over-provisioned. Out of scope.

#### B. Partition `OeOrderAcceptReject` by `RejectDtTm`

`ScalabilityRoadmap_PartitioningAndArchitecture.md` covers the partitioning roadmap. Partition switching would collapse this entire purge into a metadata operation. The CTE projection fix is a stop-gap.
