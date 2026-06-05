# lsp_RxFillingHistory_V4: Refactor Analysis (v1 to v2)

**Date:** 2026-06-05.
**Tracking sheet row:** Composite rank 49 of 97, score 38.63, P2, FoldIn_Candidates (untracked until 2026-05-13 QStore delta).
**Deployment state:** Draft, not deployed. `Refactored.sql` is the v2 candidate; no captures yet.

**Note on sources.** This proc is not in `stored_procedures/`. `Original.sql` is the body from the Orlando schemacompare dump (`MFCs/schemacompare/Orlando.sql`, script date 2026-05-12). The BolingBrook dump holds a different, older body that stops after the `#Unsorted` build (no multi-vial reconciliation, no aggregation, no final SELECT). Fleet drift is real and is tracked in §11.5.

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_RxFillingHistory_V4`

**Purpose:** Returns one row per Rx filled since the `CustomTask.DSCSAReport` watermark, with aggregated lot, expiration, serial number, GTIN, and dispenser serial detail for DSCSA reporting.

**Tables touched:**

- `SysProperty`: watermark lookup (`CustomTask.DSCSAReport`).
- `OeOrder`: active orders; window scan plus multi-vial probes.
- `OeOrderHistory`: historical orders; same three access patterns.
- `OeOrderTcdAssoc`: dispenser serial (`TcdSn`) per order.
- `OeLotcode`: lot code and expiration per order; also the final DisacardDate lookup.
- `OeSerialNumGTIN`: bridge to `InvLpnNdc`.
- `InvLpnNdc`: package serial number and GTIN.
- `OePatientCust`, `OeGroup`: existence filters only (no columns selected).
- `SecUser` (x2), `Pharmacy`, `InvPool`: joined in v1, no columns selected; removed in v2.

**Indexes used:** Unknown. No IndexExtract has been generated for this proc; predicted bindings are `OeOrder`/`OeOrderHistory` on `DateFilled` (likely absent, forcing scans), `OeLotcode`/`OeOrderTcdAssoc`/`OeSerialNumGTIN` on `(OrderId, HistoryDtTm)`. Generate `IndexExtract.xlsx` before the capture run.

**Callers:** The DSCSA custom task (watermark keyword and hourly `@EndDate` rounding imply a scheduled hourly run). Confirm.

## 2. Overview of Performance

The fleet delta of 2026-05-13 attributes 64.23 CPU hours across 2,879 executions to this proc, roughly 80 CPU-seconds per execution, with 53 distinct plans and worst server `pwdorsymp-LST01` (Orlando). The sheet's verdict reads "Severe plan proliferation: 53 plans." The proc takes no parameters, so the proliferation is not classic parameter sniffing; the likely drivers are temp-table recompiles and the steadily widening date window (§11.2).

The predicted dominant cost is the multi-vial reconciliation: both order tables are joined to `#MultiVialOrders` with `O.OrderId Like M.OrderId + '%-[0-9][0-9]'`, a non-sargable prefix match that nested-loop scans the order table once per multi-vial order. Three UNION distinct sorts over the full detail set and a chain of aggregate/split/distinct/re-aggregate STRING_AGG expressions stack on top.

The window itself is unbounded: `@StartDate` comes from a SysProperty watermark and `@EndDate` is the current hour. If the watermark is not advanced by the caller, every execution scans a growing range. This is not fixed by the v2 body and is the first thing to verify on Orlando (§11.2).

## 3. Evidence of Original

### 3.1 Cross-MFC Query Store

Fleet-level delta only; per-site Query Store breakdown not yet pulled for this proc.

| Source | Executions | Total CPU hours | Plan count | Variance ratio | Worst server |
|---|---|---|---|---|---|
| QStore_Delta_20260513 (fleet) | 2,879 | 64.23 | 53 | 1 | pwdorsymp-LST01 |

### 3.2 STATISTICS IO and STATISTICS TIME

```
(holding for v1 capture at Orlando)
```

Findings: pending capture.

## 4. Issue Identification

**Issue 1: Non-sargable LIKE prefix joins (lines 215, 226, 259, 308).** `O.OrderId Like M.OrderId + '%-[0-9][0-9]'` cannot seek and forces a nested-loop scan of `OeOrder`/`OeOrderHistory` per multi-vial order, four times. The embedded `%` also lets base id `ABC` cross-match `ABC123-01` (§11.4).

**Issue 2: UNION where UNION ALL suffices (lines 76, 134, 219, 288).** Every UNION runs a distinct sort over the full intermediate set, and every consumer downstream dedupes again via MAX, DISTINCT, or the split/distinct pattern. The dedupe is paid twice on the four largest intermediate results.

**Issue 3: Dead joins (lines 107-120, 153-171, 264-278, 314-332).** `SecUser` (x2), `Pharmacy`, and `InvPool` are left-joined and `OePatientCust` and `OeGroup` are inner-joined in all four detail branches, yet none contributes an output column. Four tables are pure overhead; two act only as existence filters.

**Issue 4: Aggregate/split/distinct/re-aggregate round trips (lines 350, 362, 390, 392).** Serial and dispenser values are STRING_AGGed into a CSV, split back apart with STRING_SPLIT, deduped, and re-aggregated, twice over for the lot strings. Deduping rows before aggregating produces the same string in one pass.

**Issue 5: Redundant #Grouped re-aggregation (lines 382-405).** `#OtherColumns` and `#GTinDisp` are already one row per RxNum, so the GROUP BY and the second split/distinct round trip are no-ops over 1:1 joins.

**Issue 6: Unreachable predicate (line 185).** `Not (OrderStatus = 'Canceled' And ...)` sits behind `OrderStatus <> 'Canceled'` and can never change the result. Removed.

**Issue 7: Misleading LEFT JOIN on OeLotcode (lines 122, 128).** The WHERE predicate `OLC.LotCode != 'DISCARD_DATE'` already discards outer rows, making the join inner. v2 writes it as an inner join.

**Issue 8: Correlated TOP 1 lookup per output row (line 415).** The DisacardDate subquery probes `OeLotCode` once per output row. Kept in v2 (the output set is the smallest set in the proc); follow-up in §11.4.

## 5. First Principles

**[[Non-SARGable Predicates]].** A predicate that wraps the column in an expression or leads with a wildcard cannot seek. The multi-vial LIKE joins are the worst form: the pattern itself is built per outer row, so the inner side is scanned per row. v2 derives the base id once and joins on equality, turning four per-row scan loops into single scans with hash joins.

**[[UNION ALL Views]].** UNION buys a distinct sort; only pay for it when duplicates must be removed at that point. Every UNION in this proc feeds a consumer that dedupes again, so v2 converts all four to UNION ALL.

**[[Dead Join Elimination]].** A join that contributes no columns is either pure overhead (outer join on a unique key: remove) or an existence filter in disguise (inner join: rewrite as EXISTS, which cannot multiply rows). v2 removes four tables and converts two to EXISTS. New note written for this refactor.

**[[String Aggregate Split Round Trip]].** Aggregating values into a delimited string and then splitting that string to dedupe is row dedupe done in string space, at LOB cost. Dedupe the rows first with DISTINCT, then aggregate once. New note written for this refactor.

**[[NOLOCK Strategy]].** v1 uses NOLOCK on every base-table read and the proc is a report; v2 keeps that strategy unchanged. The CPU reduction itself is the blocking relief: less time on CPU and fewer scan pages touched per execution.

## 6. Refactor (commented)

The v2 body lives in `Refactored.sql`. Key blocks:

```sql
-- Multi-vial probe: one scan per order table, equality join on the derived base id
Select Distinct V.BaseOrderId As OrderId
Into #YetToBeFilledMultiVials
From (
   Select Left(OrderId, Len(OrderId) - 3) As BaseOrderId
   From OeOrder With (NoLock)
   Where OrderId Like '%-[0-9][0-9]'
     And DateFilled Is Null
     And OrderStatus <> 'Canceled'
   Union All
   Select Left(OrderId, Len(OrderId) - 3)
   From OeOrderHistory With (NoLock)
   Where OrderId Like '%-[0-9][0-9]'
     And DateFilled Is Null
     And OrderStatus <> 'Canceled'
) V
Join #MultiVialOrders M On M.OrderId = V.BaseOrderId
```

```sql
-- Dedupe-first aggregation replaces the aggregate/split/distinct/re-aggregate round trip
Left Join (
   Select RxNum, LotCode, STRING_AGG(Cast(SerialNum As varchar(max)), ',') As SerialList
   From (Select Distinct RxNum, LotCode, SerialNum From #Unsorted Where SerialNum <> '') DS
   Group By RxNum, LotCode
) SL On SL.RxNum = LG.RxNum And SL.LotCode = LG.LotCode
```

```sql
-- Existence filters instead of column-less inner joins
And Exists (Select 1 From OePatientCust PC With (NoLock) Where PC.PatCustId = O.PatCustId)
And Exists (Select 1 From OeGroup G With (NoLock) Where G.GroupNum = O.GroupNum)
```

The output contract is unchanged: same columns, same names (including the `DisacardDate` spelling), same window filter and ordering, and the empty-string/NULL semantics of the v1 string expressions are preserved with `IsNull`/`NullIf`. Follow-ups that intentionally did not make this version are in §11.

## 7. Risk & Rollback

**Concerns:**

- UNION ALL conversion relies on downstream dedupe. Any duplicate rows that v1's UNION removed must be absorbed by the DISTINCT-first aggregations; the identical-result-set check in §10 is the gate.
- EXISTS conversion assumes `PatCustId` and `GroupNum` are unique in their parent tables. If a parent key were duplicated, v1 would have multiplied detail rows (later absorbed by dedupe) while v2 will not; output should be identical either way, but verify row identities.
- Multi-vial equality join tightens the v1 prefix match. Any base id that only matched through the embedded `%` wildcard (base `ABC` matching `ABC123-01`) changes behavior; this is judged a latent bug fix (§11.4) but must be confirmed against a multi-vial-heavy window.

**First 24 hours:** Compare report row counts against the prior day's runs and watch the proc's CPU per execution in Query Store on Orlando.

**Rollback:** Re-apply `Original.sql` (ALTER with the v1 body); no schema objects change.

## 8. Evidence of Refactor

```
(holding for v2 capture at Orlando under the same data state as §3.2)
```

Findings: pending capture.

## 9. Comparison & Improvement

Comparison will be v1 vs v2 at Orlando, warm cache, back to back, same data state.

| Metric | v1 | v2 | Delta |
|---|---|---|---|
| CPU ms | | | |
| Elapsed ms | | | |
| Logical reads (total) | | | |
| Logical reads (OeOrder) | | | |
| Logical reads (OeOrderHistory) | | | |
| Worktable reads | | | |
| Result rows | | | |

Findings: pending capture.

## 10. Validation Checklist

Status: pre-capture. All items pending the Orlando run.

- `[?]` Same data state. Captures not yet taken.
- `[?]` Warm cache only.
- `[?]` Non-zero result set. Stage a window with multi-vial orders present; the multi-vial path is the highest-risk change.
- `[?]` Identical result set. Row count plus row identities ordered by DateFilled, OrderId.
- `[?]` Plan shape matches prediction. Expect the four nested-loop LIKE scan loops replaced by single scans with hash joins, and the four distinct sorts gone.
- `[?]` No new error or warning messages.
- `[?]` Warm-cache elapsed time at or below original.

Net call: not deployable until all items pass on a representative window.

## 11. Open Items / Future Improvements

### 11.1 Add supporting indexes (out of scope this round by request)

Without an index on `DateFilled`, both window scans remain full scans regardless of the v2 body.

```sql
Create NonClustered Index IX_OeOrder_DateFilled On OeOrder (DateFilled) Include (RxNum);
Create NonClustered Index IX_OeOrderHistory_DateFilled On OeOrderHistory (DateFilled) Include (RxNum);
```

A second pair keyed on `(OrderId, HistoryDtTm)` covering `LotCode, ExpireDate` on `OeLotcode` would convert the detail joins and the DisacardDate lookup to seeks. Generate the IndexExtract first; some of these may already exist.

### 11.2 Verify the watermark is advancing

If `CustomTask.DSCSAReport` is never updated, every execution scans a window growing since 2024-11-25 and the per-execution cost climbs without any code change. Check the SysProperty value on Orlando and confirm the task updates it after a successful run. Also note: if the row is missing, `@StartDate` is NULL and the proc silently returns zero rows; a guard that raises an error would make that failure visible.

### 11.3 Re-measure plan proliferation after v2

53 plans for a parameterless proc points at temp-table recompile churn, not sniffing, despite the sheet's "Sniffing_Signal YES." If the count stays high after v2, capture `sys.dm_exec_query_stats` creation reasons and consider `KEEPFIXED PLAN` on the aggregation statements.

### 11.4 Latent bugs preserved or fixed, for the record

Three v1 behaviors worth a deliberate decision: the prefix cross-match (fixed in v2, see §7), the `DisacardDate` misspelling (preserved; downstream consumers may bind to it), and the DisacardDate lookup joining on the truncated base OrderId for multi-vial rows, which cannot match `OeLotCode` and returns NULL for every multi-vial Rx (preserved). If the report owner wants discard dates on multi-vial rows, the lookup needs the un-truncated vial OrderIds.

### 11.5 Fleet drift

The BolingBrook body is an older revision that returns the raw `#Unsorted` union with no multi-vial reconciliation and no aggregation, and it leaks its temp tables. Once v2 validates, deploy fleet-wide and add this proc to `stored_procedures/` so the dumps stop being the only source of truth.
