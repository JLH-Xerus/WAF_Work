# lsp_OrdGetRxStatusCounts: Refactor Analysis (v1 to v2)

**Date:** 2026-05-07
**Tracking sheet row:** 18 (Priority P3, status Not Started)

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_OrdGetRxStatusCounts`

**Purpose in one line:** Populate 38 OUTPUT counters with the number of Rxs (and Rx Requests) currently in each operational status, optionally restricted to a recent history window, an active-only flag, and a comma-separated list of address banks.

**Tables touched:**

- `OeOrder` (44 columns; the open-order workhorse, scanned for current rows)
- `OeOrderHistory` (32.98M rows; scanned for the lookback window)
- `OeOrderCurrHistoryDtTm` (32.89M rows; joined to keep only the most recent history row per OrderId when `@CountOnlyMostRecentRxForEachOrderId = 1`)
- `OeRxRequest` (open requests and recently closed requests, two branches)
- `BnkAllBanks` (lookup, used to populate the filter table when `@AddrBankFilter` is empty)
- `@RestrictAddrBank` (table variable; v1) or `#RestrictAddrBank` (temp table; v2)

**Indexes used (predicted, pending plan capture):**

| Table | Index | Key | Where used |
|---|---|---|---|
| `OeOrder` | `PK_OeOrder` (clustered) | `OrderId, HistoryDtTm` | open-order branch driver |
| `OeOrder` | `ByOrderStatusFillTypeAddrBank` | `OrderStatus, FillType, AddrBank` | candidate for the open-order branch if the optimizer picks it |
| `OeOrderHistory` | `ByHistoryDtTm` | `HistoryDtTm` | history-window branch (forced by v1's `Index(ByHistoryDtTm)` hint, preserved in v2) |
| `OeOrderCurrHistoryDtTm` | `PK_OeOrderCurrHistoryDtTm` (clustered) | `OrderId` | LEFT JOIN in v2 / INNER JOIN in v1 history branch |
| `OeRxRequest` | unknown | unknown | both OeRxRequest branches |
| `BnkAllBanks` | unknown | unknown | filter-table population when `@AddrBankFilter` is empty |
| `#RestrictAddrBank` | `IX_RestrictAddrBank` (clustered) | `AddrBank` | EXISTS probes from each branch |

The actual indexes the optimizer chooses for the v1 and v2 plans must be confirmed against `.sqlplan` files captured during the evidence run. The Section 8 placeholder calls this out.

**Index DDL inventory.** Pulled from `IndexExtract.xlsx` for the OeOrder family. OeRxRequest and BnkAllBanks are not in the extract; flagged in Section 11.

| Table | Rows | Index | Type | Key | INCLUDE | Filter |
|---|---|---|---|---|---|---|
| OeOrder | 64,041 | PK_OeOrder | clustered | OrderId, HistoryDtTm | | |
| OeOrder | | ByOrderId (UNIQUE) | nc | OrderId | | |
| OeOrder | | ByGroupNum | nc | GroupNum | | |
| OeOrder | | ByOrderStatus | nc | OrderStatus | | |
| OeOrder | | ByOrderStatusFillTypeAddrBank | nc | OrderStatus, FillType, AddrBank | | |
| OeOrder | | ByDateFilledOrderStatus | nc | DateFilled, OrderStatus | | |
| OeOrderCurrHistoryDtTm | 32,891,776 | PK_OeOrderCurrHistoryDtTm | clustered | OrderId | | |
| OeOrderHistory | 32,978,758 | PK_OeOrderHistory | clustered | OrderId, HistoryDtTm | | |
| OeOrderHistory | | ByGroupNum | nc | GroupNum | | |
| OeOrderHistory | | ByHistoryDtTm | nc | HistoryDtTm | | |
| OeOrderHistory | | ByOrderStatus | nc | OrderStatus | | |
| OeOrderHistory | | ByDateFilled | nc | DateFilled | | |
| OeOrderHistory | | ByHistoryDtTmOrderId | nc | HistoryDtTm, OrderId | | |

Three observations worth flagging.

1. `OeOrder` has no index covering `(AddrBank)` or `(AddrBank, OrderStatus)`. The EXISTS-by-AddrBank filter cannot drive the open-order branch via an index seek, even when v2 fixes the predicate to actually filter. A small narrow index on `AddrBank` (or pulling AddrBank into a covering index) is the natural follow-on schema change once v2 is deployed and the plan is captured.
2. `OeOrderHistory.ByHistoryDtTm` is keyed on `HistoryDtTm` alone. This is what the v1 hint forces the history branch onto. Adding `AddrBank` to the include list would turn the post-seek filter on AddrBank into a covered probe.
3. The filtered-index opportunities that helped Row 15 (`ByOrderStatusFillTypeAddrBank` etc.) do not directly help this proc. The aggregation reads every row that matches AddrBank and the lookback window, then groups in memory. The right wins here are filter-by-AddrBank coverage and history-window coverage, not status-specific filtered indexes.

**Callers:** Order/Track Rx forms and dashboards. The proc is called frequently from operator UIs to populate status-pool counts. Per the Query Store roll-up in Section 3.1, around 845K executions over the 30-day window across 9 of 14 reporting sites.

---

## 2. Overview of Performance

The tracking sheet flagged this proc on April single-site numbers (557M total reads, 1.3K execs, 419K avg reads/exec) and March numbers (1.1B total reads, 2K execs, 556K avg reads/exec). The current cross-MFC view from the 2026-05-07 Query Store capture shows the systemic picture: **roughly 579 billion logical reads per month across 845K executions, present in 9 of 14 reporting sites**. The detailed per-site breakdown is in Section 3.1.

Plan instability is present but modest. Six of the 9 reporting sites returned two plan variants for a single `query_id`, with avg duration deltas of a few percent across variants. The pattern is not parameter-shape volatility, it is the proc's two-block IF/ELSE structure (`@CountOnlyMostRecentRxForEachOrderId` = 0 vs 1) producing two distinct cached statements that share a query_id under the parameterized form. v2 collapses both blocks into a single statement, which should normalize this to one plan per site after warm-up.

The dominant cost driver in v1 is structural and is well described by the tracking sheet. Two 38-CASE aggregation blocks are nearly identical, differing only in whether the OeOrderHistory branch joins to OeOrderCurrHistoryDtTm. The proc takes one or the other based on `@CountOnlyMostRecentRxForEachOrderId`, but the SQL text is duplicated, with all the maintainability and plan-cache cost that implies. The `@RestrictAddrBank` table variable is joined into each branch via an EXISTS subquery, but the optimizer always estimates it at one row, which encourages nested-loop access patterns regardless of how many banks were actually passed in. And underneath all of that, the EXISTS predicate (`Where AddrBank = AddrBank`) is silently broken: both column references resolve to the inner table, so the predicate is `R.AddrBank = R.AddrBank`, always true for non-NULL rows. The intended outer-table reference is dropped by name resolution and the parameter is effectively a no-op.

v2 makes four changes. First, the WHILE-loop CSV parser is replaced with a single `STRING_SPLIT` insert. Second, `@RestrictAddrBank` becomes `#RestrictAddrBank` with a clustered index, so the optimizer sees the real cardinality. Third, the duplicated SELECT block becomes a single CTE with a conditional join inside the OeOrderHistory branch. Fourth, the EXISTS predicate is qualified to compare `R.AddrBank = O.AddrBank` against the outer table, which is what the original parameter-handling logic was always trying to do.

The fourth change is a behavior change. Callers that pass `@AddrBankFilter` in v1 are getting unfiltered counts because the predicate has no effect. After v2, they will get correctly filtered counts, which will be smaller. Section 7 walks the implications. The performance gain from the structural changes is real but secondary to that correctness fix. Validation should confirm both that v2 returns the same counts as v1 when the filter is absent or set to all banks, and that v2 returns strictly fewer counts than v1 when a narrow filter is supplied.

---

## 3. Evidence of Original (v1)

### 3.1 Query Store, cross-MFC view

**Source:** `diagnostics/querystore_outputs/QueryStore_20260507.xlsx`. Top 50 offenders per site, 30-day lookback, captured 2026-05-07. One tab per site.

**Aggregation method:** for each site, all rows whose `objectname` matches `lsp_OrdGetRxStatusCounts` are aggregated. Multiple rows with the same `query_id` indicate plan instability (the same statement under different plan variants). For this proc, the two-row pattern at most sites is the proc's two-statement structure (the IF and ELSE blocks of the duplicated SELECT) appearing as two cached plan variants of one parameterized form.

| MFC | Plan variants | Distinct query_ids | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) | Total CPU (ms) |
|-----|---------------|--------------------|------------------|-------------|----------------|--------------------|----------------|
| Orlando | 2 | 1 | 195,367 | 148,321,537,790 | 759,194 | 539.6 to 566.7 | 206,418,558 |
| Bolingbrook | 2 | 1 | 147,430 | 142,934,149,818 | 969,505 | 643.8 to 654.0 | 194,767,334 |
| NorthLake | 2 | 1 | 110,766 | 95,426,825,749 | 861,517 | 548.4 to 564.2 | 129,883,968 |
| Tolleson | 2 | 1 | 114,021 | 65,725,741,195 | 576,435 | 367.3 to 395.8 | 95,535,535 |
| Liberty | 1 | 1 | 99,695 | 45,782,852,354 | 459,229 | 130.2 | 46,486,167 |
| Memphis | 2 | 1 | 86,015 | 37,588,767,646 | 437,002 | 130.8 to 143.1 | 40,543,547 |
| Canal | 1 | 1 | 45,670 | 24,876,707,856 | 544,706 | 221.9 | 30,010,372 |
| Mechanicsville | 1 | 1 | 33,956 | 17,136,793,126 | 504,676 | 191.6 | 18,035,822 |
| West Jordan | 1 | 1 | 12,757 | 1,269,852,466 | 99,542 | 138.9 | 1,640,049 |
| BrooklynPark | | | | | | | |
| Denver | | | | | | | |
| Indy | | | | | | | |
| Kent | | | | | | | |
| Mansfield | | | | | | | |

**Roll-up across the 9 reporting sites:** 845,677 executions, **579,063,228,000 total logical reads** (684,733 avg reads/exec), 763,321,352 ms total CPU. Five sites (BrooklynPark, Denver, Indy, Kent, Mansfield) did not surface this proc in their top 50, suggesting either lower call volume or a smaller working set at those sites.

**Observations:**

- **Plan instability is mild and structural, not data-driven.** Six sites return two variants of one query_id. The variants differ by 2 to 5 percent on avg duration and avg reads, which is consistent with the two cached SELECT blocks (the duplicated IF and ELSE) being captured as two parameterized variants of the same statement. v2's consolidation should normalize this to one plan per site.
- **Tolleson has a forced plan on one variant.** The second of Tolleson's two rows shows `forcedplan = 750632189`. Worth confirming with the DBA team whether the forced plan was put in place deliberately and whether the consolidation in v2 will require the forced plan to be unforced before deployment. A forced plan that no longer compiles to a matching shape will fail to apply and may regress to the unforced plan, which is the whole point of forcing it in the first place.
- **Per-site avg reads varies by 10x.** Bolingbrook averages 970K reads/exec while West Jordan averages 100K. The proc reads counts per status across all 38 buckets at every site, so the variance is driven primarily by the size of the open-order set and the recent-history-window set at each MFC.
- **Top 4 sites contribute 78 percent of the load.** Orlando, Bolingbrook, NorthLake, and Tolleson together account for 452B of the 579B reads. Deploy v2 to those 4 sites first to capture the bulk of the systemic improvement.
- **The "broken filter" hypothesis is consistent with the cost numbers.** The avg reads/exec scales with table size rather than with what an AddrBank-filtered subset would cost. If the filter were actually applied, narrow-filter calls would read a small fraction of what fleet-wide calls do; instead, the reads per execution are uniformly high. The numbers do not contradict the hypothesis.
- **Indy does not appear in top 50.** Worth confirming whether Indy's call volume is genuinely lower or whether the proc has been replaced by a different path on that MFC.

### 3.2 SET STATISTICS IO, TIME from a representative MFC

I capture STATS IO/TIME from a representative MFC (Tolleson is the canonical pick for this series) running v1 against a representative data state, with two parameter combinations:

- `Exec lsp_OrdGetRxStatusCounts @CountOnlyMostRecentRxForEachOrderId = 1, @RangeIntoHistoryHrs = 24, @AddrBankFilter = Null, @ActiveRxOnly = 0, ...` (the "all banks, current view" path)
- `Exec lsp_OrdGetRxStatusCounts @CountOnlyMostRecentRxForEachOrderId = 1, @RangeIntoHistoryHrs = 24, @AddrBankFilter = '<known narrow list>', @ActiveRxOnly = 0, ...` (the "narrow filter, current view" path; in v1 this should produce the same counts as the all-banks call because of the bug)

```
(STATS IO + STATS TIME output for v1 goes here, ideally one block per parameter combination)
```

The plan summary should be captured against the 2026-05-07 data state into `lsp_OrdGetRxStatusCounts_original_plan.sqlplan`.

---

## 4. Issue Identification (v1)

The line numbers below reference `Original.sql` (v1).

### 4.1 EXISTS predicate is silently broken (lines 171, 179, 185, 191, 237, 242, 248, 254)

This is the highest-priority issue and the only one that is a correctness bug rather than a performance issue. The predicate appears in eight places (four in each of the duplicated SELECT blocks):

```sql
Exists (Select AddrBank From @RestrictAddrBank Where AddrBank = AddrBank)
```

SQL Server's name resolution prefers the innermost scope. Both `AddrBank` references resolve to the column on the inner table, so the predicate is `R.AddrBank = R.AddrBank`, which is true for every non-NULL row in `@RestrictAddrBank`. The intended outer-table reference (`OeOrder.AddrBank`, `OeOrderHistory.AddrBank`, `OeRxRequest.AddrBank`) is dropped silently.

The result is that `@AddrBankFilter` has no observable effect. Callers that pass a narrow list and callers that pass NULL or `''` both get the same counts. The proc's parameter contract has been silently broken since it was written, and the production read counts are larger than they would be if the filter were applied.

The fix is mechanical: alias each table and qualify both sides of the predicate (`Where R.AddrBank = O.AddrBank`). v2 applies this consistently across the four EXISTS sites in the consolidated CTE. See `[[Ambiguous Self-Comparison Predicates]]` for the broader pattern.

This is not a performance fix. It changes the proc's behavior. Section 7 walks the caller-impact implications.

### 4.2 100-percent-duplicated SELECT block (lines 128-192 vs lines 194-255)

The proc has two `SELECT` blocks that compute the 38 OUTPUT counters. They differ in exactly one place: the OeOrderHistory branch. The `@CountOnlyMostRecentRxForEachOrderId = 1` block joins `OeOrderHistory` to `OeOrderCurrHistoryDtTm` so only the current history row per `OrderId` is counted. The `@CountOnlyMostRecentRxForEachOrderId = 0` block omits the join and counts every history row in the window. Everything else is line-for-line identical.

The cost of duplication is real on three axes. First, the plan cache holds two cached forms of an effectively-the-same statement, each with its own compile cost. Second, any future maintenance change to the 38 CASE expressions must be made in two places, with the obvious risk of drift. Third, the optimizer has no opportunity to share work across the two branches; it never sees them in the same query.

v2 collapses both blocks into one CTE with a conditional gate inside the OeOrderHistory branch. The gate is `(@CountOnlyMostRecentRxForEachOrderId = 0 Or OC.OrderId Is Not Null)` against a LEFT JOIN to `OeOrderCurrHistoryDtTm`. When the parameter is 1, the LEFT JOIN match is required; when the parameter is 0, the match is irrelevant and all history rows are kept. The other three branches are unchanged.

### 4.3 `@RestrictAddrBank` is a table variable (line 82)

`Declare @RestrictAddrBank Table (AddrBank varchar(2))` is a table variable. The optimizer always estimates 1 row for table variables, regardless of the actual contents. The estimate persists through plan compilation, so every downstream operator inherits the 1-row cardinality.

For this proc the consequence is mild because the EXISTS subquery is small and the broken predicate (issue 4.1) has been masking the real cost. Once the predicate is fixed, the optimizer's estimate of "1 row in the filter table" will materially affect the plan choice for the open-order branch. The right shape is for the engine to know whether the filter contains 2 banks or 200, so the join order and access method on the OeOrder side can be chosen accordingly.

v2 converts to `#RestrictAddrBank` with a clustered index on `AddrBank`. Real statistics, real cardinality, supports the EXISTS probe as a seek. See `[[Table Variables vs Temp Tables]]`.

### 4.4 WHILE-loop CSV parser (lines 87-107)

The CSV parser is procedural. It walks the input string with `CharIndex` and `SubString`, slicing off one token at a time and inserting each into the table variable. For typical CSVs of 1 to 20 tokens the absolute cost is small, but the pattern is procedural code masquerading as SQL, and it pairs naturally with the table variable (since both predate `STRING_SPLIT`).

The trailing-comma fix-up (`If ... Set @AddrBankFilter = @AddrBankFilter + ','`) exists only to make the loop terminate. It is procedural cruft.

v2 replaces both with a single set-based INSERT from `STRING_SPLIT`, with whitespace trim and empty-token filter for defensive correctness on caller input. See `[[STRING_SPLIT vs WHILE Loop CSV Parsing]]`.

### 4.5 `Index(ByHistoryDtTm)` hint preserved in v2

v1 forces the OeOrderHistory branch onto `ByHistoryDtTm` via `With (NoLock, Index(ByHistoryDtTm))`. The hint is a tell that someone tried to fix a scan in the past. Removing it without evidence would be a net-new risk. v2 preserves the hint. The plan capture in Section 8 should confirm whether `ByHistoryDtTm` remains the optimizer's natural choice. If a different index would be selected absent the hint and produces a better plan, the hint can be removed in a follow-on revision. See `[[FORCESEEK Hints]]` for the broader pattern.

### 4.6 `Index(ByHistoryDtTm)` hint syntax differs between branches (lines 174 vs 240)

A minor point, but the v1 history branches differ in syntax. The `@CountOnlyMostRecentRxForEachOrderId = 1` block uses `OeOrderHistory As O With (NoLock, Index(ByHistoryDtTm))`, with an `As O` alias. The `@CountOnlyMostRecentRxForEachOrderId = 0` block uses `OeOrderHistory With (NoLock, Index(ByHistoryDtTm))` with no alias. v2 standardizes on the aliased form (`OeOrderHistory OH With (NoLock, Index(ByHistoryDtTm))`).

---

## 5. First Principles

### 5.1 Ambiguous Self-Comparison Predicates

When a column name exists in both the inner and outer scope of a subquery, SQL Server's name resolution always prefers the innermost scope. An unqualified reference on both sides of an equality predicate inside a subquery is therefore a self-comparison: `R.AddrBank = R.AddrBank`, which is true for every non-NULL row. The intended outer-scope reference is silently dropped at parse time, with no warning.

This is a correctness bug, not a performance bug, but it surfaces during performance work because the cost numbers do not match the stated semantics of the proc. The fix is to alias every table in scope and qualify every column reference. See [[Ambiguous Self-Comparison Predicates]] for the full pattern, the detection signals, and the systematic ways to find this class of bug.

### 5.2 Table Variables vs Temp Tables

`@RestrictAddrBank` as a table variable produces a 1-row cardinality estimate regardless of actual contents. Inside an EXISTS subquery on a small inner table, the cost of the wrong estimate is small. Once that EXISTS is fixed to actually filter the outer query, the optimizer's plan choice for the outer query depends on knowing whether the filter is narrow or broad. A `#temp` table with a clustered index gives the optimizer real statistics. See [[Table Variables vs Temp Tables]] for the migration pattern and the cases where table variables are still appropriate (small, fixed-size accumulators inside transactional units).

### 5.3 STRING_SPLIT vs WHILE Loop CSV Parsing

The WHILE-loop CSV parser was the only option before SQL Server 2016. `STRING_SPLIT` was added in compatibility level 130 and is the canonical replacement: a single set-based INSERT, no procedural string slicing, no trailing-comma fix-up. The migration is mechanical and pairs naturally with the temp-table conversion in 5.2 (one INSERT, one SELECT, one Create Index). See [[STRING_SPLIT vs WHILE Loop CSV Parsing]] for the full pattern, plus a discussion of where Table-Valued Parameters are the right answer when callers can be migrated.

### 5.4 Code Duplication Inside an IF/ELSE

The duplicated 38-CASE aggregation block is not a parameter-sniffing problem and is not directly a performance problem; it is a maintainability problem that creates plan-cache footprint and per-statement compile cost. Consolidating two near-identical SELECTs into one with a conditional join (or a conditional WHERE predicate) makes the code shorter, makes the plan cache smaller, and gives the optimizer one set of cardinality estimates instead of two. The technique is straightforward when the only difference between the branches is a single join: use a LEFT JOIN with a conditional NULL-check in the WHERE clause. The optimizer will simplify the predicate at compile time when the parameter is a constant in the call, which is the typical case.

This is closely related to [[Parameter Sniffing]] in spirit (both are about helping the optimizer see the right plan for the right parameter shape), but the immediate concern here is the maintainability of two parallel SELECT blocks, not parameter-driven plan choice.

### 5.5 Preserve Hints That Encode Past Tribal Knowledge

`Index(ByHistoryDtTm)` is preserved in v2 because removing it without evidence is a net-new risk. The hint encodes someone's prior decision that the optimizer was making a bad choice on that branch, and v2 is not the place to relitigate that decision. After v2 deploys and a clean plan is captured, a follow-on revision can evaluate whether the hint is still necessary. See [[FORCESEEK Hints]] for the broader principle (which generalizes from FORCESEEK to all index hints).

---

## 6. Refactor (commented)

The full refactored procedure lives in `Refactored.sql` as v2. The narrative below walks the changes block by block. Line references are to `Refactored.sql`.

### 6.1 Filter table: temp table with clustered index, populated by STRING_SPLIT (lines 99-128)

```sql
Drop Table If Exists #RestrictAddrBank

Create Table #RestrictAddrBank (AddrBank varchar(2) Not Null)

If @AddrBankFilter Is Not Null And @AddrBankFilter > ''
Begin
   Insert Into #RestrictAddrBank (AddrBank)
   Select Distinct LTrim(RTrim(value))
   From String_Split(@AddrBankFilter, ',')
   Where LTrim(RTrim(value)) > ''
End
Else
Begin
   Insert Into #RestrictAddrBank (AddrBank)
   Select AddrBank
   From BnkAllBanks With (NoLock)
End

Create Clustered Index IX_RestrictAddrBank On #RestrictAddrBank (AddrBank)
```

Three v1 changes are folded in here. The table variable becomes a `#temp` table. The WHILE loop becomes `STRING_SPLIT` with a trim/empty-token defense. The clustered index supports the EXISTS lookup in the consolidated CTE.

The "no filter means all banks" semantics are preserved exactly. v1 inserted from `BnkAllBanks` when `@AddrBankFilter` was NULL or empty; v2 does the same. The all-banks case is the common case in production based on the cost numbers; getting it right is more important than the narrow-filter case (which was broken in v1 and will start working in v2).

The `Distinct` is defensive against a caller passing a CSV with duplicates. It is not strictly necessary for correctness given that EXISTS short-circuits on the first match, but it keeps the temp table small and the cardinality estimate honest.

### 6.2 Single SELECT block via consolidated CTE (lines 144-202)

```sql
;With StatusCounts As
(
    -- Branch A: Open OeOrder rows.
    Select O.OrderStatus As Status, O.StatusReason
    From OeOrder O With (NoLock)
    Where O.OrderId Not Like '%[(<][1-8][)>]'
      And Exists (Select 1 From #RestrictAddrBank R Where R.AddrBank = O.AddrBank)

    Union All

    -- Branch B: OeOrderHistory rows within the lookback window.
    Select OH.OrderStatus As Status, OH.StatusReason
    From OeOrderHistory OH With (NoLock, Index(ByHistoryDtTm))
    Left Join OeOrderCurrHistoryDtTm OC With (NoLock)
        On OC.OrderId = OH.OrderId And OC.HistoryDtTm = OH.HistoryDtTm
    Where OH.HistoryDtTm > DateAdd(Hour, -@RangeIntoHistoryHrs, GetDate())
      And Exists (Select 1 From #RestrictAddrBank R Where R.AddrBank = OH.AddrBank)
      And @ActiveRxOnly = 0
      And (@CountOnlyMostRecentRxForEachOrderId = 0 Or OC.OrderId Is Not Null)

    Union All

    -- Branch C: Open OeRxRequest rows.
    Select RR.Status, '' As StatusReason
    From OeRxRequest RR With (NoLock)
    Where RR.Status Not In ('Denied', 'Canceled')
      And Exists (Select 1 From #RestrictAddrBank R Where R.AddrBank = RR.AddrBank)

    Union All

    -- Branch D: Recently closed OeRxRequest rows.
    Select RR.Status, '' As StatusReason
    From OeRxRequest RR With (NoLock)
    Where RR.Status In ('Denied', 'Canceled')
      And RR.LastStsChgDtTm > DateAdd(Hour, -@RangeIntoHistoryHrs, GetDate())
      And Exists (Select 1 From #RestrictAddrBank R Where R.AddrBank = RR.AddrBank)
)
Select
    @RxCntAwaitingWebTrx = Sum(Case When Status = 'Awaiting Web Trx' Then 1 Else 0 End),
    ... 37 more aggregations ...
From StatusCounts
```

The four UNION ALL branches map one-to-one to the four UNION ALL branches in v1's duplicated blocks. The OeOrderHistory branch (B) is the only one that changed shape: the inner JOIN to `OeOrderCurrHistoryDtTm` becomes a LEFT JOIN, and the parameter gate is moved into the WHERE predicate as `(@CountOnlyMostRecentRxForEachOrderId = 0 Or OC.OrderId Is Not Null)`.

When `@CountOnlyMostRecentRxForEachOrderId = 1`, the predicate requires the LEFT JOIN to have produced a match, which is semantically equivalent to v1's INNER JOIN. When `= 0`, the predicate is `0 = 0 Or ...` which evaluates true for every row, equivalent to v1's branch with no join at all. The 38 SUM(CASE) aggregations run exactly once over the unified result.

Each EXISTS predicate is now correctly qualified: `R.AddrBank = O.AddrBank`, `R.AddrBank = OH.AddrBank`, `R.AddrBank = RR.AddrBank`. The four sites match the four branches.

The `@ActiveRxOnly` predicate (`And @ActiveRxOnly = 0`) is preserved from v1 as a constant filter on the OeOrderHistory branch. The current behavior is "if @ActiveRxOnly = 1, exclude history rows entirely." v1 used a roundabout `1 = Case When @ActiveRxOnly = 1 Then 0 Else 1 End` formulation; v2 keeps the same effect with simpler syntax.

### 6.3 Cleanup (line 204)

```sql
Drop Table If Exists #RestrictAddrBank
```

Explicit cleanup at the end of the proc. Same rationale as the Row 15 pilot: temp tables are session-scoped, but explicit drops free tempdb pages immediately and avoid carrying state across rapid back-to-back invocations.

---

## 7. Risk & Rollback

### Risks

- **Behavior change due to the EXISTS fix.** This is the dominant risk. v1's EXISTS predicate was always true for non-NULL filter rows, which means `@AddrBankFilter` had no effect in v1. v2 fixes the predicate, so callers that pass a non-empty `@AddrBankFilter` will see strictly fewer rows than they did under v1. Any consumer that was implicitly relying on the v1 behavior (passing a filter and getting unfiltered counts) will now see different numbers. This is the correct behavior, but it is a behavior change. Validation must include at least one test where v1 and v2 are run side by side with a narrow `@AddrBankFilter` and the difference is documented. Stakeholders that consume the OUTPUT counters need to be told that the counts may shift downward when filters are in use.
- **Plan choice on the consolidated CTE.** v1's two SELECT blocks each had their own plan tuned to the parameter value at compile time. v2's single statement will be planned once with parameter sniffing applied. If the first compile happens with `@CountOnlyMostRecentRxForEachOrderId = 1` and a typical caller passes `0`, the optimizer's row estimates for the OeOrderHistory branch may be off. The risk is bounded because the OeOrderHistory branch is only one of four branches, but is worth confirming via the plan capture in Section 8.
- **Forced plan on Tolleson.** Query Store shows a forced plan (`forcedplan = 750632189`) on one Tolleson plan variant. v2's consolidated statement will not match the forced shape. Before v2 deploys to Tolleson, the forced plan must be unforced (`sp_query_store_unforce_plan`) or the deployment must explicitly accept that the forced plan will fail to apply and the proc will recompile. Confirm with the DBA team why the plan was forced before deploying.
- **`#RestrictAddrBank` cardinality on rare edge cases.** If a caller passes an `@AddrBankFilter` with hundreds of banks, the temp table will hold hundreds of rows and the optimizer may choose a hash or merge join into the outer table. The OeOrder family does not have a covering index for AddrBank, so the access method on the outer table will be a scan or a wide index seek with residual filter regardless. The risk is bounded; the absolute worst case is similar to v1's broken-filter behavior.
- **`Index(ByHistoryDtTm)` hint preserved.** No change in v2; risk is unchanged. If the optimizer's natural choice would be a different index (`ByHistoryDtTmOrderId`, for example), the hint will block that choice. Re-evaluating the hint is out of scope for v2 and is listed in Section 11 as a follow-on.

### Rollback

The proc is read-only. It populates 38 OUTPUT counters and produces no result set or data mutation. Rollback is a pure DDL revert: restore the v1 body via `ALTER PROCEDURE`. No data state is touched, so there is no compensating action.

If a regression appears, run the v1 form against the same data state to confirm. Rollback path:

```sql
-- v1 is the current committed body in stored_procedures/lsp_OrdGetRxStatusCounts.sql
-- (no prior refactor exists; v1 is the production version)

-- adjust ALTER -> CREATE if needed for the target environment, deploy
```

### Monitoring Window

For the first 24 to 72 hours after deployment, watch:

- Caller-side correctness. Any consumer that surfaces these OUTPUT counters in a UI should be checked against v1's output for parity on the all-banks case and for the expected drop on the narrow-filter case. The surface area is the Order/Track Rx forms and any dashboard that reads this proc's counters.
- Query Store plan count and avg reads/exec for the proc. Plan count should stabilize at 1 per site (down from 2). Avg reads/exec should drop substantially at sites where narrow filters are commonly passed; the absolute drop is a function of how often filters are used, which the post-deployment numbers will reveal.
- Tempdb allocation. The new `#RestrictAddrBank` is small (typically under 50 rows). At 845K monthly executions roughly one execution every 3 seconds at steady state, the per-call tempdb allocation is trivial, but worth a glance.

---

## 8. Evidence of Refactor (v2)

I capture STATS IO/TIME from the same MFC and same data state used for the v1 capture, running v2 with the same parameter combinations:

- `Exec lsp_OrdGetRxStatusCounts @CountOnlyMostRecentRxForEachOrderId = 1, @RangeIntoHistoryHrs = 24, @AddrBankFilter = Null, @ActiveRxOnly = 0, ...` (all banks, current view)
- `Exec lsp_OrdGetRxStatusCounts @CountOnlyMostRecentRxForEachOrderId = 1, @RangeIntoHistoryHrs = 24, @AddrBankFilter = '<known narrow list>', @ActiveRxOnly = 0, ...` (narrow filter, current view; v2 should return strictly smaller counts than v1 here)
- `Exec lsp_OrdGetRxStatusCounts @CountOnlyMostRecentRxForEachOrderId = 0, @RangeIntoHistoryHrs = 24, @AddrBankFilter = Null, @ActiveRxOnly = 0, ...` (all banks, full history)

```
(STATS IO + STATS TIME output for v2 goes here, ideally one block per parameter combination)
```

The post-refactor plan summary should show:

- One cached plan for the proc (down from two in v1).
- A single SELECT against `OeOrder`, `OeOrderHistory` (with the LEFT JOIN to `OeOrderCurrHistoryDtTm`), and `OeRxRequest` (twice).
- The `Index(ByHistoryDtTm)` hint applied to `OeOrderHistory`.
- The clustered index seek on `#RestrictAddrBank` for the four EXISTS probes.
- A single hash or stream aggregate for the 38 SUM(CASE) expressions, fed by the UNION ALL of the four branches.

Save the plan to `lsp_OrdGetRxStatusCounts_refactor_plan.sqlplan`.

---

## 9. Comparison & Improvement

*Filled in once both v1 and v2 STATS IO/TIME outputs are pasted above.*

| Metric | v1 (duplicated) | v2 (consolidated) | Delta | % Change |
|--------|-----------------|-------------------|-------|----------|
| Logical reads, all-banks call (recent only) | | | | |
| Logical reads, all-banks call (full history) | | | | |
| Logical reads, narrow-filter call | | | | |
| CPU time per call (ms) | | | | |
| Elapsed time per call (ms) | | | | |
| Result counter parity (all-banks case) | | | | row count must match |
| Result counter delta (narrow-filter case) | | | | v2 strictly smaller, expected |

**Plan-shape verification:**

- v1 plan should show two cached plans (one per IF/ELSE branch), each with the duplicated 38-CASE aggregation. Both plans run a UNION ALL of OeOrder, OeOrderHistory (with or without the OeOrderCurrHistoryDtTm join depending on which IF/ELSE branch), and OeRxRequest x2.
- v2 plan should show one cached plan with a CTE feeding a single 38-CASE aggregation. The OeOrderHistory branch should show a LEFT JOIN to OeOrderCurrHistoryDtTm with the predicate gate `(OC.OrderId Is Not Null Or @CountOnlyMostRecentRxForEachOrderId = 0)`.

The expected wins are:

- **Plan cache footprint cut roughly in half.** Two cached plans become one.
- **Compile cost cut roughly in half.** First-call compile pays a single CTE compile instead of two duplicated SELECT compiles.
- **Per-execution CPU and elapsed time roughly even on the all-banks case.** The same row set is being aggregated; the structural change is downstream.
- **Per-execution reads substantially lower on the narrow-filter case,** because v1's broken predicate was reading the unfiltered set and v2 actually filters. The absolute drop depends on how narrow the filter is.

If the all-banks case shows a meaningful read regression, investigate the optimizer's plan choice for the consolidated CTE; the most likely cause would be parameter sniffing on `@CountOnlyMostRecentRxForEachOrderId`.

---

## 10. Validation Checklist

Per `tasks/lessons.md`. Mark each pass or fail before declaring the refactor done.

- [ ] **Same data state.** Both v1 and v2 captures taken against the same data, ideally back to back without intervening writes to OeOrder, OeOrderHistory, OeRxRequest, OeOrderCurrHistoryDtTm.
- [ ] **Warm cache only.** Both captures are the second of two consecutive runs.
- [ ] **Non-zero result set.** All counters returned non-zero values for at least one parameter combination (the all-banks call should always populate at least Awaiting Web Trx, Initiated, Filled, Shipped, etc.).
- [ ] **Identical result set on the all-banks case.** v1 and v2 returned the same 38 OUTPUT counter values when `@AddrBankFilter` is NULL or empty.
- [ ] **Expected behavior change on narrow-filter case.** v2 returned strictly smaller (or equal) counter values than v1 when `@AddrBankFilter` is narrow. The difference matches the expected effect of the fixed predicate.
- [ ] **Plan shape matches prediction.** v2 shows one cached plan, the LEFT JOIN to OeOrderCurrHistoryDtTm with the conditional gate, and the EXISTS seeks against `#RestrictAddrBank`.
- [ ] **No new error or warning messages.** Neither run produced cardinality errors, conversion warnings, or excessive memory grant warnings.
- [ ] **Warm-cache elapsed time at or below v1.** v2 is at least as fast on every parameter combination, with the largest improvement on narrow-filter calls.

---

## 11. Open Items / Future Improvements

These are observations made during the analysis that are not in v2.

### 11.1 Index Recommendations

The follow-on indexes target the post-v2 dominant cost driver, which once the EXISTS predicate is fixed will be the AddrBank-filtered access path on OeOrder and OeOrderHistory.

#### A. Index DDL gap on OeRxRequest and BnkAllBanks

`IndexExtract.xlsx` for the Row 15 pilot covers the OeOrder family but does not include `OeRxRequest` or `BnkAllBanks`. Both tables are touched by this proc. Run `extract_index_ddl.sql` against Tolleson with `@Tables` set to `('OeRxRequest', 'BnkAllBanks')` and append the rows. The right index recommendations for these tables cannot be made until their current shape is known.

#### B. Filtered or covering index on OeOrder for AddrBank lookups

Once v2 is deployed and the EXISTS predicate is filtering correctly, the open-order branch will need a way to access OeOrder by AddrBank that does not scan the table. The current options are:

- `ByOrderStatusFillTypeAddrBank` covers `(OrderStatus, FillType, AddrBank)`. The optimizer can use this if it filters by OrderStatus first and AddrBank as a residual. For this proc, which counts every status, OrderStatus is not a filter, so this index does not help.
- A new nonclustered index `IX_OeOrder_AddrBank` keyed on `(AddrBank)` would let the optimizer seek by bank and then evaluate the OrderId Like predicate as a residual. With 64K rows in OeOrder, the index would be small (under 10 MB).

```sql
Create NonClustered Index IX_OeOrder_AddrBank
   On dbo.OeOrder (AddrBank)
   Include (OrderStatus, StatusReason, OrderId)
   With (Fillfactor = 92, Online = On);
```

The INCLUDE list covers the projected columns from the open-order branch (`OrderStatus`, `StatusReason`) and the OrderId-pattern predicate (`OrderId`). With this index, the open-order branch becomes a covered seek.

**Expected impact:** the open-order branch's reads on OeOrder drop from a clustered scan to a small range seek. Per-call magnitude depends on how narrow the typical `@AddrBankFilter` is.

#### C. Add AddrBank to OeOrderHistory.ByHistoryDtTm INCLUDE

The history branch is forced onto `ByHistoryDtTm` by the v1/v2 hint. The current index includes only the key column, so AddrBank is fetched via a key lookup back to the clustered index. Adding AddrBank to the INCLUDE list eliminates the lookup:

```sql
Drop Index ByHistoryDtTm On dbo.OeOrderHistory;

Create NonClustered Index ByHistoryDtTm
   On dbo.OeOrderHistory (HistoryDtTm)
   Include (AddrBank, OrderStatus, StatusReason, OrderId)
   With (Fillfactor = 92, Online = On);
```

The current index is 1.76 GB. Adding four narrow columns to INCLUDE will grow it modestly (rough estimate, 2 to 2.5 GB). The win is that the history branch becomes a covered range seek with no key lookups.

Alternative: create `IX_OeOrderHistory_HistoryDtTm_AddrBank` as a new index and leave `ByHistoryDtTm` alone. Costs ~2 GB more storage but avoids drop-and-rename, and the v2 hint can be changed to point at the new index in a future revision. The drop-and-recreate path keeps the index name stable, which matters because v2 (and v1) reference the index by name in the hint.

**Expected impact:** the history branch on OeOrderHistory becomes covered. Per-call reads drop on the order of 30 to 60 percent depending on how often the key lookups currently fire.

### 11.2 Proc-Level Changes

These are out of scope for v2 but ready to apply next.

#### A. Re-evaluate the `Index(ByHistoryDtTm)` hint

Once v2 deploys and a clean plan is captured, run the proc with the hint removed against a representative parameter set. If the optimizer chooses a different index (`ByHistoryDtTmOrderId`, `PK_OeOrderHistory`) and the resulting plan is cheaper, remove the hint. If the optimizer regresses to a clustered scan, leave the hint. See [[FORCESEEK Hints]] for the criteria.

#### B. Migrate `@AddrBankFilter` to a Table-Valued Parameter

The CSV contract is fragile (no escaping for commas, no whitespace handling without the v2 trim, no quoting). A TVP would eliminate the parsing entirely. Requires caller migration, which is out of scope for the v2 deployment.

```sql
Create Type AddrBankList As Table (AddrBank varchar(2) Not Null Primary Key);
Go

Alter Procedure lsp_OrdGetRxStatusCounts
   @AddrBankList AddrBankList ReadOnly,
   ...
```

The proc body becomes simpler (no parsing, no fallback to `BnkAllBanks` since the caller always passes a populated TVP). Schedule once the dominant callers have been identified.

#### C. Consider replacing the duplicated v1 (now consolidated v2) with a single output table

The proc populates 38 OUTPUT parameters via 38 SUM(CASE) expressions. An alternative shape is to GROUP BY Status (and StatusReason for the Split-% buckets) and let the caller pivot. This would simplify the proc, reduce the number of parameters, and avoid the all-or-nothing 38-CASE pattern. Requires caller migration and is more invasive than v2; not on the immediate roadmap.

### 11.3 Schema-Level Changes

These require schema changes and are noted for future consideration.

- **Audit other procs for the same `Where Col = Col` bug.** The `Ambiguous Self-Comparison Predicates` pattern is mechanical. A regex sweep over `stored_procedures/` for `Where\s+(\w+)\s*=\s*\1\b` should turn up any other instances. Worth doing before the next refactor cycle. If found, treat each one as a behavior-change deployment with the same caller-impact discussion as Section 7.
- **Sweep for other table variables in hot procs.** This proc was the second instance in the bulk run where a table variable should have been a temp table (Row 16 pilot covered the catch-all anti-pattern but did not need to convert table variables). A targeted sweep across the top 50 offenders for `Declare\s+@\w+\s+Table` should produce a small list of other migrations.
- **Sweep for legacy WHILE-loop CSV parsers.** Same pattern, same fix. Run a regex over `stored_procedures/` for `While\s+CharIndex\(','` and queue the matches as candidate refactors.
