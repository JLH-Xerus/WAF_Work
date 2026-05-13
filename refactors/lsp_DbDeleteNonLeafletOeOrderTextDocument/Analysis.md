# lsp_DbDeleteNonLeafletOeOrderTextDocument: Refactor Analysis (v3 to v4)

**Date:** 2026-05-08
**Tracking sheet row:** 35 (Priority P2, status Not Started)
**Deployment state:** Cataloged. Refactored.sql is a proposed v4 designed by the MFC DBA team and me, awaiting iA review. v4 carries an open semantic question (Section 11.1) that must be resolved with the iA team before deployment.

---

## 1. Procedure Name & Surface Area

**Procedure:** `dbo.lsp_DbDeleteNonLeafletOeOrderTextDocument`

**Purpose in one line:** Delete OeOrderTextDocument rows whose corresponding OeOrderTextDocumentClassification record marks them as Non-Leaflet and whose HistoryDtTm is older than @OlderThanXDays days, in blocks of @NumOfRowsBlockSize rows up to @MaxToDelete total.

**Tables touched:**

- `OeOrderTextDocument` (the target of the DELETE; the row data lives here).
- `OeOrderTextDocumentClassification` (the leaflet/non-leaflet classifier; joined on Id).
- In the v3 WHILE EXISTS check only: `[PWDAZNSYMPH02].pharmassist.dbo.OeOrderTextDocument` and `[PWDAZNSYMPH02].pharmassist.dbo.OeOrderTextDocumentClassification` (linked-server references; see Section 11.1).

**Indexes used (predicted from the v4 body):**

| Table | Index | Where used |
|---|---|---|
| `OeOrderTextDocumentClassification` | NC on `HistoryDtTm, IsLeaflet, Id` (assumed) | candidate-Id scan in the loop |
| `OeOrderTextDocument` | clustered on `Id` (assumed) | DELETE join |

**Callers:** the nightly maintenance procedure. Called once per night per site.

---

## 2. Overview of Performance

The procedure is on the optimization list because the cross-list capture reports it on two of four expense lists (Duration #6, Expense #19) with "algorithmic / plan rewrite candidate" as the dominant heuristic. The Duration #6 ranking is notable; the procedure is one of the slower nightly maintenance items.

The dominant cost drivers in v3 are four.

The first is the WHILE EXISTS check that queries a linked-server pair (`[PWDAZNSYMPH02].pharmassist.dbo.OeOrderTextDocument` joined to the classification table on the same linked server). Every iteration of the loop pays the cost of a remote-server round-trip plus the join, just to answer "is there more work to do." The same information is available from the immediately-preceding DELETE's `@@ROWCOUNT`. The asymmetry between the WHILE check (linked-server) and the DELETE body (local tables) is suspicious and may be a leftover from a migration; Section 11.1 carries this as an open question.

The second is the `Select Top (@NumOfRowsBlockSize) *` in the CTE. The DELETE only needs the row locator; reading every column for every candidate row is wasted I/O. The same `Select Id` narrowing was applied in earlier purge refactors (see the nightly maintenance change journal, v1-to-v2 on `lsp_DbDeleteOldInvAuditEvents`).

The third is the stray `select @NumOfRowsDeleted` statement on line 143. This returns a single-value result set to the caller on every loop iteration. The maintenance orchestrator does not consume this; it is almost certainly a debugging artifact.

The fourth is the IN-subquery shape inside the CTE (`Where Id In (Select Id From OeOrderTextDocumentClassification Where ...)`). The optimizer evaluates the inner subquery per outer row unless it is materialized. Pre-materializing the candidate-Id set into a temp table with a primary key turns the per-row evaluation into a single seek per block.

---

## 3. Evidence of Original (v3)

### 3.1 Query Store, cross-MFC view

| MFC | Plan variants | Executions (30d) | Total reads | Avg reads/exec | Avg dur range (ms) |
|-----|---------------|------------------|-------------|----------------|--------------------|
| (paste from capture) | | | | | |

### 3.2 STATISTICS IO and STATISTICS TIME from a representative MFC run (v3)

```
(paste STATS IO / STATS TIME for v3 against the same data state used for v4 here)
```

The expected v3 signature is N linked-server round-trips for the WHILE check, where N is the number of loop iterations. On a backlog of hundreds of thousands of qualifying rows the iteration count is in the hundreds.

---

## 4. Issue Identification

**Issue 1: Linked-server WHILE EXISTS check (lines 114-123 in v3).** Every iteration of the loop performs a remote-server query. The same information is available from `@@ROWCOUNT` after the DELETE.

**Issue 2: `Select Top (@NumOfRowsBlockSize) *` in the CTE (line 134 in v3).** Wide projection. Only the Id is needed.

**Issue 3: Stray debug `select @NumOfRowsDeleted` (line 143 in v3).** Returns a result set per loop iteration. Likely a leftover from development.

**Issue 4: IN-subquery shape for the candidate-Id filter inside the CTE.** Per-row evaluation. Pre-materialize.

**Issue 5: Asymmetry between the WHILE check (linked-server) and the DELETE body (local tables).** Open question for the iA team; see Section 11.1.

**Issue 6: No local-variable indirection for the three input parameters.** Program convention.

---

## 5. First Principles

**`@@ROWCOUNT`-based loop control replaces WHILE EXISTS.** The principle is the same one that drives the purge optimization recommendations in the Maintenance Operations Plan: a WHILE EXISTS check at the top of a delete loop performs a separate scan to ask whether there is more work, when the previous iteration's `@@ROWCOUNT` already carries that answer.

**Narrow CTE projection.** A DELETE only needs the row locator. Wider projections force the engine to read columns it does not use, which compounds on a multi-iteration loop.

**Pre-materialize candidate Ids into an indexed temp table.** Replaces the IN-subquery's per-row evaluation with a single seek per block. The same pattern is applied across the broader purge refactor recommendations.

**[[Parameter Sniffing]] with the local-variable form.** Low impact on a purge procedure but consistent with the program convention.

---

## 6. Refactor (commented)

The v4 body is reproduced in `Refactored.sql`. Salient blocks below.

**Local variables and a pre-allocated #BlockIds temp table.**

```sql
Declare @LocalOlderThanXDays      Int = @OlderThanXDays
-- (two more locals)

Drop Table If Exists #BlockIds
Create Table #BlockIds (Id BigInt Primary Key)
```

**`@@ROWCOUNT`-driven loop.**

```sql
While @ContinueLoop = 1 And @NumOfRowsDeleted < @LocalMaxToDelete
Begin
    Truncate Table #BlockIds

    Insert Into #BlockIds (Id)
    Select Top (@LocalNumOfRowsBlockSize) dc.Id
    From OeOrderTextDocumentClassification dc With (ReadUncommitted)
    Where dc.HistoryDtTm < @CutoffDtTm
      And dc.IsLeaflet = 'Non-Leaflet'
    Order By dc.Id
    Option (Recompile);

    Set @DeletedThisPass = @@ROWCOUNT

    If @DeletedThisPass = 0
        Set @ContinueLoop = 0
    Else
    Begin
        Delete From OeOrderTextDocument With (RowLock)
        From OeOrderTextDocument otd With (RowLock)
        Inner Join #BlockIds B On B.Id = otd.Id

        Set @DeletedThisPass = @@ROWCOUNT
        Set @NumOfRowsDeleted = @NumOfRowsDeleted + @DeletedThisPass

        If @DeletedThisPass < @LocalNumOfRowsBlockSize
            Set @ContinueLoop = 0
    End
End
```

The loop exits when either the candidate-Id selection returns zero rows or the DELETE removes fewer rows than the block size. The linked-server WHILE check is gone. The stray `select @NumOfRowsDeleted` is gone.

---

## 7. Risk & Rollback

**What could go wrong.** The most significant risk is the linked-server question. v4 assumes the local `OeOrderTextDocumentClassification` table is authoritative; if the v3 logic depended on the linked-server tables having different content than the local tables, the v4 form will produce different rows. Section 11.1 lists this as an open item that must be resolved with the iA team before deployment.

The `@@ROWCOUNT`-based loop control is semantically equivalent to the WHILE EXISTS check as long as the rate of new qualifying rows is small relative to the deletion rate. If qualifying rows are being added to the classification table during the loop (the nightly window is supposed to be a low-write window for this table, but the assumption should be confirmed), the loop may exit one block earlier than the v3 form. This is acceptable behavior because the next nightly run will catch the remaining rows.

The temp-table `#BlockIds` is `Truncate`d at the top of each iteration rather than dropped and recreated. This preserves the plan-cache of the downstream DELETE join across iterations.

**What to look for in the first 24 hours.** Total elapsed time per nightly run (expected: substantially lower because the linked-server check is gone), transaction log volume per run (expected: similar; the deletion count is governed by `@MaxToDelete`), and the absence of debug result sets in the nightly maintenance log.

**Rollback path.** The v3 body is in `Original.sql`. Redeploying v3 reverts cleanly. No schema change.

---

## 8. Evidence of Refactor (v4)

```
(paste STATS IO / STATS TIME for v4 against the same data state used for v3 here)
```

---

## 9. Comparison & Improvement

| Metric | v3 (paste) | v4 (paste) | Delta |
|---|---|---|---|
| Total elapsed time per run | | (expected: substantially lower) | |
| Linked-server round-trips per run | N | 0 | |
| Logical reads on `OeOrderTextDocument` | | (expected: lower due to narrow projection) | |
| Logical reads on `OeOrderTextDocumentClassification` | | | |
| Rows deleted per run | | (expected: identical, given the same data state) | |
| Stray result sets returned | N | 0 | |

**Verdict (to be entered once the captures land):** the headline expected delta is the elimination of N linked-server round-trips per run plus the narrowed CTE projection. The procedure is a nightly maintenance item; the absolute-time win compounds over the nightly window.

---

## 10. Validation Checklist

- Same data state. (?)
- Warm cache only. (?)
- Non-zero result set: deletion count > 0 if any qualifying rows exist. (?)
- Identical rows deleted: same Ids removed from OeOrderTextDocument given the same data state. (?)
- No new error or warning messages. (?)
- Warm-cache elapsed time at or below original. (?)
- No stray result sets returned to the maintenance orchestrator. (?)

Net call: pending resolution of the linked-server question in Section 11.1.

---

## 11. Open Items / Future Improvements

### 11.1 Resolve the linked-server question before deployment

The v3 WHILE EXISTS check queries the linked-server pair `[PWDAZNSYMPH02].pharmassist.dbo.OeOrderTextDocument` and `[PWDAZNSYMPH02].pharmassist.dbo.OeOrderTextDocumentClassification`, while the DELETE body operates on the local `OeOrderTextDocument` and uses local `OeOrderTextDocumentClassification` in the IN-subquery. The asymmetry suggests one of two things:

1. The classification data is synchronized to the local server and the linked-server reference is a leftover from a migration. v4 assumes this is the case and uses local tables throughout.

2. The classification data lives on the secondary node and the linked-server reference is intentional. In that case the WHILE check and the DELETE are operating against different sources, which is a correctness issue that v3 already has and v4 inherits.

The iA team and the iA development team that owns this code path should clarify the intent before v4 deploys. If interpretation 2 is correct, v4 should be revised to either query the linked-server tables consistently (which preserves the v3 cost) or to confirm that the local classification table is the right source (which is the v4 assumption).

### 11.2 Index recommendation on `OeOrderTextDocumentClassification(HistoryDtTm, IsLeaflet, Id)`

The candidate-Id scan filters by `HistoryDtTm < @CutoffDtTm` and `IsLeaflet = 'Non-Leaflet'` and orders by `Id`. A nonclustered index on `(IsLeaflet, HistoryDtTm)` with `Id` included would let the scan seek directly into the Non-Leaflet portion of the table and produce the candidate Ids in order. Filter the index on `IsLeaflet = 'Non-Leaflet'` if the proportion of Non-Leaflet rows is small. Tolleson-first recommendation.

### 11.3 Consider whether the nightly window can absorb a larger `@MaxToDelete`

The default `@MaxToDelete = 300000` is a transaction-log-safety value. If the local server has headroom in the nightly window and the table has accumulated a substantial backlog, a larger `@MaxToDelete` could drain the backlog faster. This is an operational tuning question, not a procedure body change.

### 11.4 Per-MFC post-deployment monitoring

Standard two-week post-deployment monitoring. Particular attention to the deletion count parity with v3 on the first night after deployment.
