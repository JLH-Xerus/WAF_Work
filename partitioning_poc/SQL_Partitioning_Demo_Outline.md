# SQL Server Partitioning — The Read-Side Trap

**Demo for the Walgreens team**
**Subject SP:** `lsp_ImgGetListOfTopXImagesToMove` (refactored)
**Subject Table:** `OeOrderHistory` (partitioned on `history_dttm`)

---

## 1. Framing (30 seconds)

Partitioning is a *manageability* feature first — sliding windows, switch-in/out, piecemeal restores, faster index maintenance on hot partitions. Read performance is a *conditional* benefit: you only get it when **partition elimination** kicks in.

Without partition elimination, partitioning makes reads **worse**. That's what we're about to prove against `OeOrderHistory`.

---

## 2. The Setup

- Same data, same indexes on both copies of `OeOrderHistory`.
- One copy is partitioned on `history_dttm`; the other is a heap/clustered single-tree.
- We run the refactored `lsp_ImgGetListOfTopXImagesToMove`, which filters on `OrderId` only — no `history_dttm` predicate.

> Talking point: "All else equal — same indexes, same rows. The only thing changing is whether the clustered index lives in one tree or many."

---

## 3. Run the Proc — Side-by-Side Numbers

| Metric           | Non-Partitioned | Partitioned     | Delta       |
|------------------|-----------------|-----------------|-------------|
| Scan count       | 67,088          | **3,087,864**   | ~46x worse  |
| Logical reads    | 268,898         | **4,199,408**   | ~15x worse  |
| Physical reads   | 31,426          | 0               | (see below) |

**About those physical reads:** the partitioned run shows zero because the pages were already in the buffer pool. That's cache state, not a partitioning win. To get a clean comparison, run:

```sql
DBCC DROPCLEANBUFFERS;
CHECKPOINT;
```

…before each execution. Otherwise, anchor the discussion on **logical reads** — that's the apples-to-apples number.

---

## 4. Why It Blew Up — The Mental Model

Each partition is essentially its own B-tree. With no `history_dttm` predicate, the optimizer can't eliminate any of them, so it has to seek `OrderId` inside **every single partition's** clustered index.

Scan count explodes because each partition contributes its own seek/scan operation. Pull up the actual execution plan and point at:

- The `Clustered Index Scan` (or Seek) operator on `OeOrderHistory`.
- **`Actual Partition Count`** = total partitions on the table.
- The **`Seek Predicate`** showing `PtnId1000` ranging across all partitions — that's the optimizer admitting it has to visit all of them.

> Talking point: "We didn't give the optimizer a partition key, so it can't skip any partitions. We turned one tree into N trees and then walked every one of them."

---

## 5. The Core Lesson

> **Partition elimination requires the partition key — or something the optimizer can derive it from — in the WHERE clause.**

If your access pattern doesn't include the partition column, you've built a slower table than you started with.

---

## 6. Remedies — In Order of Pragmatism

1. **Add the partition key to the predicate.**
   Best fix when callers actually know the date range. Pass `@StartDt` / `@EndDt` into `lsp_ImgGetListOfTopXImagesToMove` and include `AND history_dttm >= @StartDt AND history_dttm < @EndDt`. Watch partition count in the plan drop accordingly.

2. **Non-aligned (global) nonclustered index on `OrderId`.**
   A single B-tree across the whole table — no per-partition fanout. Trade-off: you lose the ability to use `SWITCH` partitions on that index, so weigh this against your archive/retention strategy.

3. **`OrderId` → `history_dttm` lookup/mapping table.**
   Small, hot, cached. Resolve the date from `OrderId` first, then hit `OeOrderHistory` with both predicates. Great when callers genuinely don't know the date up front.

4. **Reconsider the partition column itself.**
   If the dominant access path is by `OrderId`, partitioning on `history_dttm` may be the wrong call. Partitioning is a design decision, not a tuning knob — it should align with how the data is actually queried *and* managed.

---

## 7. Pitfalls Recap (the takeaways slide)

- Partitioning ≠ automatic performance.
- Aligned indexes inherit the partition scheme — and the partition-elimination requirement.
- Always compare **logical reads** with cold caches; physical reads lie.
- Scan count on a partitioned table is amplified by partition count — read it accordingly.
- If the partition key isn't in the predicate, partitioning is a tax, not a feature.

---

## 8. Closing Statement

> "Partitioning didn't fail us here — *we* failed partitioning. The fix isn't to un-partition `OeOrderHistory`; it's to make sure every read path either supplies `history_dttm` or has a global index that doesn't care which partition the row lives in."
