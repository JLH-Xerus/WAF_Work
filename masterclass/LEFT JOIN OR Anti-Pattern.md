# LEFT JOIN OR Anti-Pattern

#sql-server #performance #join-strategy #refactoring

## The Pattern

You have a row that can qualify through **two different join paths**. Path A goes through tables X and Y. Path B goes through tables M and N. The developer writes LEFT JOINs to all four tables and uses OR in the WHERE clause to accept either path:

```sql
Select i.Id, i.ContentCode
From Images i
Left Join RxAssoc IR On i.Id = IR.ImgId
Left Join OrderHistory OH On IR.OrderId = OH.OrderId
Left Join CanisterAssoc IC On i.Id = IC.ImgId
Left Join Canister C On IC.CanisterSn = C.CanisterSn
Where (i.Id = IR.ImgId Or i.Id = IC.ImgId)            -- at least one path matched
  And (OH.OrderStatus = 'Shipped' Or C.Status = 'Verified')  -- path-specific filter
```

## Why It's Devastating

This pattern creates a **perfect storm** for the optimizer:

### 1. LEFT JOINs Prevent Early Elimination
With INNER JOINs, the optimizer can eliminate non-matching rows as early as the join step. With LEFT JOINs, every row from the left table survives — the engine must produce the full Cartesian-style result before the WHERE clause filters it down.

### 2. OR Across Join Paths Blocks Index Strategy
The optimizer needs to satisfy `Condition_A OR Condition_B` where A and B involve different tables. It can't seek on table X's index AND table M's index simultaneously for an OR. Typical outcomes:

- **Index Union / Concatenation plan**: The optimizer seeks each path separately and UNIONs the results. This is the "lucky" outcome but often has high startup cost.
- **Full scan + hash match**: The optimizer gives up on seeks entirely and scans the base table, hash-joining everything. This is common with complex ORs.
- **Nested loops with a filter**: Worst case — full scan of the outer table with per-row evaluation of both paths.

### 3. Cardinality Estimation Collapse
The optimizer struggles to estimate how many rows satisfy `A OR B` when A and B have different selectivities across different tables. It often falls back to a fixed guess (30% or the "magic 10%"), leading to wrong join strategies and memory grants.

## The Fix: UNION

Split the query into two branches — one per join path — and UNION them:

```sql
-- Branch 1: Shipped Rx Images
Select i.Id, i.ContentCode
From Images i
Join RxAssoc IR On i.Id = IR.ImgId
Join OrderHistory OH On IR.OrderId = OH.OrderId
Where i.ImageData Is Not Null
  And OH.OrderStatus = 'Shipped'

Union

-- Branch 2: Verified Canister Images
Select i.Id, i.ContentCode
From Images i
Join CanisterAssoc IC On i.Id = IC.ImgId
Join Canister C On IC.CanisterSn = C.CanisterSn
Where i.ImageData Is Not Null
  And C.Status = 'Verified'
```

### Why UNION Works

1. **Each branch uses INNER JOINs**: The optimizer can push predicates down and use index seeks freely.
2. **Independent optimization**: Each branch gets its own sub-plan with accurate cardinality estimates.
3. **No cross-path OR**: The optimizer never has to reason about conditions spanning different join trees.
4. **UNION handles deduplication**: If a row qualifies through both paths, `UNION` (not `UNION ALL`) eliminates the duplicate. Use `UNION ALL` only if you can guarantee no overlap.

### UNION vs UNION ALL

- **UNION**: Deduplicates. Adds a Sort or Hash Match (Aggregate) operator. Use when a row might match both paths.
- **UNION ALL**: No dedup. Cheaper. Use only when paths are mutually exclusive by design.

## Recognizing the Pattern in the Wild

**Red flags in existing code:**

1. Multiple LEFT JOINs where the WHERE clause references columns from those LEFT-joined tables (this converts the LEFT to an effective INNER, but the optimizer doesn't always recognize it cleanly)
2. OR conditions in WHERE that span different table aliases
3. The presence of "at least one must match" logic — `(A.col = X Or B.col = Y)`
4. FORCESEEK hints on the joined tables — a previous developer tried to fix the scans

**In SET STATISTICS IO:**
- The base table shows reads far exceeding what you'd expect for a TOP N query
- Multiple joined tables show high reads even though only one path should be active per row

## Real-World Example

In [[lsp_ImgGetListOfTopXImagesToMove]] (v6→v7), the proc LEFT JOINed four tables (ImgRxImgAssoc, OeOrderHistory, ImgCanImgAssoc, CanCanister) with an OR in the WHERE clause spanning both paths. Combined with a [[UNION ALL Views]] base table (10.5M rows), this produced **1,055,104 average logical reads per execution**. The UNION refactor splits into two focused INNER JOIN branches — one for shipped Rx images, one for verified canister images — allowing the optimizer to seek efficiently on each path independently.

## Bonus: The Semantic Tell

If you see LEFT JOINs whose joined columns are then checked for NULL or compared in the WHERE clause, the developer almost certainly wanted conditional logic ("either path A or path B matches") but expressed it as LEFT JOINs out of caution. The UNION pattern is the correct way to express this intent.

## Related Concepts

- [[UNION ALL Views]] — another UNION-related optimization pattern
- [[Correlated Subqueries to CTEs]] — another "per-row" access pattern that should be set-based
- [[FORCESEEK Hints]] — often applied as a band-aid for this anti-pattern
- [[Non-SARGable Predicates]] — another pattern that blocks index seeks
