# CTE Delete Projection

#sql-server #performance #purge #cte #covering-indexes

## What the Pattern Looks Like

A common batched-delete pattern wraps a `Select Top (N)` inside a CTE and then deletes from the CTE. The pattern is good. The variant worth fixing is the version that selects every column when it only needs the primary key.

The broken-but-functional form, taken from a production purge procedure:

```sql
;With Cte As
   (
      Select Top (@NumOfRowsBlockSize) *
      From OeOrderAcceptReject
      Where RejectDtTm < @CutoffDtTm
      Order By Id
   )
Delete From Cte
```

The fixed form is identical in every way except the projection:

```sql
;With Cte As
   (
      Select Top (@NumOfRowsBlockSize) Id
      From OeOrderAcceptReject
      Where RejectDtTm < @CutoffDtTm
      Order By Id
   )
Delete From Cte
```

The two queries delete the same rows. The cost difference is entirely on the read side.

## Why It Hurts

The CTE inside a `Delete From Cte` statement is not a materialized copy of the rows. It is an inline view definition that the optimizer folds into the delete plan. The actual delete operator runs against the base table. To find a row and delete it, the engine needs only the row locator, which is the clustered key. Every other column in the CTE's projection list is information the delete operator will not use.

But the optimizer does not know that the delete operator will not use them. It treats the CTE as a query that must produce every column in its select list. That projection drives four downstream costs.

**Plan-shape constraint.** A `Select *` projection ties the plan to a structure that has every column. In practice that means a clustered index scan or seek, because the clustered index is usually the only structure that covers the full row. A narrow nonclustered index on the filter column (here `RejectDtTm`) cannot be chosen, because it does not carry the other 50-odd columns. With `Select Id` the same nonclustered index becomes a covering index, and the optimizer is free to seek by date and take the top N from a much smaller structure.

**Key lookups.** When a nonclustered index does get picked despite the wide projection, every selected row triggers a key lookup back to the clustered index to fetch the non-key columns. A block of 1,000 rows becomes 1,000 random IO operations on top of the index seek. With a PK-only projection the lookup is unnecessary, because the index already carries the row locator.

**LOB page reads.** When the underlying table has off-row data, `varchar(max)`, `varbinary(max)`, `text`, `image`, or XML, `Select *` forces the engine to touch LOB allocation pages to materialize those columns in the CTE result. The delete operator throws those reads away. `OeScriptImage` is the canonical example in this codebase: two `image` columns and `TEXTIMAGE_ON [PRIMARY]`. Selecting `*` reads the image pages for every row in the block, every iteration of the purge loop. The cost is proportional to image size, not row count.

**Memory grant.** SQL Server estimates the memory grant from the per-row size implied by the projection. A `Select *` against a 60-column table with two `varchar(1024)` fields produces a per-row estimate in the thousands of bytes. With `Order By` involved, the sort operator requests a grant on the order of tens of megabytes per pass. With `Select Id` the per-row estimate is 8 bytes and the grant collapses to near-nothing. Under memory pressure the difference shows up as `RESOURCE_SEMAPHORE` waits or, worse, tempdb spill on a sort that should have been trivial.

The transaction log volume is not affected by the projection, because the delete operator logs the same per-row delete records regardless of how the rows were identified.

## How to Recognize It

Three signals to look for:

- **`Select Top (N) *` inside a CTE that drives a `Delete From <CteName>`.** The pattern is mechanical. A grep across the purge procedures will find every instance.
- **Wide tables or tables with LOB columns appearing as the source of a batched delete.** The wider the table, the bigger the penalty. Tables with `image`, `varchar(max)`, `nvarchar(max)`, `varbinary(max)`, or `xml` are particularly bad targets for `Select *`.
- **Plans that show a clustered index scan with a top operator and an unused output column list.** The optimizer is producing every column because the CTE asked for them. The delete operator is consuming only the row locator.

## The Fix

Replace the projection with the primary key columns and only the primary key columns. The order-by column does not need to be in the projection list unless it is part of the primary key. The where clause does not change. The delete clause does not change.

```sql
-- Broken: full projection, wide grant, possible key lookups, possible LOB reads.
;With Cte As
   (Select Top (@N) * From <Table> Where <Filter> Order By <OrderCol>)
Delete From Cte

-- Fixed: PK-only projection, narrow grant, covering-index access.
;With Cte As
   (Select Top (@N) <PkCols> From <Table> Where <Filter> Order By <OrderCol>)
Delete From Cte
```

For tables with a composite primary key, include every key column in the projection. Skipping a key column is a correctness risk on tables where the optimizer might not have unique-key inference from the source.

For tables whose primary key is not `Id`, the projection must match the actual key. `OeScriptImage` keys on `RxNum`, not `Id`. The convention of `Id` is common but not universal, and the fix has to follow the schema, not the convention. The grep for the broken pattern is mechanical; the projection itself is not.

## Why This Is Not the Same as the General `Select *` Antipattern

Avoiding `Select *` in application code is a long-standing rule. The rationale there is typically about contract stability (the column list is part of the API the query exposes to callers) and projection narrowing (clients almost never need every column). Those rationales do not apply inside a CTE that feeds a delete. The CTE's "caller" is the delete operator, which only consumes the row locator. The reason to avoid `Select *` here is not contract stability. It is plan shape, key lookups, LOB reads, and memory grant. The fix is the same. The reason is different. The reason is worth knowing because it changes which procedures are highest-priority to refactor.

## Related Notes

- [[Index Key Columns vs Included Columns]] for the broader covering-index principle.
- [[TOP with ORDER BY Semantics]] for the batched-delete pattern itself.
- [[FORCESEEK Hints]] for the case where the optimizer needs help choosing the narrow index even after the projection is fixed.
