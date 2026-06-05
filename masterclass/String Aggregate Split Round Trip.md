# String Aggregate Split Round Trip

The pattern looks like this: STRING_AGG collects values into a delimited string, STRING_SPLIT immediately breaks that string back into rows, DISTINCT removes the duplicate fragments, and a second STRING_AGG glues the survivors back together.

```sql
COALESCE((Select STRING_AGG(Value, ',')
          From (Select Distinct Value
                From String_Split(STRING_AGG(Cast(COALESCE(SerialNum,'') As varchar(max)), ','), ',')
                Where value <> '') As SerialNum), '')
```

This is row deduplication performed in string space. The work the author wanted was "aggregate the distinct values." The work the engine performs is: build a varchar(max) LOB per group, feed each LOB through a table-valued function, sort the fragments for the DISTINCT, then aggregate again. Every group pays LOB construction, a TVF execution, and an extra sort, and the intermediate string can be far larger than the deduped result.

The fix is to dedupe the rows before aggregating, so the aggregation runs once over already-distinct values:

```sql
Select RxNum, LotCode, STRING_AGG(Cast(SerialNum As varchar(max)), ',') As SerialList
From (Select Distinct RxNum, LotCode, SerialNum
      From #Unsorted
      Where SerialNum <> '') DS
Group By RxNum, LotCode
```

Two semantic details to preserve when converting. First, the original drops empty fragments after the split, which is equivalent to filtering NULL and empty values before aggregating; `Where SerialNum <> ''` covers both, since NULL fails the comparison. Second, the original splits on the delimiter, so a value that itself contains the delimiter would be broken apart and deduped as fragments; the dedupe-first version treats the value as atomic. For serial numbers and similar identifiers the atomic treatment is the correct one, but confirm the domain before relying on it.

A second form of the same disease appears one level up: re-running the split/distinct/aggregate over strings that are already unique per group. In lsp_RxFillingHistory_V4 the lot-level strings carried their LotCode as a prefix, so they could not collide within an Rx, and the entire second round trip was a no-op replaced by a single STRING_AGG. Before optimizing the pattern, check whether it is doing anything at all.
