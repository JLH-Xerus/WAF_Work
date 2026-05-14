# Recommendation.md Template and Citation Library

Working reference for the May 2026 batch generation of Recommendation.md files alongside each refactor's Analysis.md. This file is the source of truth for the template structure, the hard rules, and the verified MS Learn URLs to anchor techniques against. Subagents and future sessions can read this file once instead of re-deriving the conventions.

The approved pilot lives at `refactors/lsp_SrtGetShipToteIfExists/Recommendation.md`. Match that document's tone, length, and structure.

---

## Template (five sections)

```
# Refactor Recommendation: <ProcName>

**Date:** <YYYY-MM-DD from the Analysis.md header, the earliest artifact date>
**Companion analysis:** `Analysis.md` in this folder.
**Deployment state:** <state from Analysis.md header>

---

## Problem

<One short paragraph orienting the reader: what this proc does, why it ends up on the optimization list, and where the cost concentrates.>

<Bullet list of the structural issues, one per bullet. Each bullet leads with the technique label and follows with one or two sentences of detail.>

## Recommendation

<One short paragraph stating the recommended package of fixes and any constraints (no schema change, no index change, single deployment unit, etc.).>

1. <Fix 1, one sentence stating the change.>
2. <Fix 2.>
3. ...

<Optional pointer paragraph: "The full vN body is in `Refactored.sql`. The vN-1 body is in `Original.sql` for diff and rollback.">

## First Principles

**<Principle name>.** From `masterclass/<Note Name>.md`:

> <verbatim block quote from the masterclass note>

<Two to four sentences tying the principle to this procedure. State which lines or operations the principle applies to and what the expected effect is.>

**<Next principle>.** From `masterclass/<Other Note>.md`:

> <verbatim block quote>

<Application paragraph.>

## MS Learn Citations

<One-sentence intro: "The techniques in this refactor anchor to the following Transact-SQL reference pages.">

- <Technique name>. <Sentence pointing to the [MS Learn page title](URL) and a brief note on what the page covers that is relevant.>
- <Technique name>. <Sentence + link.>
- ...

## Risk Note

<One short paragraph covering the semantic risk per change, the first-24-hours signal to watch, and the rollback path. Three to five sentences.>
```

---

## Hard Rules (non-negotiable)

1. **No em-dashes anywhere.** Not `—`, not `–` used as one. Use a comma, a period, a colon, parentheses, or rewrite the sentence. This rule has zero exceptions in the deliverable.
2. **No emojis anywhere.** No decorative pictographs of any kind.
3. **Direct quotations from masterclass notes must be verbatim.** Copy the exact text from the masterclass file. Use blockquote formatting with the `>` prefix.
4. **Reference masterclass notes by relative path** (e.g., `masterclass/Parameter Sniffing.md`), not bare wikilinks. The recommendation must stand alone for a reader who is not following the wikilink graph.
5. **Tone is peer-to-peer DBA / SQL lead.** Active voice. Specific numbers over generalizations. Lead with the conclusion in every section. Plan-shape literacy is assumed.
6. **Length budget: one page where the procedure supports it.** Match the pilot. Refactors with five issues (like the pilot) land at roughly 900 words. Smaller procedures should be shorter; larger ones can run to roughly 1,200 words if the issue count requires it.
7. **MS Learn links are inline.** Use the verified URLs in the library below. Do not invent URLs. If a technique is needed and is not in the library, run a `WebSearch` for the canonical MS Learn page before inserting.
8. **Title format:** `# Refactor Recommendation: <ProcName>`.
9. **Date in the header must match the date in the Analysis.md header for that same procedure.**

---

## MS Learn Citation Library (verified URLs)

Pulled May 2026 via `WebSearch` against `learn.microsoft.com`. All URLs return live pages.

### Query language

- WITH common_table_expression (CTE): https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql
- EXISTS (Transact-SQL): https://learn.microsoft.com/en-us/sql/t-sql/language-elements/exists-transact-sql
- UNION (set operators): https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-union-transact-sql
- TOP (Transact-SQL): https://learn.microsoft.com/en-us/sql/t-sql/queries/top-transact-sql
- ORDER BY clause: https://learn.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql
- FROM clause plus JOIN, APPLY, PIVOT: https://learn.microsoft.com/en-us/sql/t-sql/queries/from-transact-sql
- Predicates (SARGability reference): https://learn.microsoft.com/en-us/sql/t-sql/queries/predicates
- SELECT (Transact-SQL): https://learn.microsoft.com/en-us/sql/t-sql/queries/select-transact-sql
- DELETE (Transact-SQL): https://learn.microsoft.com/en-us/sql/t-sql/statements/delete-transact-sql

### Window aggregation

- OVER clause: https://learn.microsoft.com/en-us/sql/t-sql/queries/select-over-clause-transact-sql
- COUNT (Transact-SQL): https://learn.microsoft.com/en-us/sql/t-sql/functions/count-transact-sql
- Aggregate Functions: https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql

### Hints and plan control

- Table Hints (NOLOCK, READUNCOMMITTED, FORCESEEK, READPAST, ROWLOCK): https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table
- Query Hints (RECOMPILE, OPTIMIZE FOR, MAXDOP): https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query
- Recompile a Stored Procedure: https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/recompile-a-stored-procedure
- Parameter Sensitive Plan Optimization: https://learn.microsoft.com/en-us/sql/relational-databases/performance/parameter-sensitive-plan-optimization
- Transaction Locking and Row Versioning Guide: https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-locking-and-row-versioning-guide

### Indexes

- CREATE INDEX (Transact-SQL): https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql
- Create Filtered Indexes: https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-filtered-indexes
- Create Indexes with Included Columns: https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-indexes-with-included-columns
- Index Architecture and Design Guide: https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-index-design-guide

### Strings

- STRING_AGG: https://learn.microsoft.com/en-us/sql/t-sql/functions/string-agg-transact-sql
- STRING_SPLIT: https://learn.microsoft.com/en-us/sql/t-sql/functions/string-split-transact-sql

### Procedural

- sp_executesql (parameterized dynamic SQL): https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-executesql-transact-sql
- Scalar UDF Inlining: https://learn.microsoft.com/en-us/sql/relational-databases/user-defined-functions/scalar-udf-inlining

---

## Common Anti-Pattern to Citation Mapping

| Anti-pattern in the proc | Masterclass note to quote | MS Learn page to anchor against |
|---|---|---|
| Correlated scalar subquery in SELECT or WHERE | `masterclass/Correlated Subqueries to CTEs.md` | OVER clause, COUNT, WITH CTE |
| Same expensive subquery repeated across UNION branches | `masterclass/FOR XML PATH Consolidation.md` | UNION, STRING_AGG |
| Scalar UDF in SELECT or WHERE | `masterclass/Scalar UDF Parallelism Barrier.md` | Scalar UDF Inlining |
| LEFT JOIN with OR across different join paths | `masterclass/LEFT JOIN OR Anti-Pattern.md` | UNION, FROM clause + APPLY |
| Table variable joined to large table | `masterclass/Table Variables vs Temp Tables.md` | (no canonical TVAR ref; cite Index Design Guide for stats discussion) |
| Non-SARGable predicate (function on column) | `masterclass/Non-SARGable Predicates.md` | Predicates, CREATE INDEX |
| Catch-all WHERE with `OR @Param IS NULL` | `masterclass/Catch-All Query Anti-Pattern.md` and `masterclass/Parameter Sniffing.md` | sp_executesql, Query Hints (RECOMPILE) |
| Parameter sniffing volatility | `masterclass/Parameter Sniffing.md` | Recompile a Stored Procedure, Query Hints, Parameter Sensitive Plan Optimization |
| TRIM or function on column in WHERE | `masterclass/Non-SARGable Predicates.md` | Predicates |
| NOT IN subquery with NULL risk | `masterclass/NOT IN vs NOT EXISTS.md` | EXISTS |
| Scalar `(Select 1 ...) = 1` form | `masterclass/Ambiguous Self-Comparison Predicates.md` (if relevant) or inline note | EXISTS |
| UNION when label columns prevent overlap | `masterclass/UNION ALL Views.md` | UNION |
| Missing NoLock hint in an otherwise NoLock procedure | `masterclass/NOLOCK Strategy.md` | Table Hints |
| `WHILE` loop CSV parsing | `masterclass/STRING_SPLIT vs WHILE Loop CSV Parsing.md` | STRING_SPLIT |
| Multiple SELECTs into separate temp tables that can fold | `masterclass/Conditional Aggregation Consolidation.md` | WITH CTE, Aggregate Functions |
| Filtered-index opportunity for a small hot subset | `masterclass/Index Key Columns vs Included Columns.md` | Create Filtered Indexes, Create Indexes with Included Columns |
| TOP / rowgoal opportunity from a scalar parameter source | `masterclass/TOP with ORDER BY Semantics.md` | TOP, ORDER BY clause |
| FORCESEEK hint pending review | `masterclass/FORCESEEK Hints.md` | Table Hints |
| CTE-driven DELETE projection | `masterclass/CTE Delete Projection.md` | DELETE, WITH CTE |
| Plan instability across sites or call patterns | `masterclass/Query Store Triage.md` (for narrative) and `masterclass/Density Vector.md` (for the why) | Parameter Sensitive Plan Optimization, Query Hints |

If a procedure surfaces a technique not in this table, either pick the nearest equivalent or run a `WebSearch` for the canonical MS Learn page.

---

## Workflow per procedure

1. Read the procedure's `Analysis.md` end to end. The principles cited in Section 5 drive the First Principles section of the recommendation.
2. Read each cited masterclass note for the verbatim quote. Pull the principle statement, not the example block.
3. Pick MS Learn URLs from the library based on the techniques applied.
4. Use the date from the Analysis.md header (`**Date:**` line) for the recommendation's date.
5. Use the deployment state from the Analysis.md header (`**Deployment state:**` line, or inferred from the document body if not stated explicitly).
6. Write `Recommendation.md` in the same folder as `Analysis.md`.
7. Reread the file once after writing. Confirm no em-dashes, no emojis, masterclass quotes are verbatim, MS Learn URLs are from the library, and the title format is correct.
