# SQL Server Performance Masterclass

#MOC #sql-server #performance

> A hands-on collection of performance tuning techniques, learned through real-world refactoring of production stored procedures across the MFCs.

---

## The Diagnostic Toolkit

Start here. Before you can fix anything, you need to find the worst offenders and understand why they're slow.

- [[Query Store Triage]] — Rank every query by total I/O impact. Find plan instability. Detect forced plan failures. This is your scoreboard.
- [[Density Vector]] — Measure data skew to decide whether density-based estimation is safe. Includes the skew analysis query and bucket histogram.

---

## Query Plan Quality

These techniques address how the optimizer **chooses** its execution strategy. A wrong plan choice can mean 100x more I/O.

- [[Parameter Sniffing]] — The optimizer sniffs first-caller values and caches plans optimized for those values. When data is skewed, subsequent callers suffer. Fix: local variable assignment.
- [[Table Variables vs Temp Tables]] — Table variables always estimate 1 row. Temp tables get real statistics. For anything beyond trivial row counts, use temp tables.
- [[Non-SARGable Predicates]] — Function-wrapped columns prevent index seeks. The optimizer is forced to scan. Know the patterns; fix the ones you can; document the ones you can't.

---

## Query Structure Refactoring

These are the code-level anti-patterns that produce expensive plans. Each has a specific refactoring pattern.

- [[Correlated Subqueries to CTEs]] — Per-row subquery execution replaced with single-pass CTEs. The single most common "big win" refactoring.
- [[Scalar UDF Parallelism Barrier]] — Scalar functions force serial execution of the entire query. Inline them with STRING_AGG and CTEs to restore parallelism.
- [[LEFT JOIN OR Anti-Pattern]] — LEFT JOINs with OR across different join paths prevent efficient index use. Split into UNION branches with INNER JOINs.

---

## Schema and Hint Awareness

Understanding the database objects and hints you encounter during refactoring.

- [[UNION ALL Views]] — Views over UNION ALL can hide complexity and amplify other anti-patterns. Know when to look through the view to the base tables.
- [[FORCESEEK Hints]] — A tell that someone tried to fix a scan. Investigate the root cause before removing or keeping the hint.
- [[NOLOCK Strategy]] — Consistent dirty-read strategy for read-only display procedures. Apply uniformly; don't mix locked and unlocked reads.

---

## SQL Semantics

Subtle behaviors that can bite you during refactoring if you're not careful.

- [[TOP with ORDER BY Semantics]] — ORDER BY on SELECT INTO is meaningless... unless TOP is present, in which case it determines which rows are selected.

---

## The Refactoring Workflow

A repeatable process for systematic performance improvement:

1. **Triage** — Run the [[Query Store Triage]] diagnostic to rank offenders by total I/O
2. **Profile** — For top offenders, check for [[Parameter Sniffing|plan instability]] and run [[Density Vector|skew analysis]]
3. **Read the code** — Look for the anti-patterns: [[Correlated Subqueries to CTEs|correlated subqueries]], [[Scalar UDF Parallelism Barrier|scalar UDFs]], [[LEFT JOIN OR Anti-Pattern|LEFT JOIN + OR]], [[Table Variables vs Temp Tables|table variables]], [[Non-SARGable Predicates|non-SARGable predicates]]
4. **Refactor** — Apply the appropriate technique. Preserve existing comments and structure.
5. **Test** — `SET STATISTICS IO ON` / `SET STATISTICS TIME ON` side-by-side comparison
6. **Document** — Changelog entry with before/after code, rationale, and what was NOT changed
7. **Verify** — Confirm reads dropped. Check that elapsed < CPU if parallelism was restored.
8. **Re-triage** — Run the Query Store diagnostic again. Celebrate the drop. Pick the next target.

---

## Procedures Refactored

| Procedure | Version | Key Techniques Applied |
|-----------|---------|----------------------|
| lsp_DbDeleteOldInvAuditEvents | v1→v2 | Batch deletion, loop optimization |
| lsp_DbDeleteOldEvents | v2→v3 | Batch deletion, loop optimization |
| lsp_DbDeleteOldPmssData | v1→v2 | Batch deletion, loop optimization |
| lsp_DbDeleteOldWorkflowImages | v3→v4 | Batch deletion, loop optimization |
| lsp_SrtGetShipToteIfExists | v26→v27 | Collapsed double-query, UNION→OR EXISTS, removed dead code |
| lsp_RxfGetListOfManualFillGroups | v48→v49 | [[Correlated Subqueries to CTEs]], [[Scalar UDF Parallelism Barrier]], [[Table Variables vs Temp Tables]], [[Parameter Sniffing]], [[NOLOCK Strategy]] |
| lsp_ImgGetListOfTopXImagesToMove | v6→v7 | [[LEFT JOIN OR Anti-Pattern]], [[UNION ALL Views]], [[TOP with ORDER BY Semantics]] *(in progress)* |

---

## Key Metrics

> **lsp_RxfGetListOfManualFillGroups**: 62,707 → ~20,910 avg logical reads (67% reduction)
> **lsp_ImgGetListOfTopXImagesToMove**: 1,055,104 avg logical reads *(target: 80-90% reduction)*

---

*Built during live refactoring sessions, March 2026.*
