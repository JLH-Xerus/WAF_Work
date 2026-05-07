# Lessons Learned

A running log of patterns and corrections from refactoring sessions. Each entry is a rule for myself, written so a future session can pick it up at a glance.

---

## Performance Refactoring

### Always check the optimizer's index choice, not just the query shape

The biggest win on lsp_ImgGetListOfTopXImagesToMove came from the optimizer switching from PK_OeOrderHistory to ByOrderStatus when the query was reshaped. Earlier refactor attempts (UNION variants) didn't get this switch because the surrounding plan structure was constraining what the optimizer would consider. When validating a refactor, look at the actual execution plan and confirm which indexes are being used. If the optimizer is still on the same index as the original, the refactor probably isn't going to deliver the win you're hoping for, regardless of how clean the new SQL looks.

### LEFT JOIN + OR doesn't always refactor to UNION

The textbook refactor for LEFT JOIN + OR is splitting into UNION branches, but UNION can make things worse when the dominant cost is a shared base-table scan. Splitting forces two scans where one was happening. For large base tables with sparse qualifying rows, the right pattern is materialize-once into a temp table then probe each path with EXISTS. See `masterclass/LEFT JOIN OR Anti-Pattern.md` Fix #2 for the full pattern.

### LOB-column predicates are cost drivers in their own right

`ImageData IS NOT NULL` was costing roughly 68K LOB page reads per execution because LOB pointers have to be inspected to evaluate the predicate. Whenever a predicate touches a LOB column, ask whether a non-LOB column can express the same intent (a flag, an indicator, a length column). Removing one LOB predicate eliminated 100% of the LOB reads on the Img proc.

### Apples-to-apples comparison is non-negotiable

A v7 result that looked promising on 4/30 turned out to be data-state luck once we re-ran v6, v8, and v8b against the same data on 5/4. Don't draw conclusions from comparisons that span different data states. The protocol: snapshot or freeze the data, run all versions back-to-back, capture STATISTICS IO and STATISTICS TIME for each, and capture actual execution plans. Only then declare a winner.

### Cold-cache results misrepresent steady-state performance

A new query plan that picks a different index will show physical reads on its first execution because the new index pages aren't in the buffer pool yet. Always run twice and report the warm-cache numbers when comparing steady-state behavior. The Img proc refactor showed 4,555 physical reads on cold cache against the new ByOrderStatus index. After the pages were cached, those reads went to zero.

### A 0-row result set is not representative

A test that returns 0 rows can't exercise rowgoal optimization. The TOP operator can't stop early when there's nothing to find, so both the original and refactored plans do worst-case work and you learn nothing about how the refactor performs when it matters. Stage test data so the result set has rows, or pull a window from production where the proc was actually returning data.

### A near-empty driver state hides FOR XML PATH consolidation wins

The lsp_ShpGetOrdersForTopReadyToShipGroup refactor (v24→v25) collapsed three repeated FOR XML PATH expressions into one. When the driver temp tables held effectively zero qualifying rows on the test run, the FOR XML PATH inner work was barely exercised in either version, and v25 actually showed +5.8% logical reads because of the fixed-overhead cost of materializing #MultiVialReadyParents that v24 did not pay. The plan-shape comparison was still meaningful (operator count 140 → 87, UDX 6 → 2), but the cost comparison was inconclusive. When the win mechanism scales with driver row count, validate against a state where the driver is non-trivial.

### Verify the deployed body matches the tested body

The Tolleson 2026-05-07 v25 test had `Option (Maxdop 1)` commented out, while the v25 body committed in the repo had it active. Same version label, different bodies. Always diff the tested SQL against the committed SQL before drawing conclusions, and resolve any discrepancy before deployment. A "fast" test result against a body that isn't what's being deployed is worse than no test at all.

### STATS IO/TIME captures should include result-set row counts

Captures with `Set NoCount On` (which the proc itself sets) hide the `(N rows affected)` lines, which makes identical-result-set verification impossible from the text output alone. For comparison runs, either capture the result grid alongside, or run with `Set NoCount Off` at the session level, so row counts are visible.

### Query hints attached to a SELECT do not translate into a CTE branch

`Option (Maxdop 1)`, `Option (Recompile)`, and the rest of the OPTION family are statement-level hints. They can only appear at the very end of a complete top-level SELECT/INSERT/UPDATE/DELETE/MERGE statement. When v24 of lsp_ShpGetOrdersForTopReadyToShipGroup had `Option (Maxdop 1)` at the end of the Part 1 SELECT INTO, that was syntactically legal because Part 1 was its own statement. v25's CTE consolidation collapsed Parts 1, 2, 3 into branches inside `;With QualifyingGroups As (...)`, and the hint had nowhere syntactically valid to live — the committed v25 had it sitting between `Group By` and `Union All`, which is a parse error. The fix is structural, not syntactic: either keep the hinted query as its own statement and append the others via INSERT, or move the hint to the outer SELECT (which broadens its scope to all branches). Whenever consolidating multiple SELECTs into a CTE or UNION ALL, audit the original SELECTs for OPTION clauses and decide explicitly how each hint should translate.

---

## Workflow

### Plan mode for any non-trivial refactor

Don't start changing SQL until I've articulated the principle, identified the specific lines that violate it, and predicted what the plan should look like after. If the prediction doesn't match the actual plan, that's signal that the principle isn't applying the way I think.

### Verify before declaring done

Reads dropped doesn't mean done. The refactor isn't complete until I've confirmed: (1) the actual execution plan has the shape I expected, (2) warm-cache elapsed time is at or below the original, (3) the result set is identical to the original (same row count, same row identities for a deterministic query), (4) the change is captured in a changelog with before/after numbers.

### Capture lessons immediately after corrections

Every time I get something wrong and the user corrects me, the correction goes in this file before I move on. The cost of writing it down is small. The cost of making the same mistake on the next proc is large.

---

## Documentation

### Write masterclass notes in plain prose

The masterclass folder is for principles, not flair. Avoid em-dashes, avoid clipped phrasing, avoid rhetorical flourishes. Sentences should be sentences. The goal is that someone reading the notes a year from now can understand what the principle is and when to apply it without having to decode style.

### Update the relevant masterclass note when a new pattern emerges

If a refactor uncovers a counterexample or a new variant of an existing pattern, update the note that covers that pattern. The Img proc taught us that LEFT JOIN + OR has two viable refactors, not one. That belongs in the note, with the criteria for choosing between them.
