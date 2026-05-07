# Stored Procedure Deep-Dive Refactoring — Workflow Plan

**Date:** 2026-05-07
**Source:** MFC database optimization tracking sheet, starting at Row 15
**Author:** Justin Hunter / Claude

---

## Scope

Deep-dive analysis and refactoring of the stored procedures listed in the MFC database optimization tracking sheet, starting at Row 15 and proceeding through the remaining rows that have not yet been refactored. Pilot the workflow on Row 15, review, then run the rest.

---

## Pilot Target — Row 15

**Procedure:** `lsp_ShpGetOrdersForTopReadyToShipGroup`
**Priority:** P3
**Tracking sheet status:** In Progress
**Headline numbers:** 10.0B reads (Mar) / 10.3B reads (Apr) across 259K–274K execs. Appears 4x in both Query Store snapshots under 2 query IDs.
**DDL location:** `stored_procedures/lsp_ShpGetOrdersForTopReadyToShipGroup.sql` — confirmed present.
**Tracking-sheet refactor plan (already drafted):**
- Consolidate the 3 temp tables into a single UNION ALL (distinct `GroupType` labels already exist).
- Run the `FOR XML PATH` correlated subquery once against the combined set.
- Extract the multi-vial split check into a temp table or CTE computed once.
- Replace legacy `Object_Id` temp table checks with `DROP TABLE IF EXISTS`.

---

## Folder Structure

For each procedure being refactored:

```
WAF_Work/refactors/<ProcName>/
  ├── Analysis.md         <- the main deliverable, follows the template below
  ├── Original.sql        <- snapshot of the live DDL at start of analysis
  └── Refactored.sql      <- final commented refactor, deployment-ready
```

Cross-MFC Query Store outputs (one file per site, named by site) drop into:

```
WAF_Work/diagnostics/querystore_outputs/
```

---

## Analysis.md Template

Per Justin's outline plus three additions driven by `tasks/lessons.md`:

1. **Procedure Name & Surface Area** — proc name, what it does in one line, list of tables touched, list of indexes used, callers if known.
2. **Overview of Performance** — a few sentences setting the stage, sourced from the tracking sheet and the Query Store evidence.
3. **Evidence of Original** — Query Store numbers per MFC (executions, avg duration, p95 duration, total reads, plan count) plus the `SET STATISTICS IO, TIME` output from a representative warm-cache run. Justin pastes both.
4. **Issue Identification** — the specific anti-patterns and the lines they live on.
5. **First Principles** — one section per principle, written in plain prose. Reference existing `masterclass/` notes by name. If a new principle surfaces, write a fresh masterclass note instead of inflating the analysis.
6. **Refactor (commented)** — the new SQL with inline comments at every changed block explaining the why.
7. **Risk & Rollback** — what could go wrong, what to look for in the first 24 hours, how to revert. (*Addition*)
8. **Evidence of Refactor** — `SET STATISTICS IO, TIME` from the warm-cache run on the same data state. Justin pastes.
9. **Comparison & Improvement** — apples-to-apples table: original vs refactor, reads / duration / CPU / plan shape. Pass/fail against the validation checklist.
10. **Validation Checklist** — explicit pass/fail on: warm-cache time at or below original, identical result set (row count and row identities for deterministic queries), execution plan has the expected shape (the optimizer is using the index we expected), test data returned a non-zero result set. (*Addition*)

---

## Per-Proc Workflow

For each procedure, in order:

1. **Set up the folder.** Create `refactors/<ProcName>/` and copy the live DDL into `Original.sql`.
2. **Pull the tracking-sheet context.** Restate the issue and the prior refactor plan in `Analysis.md`.
3. **Pull cross-MFC evidence.** Read the latest per-site Query Store outputs from `diagnostics/querystore_outputs/`, filter to this proc, build the per-MFC table for the Evidence section.
4. **Read the DDL closely.** Map the surface area, identify anti-patterns line-by-line, document each in Issue Identification.
5. **Articulate the principles.** For each anti-pattern, write the First Principles section. Cite masterclass notes by name. If a new principle emerges, draft a masterclass note in `masterclass/` (plain prose, sentences, no flair).
6. **Write the refactor.** Save to `Refactored.sql` with full inline comments. Mirror the same commented structure into the Analysis.md "Refactor" section.
7. **Justin runs the original and refactor on a representative MFC.** Pastes STATS IO + STATS TIME for both into `Analysis.md`.
8. **Compare apples-to-apples.** Same data state, two executions each (discard cold), warm-cache numbers go in the comparison table. Run the validation checklist. If anything fails, revisit the refactor — do not declare done.
9. **Mark the tracking sheet row.** Update status, link to the Analysis.md.

---

## Apples-to-Apples Protocol (from `tasks/lessons.md`)

These rules are non-negotiable for every comparison:

- Same data state for original and refactor. No comparing across days unless data has not changed.
- Warm cache only. Run twice, discard the cold-cache numbers.
- Result set must have rows. A 0-row result does not exercise rowgoal optimization and proves nothing.
- Plans must have the shape we predicted. If the optimizer is on the same index as the original, the refactor probably did not deliver the win — revisit.
- Identical row count and row identities for deterministic queries. Different output means the refactor changed semantics.

---

## Pilot Then Expand

1. Run the workflow end-to-end on Row 15 (`lsp_ShpGetOrdersForTopReadyToShipGroup`).
2. Review with Justin. Adjust the template, folder structure, or workflow if anything feels off.
3. Codify whatever the pilot taught us into `refactors/00_Workflow.md` so the bulk run is mechanical.
4. Run the remaining rows in tracking-sheet order:

   - Row 16 — `lsp_SrtGetSortationItems` (P2)
   - Row 17 — `lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs` (P1)
   - Row 18 — `lsp_OrdGetRxStatusCounts` (P3)
   - Row 19 — `lsp_RxvGetRxsQueuedForVerification` (P3)
   - Row 20 — `lsp_PmssIwebGetTopQueuedTrx` (P3)
   - Row 21 — `lsp_RbtDetermineNextVialToProcess` (P3)
   - Rows 22–39 — cross-list expense candidates (P0/P1/P2), added 2026-04-27.

---

## Open Items Before Kickoff

- [ ] Justin drops fresh per-site Query Store top-50 outputs into `diagnostics/querystore_outputs/` (one file per MFC, named by site).
- [ ] Justin confirms the plan above. Once confirmed, I create `refactors/lsp_ShpGetOrdersForTopReadyToShipGroup/` and start drafting `Analysis.md` against the existing DDL while waiting on Justin's STATS IO/TIME paste.

---

## Review Section

### Pilot Run — Row 15 (`lsp_ShpGetOrdersForTopReadyToShipGroup`)

**Date:** 2026-05-07

**What got built:**

- `refactors/lsp_ShpGetOrdersForTopReadyToShipGroup/Original.sql` — v24 pulled from git revision `b4bf42c` (the `CREATE PROCEDURE` form before any refactor work).
- `refactors/lsp_ShpGetOrdersForTopReadyToShipGroup/Refactored.sql` — v25 as currently committed in the repo (the `ALTER PROCEDURE` form, committed in `1289c8f` on 2026-04-13).
- `refactors/lsp_ShpGetOrdersForTopReadyToShipGroup/Analysis.md` — full deep-dive with all 10 template sections plus an "Open Items / Future Improvements" section. Evidence sections (3.1, 3.2, 8) carry placeholders for Justin to paste Query Store and STATS IO/TIME captures.
- `masterclass/FOR XML PATH Consolidation.md` — net-new principle note. The pattern (same FOR XML PATH expression repeated across UNION branches → consolidate driver set first, run FOR XML PATH once) was not covered by any existing masterclass note.

**Surprise found mid-pilot:** v25 was already applied to the file in commit `1289c8f`. The deep-dive shifted from "design and write the refactor" to "document the v24→v25 diff with first principles and set up the validation evidence harness." Since the goal of the deep-dive deliverable is the comprehensive analysis doc, not the SQL itself, this didn't change the value of the work — but it did mean a workflow note for the bulk run: always check git history before assuming the file is the unrefactored baseline.

**Workflow lessons to codify before bulk run:**

1. First step on every proc should be `git log --oneline --follow stored_procedures/<ProcName>.sql` to determine whether a refactor is already in the file. If yes, pull the original from the relevant prior commit; if no, the current file is `Original.sql` and we still have to write the refactor.
2. The Analysis.md template held up well. The three additions (Surface Area up front, Risk & Rollback, Validation Checklist) all earned their place. The "Open Items / Future Improvements" section was unplanned but valuable — it captures observations made during the deep dive that don't fit into the v25 refactor (non-SARGable predicates, unverified `Maxdop 1` hint, etc.) so they don't get lost. Add to the template.
3. Wikilinks in Analysis.md should use bare `[[Note Name]]` style, matching the convention in the existing masterclass notes. Do not include the `masterclass/` folder prefix.

**Next steps awaiting Justin:**

- Sign-off on the pilot deliverable (Analysis.md content, structure, level of detail).
- Per-MFC Query Store top-50 outputs dropped into `diagnostics/querystore_outputs/`.
- v24 and v25 STATS IO/TIME captures pasted into Analysis.md sections 3.2 and 8.

Once those are in hand, fill in section 9 (Comparison & Improvement) and run section 10 (Validation Checklist). Then mark the tracking sheet row Completed and proceed to Row 16 (`lsp_SrtGetSortationItems`).
