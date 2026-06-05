# Dead Join Elimination

A dead join is a join whose table contributes no columns to the output and no predicate to the result. They accumulate in procs that were built by copying a wide query and trimming the select list without trimming the FROM clause. Each one costs reads, memory grant, and optimizer search space on every execution.

The first step is mechanical: for each table in the FROM clause, check whether any output column, WHERE predicate, GROUP BY key, or ORDER BY key references it. If nothing does, the join exists only for its row-matching side effect, and the question becomes what that side effect is.

An outer join on a unique key has no side effect at all. It cannot remove rows because it is outer, and it cannot multiply rows because the key is unique. Remove it. SQL Server can sometimes eliminate these itself, but only when a trusted foreign key or unique constraint proves safety, and NOLOCK-heavy legacy schemas often lack the trusted constraints, so the optimizer pays the join anyway.

An inner join with no referenced columns is an existence filter in disguise: it removes rows that have no match, and it multiplies rows when the join key is duplicated on the other side. Rewrite it as EXISTS. EXISTS preserves the filtering, cannot multiply rows, and lets the optimizer use a semi join, which stops probing after the first match. If a trusted foreign key guarantees every row has a match, the EXISTS can be removed entirely, but that is a second decision with its own evidence requirement.

The lsp_RxFillingHistory_V4 refactor is the reference case. Four detail branches each joined SecUser twice, Pharmacy, and InvPool (outer, no columns: removed) plus OePatientCust and OeGroup (inner, no columns: converted to EXISTS). Six tables left the FROM clause of every branch with no change to the output.

When removing a dead join, state in the analysis which case it was (no side effect, or existence filter) and what guarantees the removal relies on. The identical-result-set validation is the safety net for both cases.
