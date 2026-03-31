-----------------------------------------------------------------------
-- Diagnostic: Forced Plan Failure Audit
--
-- Purpose:
--     Surfaces all queries with forced plans that are failing to apply.
--     Forced plan failures are SILENT performance bombs — the optimizer
--     falls back to whatever plan it generates, but every fallback is
--     a recompile with unpredictable results. The overhead of plan forcing
--     is paid with none of the stability benefits.
--
-- Usage:
--     Run against the target database. Requires Query Store enabled.
--     No parameters — this reports all forced plan failures in the store.
--
-- What to look for:
--     - High FailureCount: The plan has been failing consistently.
--       Common reasons:
--         NO_PLAN: The original plan can't be reproduced (schema change,
--                  dropped index, incompatible SET options)
--         GENERAL_FAILURE: Catch-all for compilation failures
--     - Stale forced plans: Plans forced months ago on procs that have
--       since been modified. The force is doing nothing but adding overhead.
--
-- Action:
--     1. Evaluate whether the forced plan is still needed
--     2. If the proc has been refactored, UNFORCE the plan:
--        EXEC sp_query_store_unforce_plan @query_id, @plan_id
--     3. If the proc still needs help, fix the root cause (parameter
--        sniffing, missing index) rather than re-forcing
-----------------------------------------------------------------------

Select
      Coalesce(Object_Name(q.object_id), '<ad-hoc>')   As ObjectName
    , p.query_id                                         As QueryId
    , p.plan_id                                          As PlanId
    , p.force_failure_count                              As FailureCount
    , p.last_force_failure_reason_desc                   As FailReason
    , p.last_compile_start_time                          As LastCompile
    , p.last_execution_time                              As LastExecution
    , rs.AvgReads
    , rs.AvgDurMs
    , rs.Execs
    , Left(qt.query_sql_text, 200)                       As QueryText
From
    sys.query_store_plan p With (NoLock)
    Join sys.query_store_query q With (NoLock)
        On q.query_id = p.query_id
    Join sys.query_store_query_text qt With (NoLock)
        On qt.query_text_id = q.query_text_id
    Outer Apply (
        Select
              Cast(Avg(rs2.avg_logical_io_reads) As BigInt)    As AvgReads
            , Cast(Avg(rs2.avg_duration) / 1000.0 As Decimal(12,2)) As AvgDurMs
            , Sum(rs2.count_executions)                        As Execs
        From sys.query_store_runtime_stats rs2 With (NoLock)
        Where rs2.plan_id = p.plan_id
    ) rs
Where
    p.is_forced_plan = 1
    And p.force_failure_count > 0
Order By
    p.force_failure_count Desc
