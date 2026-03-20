-----------------------------------------------------------------------
-- Diagnostic: Query Store — Top Offenders by Total Logical Reads
--
-- Purpose:
--     Surfaces the stored procedures and queries consuming the most
--     I/O across the system. Ranks by total logical reads (avg * execs)
--     so high-frequency lightweight procs and low-frequency heavy procs
--     both surface appropriately.
--
-- Usage:
--     Run against the target database. Requires Query Store to be enabled.
--     Adjust @LookbackHours to control the analysis window.
--     Results are ordered by TotalReads descending — the top of the list
--     is where refactoring effort pays off the most.
--
-- What to look for:
--     - High TotalReads with high AvgReads: expensive proc, called often — top priority
--     - High TotalReads with low AvgReads but huge Execs: hot-path proc, small per-call wins compound
--     - High AvgReads with low Execs: occasional heavy hitter — may not be worth optimizing unless it blocks
--     - Forced plan failures > 0: plan instability, same pattern as lsp_RxfGetListOfManualFillGroups
--     - AvgDur >> AvgCPU: waiting on I/O or blocking, not compute-bound
--     - AvgCPU >> AvgDur: parallelism in play (good sign)
--
-- Columns:
--     ObjectName          - Proc or object name (NULL for ad-hoc queries)
--     QueryId             - Query Store query ID for drill-down
--     AvgDurMs            - Average duration in milliseconds
--     AvgCPUMs            - Average CPU time in milliseconds
--     AvgReads            - Average logical reads per execution
--     AvgWrites           - Average logical writes per execution
--     Execs               - Total execution count in the window
--     TotalReads          - AvgReads * Execs — the ranking metric
--     TotalCPUMs          - AvgCPUMs * Execs — total CPU burn
--     LastExec            - Most recent execution timestamp
--     ForcedPlan          - Plan ID if a plan is force-pinned (NULL if not)
--     PlanFailures        - Count of forced plan application failures
--     FailReason          - Why the forced plan failed (if applicable)
--     QueryText           - First 200 chars of the query for identification
-----------------------------------------------------------------------

Declare @LookbackHours Int = 24

;With RuntimeStats As
(
    Select
        rs.plan_id,
        rs.count_executions,
        rs.avg_duration,
        rs.avg_cpu_time,
        rs.avg_logical_io_reads,
        rs.avg_logical_io_writes,
        rs.last_execution_time
    From
        sys.query_store_runtime_stats rs With (NoLock)
    Where
        rs.last_execution_time >= DateAdd(Hour, -@LookbackHours, GetUtcDate())
),
PlanStats As
(
    Select
        p.plan_id,
        p.query_id,
        p.is_forced_plan,
        p.force_failure_count,
        p.last_force_failure_reason_desc,
        Sum(rs.count_executions)                                    As Execs,
        Avg(rs.avg_duration) / 1000.0                               As AvgDurMs,
        Avg(rs.avg_cpu_time) / 1000.0                                As AvgCPUMs,
        Avg(rs.avg_logical_io_reads)                                 As AvgReads,
        Avg(rs.avg_logical_io_writes)                                As AvgWrites,
        Max(rs.last_execution_time)                                  As LastExec
    From
        sys.query_store_plan p With (NoLock)
        Join RuntimeStats rs On rs.plan_id = p.plan_id
    Group By
        p.plan_id, p.query_id, p.is_forced_plan,
        p.force_failure_count, p.last_force_failure_reason_desc
),
QueryInfo As
(
    Select
        ps.*,
        q.object_id,
        qt.query_sql_text,
        -- Total reads = avg per exec * number of execs — the ranking metric
        Cast(ps.AvgReads * ps.Execs As BigInt)                      As TotalReads,
        Cast(ps.AvgCPUMs * ps.Execs As BigInt)                      As TotalCPUMs
    From
        PlanStats ps
        Join sys.query_store_query q With (NoLock) On q.query_id = ps.query_id
        Join sys.query_store_query_text qt With (NoLock) On qt.query_text_id = q.query_text_id
)
Select Top 50
    Coalesce(Object_Name(qi.object_id), '<ad-hoc>') As ObjectName,
    qi.query_id                                      As QueryId,
    Cast(qi.AvgDurMs As Decimal(12,4))               As AvgDurMs,
    Cast(qi.AvgCPUMs As Decimal(12,4))               As AvgCPUMs,
    Cast(qi.AvgReads As BigInt)                       As AvgReads,
    Cast(qi.AvgWrites As BigInt)                      As AvgWrites,
    qi.Execs,
    qi.TotalReads,
    qi.TotalCPUMs,
    qi.LastExec,
    Case When qi.is_forced_plan = 1
         Then qi.plan_id
         Else Null
    End                                              As ForcedPlan,
    Case When qi.force_failure_count > 0
         Then qi.force_failure_count
         Else Null
    End                                              As PlanFailures,
    qi.last_force_failure_reason_desc                As FailReason,
    Left(qi.query_sql_text, 200)                     As QueryText
From
    QueryInfo qi
Order By
    qi.TotalReads Desc
