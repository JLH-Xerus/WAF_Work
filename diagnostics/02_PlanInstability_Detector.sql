-----------------------------------------------------------------------
-- Diagnostic: Plan Instability Detector
--
-- Purpose:
--     Identifies queries suffering from parameter sniffing by finding
--     query_ids with multiple cached plans whose performance varies
--     wildly. A ReadRatio of 10x+ is a strong signal; 100x is critical.
--
-- Usage:
--     Run against the target database. Requires Query Store enabled.
--     Adjust @LookbackHours and @MinReadRatio to control sensitivity.
--
-- Output:
--     One row per unstable query, ranked by ReadRatio descending.
--     Use QueryId to drill into specific plans via:
--       SELECT * FROM sys.query_store_plan WHERE query_id = <id>
--
-- What to look for:
--     - ReadRatio > 100x: Critical parameter sniffing. Mitigate immediately.
--     - ReadRatio 10-100x: Significant instability. Local variable mitigation.
--     - ReadRatio 2-10x: Moderate. May be acceptable depending on frequency.
--     - High Execs + High ReadRatio: Top priority — instability * volume = pain.
--     - BestPlanReads vs WorstPlanReads: Shows the optimizer's best case vs
--       worst case. If BestPlanReads is acceptable, the query itself is fine —
--       it just needs a stable plan.
-----------------------------------------------------------------------

Declare @LookbackHours Int = 24
Declare @MinReadRatio Float = 2.0     -- Minimum read ratio to surface

;With PlanPerformance As
(
    Select
          qsp.query_id
        , qsp.plan_id
        , Avg(qsrs.avg_logical_io_reads)  As AvgReads
        , Avg(qsrs.avg_duration) / 1000.0 As AvgDurMs
        , Avg(qsrs.avg_cpu_time) / 1000.0 As AvgCpuMs
        , Sum(qsrs.count_executions)       As PlanExecs
        , Max(qsrs.last_execution_time)    As LastExec
    From
        sys.query_store_plan qsp With (NoLock)
        Join sys.query_store_runtime_stats qsrs With (NoLock)
            On qsrs.plan_id = qsp.plan_id
    Where
        qsrs.last_execution_time >= DateAdd(Hour, -@LookbackHours, GetUtcDate())
    Group By
        qsp.query_id, qsp.plan_id
),
QueryInstability As
(
    Select
          query_id
        , Count(Distinct plan_id)    As PlanCount
        , Sum(PlanExecs)             As TotalExecs
        , Min(AvgReads)              As BestPlanReads
        , Max(AvgReads)              As WorstPlanReads
        , Min(AvgDurMs)              As BestPlanDurMs
        , Max(AvgDurMs)              As WorstPlanDurMs
        , Max(LastExec)              As LastExec
    From
        PlanPerformance
    Group By
        query_id
    Having
        Count(Distinct plan_id) > 1
)
Select Top 50
      Coalesce(Object_Name(q.object_id), '<ad-hoc>')   As ObjectName
    , qi.query_id                                        As QueryId
    , qi.PlanCount
    , qi.TotalExecs
    , Cast(qi.BestPlanReads As BigInt)                   As BestPlanReads
    , Cast(qi.WorstPlanReads As BigInt)                  As WorstPlanReads
    , Cast(qi.WorstPlanReads / NullIf(qi.BestPlanReads, 0) As Decimal(10,1))
                                                         As ReadRatio
    , Cast(qi.BestPlanDurMs As Decimal(12,2))            As BestPlanDurMs
    , Cast(qi.WorstPlanDurMs As Decimal(12,2))           As WorstPlanDurMs
    , Cast(qi.WorstPlanDurMs / NullIf(qi.BestPlanDurMs, 0) As Decimal(10,1))
                                                         As DurRatio
    , qi.LastExec
    , Left(qt.query_sql_text, 200)                       As QueryText
From
    QueryInstability qi
    Join sys.query_store_query q With (NoLock)
        On q.query_id = qi.query_id
    Join sys.query_store_query_text qt With (NoLock)
        On qt.query_text_id = q.query_text_id
Where
    qi.WorstPlanReads / NullIf(qi.BestPlanReads, 0) >= @MinReadRatio
Order By
    Cast(qi.WorstPlanReads / NullIf(qi.BestPlanReads, 0) As Float) Desc
