-----------------------------------------------------------------------
-- Diagnostic: Statistics Staleness Report
--
-- Purpose:
--     Identifies tables where statistics may be stale — the row
--     modification counter (rows changed since last stats update)
--     is a significant percentage of total rows. Stale statistics
--     cause the optimizer to make wrong cardinality estimates, leading
--     to bad plans (wrong join strategies, memory grants, parallelism).
--
-- Usage:
--     Run against the target database. Read-only.
--     Adjust @MinModPct to control sensitivity.
--
-- What to look for:
--     - ModPct > 20%: Statistics are likely stale. Consider UPDATE
--       STATISTICS on the table.
--     - ModPct > 50%: Statistics are almost certainly stale. The
--       optimizer's cardinality estimates are unreliable.
--     - Large tables with high ModPct: Top priority — bad estimates
--       on large tables produce the worst plans.
--     - Tables showing up in the Query Store top offenders with high
--       ModPct: Strong correlation between stale stats and bad plans.
--
-- Auto-update threshold:
--     SQL Server auto-updates statistics when ~20% of rows change
--     (or sqrt(1000 * table rows) with trace flag 2371). But this
--     only triggers on the next query compilation, and the threshold
--     can be too high for large tables.
-----------------------------------------------------------------------

Declare @MinModPct Float = 10.0   -- Minimum modification % to surface

Select
      Object_Name(sp.object_id)                As TableName
    , sp.stats_id                               As StatsId
    , s.name                                    As StatsName
    , sp.rows                                   As TotalRows
    , sp.modification_counter                   As RowsModified
    , Cast(Case
        When sp.rows > 0
        Then sp.modification_counter * 100.0 / sp.rows
        Else 0
      End As Decimal(10,2))                     As ModPct
    , sp.last_updated                           As LastUpdated
    , DateDiff(Day, sp.last_updated, GetDate()) As DaysSinceUpdate
    , sp.rows_sampled                           As RowsSampled
    , Cast(Case
        When sp.rows > 0
        Then sp.rows_sampled * 100.0 / sp.rows
        Else 0
      End As Decimal(5,2))                      As SamplePct
    , sp.steps                                  As HistogramSteps
From
    sys.stats s
    Cross Apply sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
Where
    ObjectProperty(s.object_id, 'IsUserTable') = 1
    And sp.rows > 0
    And Case
        When sp.rows > 0
        Then sp.modification_counter * 100.0 / sp.rows
        Else 0
    End >= @MinModPct
Order By
    sp.modification_counter * 100.0 / NullIf(sp.rows, 0) Desc
