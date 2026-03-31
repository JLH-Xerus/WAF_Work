-----------------------------------------------------------------------
-- Diagnostic: Data Velocity & Capacity Assessment
--
-- Purpose:
--     Measures the RATE of data accumulation across key tables to
--     answer: "How fast is this site growing, and where is the
--     pressure building?" This is critical for:
--       - Predicting when tables cross performance thresholds
--       - Comparing growth rates across the 15 site instances
--       - Identifying tables that need archival/purge strategies
--       - Sizing index maintenance windows
--       - Forecasting storage and tempdb requirements
--
-- Usage:
--     Run against the target database. Read-only, uses NoLock.
--     Returns five sections:
--       A. Table size inventory (rows, reserved space, index space)
--       B. Data velocity — rows created per day/week/month using
--          date columns that indicate record creation
--       C. Growth trend — daily row counts for the last 30 days
--          to detect acceleration or seasonal patterns
--       D. Archival opportunity — rows by age bucket to identify
--          how much historical data could be purged
--       E. Growth projection — linear extrapolation of current
--          velocity to 30/90/180 day forecasts
--
-- IMPORTANT — Configuring the table list:
--     Section B-E use a table of (TableName, DateColumn) pairs.
--     You MUST configure this for your environment. The defaults
--     below are based on the pharmacy fulfillment schema we've
--     been working with. Add, remove, or adjust as needed.
--
-- What to look for:
--     - High daily velocity on tables that appear in the Query Store
--       top offenders: growth amplifies bad plans over time
--     - Divergent velocity across sites: a site with 2x the growth
--       rate will hit performance cliffs sooner
--     - Tables with millions of rows but no archival: candidates
--       for retention policies
--     - Acceleration in the trend data: steady growth is plannable;
--       accelerating growth is an emergency
-----------------------------------------------------------------------


-----------------------------------------------------------------------
-- Section A: Table Size Inventory
--
-- All user tables ranked by total reserved space. Gives you the
-- current state before measuring velocity.
-----------------------------------------------------------------------
Select
      Object_Name(t.object_id)              As TableName
    , Sum(p.rows)                            As [RowCount]
    , Cast(Sum(a.total_pages) * 8.0 / 1024
           As Decimal(12,2))                 As TotalReservedMB
    , Cast(Sum(a.used_pages) * 8.0 / 1024
           As Decimal(12,2))                 As UsedMB
    , Cast(Sum(Case When i.type_desc <> 'HEAP'
                    Then a.used_pages Else 0 End)
           * 8.0 / 1024 As Decimal(12,2))   As IndexMB
    , Count(Distinct i.index_id)             As IndexCount
From
    sys.tables t
    Join sys.indexes i On i.object_id = t.object_id
    Join sys.partitions p On p.object_id = i.object_id
         And p.index_id = i.index_id
    Join sys.allocation_units a On a.container_id = p.hobt_id
Where
    t.is_ms_shipped = 0
Group By
    t.object_id
Order By
    Sum(a.total_pages) Desc


-----------------------------------------------------------------------
-- Section B-E: Data Velocity, Trends, Archival, and Projections
--
-- CONFIGURE THIS: Add your table/date-column pairs below.
-- Each row defines a table to measure and the column that
-- indicates when the record was created.
-----------------------------------------------------------------------

-- Build the list of tables to analyze
Declare @Tables Table (
    TableName   NVarchar(256),
    DateColumn  NVarchar(256)
)

-- *** ADD YOUR TABLE/DATE COLUMN PAIRS HERE ***
-- Format: (TableName, DateColumnThatIndicatesCreation)
Insert Into @Tables Values
      ('ImgImage',                    'CreatedDtTm')
    , ('OeOrder',                     'OrderDate')
    , ('OeOrderHistory',              'DtTm')
    , ('OeGroup',                     'CreatedDtTm')
    , ('SrtShipToteShipmentAssoc',    'CreatedDtTm')
    , ('ShpManifest',                 'CreatedDtTm')
    , ('InvAuditEvent',               'DtTm')
    , ('WfEvent',                     'DtTm')
    , ('PmssData',                    'CreatedDtTm')
    , ('WfWorkflowImage',            'CreatedDtTm')
-- Add more tables as needed for each site:
-- , ('YourTable',                    'YourDateColumn')


-----------------------------------------------------------------------
-- Section B: Data Velocity — Rows per Day / Week / Month
-----------------------------------------------------------------------
Declare @VelocitySql NVarchar(Max) = N''

Select @VelocitySql = @VelocitySql +
    Case When @VelocitySql <> N'' Then N' Union All ' Else N'' End +
    N'Select
          ' + QuoteName(T.TableName, '''') + N' As TableName
        , Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -1, GetDate())
                     Then 1 End)   As RowsLast24h
        , Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -7, GetDate())
                     Then 1 End)   As RowsLast7d
        , Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -30, GetDate())
                     Then 1 End)   As RowsLast30d
        , Cast(Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -7, GetDate())
                          Then 1 End) / 7.0
               As Decimal(12,1))   As AvgRowsPerDay_7d
        , Cast(Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -30, GetDate())
                          Then 1 End) / 30.0
               As Decimal(12,1))   As AvgRowsPerDay_30d
        , Count(*)                 As TotalRows
    From ' + QuoteName(T.TableName) + N' With (NoLock)'
From @Tables T

If @VelocitySql <> N''
Begin
    Set @VelocitySql = N'Select * From (' + @VelocitySql + N') V Order By RowsLast30d Desc'
    Exec sp_executesql @VelocitySql
End


-----------------------------------------------------------------------
-- Section C: Growth Trend — Daily Row Counts (Last 30 Days)
--
-- One row per table per day. Use this to detect acceleration,
-- deceleration, weekly cycles, or anomalous spikes.
-----------------------------------------------------------------------
Declare @TrendSql NVarchar(Max) = N''

Select @TrendSql = @TrendSql +
    Case When @TrendSql <> N'' Then N' Union All ' Else N'' End +
    N'Select
          ' + QuoteName(T.TableName, '''') + N' As TableName
        , Cast(' + QuoteName(T.DateColumn) + N' As Date) As CreatedDate
        , Count(*)                                        As RowsCreated
    From ' + QuoteName(T.TableName) + N' With (NoLock)
    Where ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -30, GetDate())
    Group By Cast(' + QuoteName(T.DateColumn) + N' As Date)'
From @Tables T

If @TrendSql <> N''
Begin
    Set @TrendSql = N'Select * From (' + @TrendSql + N') Trend Order By TableName, CreatedDate'
    Exec sp_executesql @TrendSql
End


-----------------------------------------------------------------------
-- Section D: Archival Opportunity — Rows by Age Bucket
--
-- Shows how much data exists in each age bracket. Helps quantify
-- the benefit of retention policies (e.g., "purging data older
-- than 90 days would remove 8M rows from ImgImage").
-----------------------------------------------------------------------
Declare @ArchivalSql NVarchar(Max) = N''

Select @ArchivalSql = @ArchivalSql +
    Case When @ArchivalSql <> N'' Then N' Union All ' Else N'' End +
    N'Select
          ' + QuoteName(T.TableName, '''') + N' As TableName
        , Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -7, GetDate())
                     Then 1 End)    As [0_7d]
        , Count(Case When ' + QuoteName(T.DateColumn) + N' Between DateAdd(Day, -30, GetDate())
                     And DateAdd(Day, -7, GetDate()) Then 1 End)
                                    As [8_30d]
        , Count(Case When ' + QuoteName(T.DateColumn) + N' Between DateAdd(Day, -90, GetDate())
                     And DateAdd(Day, -30, GetDate()) Then 1 End)
                                    As [31_90d]
        , Count(Case When ' + QuoteName(T.DateColumn) + N' Between DateAdd(Day, -180, GetDate())
                     And DateAdd(Day, -90, GetDate()) Then 1 End)
                                    As [91_180d]
        , Count(Case When ' + QuoteName(T.DateColumn) + N' Between DateAdd(Day, -365, GetDate())
                     And DateAdd(Day, -180, GetDate()) Then 1 End)
                                    As [181_365d]
        , Count(Case When ' + QuoteName(T.DateColumn) + N' < DateAdd(Day, -365, GetDate())
                     Then 1 End)    As [Over_1yr]
        , Count(*)                  As Total
    From ' + QuoteName(T.TableName) + N' With (NoLock)'
From @Tables T

If @ArchivalSql <> N''
Begin
    Set @ArchivalSql = N'Select * From (' + @ArchivalSql + N') A Order By Total Desc'
    Exec sp_executesql @ArchivalSql
End


-----------------------------------------------------------------------
-- Section E: Growth Projection
--
-- Linear extrapolation: uses the 30-day average daily rate to
-- project forward. Simple but effective for capacity planning.
-- Compare across sites to see which ones hit thresholds first.
-----------------------------------------------------------------------
Declare @ProjectSql NVarchar(Max) = N''

Select @ProjectSql = @ProjectSql +
    Case When @ProjectSql <> N'' Then N' Union All ' Else N'' End +
    N'Select
          ' + QuoteName(T.TableName, '''') + N'  As TableName
        , Count(*)                               As CurrentRows
        , Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -30, GetDate())
                     Then 1 End)                 As Last30dRows
        , Cast(Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -30, GetDate())
                          Then 1 End) / 30.0
               As Decimal(12,1))                 As DailyRate
        -- 30-day projection
        , Count(*) + Cast(Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -30, GetDate())
                                     Then 1 End) / 30.0 * 30
               As BigInt)                        As [Projected_30d]
        -- 90-day projection
        , Count(*) + Cast(Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -30, GetDate())
                                     Then 1 End) / 30.0 * 90
               As BigInt)                        As [Projected_90d]
        -- 180-day projection
        , Count(*) + Cast(Count(Case When ' + QuoteName(T.DateColumn) + N' >= DateAdd(Day, -30, GetDate())
                                     Then 1 End) / 30.0 * 180
               As BigInt)                        As [Projected_180d]
    From ' + QuoteName(T.TableName) + N' With (NoLock)'
From @Tables T

If @ProjectSql <> N''
Begin
    Set @ProjectSql = N'Select * From (' + @ProjectSql + N') P Order By DailyRate Desc'
    Exec sp_executesql @ProjectSql
End
