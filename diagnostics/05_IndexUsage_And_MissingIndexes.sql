-----------------------------------------------------------------------
-- Diagnostic: Index Usage & Missing Index Report
--
-- Purpose:
--     Two-part report:
--       Part A: Existing indexes ranked by usage — identifies unused
--               indexes (write overhead with no read benefit) and
--               heavily-scanned indexes (potential for tuning).
--       Part B: Missing indexes the optimizer has requested, ranked
--               by estimated improvement impact.
--
-- Usage:
--     Run against the target database. Read-only.
--     Counters are cumulative since last server restart.
--     A recently restarted server will have low counts — wait for a
--     representative workload period before drawing conclusions.
--
-- Part A — What to look for:
--     - UserScans >> UserSeeks: The index is being scanned, not seeked.
--       Either the queries aren't SARGable or the index key doesn't
--       match the predicates.
--     - UserSeeks = 0, UserScans = 0, UserLookups = 0: The index is
--       never read. It's pure write overhead. Candidate for removal
--       (verify with Query Store first — some indexes are only used
--       by rare but critical queries).
--     - High UserUpdates with low reads: Write-heavy index with
--       minimal read benefit. Evaluate whether it's needed.
--
-- Part B — What to look for:
--     - High AvgUserImpact (>80%): The optimizer strongly believes
--       this index would help.
--     - High UserSeeks: Many queries would benefit.
--     - Included columns: The optimizer wants a covering index.
--       Consider whether the included columns are worth the space.
-----------------------------------------------------------------------

-----------------------------------------------------------------------
-- Part A: Existing Index Usage Statistics
-----------------------------------------------------------------------
Select
      Object_Name(i.object_id)          As TableName
    , i.name                             As IndexName
    , i.type_desc                        As IndexType
    , s.user_seeks                       As UserSeeks
    , s.user_scans                       As UserScans
    , s.user_lookups                     As UserLookups
    , s.user_updates                     As UserUpdates
    , s.last_user_seek                   As LastSeek
    , s.last_user_scan                   As LastScan
    , s.last_user_update                 As LastUpdate
    , Cast(
        Case
            When (s.user_seeks + s.user_scans + s.user_lookups) = 0
            Then 0
            Else s.user_seeks * 100.0
                 / (s.user_seeks + s.user_scans + s.user_lookups)
        End As Decimal(5,2))             As SeekPct
    -- Read-to-Write ratio: < 1.0 means more writes than reads
    , Cast(
        Case
            When s.user_updates = 0 Then 999.99
            Else (s.user_seeks + s.user_scans + s.user_lookups) * 1.0
                 / s.user_updates
        End As Decimal(10,2))            As ReadToWriteRatio
From
    sys.indexes i
    Join sys.dm_db_index_usage_stats s
        On s.object_id = i.object_id
           And s.index_id = i.index_id
           And s.database_id = Db_Id()
Where
    ObjectProperty(i.object_id, 'IsUserTable') = 1
    And i.type_desc <> 'HEAP'
Order By
    (s.user_seeks + s.user_scans + s.user_lookups) Asc,
    s.user_updates Desc


-----------------------------------------------------------------------
-- Part B: Missing Index Recommendations
-----------------------------------------------------------------------
Select Top 30
      Object_Name(mid.object_id)          As TableName
    , mid.equality_columns                 As EqualityColumns
    , mid.inequality_columns               As InequalityColumns
    , mid.included_columns                 As IncludedColumns
    , migs.unique_compiles                 As UniqueCompiles
    , migs.user_seeks                      As UserSeeks
    , migs.user_scans                      As UserScans
    , Cast(migs.avg_total_user_cost As Decimal(10,4))
                                           As AvgQueryCost
    , Cast(migs.avg_user_impact As Decimal(5,2))
                                           As AvgUserImpact
    -- Composite score: estimated improvement * frequency
    , Cast(migs.avg_total_user_cost
           * migs.avg_user_impact
           * (migs.user_seeks + migs.user_scans)
           As Decimal(18,2))               As ImprovementScore
From
    sys.dm_db_missing_index_details mid
    Join sys.dm_db_missing_index_groups mig
        On mig.index_handle = mid.index_handle
    Join sys.dm_db_missing_index_group_stats migs
        On migs.group_handle = mig.index_group_handle
Where
    mid.database_id = Db_Id()
Order By
    migs.avg_total_user_cost
    * migs.avg_user_impact
    * (migs.user_seeks + migs.user_scans) Desc
