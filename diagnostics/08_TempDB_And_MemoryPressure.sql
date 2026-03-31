-----------------------------------------------------------------------
-- Diagnostic: TempDB & Memory Pressure Snapshot
--
-- Purpose:
--     Three-part snapshot of resource pressure that directly impacts
--     query performance:
--       Part A: TempDB file space usage — are sort spills, table
--               variables, and temp tables consuming all the space?
--       Part B: Buffer pool breakdown — which tables are hogging
--               the cache? Large scans from bad plans evict useful
--               pages and degrade everything.
--       Part C: Memory grants — queries waiting for or using excessive
--               memory grants. Bad cardinality estimates cause
--               over-granting (wasted memory) or under-granting
--               (spills to tempdb).
--
-- Usage:
--     Run against master (Parts A, C) or target database (Part B).
--     Read-only. Point-in-time snapshot.
--
-- What to look for:
--     - Part A: High user_object_reserved_pages = heavy temp table /
--       table variable usage. High internal_object_reserved_pages =
--       sort spills and hash operations.
--     - Part B: Tables with huge page counts in the buffer pool that
--       don't match their access frequency. A 10M-row table consuming
--       40% of the buffer pool is pushing everything else to disk.
--     - Part C: Queries with grant_kb >> used_kb are over-granted
--       (bad estimates, wasted memory). Queries with wait status
--       are starving for memory.
-----------------------------------------------------------------------

-----------------------------------------------------------------------
-- Part A: TempDB Space Usage
-----------------------------------------------------------------------
Select
      'TempDB Space Usage' As Analysis
    , Cast(Sum(user_object_reserved_page_count) * 8.0 / 1024 As Decimal(10,2))
                                                As UserObjects_MB
    , Cast(Sum(internal_object_reserved_page_count) * 8.0 / 1024 As Decimal(10,2))
                                                As InternalObjects_MB
    , Cast(Sum(version_store_reserved_page_count) * 8.0 / 1024 As Decimal(10,2))
                                                As VersionStore_MB
    , Cast(Sum(unallocated_extent_page_count) * 8.0 / 1024 As Decimal(10,2))
                                                As FreeSpace_MB
From
    sys.dm_db_file_space_usage


-----------------------------------------------------------------------
-- Part B: Buffer Pool — Top 20 Tables by Cached Pages
--
-- NOTE: Run this against the TARGET database, not master.
-----------------------------------------------------------------------
Select Top 20
      Object_Name(p.object_id)          As TableName
    , i.name                             As IndexName
    , i.type_desc                        As IndexType
    , Count(b.page_id)                   As CachedPages
    , Cast(Count(b.page_id) * 8.0 / 1024 As Decimal(10,2))
                                         As CachedMB
    , Cast(Count(b.page_id) * 100.0
           / (Select Count(*) From sys.dm_os_buffer_descriptors
              Where database_id = Db_Id())
           As Decimal(5,2))              As PctOfBufferPool
From
    sys.dm_os_buffer_descriptors b
    Join sys.allocation_units a
        On a.allocation_unit_id = b.allocation_unit_id
    Join sys.partitions p
        On p.hobt_id = a.container_id
    Join sys.indexes i
        On i.object_id = p.object_id
           And i.index_id = p.index_id
Where
    b.database_id = Db_Id()
    And ObjectProperty(p.object_id, 'IsUserTable') = 1
Group By
    p.object_id, i.name, i.type_desc
Order By
    Count(b.page_id) Desc


-----------------------------------------------------------------------
-- Part C: Current Memory Grants
-----------------------------------------------------------------------
Select
      session_id                          As SessionId
    , Cast(requested_memory_kb / 1024.0 As Decimal(10,2))
                                          As RequestedMB
    , Cast(granted_memory_kb / 1024.0 As Decimal(10,2))
                                          As GrantedMB
    , Cast(used_memory_kb / 1024.0 As Decimal(10,2))
                                          As UsedMB
    , Cast(max_used_memory_kb / 1024.0 As Decimal(10,2))
                                          As MaxUsedMB
    , Cast(Case
        When granted_memory_kb > 0
        Then used_memory_kb * 100.0 / granted_memory_kb
        Else 0
      End As Decimal(5,2))                As GrantUtilizationPct
    , wait_time_ms                        As WaitTimeMs
    , is_next_candidate                   As IsNextCandidate
    , dop                                 As DegreeOfParallelism
    , Cast(query_cost As Decimal(12,2))   As QueryCost
    , Left(t.text, 200)                   As QueryText
From
    sys.dm_exec_query_memory_grants mg
    Cross Apply sys.dm_exec_sql_text(mg.sql_handle) t
Order By
    mg.granted_memory_kb Desc
