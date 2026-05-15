/*
================================================================================
  SQL Server 2019 Table Partitioning POC - Module 5
  Monitoring & Performance Validation
================================================================================
  Author:       Justin Hunter
  Date:         2026-02-23
  SQL Server:   2019 (Enterprise Edition)
  Purpose:      Comprehensive monitoring toolkit and performance validation
                framework for the partitioned environment

  Module 5 covers:
    5.1  Performance baseline capture (before vs after partitioning)
    5.2  Partition health dashboard (sizes, row counts, compression)
    5.3  Query performance validation (partition elimination verification)
    5.4  Wait statistics analysis for partitioned workloads
    5.5  Buffer pool efficiency monitoring
    5.6  Statistics quality assessment
    5.7  I/O performance monitoring
    5.8  Operational alerts and thresholds
    5.9  POC sign-off checklist

  Prerequisites:
    - Modules 1-4 completed (partitioned tables, indexes, SWITCH, maintenance)
    - Enterprise Edition (for certain DMV access and online operations)

  IMPORTANT:
    Run these queries in the target database. Many queries reference
    sys.dm_* DMVs which are instance-scoped. The monitoring procedures
    create persistent tables for trend tracking.
================================================================================
*/

USE PartitioningPOC;
GO

-- ============================================================================
-- SECTION 5.1: PERFORMANCE BASELINE CAPTURE
-- ============================================================================
/*
  Before validating improvements, we need baselines. This section creates
  a framework for capturing and comparing performance metrics at two points
  in time: before partitioning (legacy) and after partitioning (POC).

  Strategy:
    1. Capture baseline metrics into a snapshot table
    2. Run the same capture after partitioning is deployed
    3. Compare the two snapshots side by side
*/

-- Baseline snapshot storage
IF OBJECT_ID('dbo.PerformanceBaseline', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.PerformanceBaseline (
        BaselineID      INT IDENTITY(1,1) PRIMARY KEY,
        SnapshotName    NVARCHAR(50)    NOT NULL,  -- 'PRE_PARTITION' or 'POST_PARTITION'
        CapturedAt      DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
        MetricCategory  NVARCHAR(50)    NOT NULL,  -- 'QUERY', 'IO', 'INDEX', 'WAIT', 'SPACE'
        MetricName      NVARCHAR(200)   NOT NULL,
        MetricValue     DECIMAL(18,4)   NULL,
        MetricUnit      NVARCHAR(50)    NULL,
        Details         NVARCHAR(MAX)   NULL
    );

    CREATE NONCLUSTERED INDEX IX_Baseline_Snapshot
        ON dbo.PerformanceBaseline (SnapshotName, MetricCategory);
END
GO

-- ============================================================================
-- Procedure: usp_CapturePerformanceBaseline
-- Purpose:   Captures a comprehensive performance snapshot for comparison
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_CapturePerformanceBaseline
    @SnapshotName   NVARCHAR(50)    -- 'PRE_PARTITION' or 'POST_PARTITION'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME2(3) = SYSUTCDATETIME();

    PRINT '=== Capturing Performance Baseline: ' + @SnapshotName + ' ===';
    PRINT 'Started: ' + CONVERT(VARCHAR(30), @StartTime, 121);

    -- -----------------------------------------------------------------------
    -- SPACE METRICS
    -- -----------------------------------------------------------------------
    PRINT '  Capturing space metrics...';

    -- Total database size
    INSERT INTO dbo.PerformanceBaseline (SnapshotName, MetricCategory, MetricName, MetricValue, MetricUnit)
    SELECT @SnapshotName, 'SPACE', 'Database Size (MB)',
           SUM(size * 8.0 / 1024), 'MB'
    FROM sys.database_files;

    -- Per-table sizes (top 20 tables by size)
    INSERT INTO dbo.PerformanceBaseline (SnapshotName, MetricCategory, MetricName, MetricValue, MetricUnit, Details)
    SELECT TOP 20
           @SnapshotName, 'SPACE',
           'Table Size: ' + SCHEMA_NAME(t.schema_id) + '.' + t.name,
           SUM(a.total_pages) * 8.0 / 1024, 'MB',
           'Rows: ' + FORMAT(SUM(p.rows), 'N0')
    FROM sys.tables t
    JOIN sys.indexes i ON t.object_id = i.object_id
    JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    JOIN sys.allocation_units a ON p.partition_id = a.container_id
    GROUP BY t.schema_id, t.name
    ORDER BY SUM(a.total_pages) DESC;

    -- Total index space
    INSERT INTO dbo.PerformanceBaseline (SnapshotName, MetricCategory, MetricName, MetricValue, MetricUnit)
    SELECT @SnapshotName, 'SPACE', 'Total Index Space (MB)',
           SUM(a.total_pages) * 8.0 / 1024, 'MB'
    FROM sys.indexes i
    JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    JOIN sys.allocation_units a ON p.partition_id = a.container_id
    WHERE i.index_id > 1;

    -- -----------------------------------------------------------------------
    -- INDEX METRICS
    -- -----------------------------------------------------------------------
    PRINT '  Capturing index metrics...';

    -- Total number of indexes
    INSERT INTO dbo.PerformanceBaseline (SnapshotName, MetricCategory, MetricName, MetricValue, MetricUnit)
    SELECT @SnapshotName, 'INDEX', 'Total Index Count',
           COUNT(*), 'count'
    FROM sys.indexes
    WHERE index_id > 0 AND is_hypothetical = 0;

    -- Filtered index count
    INSERT INTO dbo.PerformanceBaseline (SnapshotName, MetricCategory, MetricName, MetricValue, MetricUnit)
    SELECT @SnapshotName, 'INDEX', 'Filtered Index Count',
           COUNT(*), 'count'
    FROM sys.indexes
    WHERE has_filter = 1;

    -- Index usage summary (reads vs writes)
    INSERT INTO dbo.PerformanceBaseline (SnapshotName, MetricCategory, MetricName, MetricValue, MetricUnit, Details)
    SELECT @SnapshotName, 'INDEX', 'Index Usage - Total Reads',
           SUM(user_seeks + user_scans + user_lookups), 'operations',
           'Since last restart: ' + CONVERT(VARCHAR(30),
               (SELECT sqlserver_start_time FROM sys.dm_os_sys_info), 121);

    INSERT INTO dbo.PerformanceBaseline (SnapshotName, MetricCategory, MetricName, MetricValue, MetricUnit)
    SELECT @SnapshotName, 'INDEX', 'Index Usage - Total Writes',
           SUM(user_updates), 'operations'
    FROM sys.dm_db_index_usage_stats
    WHERE database_id = DB_ID();

    -- -----------------------------------------------------------------------
    -- WAIT STATISTICS
    -- -----------------------------------------------------------------------
    PRINT '  Capturing wait statistics...';

    INSERT INTO dbo.PerformanceBaseline (SnapshotName, MetricCategory, MetricName, MetricValue, MetricUnit, Details)
    SELECT TOP 15
           @SnapshotName, 'WAIT',
           'Wait: ' + wait_type,
           wait_time_ms, 'ms',
           'Signal wait: ' + CAST(signal_wait_time_ms AS VARCHAR(20)) + ' ms, '
           + 'Waiting tasks: ' + CAST(waiting_tasks_count AS VARCHAR(20))
    FROM sys.dm_os_wait_stats
    WHERE wait_type NOT IN (
        'SLEEP_TASK', 'BROKER_TASK_STOP', 'BROKER_EVENTHANDLER',
        'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT', 'LAZYWRITER_SLEEP',
        'RESOURCE_QUEUE', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR',
        'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH',
        'XE_TIMER_EVENT', 'BROKER_TO_FLUSH', 'BROKER_RECEIVE_WAITFOR',
        'CLR_SEMAPHORE', 'DIRTY_PAGE_POLL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
        'ONDEMAND_TASK_QUEUE', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
        'SP_SERVER_DIAGNOSTICS_SLEEP', 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
        'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
        'QDS_ASYNC_QUEUE', 'PREEMPTIVE_OS_AUTHENTICATIONOPS',
        'PREEMPTIVE_OS_GETPROCADDRESS'
    )
    AND wait_time_ms > 0
    ORDER BY wait_time_ms DESC;

    -- -----------------------------------------------------------------------
    -- I/O METRICS
    -- -----------------------------------------------------------------------
    PRINT '  Capturing I/O metrics...';

    -- File-level I/O stats
    INSERT INTO dbo.PerformanceBaseline (SnapshotName, MetricCategory, MetricName, MetricValue, MetricUnit, Details)
    SELECT @SnapshotName, 'IO',
           'File IO: ' + f.name,
           s.io_stall, 'ms (total stall)',
           'Reads: ' + CAST(s.num_of_reads AS VARCHAR(20))
           + ', Writes: ' + CAST(s.num_of_writes AS VARCHAR(20))
           + ', Read stall: ' + CAST(s.io_stall_read_ms AS VARCHAR(20)) + ' ms'
           + ', Write stall: ' + CAST(s.io_stall_write_ms AS VARCHAR(20)) + ' ms'
    FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) s
    JOIN sys.database_files f ON s.file_id = f.file_id;

    -- -----------------------------------------------------------------------
    -- QUERY METRICS (from plan cache)
    -- -----------------------------------------------------------------------
    PRINT '  Capturing top queries by CPU...';

    INSERT INTO dbo.PerformanceBaseline (SnapshotName, MetricCategory, MetricName, MetricValue, MetricUnit, Details)
    SELECT TOP 10
           @SnapshotName, 'QUERY',
           'Top CPU Query #' + CAST(ROW_NUMBER() OVER (ORDER BY qs.total_worker_time DESC) AS VARCHAR(5)),
           qs.total_worker_time / 1000.0, 'ms (total CPU)',
           'Executions: ' + CAST(qs.execution_count AS VARCHAR(20))
           + ', Avg CPU: ' + CAST(qs.total_worker_time / NULLIF(qs.execution_count, 0) / 1000.0 AS VARCHAR(20)) + ' ms'
           + ', Avg reads: ' + CAST(qs.total_logical_reads / NULLIF(qs.execution_count, 0) AS VARCHAR(20))
           + ' | ' + SUBSTRING(st.text, (qs.statement_start_offset / 2) + 1,
                CASE WHEN qs.statement_end_offset = -1 THEN LEN(st.text)
                     ELSE (qs.statement_end_offset - qs.statement_start_offset) / 2 + 1 END)
    FROM sys.dm_exec_query_stats qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
    ORDER BY qs.total_worker_time DESC;

    PRINT '  Baseline capture complete for: ' + @SnapshotName;
    PRINT '  Duration: ' + CAST(DATEDIFF(MILLISECOND, @StartTime, SYSUTCDATETIME()) AS VARCHAR(10)) + ' ms';
END
GO

-- ============================================================================
-- Compare baselines side by side
-- ============================================================================
/*
  Run this after capturing both PRE_PARTITION and POST_PARTITION baselines
  to see the delta for each metric.
*/

-- Space comparison
SELECT
    pre.MetricName,
    pre.MetricValue   AS [Before],
    post.MetricValue  AS [After],
    post.MetricValue - pre.MetricValue AS [Delta],
    CASE WHEN pre.MetricValue > 0
         THEN CAST(((post.MetricValue - pre.MetricValue) / pre.MetricValue) * 100 AS DECIMAL(8,1))
         ELSE NULL END AS [Change%],
    pre.MetricUnit
FROM dbo.PerformanceBaseline pre
JOIN dbo.PerformanceBaseline post
    ON pre.MetricCategory = post.MetricCategory
    AND pre.MetricName = post.MetricName
WHERE pre.SnapshotName = 'PRE_PARTITION'
  AND post.SnapshotName = 'POST_PARTITION'
  AND pre.MetricCategory = 'SPACE'
ORDER BY ABS(post.MetricValue - pre.MetricValue) DESC;
GO


-- ============================================================================
-- SECTION 5.2: PARTITION HEALTH DASHBOARD
-- ============================================================================
/*
  The partition health dashboard is the single most important monitoring
  view for the partitioned environment. It shows every partition's size,
  row count, compression state, and data temperature at a glance.
*/

-- ============================================================================
-- Query 5.2.1: Comprehensive Partition Map
-- Shows every partition with size, rows, compression, and temperature
-- ============================================================================
SELECT
    OBJECT_SCHEMA_NAME(p.object_id) + '.' + OBJECT_NAME(p.object_id)  AS TableName,
    p.partition_number                                                  AS PartNum,
    pf.name                                                             AS PartFunction,
    CONVERT(VARCHAR(10), CAST(prv.value AS DATE), 120)                 AS BoundaryDate,
    FORMAT(p.rows, 'N0')                                               AS RowCount,
    CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(12,2))           AS SizeMB,
    CAST(SUM(a.used_pages)  * 8.0 / 1024 AS DECIMAL(12,2))           AS UsedMB,
    p.data_compression_desc                                             AS Compression,
    CASE
        WHEN prv.value IS NULL THEN 'EMPTY'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE())  THEN 'HOT'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE())  THEN 'WARM'
        ELSE 'COLD'
    END                                                                 AS Temperature,
    ds.name                                                             AS Filegroup,
    fg.is_read_only                                                     AS FG_ReadOnly
FROM sys.partitions p
JOIN sys.indexes i
    ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.allocation_units a
    ON p.partition_id = a.container_id
JOIN sys.partition_schemes ps
    ON i.data_space_id = ps.data_space_id
JOIN sys.partition_functions pf
    ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv
    ON pf.function_id = prv.function_id
    AND p.partition_number = prv.boundary_id + 1  -- RANGE RIGHT adjustment
LEFT JOIN sys.destination_data_spaces dds
    ON ps.data_space_id = dds.partition_scheme_id
    AND p.partition_number = dds.destination_id
LEFT JOIN sys.data_spaces ds
    ON dds.data_space_id = ds.data_space_id
LEFT JOIN sys.filegroups fg
    ON ds.data_space_id = fg.data_space_id
WHERE i.index_id IN (0, 1)  -- Heap or clustered index only
  AND p.rows > 0            -- Skip empty partitions
GROUP BY p.object_id, p.partition_number, pf.name, prv.value,
         p.rows, p.data_compression_desc, ds.name, fg.is_read_only
ORDER BY OBJECT_NAME(p.object_id), p.partition_number;
GO


-- ============================================================================
-- Query 5.2.2: Partition Size Distribution Summary
-- Quick overview of data distribution across temperature tiers
-- ============================================================================
SELECT
    OBJECT_SCHEMA_NAME(p.object_id) + '.' + OBJECT_NAME(p.object_id) AS TableName,
    CASE
        WHEN prv.value IS NULL THEN 'EMPTY'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE())  THEN 'HOT'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE())  THEN 'WARM'
        ELSE 'COLD'
    END                                                                 AS Temperature,
    COUNT(DISTINCT p.partition_number)                                  AS PartitionCount,
    FORMAT(SUM(p.rows), 'N0')                                         AS TotalRows,
    CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(12,2))           AS TotalSizeMB,
    CAST(SUM(a.total_pages) * 8.0 / 1024 / 1024 AS DECIMAL(12,2))   AS TotalSizeGB
FROM sys.partitions p
JOIN sys.indexes i
    ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.allocation_units a
    ON p.partition_id = a.container_id
JOIN sys.partition_schemes ps
    ON i.data_space_id = ps.data_space_id
JOIN sys.partition_functions pf
    ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv
    ON pf.function_id = prv.function_id
    AND p.partition_number = prv.boundary_id + 1
WHERE i.index_id IN (0, 1)
GROUP BY p.object_id,
    CASE
        WHEN prv.value IS NULL THEN 'EMPTY'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE())  THEN 'HOT'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE())  THEN 'WARM'
        ELSE 'COLD'
    END
ORDER BY OBJECT_NAME(p.object_id),
    CASE
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE()) THEN 1
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE()) THEN 2
        ELSE 3
    END;
GO


-- ============================================================================
-- Query 5.2.3: Compression Audit
-- Verifies compression is applied correctly per temperature tier
-- ============================================================================
SELECT
    OBJECT_SCHEMA_NAME(p.object_id) + '.' + OBJECT_NAME(p.object_id) AS TableName,
    i.name                                                              AS IndexName,
    p.partition_number                                                  AS PartNum,
    CONVERT(VARCHAR(10), CAST(prv.value AS DATE), 120)                 AS BoundaryDate,
    p.data_compression_desc                                             AS ActualCompression,
    CASE
        WHEN prv.value IS NULL THEN 'N/A'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE())  THEN 'NONE'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE())  THEN 'ROW'
        ELSE 'PAGE'
    END                                                                 AS ExpectedCompression,
    CASE
        WHEN p.data_compression_desc =
            CASE
                WHEN prv.value IS NULL THEN p.data_compression_desc
                WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE())  THEN 'NONE'
                WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE())  THEN 'ROW'
                ELSE 'PAGE'
            END THEN 'OK'
        ELSE '*** MISMATCH ***'
    END                                                                 AS Status
FROM sys.partitions p
JOIN sys.indexes i
    ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.partition_schemes ps
    ON i.data_space_id = ps.data_space_id
JOIN sys.partition_functions pf
    ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv
    ON pf.function_id = prv.function_id
    AND p.partition_number = prv.boundary_id + 1
WHERE i.index_id IN (0, 1)
  AND p.rows > 0
ORDER BY OBJECT_NAME(p.object_id), p.partition_number;
GO


-- ============================================================================
-- SECTION 5.3: QUERY PERFORMANCE VALIDATION
-- ============================================================================
/*
  The most critical validation: proving that queries actually USE partition
  elimination. If partition elimination is not happening, the partitioned
  table can actually be SLOWER than the unpartitioned original.
*/

-- ============================================================================
-- Query 5.3.1: Partition Elimination Test Framework
-- Runs a query and checks the actual execution plan for partition access
-- ============================================================================

-- Enable XML plan capture for the session
SET STATISTICS XML ON;
GO

/*
  IMPORTANT: Replace the table and column names below with your actual
  partitioned table. The key test: does the Actual Partition Count in the
  execution plan match the number of partitions your WHERE clause targets?

  Example test query — targets only the current month:
*/
SELECT COUNT(*) AS TransactionCount
FROM dbo.Transactions
WHERE TransactionDate >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
  AND TransactionDate <  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) + 1, 0);
GO

SET STATISTICS XML OFF;
GO

/*
  In the XML execution plan, look for the <RuntimePartitionSummary> element:

  <RuntimePartitionSummary>
    <PartitionsAccessed PartitionCount="1">
      <PartitionRange Start="42" End="42" />
    </PartitionsAccessed>
  </RuntimePartitionSummary>

  PartitionCount="1" means partition elimination worked perfectly.
  If PartitionCount equals the total number of partitions, elimination FAILED.
*/

-- ============================================================================
-- Query 5.3.2: Automated Partition Elimination Validator
-- Checks plan cache for queries that scanned too many partitions
-- ============================================================================
SELECT
    qs.execution_count                                           AS Executions,
    qs.total_logical_reads / NULLIF(qs.execution_count, 0)      AS AvgLogicalReads,
    qs.total_worker_time / NULLIF(qs.execution_count, 0) / 1000 AS AvgCPU_ms,
    SUBSTRING(st.text, (qs.statement_start_offset / 2) + 1,
        CASE WHEN qs.statement_end_offset = -1 THEN LEN(st.text)
             ELSE (qs.statement_end_offset - qs.statement_start_offset) / 2 + 1
        END)                                                     AS QueryText,
    TRY_CAST(qp.query_plan AS XML).value(
        '(//RelOp//RuntimePartitionSummary/PartitionsAccessed/@PartitionCount)[1]',
        'INT')                                                   AS PartitionsAccessed
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
WHERE st.dbid = DB_ID()
  AND TRY_CAST(qp.query_plan AS XML).value(
        '(//RelOp//RuntimePartitionSummary/PartitionsAccessed/@PartitionCount)[1]',
        'INT') > 3  -- Flag queries scanning more than 3 partitions
ORDER BY qs.total_logical_reads DESC;
GO


-- ============================================================================
-- Query 5.3.3: Top Queries by Logical Reads (Post-Partition)
-- Identifies queries that may not be benefiting from partitioning
-- ============================================================================
SELECT TOP 20
    qs.total_logical_reads / NULLIF(qs.execution_count, 0)     AS AvgLogicalReads,
    qs.total_worker_time / NULLIF(qs.execution_count, 0) / 1000 AS AvgCPU_ms,
    qs.total_elapsed_time / NULLIF(qs.execution_count, 0) / 1000 AS AvgElapsed_ms,
    qs.execution_count                                           AS Executions,
    qs.total_logical_reads                                       AS TotalReads,
    SUBSTRING(st.text, (qs.statement_start_offset / 2) + 1,
        CASE WHEN qs.statement_end_offset = -1 THEN 200
             ELSE LEAST((qs.statement_end_offset - qs.statement_start_offset) / 2 + 1, 200)
        END)                                                     AS QueryText
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
WHERE st.dbid = DB_ID()
ORDER BY qs.total_logical_reads / NULLIF(qs.execution_count, 0) DESC;
GO


-- ============================================================================
-- Query 5.3.4: Stored Procedure Performance Summary
-- Shows stored procedures that hit the partitioned tables
-- ============================================================================
SELECT TOP 20
    OBJECT_SCHEMA_NAME(ps.object_id) + '.' + OBJECT_NAME(ps.object_id) AS ProcName,
    ps.execution_count                                                   AS Executions,
    ps.total_worker_time / NULLIF(ps.execution_count, 0) / 1000        AS AvgCPU_ms,
    ps.total_elapsed_time / NULLIF(ps.execution_count, 0) / 1000       AS AvgElapsed_ms,
    ps.total_logical_reads / NULLIF(ps.execution_count, 0)              AS AvgLogicalReads,
    ps.total_logical_writes / NULLIF(ps.execution_count, 0)             AS AvgLogicalWrites,
    ps.total_physical_reads / NULLIF(ps.execution_count, 0)             AS AvgPhysicalReads,
    ps.cached_time                                                       AS CachedTime,
    ps.last_execution_time                                               AS LastExec
FROM sys.dm_exec_procedure_stats ps
WHERE ps.database_id = DB_ID()
ORDER BY ps.total_worker_time DESC;
GO


-- ============================================================================
-- SECTION 5.4: WAIT STATISTICS ANALYSIS
-- ============================================================================
/*
  Wait statistics tell you WHERE the server is spending time waiting.
  For a partitioned OLTP workload, the important waits are:

  - PAGEIOLATCH_*    : Physical I/O waits (SAN latency)
  - PAGELATCH_*      : In-memory page contention (hot page problems)
  - LCK_M_*          : Lock waits (blocking)
  - CXPACKET/CXCONSUMER : Parallelism waits (common on large scans)
  - WRITELOG          : Transaction log write latency
  - SOS_SCHEDULER_YIELD : CPU pressure

  After partitioning, you should see LOWER PAGEIOLATCH waits because:
    1. Hot data fits in the buffer pool (fewer physical reads)
    2. Filtered indexes are smaller (fewer pages to scan)
    3. Compressed cold data takes fewer pages
*/

-- ============================================================================
-- Query 5.4.1: Current Wait Statistics (filtered for relevant waits)
-- ============================================================================
SELECT
    wait_type,
    waiting_tasks_count                                       AS WaitCount,
    wait_time_ms                                              AS TotalWaitMs,
    wait_time_ms - signal_wait_time_ms                       AS ResourceWaitMs,
    signal_wait_time_ms                                       AS SignalWaitMs,
    CAST(100.0 * wait_time_ms / NULLIF(SUM(wait_time_ms) OVER(), 0)
         AS DECIMAL(5,1))                                     AS WaitPct,
    CAST(wait_time_ms * 1.0 / NULLIF(waiting_tasks_count, 0)
         AS DECIMAL(12,2))                                    AS AvgWaitMs
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    -- Filter out idle/background waits
    'SLEEP_TASK', 'BROKER_TASK_STOP', 'BROKER_EVENTHANDLER',
    'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT', 'LAZYWRITER_SLEEP',
    'RESOURCE_QUEUE', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR',
    'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH',
    'XE_TIMER_EVENT', 'BROKER_TO_FLUSH', 'BROKER_RECEIVE_WAITFOR',
    'CLR_SEMAPHORE', 'DIRTY_PAGE_POLL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
    'ONDEMAND_TASK_QUEUE', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
    'SP_SERVER_DIAGNOSTICS_SLEEP', 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
    'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
    'QDS_ASYNC_QUEUE', 'PREEMPTIVE_OS_AUTHENTICATIONOPS',
    'PREEMPTIVE_OS_GETPROCADDRESS', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
    'SLEEP_BPOOL_FLUSH', 'SLEEP_DBSTARTUP', 'SLEEP_DCOMSTARTUP',
    'SLEEP_MASTERDBREADY', 'SLEEP_MASTERMDREADY', 'SLEEP_MASTERUPGRADED',
    'SLEEP_MSDBSTARTUP', 'SLEEP_SYSTEMTASK', 'SLEEP_TEMPDBSTARTUP'
)
AND wait_time_ms > 0
ORDER BY wait_time_ms DESC;
GO


-- ============================================================================
-- Query 5.4.2: Wait Statistics Snapshot (for before/after comparison)
-- Run this before clearing waits, then again after a soak period
-- ============================================================================
/*
  To get a clean before/after:
    1. Clear wait stats:    DBCC SQLPERF('sys.dm_os_wait_stats', CLEAR);
    2. Let the system run for a representative period (e.g., 1 business day)
    3. Run Query 5.4.1 again to capture post-partition wait profile
*/


-- ============================================================================
-- SECTION 5.5: BUFFER POOL EFFICIENCY
-- ============================================================================
/*
  One of the primary benefits of partitioning + filtered indexes is
  improved buffer pool efficiency. Hot data should fit entirely in memory,
  eliminating physical reads for OLTP queries.
*/

-- ============================================================================
-- Query 5.5.1: Buffer Pool Usage by Object
-- Shows which tables/indexes are consuming buffer pool memory
-- ============================================================================
SELECT TOP 30
    OBJECT_SCHEMA_NAME(p.object_id) + '.' + OBJECT_NAME(p.object_id) AS ObjectName,
    i.name                                                              AS IndexName,
    i.type_desc                                                         AS IndexType,
    COUNT(b.page_id)                                                    AS BufferedPages,
    CAST(COUNT(b.page_id) * 8.0 / 1024 AS DECIMAL(12,2))             AS BufferedMB,
    CAST(100.0 * COUNT(b.page_id) /
        NULLIF((SELECT COUNT(*) FROM sys.dm_os_buffer_descriptors
                WHERE database_id = DB_ID()), 0)
        AS DECIMAL(5,2))                                                AS PctOfBufferPool,
    SUM(CASE WHEN b.is_modified = 1 THEN 1 ELSE 0 END)                AS DirtyPages
FROM sys.dm_os_buffer_descriptors b
JOIN sys.allocation_units a ON b.allocation_unit_id = a.allocation_unit_id
JOIN sys.partitions p ON a.container_id = p.partition_id
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
WHERE b.database_id = DB_ID()
GROUP BY p.object_id, i.name, i.type_desc
ORDER BY COUNT(b.page_id) DESC;
GO


-- ============================================================================
-- Query 5.5.2: Buffer Pool Usage by Partition (Critical!)
-- Shows if cold partitions are consuming buffer pool memory
-- ============================================================================
SELECT
    OBJECT_SCHEMA_NAME(p.object_id) + '.' + OBJECT_NAME(p.object_id) AS TableName,
    p.partition_number                                                  AS PartNum,
    CONVERT(VARCHAR(10), CAST(prv.value AS DATE), 120)                 AS BoundaryDate,
    CASE
        WHEN prv.value IS NULL THEN 'EMPTY'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE())  THEN 'HOT'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE())  THEN 'WARM'
        ELSE 'COLD'
    END                                                                 AS Temperature,
    COUNT(b.page_id)                                                    AS BufferedPages,
    CAST(COUNT(b.page_id) * 8.0 / 1024 AS DECIMAL(12,2))             AS BufferedMB,
    FORMAT(p.rows, 'N0')                                               AS RowCount
FROM sys.dm_os_buffer_descriptors b
JOIN sys.allocation_units a ON b.allocation_unit_id = a.allocation_unit_id
JOIN sys.partitions p ON a.container_id = p.partition_id
JOIN sys.indexes i
    ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.partition_schemes ps
    ON i.data_space_id = ps.data_space_id
JOIN sys.partition_functions pf
    ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv
    ON pf.function_id = prv.function_id
    AND p.partition_number = prv.boundary_id + 1
WHERE b.database_id = DB_ID()
  AND i.index_id IN (0, 1)
GROUP BY p.object_id, p.partition_number, prv.value, p.rows
ORDER BY OBJECT_NAME(p.object_id), p.partition_number;
GO

/*
  EXPECTED RESULT:
    - HOT partitions: Large buffer pool presence (good!)
    - COLD partitions: Minimal or zero buffer pool presence (good!)

  If cold partitions have significant buffer pool usage, it means:
    - Queries are scanning cold data unnecessarily
    - Missing partition elimination (check WHERE clauses)
    - A reporting query is touching all partitions without a date filter
*/


-- ============================================================================
-- Query 5.5.3: Buffer Pool Summary by Temperature
-- ============================================================================
SELECT
    CASE
        WHEN prv.value IS NULL THEN 'EMPTY'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE())  THEN 'HOT'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE())  THEN 'WARM'
        ELSE 'COLD'
    END                                                                  AS Temperature,
    COUNT(DISTINCT p.partition_number)                                   AS PartitionCount,
    SUM(bc.BufferedPages)                                                AS TotalBufferedPages,
    CAST(SUM(bc.BufferedPages) * 8.0 / 1024 AS DECIMAL(12,2))          AS TotalBufferedMB,
    CAST(100.0 * SUM(bc.BufferedPages) /
        NULLIF((SELECT COUNT(*) FROM sys.dm_os_buffer_descriptors
                WHERE database_id = DB_ID()), 0)
        AS DECIMAL(5,2))                                                 AS PctOfBufferPool
FROM sys.partitions p
JOIN sys.indexes i
    ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.partition_schemes ps
    ON i.data_space_id = ps.data_space_id
JOIN sys.partition_functions pf
    ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv
    ON pf.function_id = prv.function_id
    AND p.partition_number = prv.boundary_id + 1
LEFT JOIN (
    SELECT a.container_id, COUNT(b.page_id) AS BufferedPages
    FROM sys.dm_os_buffer_descriptors b
    JOIN sys.allocation_units a ON b.allocation_unit_id = a.allocation_unit_id
    WHERE b.database_id = DB_ID()
    GROUP BY a.container_id
) bc ON p.partition_id = bc.container_id
WHERE i.index_id IN (0, 1)
GROUP BY
    CASE
        WHEN prv.value IS NULL THEN 'EMPTY'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE())  THEN 'HOT'
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE())  THEN 'WARM'
        ELSE 'COLD'
    END
ORDER BY
    CASE
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE()) THEN 1
        WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE()) THEN 2
        ELSE 3
    END;
GO


-- ============================================================================
-- SECTION 5.6: STATISTICS QUALITY ASSESSMENT
-- ============================================================================
/*
  Poor statistics lead to bad execution plans. On partitioned tables,
  statistics quality is especially important because the optimizer uses
  them for partition elimination decisions.
*/

-- ============================================================================
-- Query 5.6.1: Statistics Health Check
-- Shows last update time, row counts, and modification counters
-- ============================================================================
SELECT
    OBJECT_SCHEMA_NAME(s.object_id) + '.' + OBJECT_NAME(s.object_id) AS TableName,
    s.name                                                              AS StatName,
    s.auto_created                                                      AS AutoCreated,
    s.is_incremental                                                    AS IsIncremental,
    STATS_DATE(s.object_id, s.stats_id)                                AS LastUpdated,
    DATEDIFF(DAY, STATS_DATE(s.object_id, s.stats_id), GETDATE())     AS DaysStale,
    sp.rows                                                             AS TableRows,
    sp.rows_sampled                                                     AS RowsSampled,
    CASE WHEN sp.rows > 0
         THEN CAST(100.0 * sp.rows_sampled / sp.rows AS DECIMAL(5,1))
         ELSE 0
    END                                                                 AS SamplePct,
    sp.modification_counter                                             AS ModCounter,
    CASE WHEN sp.rows > 0
         THEN CAST(100.0 * sp.modification_counter / sp.rows AS DECIMAL(8,2))
         ELSE 0
    END                                                                 AS ModPct
FROM sys.stats s
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
  AND sp.rows > 10000  -- Only care about non-trivial tables
ORDER BY sp.modification_counter DESC;
GO


-- ============================================================================
-- Query 5.6.2: Statistics Staleness Alert
-- Flags statistics that need attention
-- ============================================================================
SELECT
    OBJECT_SCHEMA_NAME(s.object_id) + '.' + OBJECT_NAME(s.object_id) AS TableName,
    s.name                                                              AS StatName,
    STATS_DATE(s.object_id, s.stats_id)                                AS LastUpdated,
    DATEDIFF(DAY, STATS_DATE(s.object_id, s.stats_id), GETDATE())     AS DaysStale,
    sp.modification_counter                                             AS ModCounter,
    CASE
        WHEN STATS_DATE(s.object_id, s.stats_id) IS NULL
            THEN 'CRITICAL: Never updated'
        WHEN DATEDIFF(DAY, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 7
            AND sp.modification_counter > 100000
            THEN 'WARNING: Stale + high modifications'
        WHEN sp.modification_counter > sp.rows * 0.20
            THEN 'WARNING: >20% rows modified since last update'
        WHEN DATEDIFF(DAY, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 30
            THEN 'INFO: >30 days old (may be fine for cold data)'
        ELSE 'OK'
    END                                                                 AS AlertLevel
FROM sys.stats s
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
  AND sp.rows > 10000
  AND (
    STATS_DATE(s.object_id, s.stats_id) IS NULL
    OR DATEDIFF(DAY, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 7
    OR sp.modification_counter > sp.rows * 0.10
  )
ORDER BY
    CASE
        WHEN STATS_DATE(s.object_id, s.stats_id) IS NULL THEN 1
        WHEN sp.modification_counter > sp.rows * 0.20 THEN 2
        ELSE 3
    END,
    sp.modification_counter DESC;
GO


-- ============================================================================
-- Query 5.6.3: Incremental Statistics Verification
-- Confirms incremental statistics are properly enabled
-- ============================================================================
SELECT
    OBJECT_SCHEMA_NAME(s.object_id) + '.' + OBJECT_NAME(s.object_id) AS TableName,
    s.name                                                              AS StatName,
    s.is_incremental                                                    AS IsIncremental,
    CASE WHEN s.is_incremental = 1 THEN 'OK - Incremental enabled'
         ELSE 'REVIEW - Not incremental (consider enabling)'
    END                                                                 AS Recommendation
FROM sys.stats s
JOIN sys.tables t ON s.object_id = t.object_id
JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
WHERE s.stats_id > 0
  AND OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
ORDER BY s.is_incremental, OBJECT_NAME(s.object_id), s.name;
GO


-- ============================================================================
-- SECTION 5.7: I/O PERFORMANCE MONITORING
-- ============================================================================
/*
  I/O is the #1 bottleneck for large partitioned tables. Monitoring
  file-level and per-object I/O helps identify hot spots and validates
  that partitioning is reducing I/O for hot workloads.
*/

-- ============================================================================
-- Query 5.7.1: File-Level I/O Statistics
-- Shows read/write latency per database file (maps to filegroups)
-- ============================================================================
SELECT
    f.name                                                     AS FileName,
    f.physical_name                                            AS PhysicalPath,
    fg.name                                                    AS Filegroup,
    CAST(s.num_of_reads AS BIGINT)                            AS Reads,
    CAST(s.num_of_writes AS BIGINT)                           AS Writes,
    CAST(s.num_of_bytes_read / 1048576.0 AS DECIMAL(12,2))   AS ReadMB,
    CAST(s.num_of_bytes_written / 1048576.0 AS DECIMAL(12,2)) AS WrittenMB,
    -- Read latency
    CASE WHEN s.num_of_reads > 0
         THEN CAST(s.io_stall_read_ms * 1.0 / s.num_of_reads AS DECIMAL(10,2))
         ELSE 0 END                                            AS AvgReadLatencyMs,
    -- Write latency
    CASE WHEN s.num_of_writes > 0
         THEN CAST(s.io_stall_write_ms * 1.0 / s.num_of_writes AS DECIMAL(10,2))
         ELSE 0 END                                            AS AvgWriteLatencyMs,
    -- Overall latency
    CASE WHEN (s.num_of_reads + s.num_of_writes) > 0
         THEN CAST(s.io_stall * 1.0 / (s.num_of_reads + s.num_of_writes) AS DECIMAL(10,2))
         ELSE 0 END                                            AS AvgLatencyMs,
    -- Flag high latency
    CASE
        WHEN s.num_of_reads > 0
             AND s.io_stall_read_ms * 1.0 / s.num_of_reads > 20 THEN '*** HIGH READ LATENCY ***'
        WHEN s.num_of_writes > 0
             AND s.io_stall_write_ms * 1.0 / s.num_of_writes > 20 THEN '*** HIGH WRITE LATENCY ***'
        ELSE 'OK'
    END                                                        AS LatencyAlert
FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) s
JOIN sys.database_files f ON s.file_id = f.file_id
LEFT JOIN sys.filegroups fg ON f.data_space_id = fg.data_space_id
ORDER BY s.io_stall DESC;
GO


-- ============================================================================
-- Query 5.7.2: Per-Index I/O Profile
-- Shows which indexes generate the most reads and writes
-- ============================================================================
SELECT TOP 25
    OBJECT_SCHEMA_NAME(ius.object_id) + '.' + OBJECT_NAME(ius.object_id) AS TableName,
    i.name                                                                 AS IndexName,
    i.type_desc                                                            AS IndexType,
    i.has_filter                                                           AS IsFiltered,
    ius.user_seeks                                                         AS Seeks,
    ius.user_scans                                                         AS Scans,
    ius.user_lookups                                                       AS Lookups,
    ius.user_seeks + ius.user_scans + ius.user_lookups                    AS TotalReads,
    ius.user_updates                                                       AS Writes,
    CASE WHEN (ius.user_seeks + ius.user_scans + ius.user_lookups) > 0
         THEN CAST(ius.user_updates * 1.0 /
              (ius.user_seeks + ius.user_scans + ius.user_lookups) AS DECIMAL(8,2))
         ELSE 999.99
    END                                                                    AS WriteToReadRatio,
    ius.last_user_seek                                                     AS LastSeek,
    ius.last_user_scan                                                     AS LastScan
FROM sys.dm_db_index_usage_stats ius
JOIN sys.indexes i ON ius.object_id = i.object_id AND ius.index_id = i.index_id
WHERE ius.database_id = DB_ID()
  AND ius.object_id > 100  -- Skip system objects
ORDER BY (ius.user_seeks + ius.user_scans + ius.user_lookups) DESC;
GO


-- ============================================================================
-- SECTION 5.8: OPERATIONAL ALERTS & THRESHOLDS
-- ============================================================================
/*
  This section creates a stored procedure that runs as a daily health
  check, evaluating multiple metrics against defined thresholds and
  producing a summary report.
*/

-- ============================================================================
-- Procedure: usp_PartitionHealthCheck
-- Purpose:   Daily operational health check for the partitioned environment
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_PartitionHealthCheck
    @AlertOnly  BIT = 0  -- 1 = show only items that need attention
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Results TABLE (
        CheckID     INT IDENTITY(1,1),
        Category    NVARCHAR(50),
        CheckName   NVARCHAR(200),
        Status      NVARCHAR(20),   -- 'OK', 'WARNING', 'CRITICAL'
        Details     NVARCHAR(MAX)
    );

    PRINT '============================================';
    PRINT '  PARTITION HEALTH CHECK';
    PRINT '  Run at: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
    PRINT '============================================';

    -- -----------------------------------------------------------------------
    -- CHECK 1: Partition count approaching limit
    -- -----------------------------------------------------------------------
    INSERT INTO @Results (Category, CheckName, Status, Details)
    SELECT 'PARTITION', 'Partition Count',
        CASE WHEN MAX(partition_number) > 12000 THEN 'CRITICAL'
             WHEN MAX(partition_number) > 10000 THEN 'WARNING'
             ELSE 'OK' END,
        'Current max partition number: ' + CAST(MAX(partition_number) AS VARCHAR(10))
        + ' of 15,000 limit'
    FROM sys.partitions
    WHERE OBJECTPROPERTY(object_id, 'IsUserTable') = 1;

    -- -----------------------------------------------------------------------
    -- CHECK 2: Empty partitions (sliding window health)
    -- -----------------------------------------------------------------------
    INSERT INTO @Results (Category, CheckName, Status, Details)
    SELECT 'PARTITION', 'Future Empty Partitions Available',
        CASE WHEN COUNT(*) >= 3 THEN 'OK'
             WHEN COUNT(*) >= 1 THEN 'WARNING'
             ELSE 'CRITICAL' END,
        CAST(COUNT(*) AS VARCHAR(10)) + ' empty future partitions available. '
        + 'Need at least 3 for sliding window buffer.'
    FROM sys.partitions p
    JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
    JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
    LEFT JOIN sys.partition_range_values prv
        ON pf.function_id = prv.function_id
        AND p.partition_number = prv.boundary_id + 1
    WHERE i.index_id IN (0, 1)
      AND p.rows = 0
      AND (prv.value IS NULL OR CAST(prv.value AS DATE) > GETDATE());

    -- -----------------------------------------------------------------------
    -- CHECK 3: Compression mismatches
    -- -----------------------------------------------------------------------
    INSERT INTO @Results (Category, CheckName, Status, Details)
    SELECT 'COMPRESSION', 'Compression Tier Compliance',
        CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'WARNING' END,
        CASE WHEN COUNT(*) = 0
            THEN 'All partitions match expected compression tiers.'
            ELSE CAST(COUNT(*) AS VARCHAR(10)) + ' partitions have compression mismatches. '
                 + 'Run usp_CompressionTierManagement to fix.'
        END
    FROM sys.partitions p
    JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
    JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
    LEFT JOIN sys.partition_range_values prv
        ON pf.function_id = prv.function_id
        AND p.partition_number = prv.boundary_id + 1
    WHERE i.index_id IN (0, 1)
      AND p.rows > 0
      AND p.data_compression_desc <>
          CASE
              WHEN prv.value IS NULL THEN p.data_compression_desc
              WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -2, GETDATE()) THEN 'NONE'
              WHEN CAST(prv.value AS DATE) > DATEADD(MONTH, -4, GETDATE()) THEN 'ROW'
              ELSE 'PAGE'
          END;

    -- -----------------------------------------------------------------------
    -- CHECK 4: Stale statistics on hot partitions
    -- -----------------------------------------------------------------------
    INSERT INTO @Results (Category, CheckName, Status, Details)
    SELECT 'STATISTICS', 'Hot Partition Statistics Freshness',
        CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'WARNING' END,
        CASE WHEN COUNT(*) = 0
            THEN 'All statistics on partitioned tables updated within 7 days.'
            ELSE CAST(COUNT(*) AS VARCHAR(10))
                 + ' statistics objects are >7 days stale with significant modifications.'
        END
    FROM sys.stats s
    CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
    WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
      AND sp.rows > 10000
      AND DATEDIFF(DAY, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 7
      AND sp.modification_counter > 100000;

    -- -----------------------------------------------------------------------
    -- CHECK 5: Index fragmentation on hot partitions
    -- -----------------------------------------------------------------------
    -- (Only checks partitions that SHOULD be maintained — avoids scanning cold)
    DECLARE @HotFragAlert INT = 0;
    SELECT @HotFragAlert = COUNT(*)
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ps
    WHERE ps.index_id > 0
      AND ps.page_count > 1000
      AND ps.avg_fragmentation_in_percent > 30
      AND ps.partition_number >= (
          SELECT MAX(prv.boundary_id)
          FROM sys.partition_functions pf
          JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id
          WHERE CAST(prv.value AS DATE) <= DATEADD(MONTH, -2, GETDATE())
      );

    INSERT INTO @Results (Category, CheckName, Status, Details)
    VALUES ('INDEX', 'Hot Partition Fragmentation',
        CASE WHEN @HotFragAlert = 0 THEN 'OK'
             WHEN @HotFragAlert <= 5 THEN 'WARNING'
             ELSE 'CRITICAL' END,
        CASE WHEN @HotFragAlert = 0
            THEN 'No hot partition indexes with >30% fragmentation.'
            ELSE CAST(@HotFragAlert AS VARCHAR(10))
                 + ' hot partition indexes exceed 30% fragmentation. '
                 + 'Check if nightly maintenance is running.'
        END);

    -- -----------------------------------------------------------------------
    -- CHECK 6: Maintenance job health (from Module 4 log)
    -- -----------------------------------------------------------------------
    IF OBJECT_ID('dbo.MaintenanceLog', 'U') IS NOT NULL
    BEGIN
        -- Check for recent failures
        DECLARE @RecentFailures INT;
        SELECT @RecentFailures = COUNT(*)
        FROM dbo.MaintenanceLog
        WHERE Status = 'Failed'
          AND ActionTime >= DATEADD(DAY, -1, GETDATE());

        INSERT INTO @Results (Category, CheckName, Status, Details)
        VALUES ('MAINTENANCE', 'Recent Maintenance Failures',
            CASE WHEN @RecentFailures = 0 THEN 'OK'
                 WHEN @RecentFailures <= 2 THEN 'WARNING'
                 ELSE 'CRITICAL' END,
            CASE WHEN @RecentFailures = 0
                THEN 'No maintenance failures in the last 24 hours.'
                ELSE CAST(@RecentFailures AS VARCHAR(10))
                     + ' maintenance failures in the last 24 hours. Review MaintenanceLog.'
            END);

        -- Check that maintenance actually ran
        DECLARE @LastMaintenance DATETIME;
        SELECT @LastMaintenance = MAX(ActionTime)
        FROM dbo.MaintenanceLog
        WHERE Status = 'Completed';

        INSERT INTO @Results (Category, CheckName, Status, Details)
        VALUES ('MAINTENANCE', 'Last Successful Maintenance',
            CASE WHEN @LastMaintenance >= DATEADD(DAY, -1, GETDATE()) THEN 'OK'
                 WHEN @LastMaintenance >= DATEADD(DAY, -3, GETDATE()) THEN 'WARNING'
                 ELSE 'CRITICAL' END,
            'Last successful maintenance action: '
            + ISNULL(CONVERT(VARCHAR(30), @LastMaintenance, 121), 'NEVER'));
    END
    ELSE
    BEGIN
        INSERT INTO @Results (Category, CheckName, Status, Details)
        VALUES ('MAINTENANCE', 'Maintenance Log Table',
            'WARNING', 'MaintenanceLog table not found. Deploy Module 4 procedures first.');
    END

    -- -----------------------------------------------------------------------
    -- CHECK 7: Buffer pool health (cold data evicting hot data)
    -- -----------------------------------------------------------------------
    DECLARE @ColdBufferPct DECIMAL(5,2);
    SELECT @ColdBufferPct = ISNULL(
        CAST(100.0 * SUM(CASE WHEN CAST(prv.value AS DATE) < DATEADD(MONTH, -4, GETDATE())
                              THEN bc.BufferedPages ELSE 0 END)
             / NULLIF(SUM(bc.BufferedPages), 0) AS DECIMAL(5,2)), 0)
    FROM sys.partitions p
    JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
    JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
    LEFT JOIN sys.partition_range_values prv
        ON pf.function_id = prv.function_id
        AND p.partition_number = prv.boundary_id + 1
    LEFT JOIN (
        SELECT a.container_id, COUNT(*) AS BufferedPages
        FROM sys.dm_os_buffer_descriptors b
        JOIN sys.allocation_units a ON b.allocation_unit_id = a.allocation_unit_id
        WHERE b.database_id = DB_ID()
        GROUP BY a.container_id
    ) bc ON p.partition_id = bc.container_id
    WHERE i.index_id IN (0, 1);

    INSERT INTO @Results (Category, CheckName, Status, Details)
    VALUES ('BUFFER_POOL', 'Cold Data in Buffer Pool',
        CASE WHEN @ColdBufferPct < 10 THEN 'OK'
             WHEN @ColdBufferPct < 25 THEN 'WARNING'
             ELSE 'CRITICAL' END,
        'Cold partition data represents ' + CAST(@ColdBufferPct AS VARCHAR(10))
        + '% of buffer pool. Target: <10%.');

    -- -----------------------------------------------------------------------
    -- CHECK 8: I/O latency
    -- -----------------------------------------------------------------------
    INSERT INTO @Results (Category, CheckName, Status, Details)
    SELECT 'IO', 'File Latency: ' + f.name,
        CASE WHEN (s.num_of_reads + s.num_of_writes) = 0 THEN 'OK'
             WHEN s.io_stall * 1.0 / (s.num_of_reads + s.num_of_writes) > 20 THEN 'CRITICAL'
             WHEN s.io_stall * 1.0 / (s.num_of_reads + s.num_of_writes) > 10 THEN 'WARNING'
             ELSE 'OK' END,
        'Avg latency: '
        + CAST(CASE WHEN (s.num_of_reads + s.num_of_writes) > 0
                    THEN s.io_stall * 1.0 / (s.num_of_reads + s.num_of_writes)
                    ELSE 0 END AS VARCHAR(10)) + ' ms'
    FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) s
    JOIN sys.database_files f ON s.file_id = f.file_id;

    -- -----------------------------------------------------------------------
    -- OUTPUT RESULTS
    -- -----------------------------------------------------------------------
    IF @AlertOnly = 1
        SELECT Category, CheckName, Status, Details
        FROM @Results
        WHERE Status IN ('WARNING', 'CRITICAL')
        ORDER BY
            CASE Status WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
            Category, CheckID;
    ELSE
        SELECT Category, CheckName, Status, Details
        FROM @Results
        ORDER BY
            CASE Status WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
            Category, CheckID;

    -- Summary counts
    SELECT
        SUM(CASE WHEN Status = 'OK' THEN 1 ELSE 0 END)        AS [OK],
        SUM(CASE WHEN Status = 'WARNING' THEN 1 ELSE 0 END)   AS [Warnings],
        SUM(CASE WHEN Status = 'CRITICAL' THEN 1 ELSE 0 END)  AS [Critical]
    FROM @Results;
END
GO


-- ============================================================================
-- SECTION 5.9: POC SIGN-OFF CHECKLIST
-- ============================================================================
/*
  This section provides a comprehensive validation checklist for the
  partitioning POC. Run each check and document the results before
  presenting the POC for team sign-off.

  ┌────┬────────────────────────────────────────────┬──────────┬─────────────┐
  │ #  │  Validation Item                           │  Module  │  Status     │
  ├────┼────────────────────────────────────────────┼──────────┼─────────────┤
  │  1 │  Partition function created correctly       │  1       │  □ Pass     │
  │  2 │  Partition scheme maps to correct FGs       │  1       │  □ Pass     │
  │  3 │  Data migrated with zero data loss          │  1b      │  □ Pass     │
  │  4 │  Row counts match pre/post migration        │  1b      │  □ Pass     │
  │  5 │  All indexes are partition-aligned           │  2       │  □ Pass     │
  │  6 │  Filtered indexes created on hot data        │  2       │  □ Pass     │
  │  7 │  OPTION(RECOMPILE) on filtered index procs  │  2       │  □ Pass     │
  │  8 │  SWITCH operation completes < 1 second       │  3       │  □ Pass     │
  │  9 │  Sliding window advances correctly           │  3       │  □ Pass     │
  │ 10 │  Nightly maintenance completes in window     │  4       │  □ Pass     │
  │ 11 │  Statistics are incremental                  │  4       │  □ Pass     │
  │ 12 │  Partition elimination verified               │  5       │  □ Pass     │
  │ 13 │  Buffer pool dominated by hot data            │  5       │  □ Pass     │
  │ 14 │  I/O latency within acceptable thresholds     │  5       │  □ Pass     │
  │ 15 │  No compression mismatches                    │  5       │  □ Pass     │
  │ 16 │  Health check procedure returns all OK        │  5       │  □ Pass     │
  └────┴────────────────────────────────────────────┴──────────┴─────────────┘
*/

-- ============================================================================
-- Procedure: usp_POCSignOffValidation
-- Purpose:   Runs all POC validation checks and produces a pass/fail report
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_POCSignOffValidation
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Results TABLE (
        CheckNum    INT,
        Category    NVARCHAR(50),
        CheckName   NVARCHAR(200),
        Result      NVARCHAR(20),   -- 'PASS', 'FAIL', 'REVIEW'
        Details     NVARCHAR(MAX)
    );

    PRINT '============================================';
    PRINT '  POC SIGN-OFF VALIDATION';
    PRINT '  Run at: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
    PRINT '============================================';

    -- -----------------------------------------------------------------------
    -- CHECK 1: Partition function exists
    -- -----------------------------------------------------------------------
    INSERT INTO @Results
    SELECT 1, 'Module 1', 'Partition Function Exists',
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
        'Found ' + CAST(COUNT(*) AS VARCHAR(5)) + ' partition function(s).'
    FROM sys.partition_functions;

    -- -----------------------------------------------------------------------
    -- CHECK 2: Partition scheme exists and maps correctly
    -- -----------------------------------------------------------------------
    INSERT INTO @Results
    SELECT 2, 'Module 1', 'Partition Scheme Exists',
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
        'Found ' + CAST(COUNT(*) AS VARCHAR(5)) + ' partition scheme(s).'
    FROM sys.partition_schemes;

    -- -----------------------------------------------------------------------
    -- CHECK 3: Partitioned tables exist with multiple partitions
    -- -----------------------------------------------------------------------
    INSERT INTO @Results
    SELECT 3, 'Module 1', 'Tables Are Partitioned',
        CASE WHEN MAX(partition_number) > 1 THEN 'PASS' ELSE 'FAIL' END,
        'Max partition number: ' + CAST(ISNULL(MAX(partition_number), 0) AS VARCHAR(10))
    FROM sys.partitions
    WHERE OBJECTPROPERTY(object_id, 'IsUserTable') = 1
      AND index_id IN (0, 1);

    -- -----------------------------------------------------------------------
    -- CHECK 4: All NCIs are partition-aligned
    -- -----------------------------------------------------------------------
    DECLARE @NonAligned INT;
    SELECT @NonAligned = COUNT(*)
    FROM sys.indexes i
    JOIN sys.tables t ON i.object_id = t.object_id
    WHERE i.index_id > 1
      AND i.data_space_id NOT IN (SELECT data_space_id FROM sys.partition_schemes)
      AND EXISTS (
          SELECT 1 FROM sys.indexes ci
          JOIN sys.partition_schemes ps ON ci.data_space_id = ps.data_space_id
          WHERE ci.object_id = i.object_id AND ci.index_id IN (0, 1)
      );

    INSERT INTO @Results
    VALUES (4, 'Module 2', 'All Indexes Partition-Aligned',
        CASE WHEN @NonAligned = 0 THEN 'PASS' ELSE 'FAIL' END,
        CASE WHEN @NonAligned = 0
            THEN 'All nonclustered indexes are aligned with the partition scheme.'
            ELSE CAST(@NonAligned AS VARCHAR(5)) + ' non-aligned indexes found.' END);

    -- -----------------------------------------------------------------------
    -- CHECK 5: Filtered indexes exist
    -- -----------------------------------------------------------------------
    INSERT INTO @Results
    SELECT 5, 'Module 2', 'Filtered Indexes Deployed',
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'REVIEW' END,
        'Found ' + CAST(COUNT(*) AS VARCHAR(5)) + ' filtered index(es).'
    FROM sys.indexes
    WHERE has_filter = 1;

    -- -----------------------------------------------------------------------
    -- CHECK 6: Incremental statistics enabled
    -- -----------------------------------------------------------------------
    DECLARE @IncrStats INT, @NonIncrStats INT;
    SELECT
        @IncrStats = SUM(CASE WHEN s.is_incremental = 1 THEN 1 ELSE 0 END),
        @NonIncrStats = SUM(CASE WHEN s.is_incremental = 0 THEN 1 ELSE 0 END)
    FROM sys.stats s
    JOIN sys.tables t ON s.object_id = t.object_id
    JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
    JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id;

    INSERT INTO @Results
    VALUES (6, 'Module 4', 'Incremental Statistics',
        CASE WHEN ISNULL(@IncrStats, 0) > 0 THEN 'PASS' ELSE 'REVIEW' END,
        'Incremental: ' + CAST(ISNULL(@IncrStats, 0) AS VARCHAR(5))
        + ', Non-incremental: ' + CAST(ISNULL(@NonIncrStats, 0) AS VARCHAR(5)));

    -- -----------------------------------------------------------------------
    -- CHECK 7: Compression applied to cold partitions
    -- -----------------------------------------------------------------------
    DECLARE @UncompressedCold INT;
    SELECT @UncompressedCold = COUNT(*)
    FROM sys.partitions p
    JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
    JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
    LEFT JOIN sys.partition_range_values prv
        ON pf.function_id = prv.function_id
        AND p.partition_number = prv.boundary_id + 1
    WHERE i.index_id IN (0, 1)
      AND p.rows > 0
      AND prv.value IS NOT NULL
      AND CAST(prv.value AS DATE) < DATEADD(MONTH, -4, GETDATE())
      AND p.data_compression_desc = 'NONE';

    INSERT INTO @Results
    VALUES (7, 'Module 4', 'Cold Partition Compression',
        CASE WHEN ISNULL(@UncompressedCold, 0) = 0 THEN 'PASS' ELSE 'WARNING' END,
        CASE WHEN ISNULL(@UncompressedCold, 0) = 0
            THEN 'All cold partitions have compression applied.'
            ELSE CAST(@UncompressedCold AS VARCHAR(5))
                 + ' cold partitions still uncompressed.' END);

    -- -----------------------------------------------------------------------
    -- CHECK 8: Health check passes
    -- -----------------------------------------------------------------------
    INSERT INTO @Results
    VALUES (8, 'Module 5', 'Health Check (run separately)',
        'REVIEW', 'Run EXEC usp_PartitionHealthCheck to verify all checks pass.');

    -- -----------------------------------------------------------------------
    -- OUTPUT
    -- -----------------------------------------------------------------------
    SELECT CheckNum, Category, CheckName, Result, Details
    FROM @Results
    ORDER BY CheckNum;

    -- Summary
    SELECT
        SUM(CASE WHEN Result = 'PASS' THEN 1 ELSE 0 END)    AS [Passed],
        SUM(CASE WHEN Result = 'FAIL' THEN 1 ELSE 0 END)    AS [Failed],
        SUM(CASE WHEN Result = 'REVIEW' THEN 1 ELSE 0 END)  AS [Needs Review]
    FROM @Results;

    PRINT '';
    PRINT '============================================';
    PRINT '  VALIDATION COMPLETE';
    PRINT '============================================';
END
GO


-- ============================================================================
-- QUICK REFERENCE: Key monitoring queries to bookmark
-- ============================================================================
/*
  Daily Operations:
    EXEC dbo.usp_PartitionHealthCheck;                    -- Full health check
    EXEC dbo.usp_PartitionHealthCheck @AlertOnly = 1;     -- Alerts only

  Before POC Sign-Off:
    EXEC dbo.usp_POCSignOffValidation;                    -- Full validation

  Performance Baselines:
    EXEC dbo.usp_CapturePerformanceBaseline @SnapshotName = 'PRE_PARTITION';
    -- ... deploy partitioning ...
    EXEC dbo.usp_CapturePerformanceBaseline @SnapshotName = 'POST_PARTITION';
    -- Then run the comparison query in Section 5.1

  Ad-Hoc Monitoring:
    Query 5.2.1  — Partition map (sizes, rows, temperatures)
    Query 5.2.2  — Size distribution by temperature
    Query 5.3.2  — Partition elimination audit (plan cache)
    Query 5.5.2  — Buffer pool by partition (cold data leak?)
    Query 5.6.1  — Statistics freshness
    Query 5.7.1  — I/O latency per file

  Troubleshooting:
    Query 5.3.3  — Top queries by logical reads
    Query 5.4.1  — Wait statistics
    Query 5.6.2  — Stale statistics alerts
    Query 5.7.2  — Per-index I/O profile
*/

PRINT '===============================================================';
PRINT '  Module 5: Monitoring & Performance Validation — loaded.';
PRINT '  Run usp_PartitionHealthCheck for a quick health assessment.';
PRINT '  Run usp_POCSignOffValidation for full POC validation.';
PRINT '===============================================================';
GO
