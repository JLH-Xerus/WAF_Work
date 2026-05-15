/*
================================================================================
  SQL Server 2019 Table Partitioning POC - Module 2
  Indexing Strategy for High-Volume OLTP with Hot/Cold Data
================================================================================
  Author:       Justin Hunter
  Date:         2026-02-22
  SQL Server:   2019 (Enterprise Edition)
  Purpose:      Comprehensive indexing strategy for partitioned tables with
                emphasis on filtered indexes, maintenance, and query performance

  Module 2 covers:
    2.1  Indexing fundamentals in a partitioned context
    2.2  Aligned vs non-aligned indexes
    2.3  Filtered indexes for hot/cold data
    2.4  Columnstore indexes for analytical queries on cold data
    2.5  Index maintenance strategy (partition-aware)
    2.6  Diagnosing index problems (missing indexes, unused indexes, overlap)
    2.7  Stored procedure and query performance considerations
================================================================================
*/

USE PartitioningPOC;
GO

-- ============================================================================
-- SECTION 2.1: INDEXING IN A PARTITIONED CONTEXT
-- ============================================================================
/*
  Indexing a partitioned table follows the same principles as indexing any
  table, but with important additional considerations:

  1. PARTITION ALIGNMENT: Should the index be "aligned" with the partition
     scheme (partition key in the index, index built ON the scheme)?

  2. PARTITION ELIMINATION: The optimizer can only skip partitions if the
     partition key is in the query predicate. Index design must support this.

  3. MAINTENANCE GRANULARITY: Aligned indexes can be rebuilt per-partition.
     Non-aligned indexes must be rebuilt as a whole.

  4. FILTERED INDEXES: SQL Server supports filtered indexes with WHERE
     clauses. These are extremely powerful for hot/cold workloads because
     you can build indexes that ONLY cover the active data.

  5. COLUMNSTORE: For analytical queries against archived (cold) data,
     columnstore indexes provide massive compression and scan performance.
*/


-- ============================================================================
-- SECTION 2.2: ALIGNED VS NON-ALIGNED INDEXES
-- ============================================================================
/*
  ALIGNED INDEX:
    - Built ON the same partition scheme as the table
    - The partition key is included in the index key or as an included column
    - Each index partition corresponds 1:1 with a table partition
    - CAN be maintained (rebuilt, reorganized) per-partition
    - REQUIRED for partition SWITCH operations
    - Enables collocated joins between partitioned tables

  NON-ALIGNED INDEX:
    - Built on a specific filegroup (e.g., PRIMARY) or a different scheme
    - Does NOT include the partition key
    - The index is a single structure spanning all partitions
    - CANNOT be maintained per-partition (whole-index rebuild only)
    - BLOCKS partition SWITCH operations
    - Can enforce global uniqueness without the partition key

  RECOMMENDATION: Default to aligned indexes. Only use non-aligned when
  you absolutely need global uniqueness on a column other than the
  partition key AND you don't need SWITCH operations.

  In your scenario (OLTP with SWITCH-based data lifecycle), ALL indexes
  should be aligned.
*/

-- Example: Aligned nonclustered index
-- The partition key (TransactionDate) is in the index key
CREATE NONCLUSTERED INDEX IX_Transactions_StatusDate_Aligned
ON dbo.Transactions (StatusCode, TransactionDate)
INCLUDE (AccountID, Amount)
ON ps_TransactionDate(TransactionDate);  -- ALIGNED
GO

-- Example: What a non-aligned index looks like (for illustration only)
-- CREATE NONCLUSTERED INDEX IX_Transactions_RefNum_NonAligned
-- ON dbo.Transactions (ReferenceNumber)
-- ON [PRIMARY];  -- NON-ALIGNED: lives on PRIMARY, not the partition scheme
-- GO
-- WARNING: This blocks SWITCH. Don't do this in your scenario.

-- Verify alignment
SELECT
    i.name                  AS IndexName,
    i.type_desc             AS IndexType,
    ds.name                 AS DataSpace,
    ds.type_desc            AS DataSpaceType,
    CASE ds.type_desc
        WHEN 'PARTITION_SCHEME' THEN 'ALIGNED'
        ELSE 'NON-ALIGNED'
    END                     AS Alignment
FROM sys.indexes i
JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
WHERE i.object_id = OBJECT_ID('dbo.Transactions')
ORDER BY i.index_id;
GO


-- ============================================================================
-- SECTION 2.3: FILTERED INDEXES FOR HOT/COLD DATA
-- ============================================================================
/*
  Filtered indexes are the single most impactful indexing technique for your
  workload. Here's why:

  THE PROBLEM:
    Your Transactions table has 6 billion rows, but only the last ~60 days
    are "active." That's maybe 200-300 million rows. The other 5.7+ billion
    rows are dead weight in every index. Queries looking for active
    transactions must wade through index entries for years of archived data.

  THE SOLUTION:
    A filtered index with a WHERE clause that targets only active data.
    This index is:
      - DRAMATICALLY smaller (5-10% of the full index size)
      - DRAMATICALLY faster to scan and seek
      - DRAMATICALLY faster to maintain (rebuild, statistics update)
      - Always in the buffer pool (because it's small enough to fit)

  THE CATCH:
    The filter predicate must be a constant expression. You CANNOT use
    GETDATE() or any dynamic expression in the filter. This means you
    need a maintenance process that periodically drops and recreates
    filtered indexes with updated date boundaries.

    BUT: this is a feature, not a bug. The maintenance process is lightweight
    (creating a filtered index on 200M rows takes minutes, not hours) and
    gives you precise control over what's "hot."

  IMPORTANT QUERY COMPATIBILITY RULES:
    1. Queries must include the filter predicate (or a subset of it) in the
       WHERE clause for the optimizer to consider the filtered index.
    2. Parameterized queries (including stored procedures) may NOT use
       filtered indexes unless you use OPTION (RECOMPILE) or the parameter
       value is sniffed to match the filter.
    3. The safest approach: use literal values in queries or add
       OPTION (RECOMPILE) to critical queries that should use filtered indexes.
*/

-- ----------------------------------------------------------------------------
-- 2.3.1  Filtered index on active transactions (StatusCode-based)
-- ----------------------------------------------------------------------------
/*
  If your StatusCode reliably indicates "active" vs "completed/archived",
  this is the simplest approach. The filter doesn't need date-based
  maintenance.
*/

CREATE NONCLUSTERED INDEX IX_Transactions_Active_ByAccount
ON dbo.Transactions (AccountID, TransactionDate)
INCLUDE (Amount, TransactionTypeID, ReferenceNumber)
WHERE StatusCode = 'A'
ON ps_TransactionDate(TransactionDate);
GO

-- This index covers queries like:
-- SELECT AccountID, TransactionDate, Amount, TransactionTypeID, ReferenceNumber
-- FROM dbo.Transactions
-- WHERE StatusCode = 'A'
-- AND AccountID = @AccountID
-- AND TransactionDate >= '2026-01-01';


-- ----------------------------------------------------------------------------
-- 2.3.2  Filtered index on recent transactions (date-based)
-- ----------------------------------------------------------------------------
/*
  When the "hot" definition is time-based (last 60 days), use a date filter.
  This requires periodic maintenance to update the boundary date.

  STRATEGY: Set the boundary to 90 days ago (buffer beyond the 60-day active
  window) and refresh monthly. This way the index always covers the full
  active window plus a 30-day buffer.
*/

-- Create the filtered index with a static date boundary
-- This date should be refreshed monthly by your maintenance job
CREATE NONCLUSTERED INDEX IX_Transactions_Recent_ByAccount
ON dbo.Transactions (AccountID, TransactionDate DESC)
INCLUDE (Amount, StatusCode, TransactionTypeID)
WHERE TransactionDate >= '2025-11-24'   -- ~90 days before today (2026-02-22)
ON ps_TransactionDate(TransactionDate);
GO

-- Queries that benefit from this index:
-- SELECT TOP 20 *
-- FROM dbo.Transactions
-- WHERE AccountID = @AccountID
-- AND TransactionDate >= '2025-12-01'   -- Within the filter range
-- ORDER BY TransactionDate DESC;
--
-- The optimizer sees that '2025-12-01' >= '2025-11-24' (the filter boundary)
-- and knows the filtered index covers this query.


-- ----------------------------------------------------------------------------
-- 2.3.3  Filtered index maintenance procedure
-- ----------------------------------------------------------------------------
/*
  This procedure refreshes date-based filtered indexes by dropping and
  recreating them with an updated boundary date. Run this monthly or as
  part of your maintenance window.

  WHY DROP/CREATE INSTEAD OF ALTER?
    SQL Server does not support ALTER INDEX to change a filter predicate.
    You must DROP and CREATE. The good news: because the filtered index
    is small (only recent data), the CREATE is fast.
*/

CREATE OR ALTER PROCEDURE dbo.usp_RefreshFilteredIndexes
    @DaysBack       INT = 90,       -- How far back the filter should reach
    @DryRun         BIT = 0         -- 1 = print statements without executing
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CutoffDate     DATE = DATEADD(DAY, -@DaysBack, CAST(GETDATE() AS DATE));
    DECLARE @CutoffStr      VARCHAR(10) = CONVERT(VARCHAR(10), @CutoffDate, 120);
    DECLARE @SQL            NVARCHAR(MAX);

    PRINT 'Refreshing filtered indexes with cutoff date: ' + @CutoffStr;

    -- Drop existing filtered index
    SET @SQL = N'
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = ''IX_Transactions_Recent_ByAccount''
               AND object_id = OBJECT_ID(''dbo.Transactions''))
        DROP INDEX IX_Transactions_Recent_ByAccount ON dbo.Transactions;';

    PRINT @SQL;
    IF @DryRun = 0 EXEC sp_executesql @SQL;

    -- Recreate with updated filter
    SET @SQL = N'
    CREATE NONCLUSTERED INDEX IX_Transactions_Recent_ByAccount
    ON dbo.Transactions (AccountID, TransactionDate DESC)
    INCLUDE (Amount, StatusCode, TransactionTypeID)
    WHERE TransactionDate >= ''' + @CutoffStr + '''
    ON ps_TransactionDate(TransactionDate)
    WITH (ONLINE = ON, SORT_IN_TEMPDB = ON, DATA_COMPRESSION = ROW);';

    PRINT @SQL;
    IF @DryRun = 0 EXEC sp_executesql @SQL;

    PRINT 'Filtered index refresh complete.';
END
GO

-- Test it
EXEC dbo.usp_RefreshFilteredIndexes @DaysBack = 90, @DryRun = 1;
GO


-- ----------------------------------------------------------------------------
-- 2.3.4  Filtered indexes and stored procedures: the parameterization trap
-- ----------------------------------------------------------------------------
/*
  THIS IS THE MOST COMMON PITFALL WITH FILTERED INDEXES.

  SQL Server's query optimizer will NOT use a filtered index when the query
  uses parameters (variables or procedure parameters) in the WHERE clause
  that correspond to the filter predicate -- UNLESS:

    a) The query includes OPTION (RECOMPILE), OR
    b) The parameter values are compile-time constants (rare)

  WHY? Because at compile time, the optimizer doesn't know if the parameter
  value will fall within the filter range. It plays it safe and ignores the
  filtered index.

  SOLUTION: Add OPTION (RECOMPILE) to queries that should use filtered
  indexes. The recompile cost is trivial compared to scanning a 6B-row
  full index.

  DEMONSTRATION:
*/

-- This stored procedure will NOT use the filtered index (no RECOMPILE):
CREATE OR ALTER PROCEDURE dbo.usp_GetActiveTransactions_BAD
    @AccountID INT
AS
BEGIN
    SET NOCOUNT ON;

    -- The optimizer cannot prove @AccountID matches the filter
    -- and more importantly, it doesn't know if the date parameter
    -- will be within the filtered range
    SELECT TransactionID, TransactionDate, Amount, StatusCode
    FROM dbo.Transactions
    WHERE AccountID = @AccountID
    AND StatusCode = 'A'
    ORDER BY TransactionDate DESC;
END
GO

-- This stored procedure WILL use the filtered index:
CREATE OR ALTER PROCEDURE dbo.usp_GetActiveTransactions_GOOD
    @AccountID INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT TransactionID, TransactionDate, Amount, StatusCode
    FROM dbo.Transactions
    WHERE AccountID = @AccountID
    AND StatusCode = 'A'
    ORDER BY TransactionDate DESC
    OPTION (RECOMPILE);  -- Forces the optimizer to evaluate the actual parameter value
END
GO

-- Compare execution plans:
SET STATISTICS IO ON;
EXEC dbo.usp_GetActiveTransactions_BAD @AccountID = 1;
EXEC dbo.usp_GetActiveTransactions_GOOD @AccountID = 1;
SET STATISTICS IO OFF;
GO
-- Check the execution plans: the _GOOD version should show the filtered index


-- ----------------------------------------------------------------------------
-- 2.3.5  Filtered index design patterns for OLTP hot/cold
-- ----------------------------------------------------------------------------
/*
  Here are the filtered index patterns most useful for your workload.
  Each targets a specific query pattern.
*/

-- Pattern 1: Active transactions by account (most common OLTP lookup)
-- Covers: "Show me all active transactions for account X"
CREATE NONCLUSTERED INDEX IX_Filt_ActiveByAccount
ON dbo.Transactions (AccountID, TransactionDate DESC)
INCLUDE (Amount, StatusCode, TransactionTypeID, ReferenceNumber)
WHERE StatusCode = 'A'
ON ps_TransactionDate(TransactionDate);
GO

-- Pattern 2: Recent transactions by type (for reporting/dashboards)
-- Covers: "How many Type 3 transactions in the last 30 days?"
CREATE NONCLUSTERED INDEX IX_Filt_RecentByType
ON dbo.Transactions (TransactionTypeID, TransactionDate)
INCLUDE (Amount, AccountID)
WHERE TransactionDate >= '2025-11-24'
ON ps_TransactionDate(TransactionDate);
GO

-- Pattern 3: Pending/incomplete transactions (operational monitoring)
-- Covers: "Show all transactions that haven't completed yet"
CREATE NONCLUSTERED INDEX IX_Filt_Pending
ON dbo.Transactions (CreatedDate DESC)
INCLUDE (AccountID, Amount, StatusCode, TransactionTypeID)
WHERE StatusCode = 'A'
ON ps_TransactionDate(TransactionDate);
GO

-- Pattern 4: High-value active transactions (fraud/risk monitoring)
-- Covers: "Flag all active transactions over $10,000"
CREATE NONCLUSTERED INDEX IX_Filt_HighValue
ON dbo.Transactions (Amount DESC, TransactionDate DESC)
INCLUDE (AccountID, StatusCode, ReferenceNumber)
WHERE StatusCode = 'A' AND Amount > 10000.00
ON ps_TransactionDate(TransactionDate);
GO


-- ============================================================================
-- SECTION 2.4: COLUMNSTORE INDEXES FOR COLD DATA ANALYTICS
-- ============================================================================
/*
  While filtered indexes optimize access to HOT data, columnstore indexes
  are the best strategy for analytical queries against COLD data (archived
  transactions).

  WHY COLUMNSTORE FOR COLD DATA?
    - Massive compression (10x-15x vs row store) = less storage, less I/O
    - Batch mode execution = orders of magnitude faster for aggregations
    - Segment elimination = columnstore's version of partition elimination
    - Perfect for queries like: "Total transactions by type for 2024"

  COLUMNSTORE OPTIONS:
    1. CLUSTERED COLUMNSTORE INDEX (CCI): Replaces the B-tree structure.
       Not suitable for your OLTP table (writes would be slow).

    2. NONCLUSTERED COLUMNSTORE INDEX (NCCI): Overlays the existing B-tree.
       This is the right choice -- it adds analytical capability without
       affecting OLTP write performance.

  PARTITION-LEVEL STRATEGY:
    Apply the NCCI only to COLD partitions using a filtered WHERE clause.
    Hot partitions (recent data) remain B-tree only for optimal write perf.
*/

-- Nonclustered columnstore on cold data only
-- This covers analytical queries against historical transactions
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Transactions_Cold
ON dbo.Transactions (
    TransactionDate,
    AccountID,
    TransactionTypeID,
    Amount,
    StatusCode
)
WHERE TransactionDate < '2025-12-01'   -- Only cold data (before hot window)
ON ps_TransactionDate(TransactionDate);
GO

/*
  Queries that benefit:
    SELECT TransactionTypeID, MONTH(TransactionDate),
           COUNT(*), SUM(Amount), AVG(Amount)
    FROM dbo.Transactions
    WHERE TransactionDate >= '2025-01-01' AND TransactionDate < '2025-12-01'
    GROUP BY TransactionTypeID, MONTH(TransactionDate)
    OPTION (RECOMPILE);

  The optimizer uses batch mode on the columnstore index, scanning compressed
  column segments instead of individual rows. For billions of archived rows,
  this is 10-100x faster than a B-tree scan.
*/


-- ============================================================================
-- SECTION 2.5: INDEX MAINTENANCE STRATEGY (PARTITION-AWARE)
-- ============================================================================
/*
  Traditional index maintenance (rebuild all indexes on a schedule) doesn't
  scale to billions of rows. With partitioning, you can be surgical:

  HOT PARTITIONS (recent months):
    - High write activity = high fragmentation
    - Rebuild ONLINE weekly (or even daily for the current partition)
    - Use ROW compression (good compression with minimal CPU for writes)

  COLD PARTITIONS (older months):
    - Zero or near-zero writes = low/no fragmentation after initial load
    - Rebuild ONCE after the partition transitions from hot to cold
    - Use PAGE compression (better compression, acceptable since no writes)
    - After that, skip maintenance entirely (no writes = no fragmentation)

  ARCHIVE PARTITION:
    - Never changes
    - Compress with PAGE (or even COLUMNSTORE via NCCI)
    - Exclude from all maintenance jobs

  THE KEY INSIGHT: In your workload, 95%+ of fragmentation happens in the
  current and previous month's partitions. Your maintenance job should
  focus exclusively there and ignore everything else.
*/

-- ----------------------------------------------------------------------------
-- 2.5.1  Partition-aware maintenance procedure
-- ----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE dbo.usp_PartitionAwareMaintenance
    @TableName          SYSNAME,
    @HotPartitionCount  INT = 3,        -- Number of recent partitions to maintain
    @FragThreshold_Reorg    FLOAT = 10.0,
    @FragThreshold_Rebuild  FLOAT = 30.0,
    @OnlineRebuild      BIT = 1,
    @DryRun             BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ObjectID       INT = OBJECT_ID(@TableName);
    DECLARE @MaxPartition   INT;
    DECLARE @MinHotPartition INT;
    DECLARE @SQL            NVARCHAR(MAX);

    IF @ObjectID IS NULL
    BEGIN
        RAISERROR('Table not found: %s', 16, 1, @TableName);
        RETURN;
    END

    -- Find the range of partitions to maintain
    SELECT @MaxPartition = MAX(partition_number)
    FROM sys.partitions
    WHERE object_id = @ObjectID AND index_id IN (0, 1);

    SET @MinHotPartition = @MaxPartition - @HotPartitionCount + 1;
    IF @MinHotPartition < 1 SET @MinHotPartition = 1;

    PRINT '================================================================';
    PRINT 'Partition-Aware Index Maintenance for: ' + @TableName;
    PRINT 'Hot partitions: ' + CAST(@MinHotPartition AS VARCHAR) + ' to ' + CAST(@MaxPartition AS VARCHAR);
    PRINT 'Fragmentation thresholds: REORG=' + CAST(@FragThreshold_Reorg AS VARCHAR)
        + '%, REBUILD=' + CAST(@FragThreshold_Rebuild AS VARCHAR) + '%';
    PRINT '================================================================';

    -- Cursor through hot partition indexes
    DECLARE maint_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            i.name AS IndexName,
            ips.partition_number,
            ips.avg_fragmentation_in_percent,
            ips.page_count,
            p.data_compression_desc
        FROM sys.indexes i
        CROSS APPLY sys.dm_db_index_physical_stats(
            DB_ID(), @ObjectID, i.index_id, NULL, 'LIMITED'
        ) ips
        JOIN sys.partitions p
            ON p.object_id = i.object_id
            AND p.index_id = i.index_id
            AND p.partition_number = ips.partition_number
        WHERE i.object_id = @ObjectID
        AND i.type > 0  -- Skip heaps
        AND ips.partition_number BETWEEN @MinHotPartition AND @MaxPartition
        AND ips.page_count > 1000  -- Skip tiny partitions
        AND ips.avg_fragmentation_in_percent > @FragThreshold_Reorg
        ORDER BY ips.partition_number, i.index_id;

    DECLARE @IndexName      SYSNAME;
    DECLARE @PartNum        INT;
    DECLARE @FragPct        FLOAT;
    DECLARE @PageCount      BIGINT;
    DECLARE @Compression    VARCHAR(60);

    OPEN maint_cursor;
    FETCH NEXT FROM maint_cursor INTO @IndexName, @PartNum, @FragPct, @PageCount, @Compression;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @FragPct >= @FragThreshold_Rebuild
        BEGIN
            -- REBUILD this partition
            SET @SQL = N'ALTER INDEX ' + QUOTENAME(@IndexName)
                + N' ON ' + @TableName
                + N' REBUILD PARTITION = ' + CAST(@PartNum AS NVARCHAR(10))
                + CASE WHEN @OnlineRebuild = 1 THEN N' WITH (ONLINE = ON, SORT_IN_TEMPDB = ON)' ELSE N'' END
                + N';';

            PRINT 'REBUILD: ' + @IndexName + ' partition ' + CAST(@PartNum AS VARCHAR)
                + ' (frag=' + CAST(CAST(@FragPct AS DECIMAL(5,1)) AS VARCHAR) + '%, pages=' + CAST(@PageCount AS VARCHAR) + ')';
        END
        ELSE
        BEGIN
            -- REORGANIZE this partition
            SET @SQL = N'ALTER INDEX ' + QUOTENAME(@IndexName)
                + N' ON ' + @TableName
                + N' REORGANIZE PARTITION = ' + CAST(@PartNum AS NVARCHAR(10))
                + N';';

            PRINT 'REORG:   ' + @IndexName + ' partition ' + CAST(@PartNum AS VARCHAR)
                + ' (frag=' + CAST(CAST(@FragPct AS DECIMAL(5,1)) AS VARCHAR) + '%, pages=' + CAST(@PageCount AS VARCHAR) + ')';
        END

        IF @DryRun = 0
            EXEC sp_executesql @SQL;

        FETCH NEXT FROM maint_cursor INTO @IndexName, @PartNum, @FragPct, @PageCount, @Compression;
    END

    CLOSE maint_cursor;
    DEALLOCATE maint_cursor;

    -- Update statistics on hot partitions
    PRINT '';
    PRINT 'Updating statistics on hot partitions...';

    SET @SQL = N'UPDATE STATISTICS ' + @TableName + N' WITH RESAMPLE;';
    PRINT @SQL;
    IF @DryRun = 0
        EXEC sp_executesql @SQL;

    PRINT '================================================================';
    PRINT 'Maintenance complete.';
    PRINT '================================================================';
END
GO

-- Test it
EXEC dbo.usp_PartitionAwareMaintenance
    @TableName = 'dbo.Transactions',
    @HotPartitionCount = 3,
    @DryRun = 1;
GO


-- ----------------------------------------------------------------------------
-- 2.5.2  Compression strategy by partition temperature
-- ----------------------------------------------------------------------------
/*
  Compression reduces storage and I/O at the cost of CPU. The right
  compression level depends on the partition's temperature:

  ┌──────────────┬─────────────────┬────────────────────────────────────────┐
  │  Temperature  │  Compression    │  Rationale                             │
  ├──────────────┼─────────────────┼────────────────────────────────────────┤
  │  HOT          │  NONE or ROW    │  High write throughput is priority.    │
  │  (current)    │                 │  ROW adds minimal CPU overhead.        │
  ├──────────────┼─────────────────┼────────────────────────────────────────┤
  │  WARM         │  ROW            │  Moderate writes. ROW is a good        │
  │  (1-2 months) │                 │  balance.                              │
  ├──────────────┼─────────────────┼────────────────────────────────────────┤
  │  COLD         │  PAGE           │  No/minimal writes. PAGE gives 50-70%  │
  │  (3+ months)  │                 │  compression. Reads benefit from       │
  │               │                 │  reduced I/O.                          │
  ├──────────────┼─────────────────┼────────────────────────────────────────┤
  │  ARCHIVE      │  PAGE +         │  Never written. Maximum compression.   │
  │  (years old)  │  Columnstore    │  NCCI overlay for analytics.           │
  └──────────────┴─────────────────┴────────────────────────────────────────┘
*/

-- Apply PAGE compression to cold partitions (partition 1 = archive)
ALTER TABLE dbo.Transactions
REBUILD PARTITION = 1
WITH (DATA_COMPRESSION = PAGE, ONLINE = ON);
GO

-- Apply ROW compression to warm partitions
ALTER TABLE dbo.Transactions
REBUILD PARTITION = 5
WITH (DATA_COMPRESSION = ROW, ONLINE = ON);
GO

-- Verify compression status across all partitions
SELECT
    p.partition_number,
    p.rows,
    p.data_compression_desc AS Compression,
    fg.name AS Filegroup,
    SUM(a.total_pages) * 8 / 1024 AS TotalSize_MB
FROM sys.partitions p
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.destination_data_spaces dds
    ON i.data_space_id = dds.partition_scheme_id
    AND p.partition_number = dds.destination_id
JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE p.object_id = OBJECT_ID('dbo.Transactions')
AND i.type = 1
GROUP BY p.partition_number, p.rows, p.data_compression_desc, fg.name
ORDER BY p.partition_number;
GO


-- ============================================================================
-- SECTION 2.6: DIAGNOSING INDEX PROBLEMS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 2.6.1  Missing index DMV analysis
-- ----------------------------------------------------------------------------
/*
  SQL Server tracks index recommendations via the missing index DMVs.
  These are accumulated since the last service restart and represent
  actual query patterns that the optimizer wished it had an index for.

  IMPORTANT CAVEATS:
    - Missing index suggestions don't account for write overhead
    - They don't consider existing indexes that partially cover the need
    - They suggest individual indexes per query, leading to overlap
    - Use these as INPUT to your design, not as the final answer
*/

SELECT TOP 30
    CONVERT(DECIMAL(18,2), migs.avg_total_user_cost
        * migs.avg_user_impact
        * (migs.user_seeks + migs.user_scans)) AS ImprovementScore,
    migs.user_seeks,
    migs.user_scans,
    CONVERT(DECIMAL(5,2), migs.avg_user_impact) AS AvgImpactPct,

    DB_NAME(mid.database_id)                AS DatabaseName,
    OBJECT_NAME(mid.object_id, mid.database_id) AS TableName,

    mid.equality_columns                    AS EqualityColumns,
    mid.inequality_columns                  AS InequalityColumns,
    mid.included_columns                    AS IncludedColumns,

    -- Generate a CREATE INDEX statement
    'CREATE NONCLUSTERED INDEX IX_Missing_'
        + CAST(ROW_NUMBER() OVER (ORDER BY migs.avg_total_user_cost
            * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) DESC) AS VARCHAR)
        + ' ON ' + mid.statement
        + ' (' + ISNULL(mid.equality_columns, '')
        + CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL
            THEN ', ' ELSE '' END
        + ISNULL(mid.inequality_columns, '')
        + ')'
        + ISNULL(' INCLUDE (' + mid.included_columns + ')', '')
        + ';'                                AS SuggestedIndex

FROM sys.dm_db_missing_index_group_stats migs
JOIN sys.dm_db_missing_index_groups mig ON migs.group_handle = mig.index_group_handle
JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
WHERE mid.database_id = DB_ID()
ORDER BY ImprovementScore DESC;
GO


-- ----------------------------------------------------------------------------
-- 2.6.2  Unused index identification
-- ----------------------------------------------------------------------------
/*
  Indexes that are never read but constantly updated are pure overhead.
  They slow down every INSERT, UPDATE, and DELETE while providing zero
  read benefit.

  RULE OF THUMB: If an index has zero seeks and zero scans but significant
  updates over a representative period (full business cycle), it's a
  candidate for removal.

  CAUTION: Some indexes may be used only during month-end, quarter-end,
  or year-end processing. Verify across a full business cycle before
  dropping.
*/

SELECT
    OBJECT_NAME(i.object_id)            AS TableName,
    i.name                              AS IndexName,
    i.type_desc                         AS IndexType,
    ISNULL(ius.user_seeks, 0)           AS UserSeeks,
    ISNULL(ius.user_scans, 0)           AS UserScans,
    ISNULL(ius.user_lookups, 0)         AS UserLookups,
    ISNULL(ius.user_updates, 0)         AS UserUpdates,

    -- Cost: how much write overhead this index generates
    ISNULL(ius.user_updates, 0)
        - (ISNULL(ius.user_seeks, 0) + ISNULL(ius.user_scans, 0))
                                        AS WriteOverhead,

    -- Size of the index
    (SELECT SUM(a.total_pages) * 8 / 1024
     FROM sys.partitions p
     JOIN sys.allocation_units a ON p.partition_id = a.container_id
     WHERE p.object_id = i.object_id AND p.index_id = i.index_id
    )                                   AS IndexSize_MB,

    -- Index key columns for reference
    STUFF((
        SELECT ', ' + c.name
        FROM sys.index_columns ic
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
            AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH('')
    ), 1, 2, '')                        AS KeyColumns,

    -- Filter predicate if it's a filtered index
    i.filter_definition                 AS FilterPredicate

FROM sys.indexes i
LEFT JOIN sys.dm_db_index_usage_stats ius
    ON i.object_id = ius.object_id
    AND i.index_id = ius.index_id
    AND ius.database_id = DB_ID()
WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
AND i.type > 0              -- Exclude heaps
AND i.is_primary_key = 0    -- Don't suggest dropping PKs
AND i.is_unique = 0         -- Don't suggest dropping unique constraints
AND ISNULL(ius.user_seeks, 0) + ISNULL(ius.user_scans, 0) = 0  -- Never read
AND ISNULL(ius.user_updates, 0) > 100  -- But frequently written to
ORDER BY ISNULL(ius.user_updates, 0) DESC;
GO


-- ----------------------------------------------------------------------------
-- 2.6.3  Overlapping index detection
-- ----------------------------------------------------------------------------
/*
  Overlapping indexes are indexes where one index is a subset of another.
  The subset index is redundant -- the wider index covers all its queries.

  Example:
    Index A: (AccountID, TransactionDate) INCLUDE (Amount)
    Index B: (AccountID, TransactionDate) INCLUDE (Amount, StatusCode)

  Index A is redundant because Index B covers everything A does plus more.
  Drop Index A to save space and write overhead.
*/

;WITH IndexColumns AS (
    SELECT
        i.object_id,
        i.index_id,
        i.name AS IndexName,
        STUFF((
            SELECT ', ' + c.name
            FROM sys.index_columns ic
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                AND ic.is_included_column = 0
            ORDER BY ic.key_ordinal
            FOR XML PATH('')
        ), 1, 2, '') AS KeyColumns,
        STUFF((
            SELECT ', ' + c.name
            FROM sys.index_columns ic
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                AND ic.is_included_column = 1
            ORDER BY ic.column_id
            FOR XML PATH('')
        ), 1, 2, '') AS IncludedColumns,
        i.filter_definition
    FROM sys.indexes i
    WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
    AND i.type IN (1, 2)  -- Clustered and nonclustered
)
SELECT
    OBJECT_NAME(a.object_id)    AS TableName,
    a.IndexName                 AS NarrowIndex,
    a.KeyColumns                AS NarrowKeys,
    a.IncludedColumns           AS NarrowIncludes,
    b.IndexName                 AS WiderIndex,
    b.KeyColumns                AS WiderKeys,
    b.IncludedColumns           AS WiderIncludes,
    '-- Potential duplicate: ' + a.IndexName + ' may be covered by ' + b.IndexName AS Analysis
FROM IndexColumns a
JOIN IndexColumns b ON a.object_id = b.object_id
    AND a.index_id <> b.index_id
    AND b.KeyColumns LIKE a.KeyColumns + '%'  -- Wider starts with same key prefix
    AND ISNULL(a.filter_definition, '') = ISNULL(b.filter_definition, '')  -- Same filter
WHERE a.index_id > 1  -- Don't flag the clustered index
ORDER BY OBJECT_NAME(a.object_id), a.IndexName;
GO


-- ============================================================================
-- SECTION 2.7: STORED PROCEDURE & QUERY PERFORMANCE CONSIDERATIONS
-- ============================================================================
/*
  Beyond indexing, there are query-level patterns that make or break
  performance in a partitioned environment.
*/

-- ----------------------------------------------------------------------------
-- 2.7.1  Parameter sniffing and partitioned tables
-- ----------------------------------------------------------------------------
/*
  Parameter sniffing is when SQL Server compiles a plan based on the first
  parameter value used, then reuses that plan for all subsequent calls.
  This is usually fine, but with partitioned tables it can be catastrophic:

  SCENARIO: A stored procedure runs with AccountID = 5 (which has 10 rows
  in the current partition). The optimizer creates a plan with a nested loop.
  Later, AccountID = 99999 (which has 10 million rows) reuses the nested
  loop plan and runs for hours.

  WORSE SCENARIO: The first execution scans a HOT partition (small, fast).
  The cached plan looks great. A later execution with a date range in a
  COLD partition (billions of rows) reuses the plan and the server tanks.

  SOLUTIONS (in order of preference):

  1. OPTION (RECOMPILE): Best for queries where parameter values vary widely.
     The recompile cost (< 1ms) is negligible compared to a bad plan.

  2. OPTIMIZE FOR UNKNOWN: Tells the optimizer to use average statistics
     instead of sniffing. Good middle ground.

  3. Plan guides / query store hints: Force a specific plan shape for
     critical queries.

  4. OPTION (USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE')): For queries
     that should always parallelize across partitions.
*/

-- BAD: Vulnerable to parameter sniffing
CREATE OR ALTER PROCEDURE dbo.usp_GetTransactionsByDateRange_BAD
    @StartDate DATETIME2(3),
    @EndDate   DATETIME2(3)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT TransactionID, AccountID, Amount, StatusCode
    FROM dbo.Transactions
    WHERE TransactionDate >= @StartDate
    AND TransactionDate < @EndDate;
END
GO

-- GOOD: Immune to parameter sniffing, always gets fresh plan
CREATE OR ALTER PROCEDURE dbo.usp_GetTransactionsByDateRange_GOOD
    @StartDate DATETIME2(3),
    @EndDate   DATETIME2(3)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT TransactionID, AccountID, Amount, StatusCode
    FROM dbo.Transactions
    WHERE TransactionDate >= @StartDate
    AND TransactionDate < @EndDate
    OPTION (RECOMPILE);
END
GO


-- ----------------------------------------------------------------------------
-- 2.7.2  Ensuring partition elimination in queries
-- ----------------------------------------------------------------------------
/*
  Partition elimination is the optimizer's ability to skip partitions that
  cannot contain matching rows. It only works with sargable predicates
  directly on the partition key.

  Use this query to verify partition elimination in your execution plans:
*/

-- Check which partitions are accessed by a specific query
-- Use SET STATISTICS PROFILE or look at the Actual Execution Plan

-- Good: Direct comparison on partition key = elimination
SELECT COUNT(*), SUM(Amount)
FROM dbo.Transactions
WHERE TransactionDate >= '2026-01-01'
AND TransactionDate < '2026-02-01'
OPTION (RECOMPILE);
GO
-- Execution plan should show: Actual Partition Count = 1

-- Bad: Function on partition key = NO elimination (scans ALL partitions)
SELECT COUNT(*), SUM(Amount)
FROM dbo.Transactions
WHERE YEAR(TransactionDate) = 2026
AND MONTH(TransactionDate) = 1
OPTION (RECOMPILE);
GO
-- Execution plan shows: Actual Partition Count = 7 (ALL partitions scanned!)

-- Verify partition elimination via DMV
SELECT
    qp.query_plan,
    -- Look for <RuntimePartitionSummary> in the XML
    -- PartitionsAccessed should be < total partitions
    qs.execution_count,
    qs.total_logical_reads,
    qs.total_elapsed_time / 1000 AS TotalElapsedMs
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
WHERE qp.query_plan IS NOT NULL
AND CAST(qp.query_plan AS NVARCHAR(MAX)) LIKE '%Transactions%'
ORDER BY qs.total_logical_reads DESC;
GO


-- ----------------------------------------------------------------------------
-- 2.7.3  Query patterns to avoid on partitioned tables
-- ----------------------------------------------------------------------------
/*
  ANTI-PATTERN 1: Functions on the partition key
    BAD:  WHERE YEAR(TransactionDate) = 2026
    GOOD: WHERE TransactionDate >= '2026-01-01' AND TransactionDate < '2027-01-01'

  ANTI-PATTERN 2: Implicit conversions on the partition key
    BAD:  WHERE TransactionDate = '2026-01-15'  (if column is DATETIME2 but
          the literal is interpreted as DATETIME -- implicit conversion)
    GOOD: WHERE TransactionDate = CAST('2026-01-15' AS DATETIME2(3))

  ANTI-PATTERN 3: OR predicates that span partitions unpredictably
    BAD:  WHERE TransactionDate = '2026-01-15' OR AccountID = 12345
          (The OR prevents partition elimination on TransactionDate)
    GOOD: Split into two queries with UNION ALL

  ANTI-PATTERN 4: Correlated subqueries without partition key
    BAD:  WHERE EXISTS (SELECT 1 FROM OtherTable WHERE OtherTable.ID = Transactions.TransactionID)
          (No partition key = scan all partitions)
    GOOD: Add TransactionDate to the correlation

  ANTI-PATTERN 5: SELECT * with no date filter
    BAD:  SELECT TOP 100 * FROM dbo.Transactions WHERE AccountID = 12345
          (Scans all partitions to find the first 100 rows)
    GOOD: SELECT TOP 100 * FROM dbo.Transactions
          WHERE AccountID = 12345
          AND TransactionDate >= DATEADD(DAY, -60, GETDATE())
*/


-- ----------------------------------------------------------------------------
-- 2.7.4  Index hints and forced plans (use sparingly)
-- ----------------------------------------------------------------------------
/*
  In some cases, the optimizer chooses the wrong index despite having a
  good filtered index available. This can happen when:
    - Statistics are stale
    - The filtered index filter is complex
    - Parameter sniffing leads to a suboptimal plan

  You can use index hints as a last resort, but always prefer:
    1. OPTION (RECOMPILE) first
    2. UPDATE STATISTICS second
    3. Index hints third
    4. Query Store forced plans fourth
*/

-- Example: Force use of a specific filtered index
SELECT TransactionID, Amount, StatusCode
FROM dbo.Transactions WITH (INDEX(IX_Filt_ActiveByAccount))
WHERE AccountID = 12345
AND StatusCode = 'A'
AND TransactionDate >= '2026-01-01';
GO


-- ============================================================================
-- SECTION 2.8: INDEX STRATEGY SUMMARY
-- ============================================================================
/*
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  INDEX STRATEGY SUMMARY FOR HOT/COLD OLTP                              │
  ├──────────────────────────────────────────────────────────────────────────┤
  │                                                                        │
  │  1. ALL indexes should be ALIGNED with the partition scheme             │
  │     (enables per-partition maintenance and SWITCH operations)           │
  │                                                                        │
  │  2. Use FILTERED INDEXES on StatusCode = 'A' for active-data lookups   │
  │     (5-10% of the full index size, always in buffer pool)              │
  │                                                                        │
  │  3. Use date-based FILTERED INDEXES for time-range queries             │
  │     (refresh the boundary monthly via maintenance procedure)           │
  │                                                                        │
  │  4. Add a NONCLUSTERED COLUMNSTORE on cold data for analytics          │
  │     (10-100x faster for aggregations, massive compression)             │
  │                                                                        │
  │  5. COMPRESS by temperature: NONE/ROW for hot, PAGE for cold           │
  │                                                                        │
  │  6. MAINTAIN only hot partitions; skip cold partitions entirely         │
  │                                                                        │
  │  7. Always use OPTION (RECOMPILE) with filtered indexes in SPs         │
  │                                                                        │
  │  8. Audit regularly: remove unused indexes, consolidate overlaps       │
  │                                                                        │
  │  9. Ensure all queries use sargable predicates on the partition key    │
  │                                                                        │
  │  10. Monitor for parameter sniffing issues on partitioned tables       │
  │                                                                        │
  └──────────────────────────────────────────────────────────────────────────┘
*/

PRINT '================================================================================';
PRINT '  Module 2 Complete: Indexing Strategy for Hot/Cold OLTP';
PRINT '  Next: Module 3 - Partition SWITCH Operations & Data Lifecycle';
PRINT '================================================================================';
GO
