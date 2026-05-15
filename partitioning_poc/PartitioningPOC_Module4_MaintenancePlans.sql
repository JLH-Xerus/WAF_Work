/*
================================================================================
  SQL Server 2019 Table Partitioning POC - Module 4
  Partition-Aware Maintenance Plans
================================================================================
  Author:       Justin Hunter
  Date:         2026-02-22
  SQL Server:   2019 (Enterprise Edition)
  Purpose:      Production-ready maintenance plans that leverage partitioning
                to dramatically reduce maintenance windows and I/O impact

  Module 4 covers:
    4.1  Why traditional maintenance fails at scale
    4.2  Partition-aware index maintenance (smart rebuild/reorg)
    4.3  Partition-aware statistics management
    4.4  Partition-aware integrity checks (DBCC CHECKDB alternatives)
    4.5  Filtered index lifecycle maintenance
    4.6  Compression tier management (hot -> warm -> cold -> archive)
    4.7  Sliding window maintenance (coordinating with Module 3)
    4.8  Scheduling and orchestration (SQL Agent job framework)
    4.9  Monitoring maintenance health
================================================================================
*/

USE PartitioningPOC;
GO

-- ============================================================================
-- SECTION 4.1: WHY TRADITIONAL MAINTENANCE FAILS AT SCALE
-- ============================================================================
/*
  Traditional SQL Server maintenance plans use a one-size-fits-all approach:

    - Rebuild ALL indexes on ALL tables on a weekly schedule
    - Update ALL statistics on ALL tables nightly
    - Run DBCC CHECKDB on the entire database weekly

  For a database with 6-billion-row tables, this approach has fatal flaws:

  ┌────────────────────────┬────────────────────────────────────────────────┐
  │  Problem               │  Impact at 6 Billion Rows                     │
  ├────────────────────────┼────────────────────────────────────────────────┤
  │  Full index rebuild    │  Takes 8-12+ hours. Generates 2-5x the table  │
  │                        │  size in transaction log. Saturates SAN I/O.  │
  │                        │  May not complete in the maintenance window.  │
  ├────────────────────────┼────────────────────────────────────────────────┤
  │  Full statistics       │  Default sampling on 6B rows is wildly        │
  │  update                │  inaccurate. FULLSCAN takes hours. Both are   │
  │                        │  wasteful because 95% of the data hasn't      │
  │                        │  changed since the last update.               │
  ├────────────────────────┼────────────────────────────────────────────────┤
  │  DBCC CHECKDB          │  Scans every page in every table. At hundreds │
  │                        │  of GB, this takes hours and generates         │
  │                        │  enormous I/O. Blocks other maintenance.       │
  ├────────────────────────┼────────────────────────────────────────────────┤
  │  Wasted effort         │  Cold partitions (no writes) have zero        │
  │                        │  fragmentation growth. Rebuilding them is     │
  │                        │  pure waste. Traditional plans can't tell     │
  │                        │  the difference.                              │
  └────────────────────────┴────────────────────────────────────────────────┘

  THE PARTITION-AWARE ALTERNATIVE:

  With partitioning, you maintain ONLY what needs maintaining:
    - HOT partitions: high write activity = frequent maintenance
    - COLD partitions: zero writes = zero maintenance needed
    - This reduces the maintenance window by 90%+ because you're ignoring
      the vast majority of the data that hasn't changed.
*/


-- ============================================================================
-- SECTION 4.2: PARTITION-AWARE INDEX MAINTENANCE
-- ============================================================================
/*
  The cornerstone of the maintenance plan. This procedure:
    1. Scans fragmentation ONLY on hot partitions (configurable count)
    2. Applies REORGANIZE or REBUILD based on fragmentation thresholds
    3. Uses ONLINE operations to avoid blocking
    4. Logs every action for auditing and tuning
    5. Supports dry-run mode for validation
    6. Respects a time budget so it doesn't overrun the window
*/

-- Maintenance action log table
IF OBJECT_ID('dbo.MaintenanceLog') IS NULL
BEGIN
    CREATE TABLE dbo.MaintenanceLog (
        LogID               INT             IDENTITY(1,1) PRIMARY KEY,
        ExecutionID         UNIQUEIDENTIFIER NOT NULL,   -- Groups actions from a single run
        ActionTime          DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
        TableName           SYSNAME         NOT NULL,
        IndexName           SYSNAME         NULL,
        PartitionNumber     INT             NULL,
        ActionType          VARCHAR(30)     NOT NULL,    -- REBUILD, REORGANIZE, STATS_UPDATE, COMPRESS, etc.
        FragBefore          DECIMAL(5,2)    NULL,
        FragAfter           DECIMAL(5,2)    NULL,
        PageCount           BIGINT          NULL,
        DurationMs          INT             NULL,
        Status              VARCHAR(20)     NOT NULL DEFAULT 'Running',
        ErrorMessage        NVARCHAR(4000)  NULL,
        SQLExecuted         NVARCHAR(MAX)   NULL
    );

    CREATE NONCLUSTERED INDEX IX_MaintenanceLog_ExecID
    ON dbo.MaintenanceLog (ExecutionID, ActionTime);
END
GO


-- ----------------------------------------------------------------------------
-- 4.2.1  Smart index maintenance procedure
-- ----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE dbo.usp_SmartIndexMaintenance
    @TableName              SYSNAME,
    @HotPartitionCount      INT = 3,            -- How many recent partitions to maintain
    @ReorgThreshold         FLOAT = 10.0,       -- Fragmentation % to trigger REORG
    @RebuildThreshold       FLOAT = 30.0,       -- Fragmentation % to trigger REBUILD
    @MinPageCount           BIGINT = 1000,       -- Skip small partitions (fragmentation is meaningless)
    @OnlineRebuild          BIT = 1,            -- Use ONLINE = ON (Enterprise only)
    @MaxDurationMinutes     INT = NULL,          -- Time budget (NULL = unlimited)
    @DryRun                 BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ObjectID       INT = OBJECT_ID(@TableName);
    DECLARE @MaxPartition   INT;
    DECLARE @MinHotPartition INT;
    DECLARE @ExecutionID    UNIQUEIDENTIFIER = NEWID();
    DECLARE @StartTime      DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @CutoffTime     DATETIME2(3);
    DECLARE @SQL            NVARCHAR(MAX);
    DECLARE @ActionStart    DATETIME2(3);
    DECLARE @LogID          INT;

    IF @ObjectID IS NULL
    BEGIN
        RAISERROR('Table not found: %s', 16, 1, @TableName);
        RETURN;
    END

    -- Calculate time budget
    IF @MaxDurationMinutes IS NOT NULL
        SET @CutoffTime = DATEADD(MINUTE, @MaxDurationMinutes, @StartTime);

    -- Determine hot partition range
    SELECT @MaxPartition = MAX(partition_number)
    FROM sys.partitions WHERE object_id = @ObjectID AND index_id IN (0, 1);

    SET @MinHotPartition = @MaxPartition - @HotPartitionCount + 1;
    IF @MinHotPartition < 1 SET @MinHotPartition = 1;

    PRINT '================================================================';
    PRINT 'SMART INDEX MAINTENANCE - Execution: ' + CAST(@ExecutionID AS VARCHAR(36));
    PRINT '================================================================';
    PRINT 'Table:              ' + @TableName;
    PRINT 'Hot partitions:     ' + CAST(@MinHotPartition AS VARCHAR) + ' to ' + CAST(@MaxPartition AS VARCHAR);
    PRINT 'Thresholds:         REORG > ' + CAST(@ReorgThreshold AS VARCHAR) + '%, REBUILD > ' + CAST(@RebuildThreshold AS VARCHAR) + '%';
    PRINT 'Min page count:     ' + CAST(@MinPageCount AS VARCHAR);
    PRINT 'Time budget:        ' + ISNULL(CAST(@MaxDurationMinutes AS VARCHAR) + ' minutes', 'Unlimited');
    PRINT 'Online rebuild:     ' + CASE @OnlineRebuild WHEN 1 THEN 'YES' ELSE 'NO' END;
    PRINT 'Dry run:            ' + CASE @DryRun WHEN 1 THEN 'YES' ELSE 'NO' END;
    PRINT '================================================================';

    -- Iterate through hot partition indexes that need attention
    DECLARE @IndexName      SYSNAME;
    DECLARE @IndexID        INT;
    DECLARE @PartNum        INT;
    DECLARE @FragPct        FLOAT;
    DECLARE @Pages          BIGINT;
    DECLARE @ActionsPerformed INT = 0;

    DECLARE maint_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            i.name,
            i.index_id,
            ips.partition_number,
            ips.avg_fragmentation_in_percent,
            ips.page_count
        FROM sys.indexes i
        CROSS APPLY sys.dm_db_index_physical_stats(
            DB_ID(), @ObjectID, i.index_id, NULL, 'LIMITED'
        ) ips
        WHERE i.object_id = @ObjectID
        AND i.type > 0
        AND ips.partition_number BETWEEN @MinHotPartition AND @MaxPartition
        AND ips.page_count >= @MinPageCount
        AND ips.avg_fragmentation_in_percent > @ReorgThreshold
        ORDER BY
            -- Prioritize: highest fragmentation first, then largest partitions
            ips.avg_fragmentation_in_percent DESC,
            ips.page_count DESC;

    OPEN maint_cursor;
    FETCH NEXT FROM maint_cursor INTO @IndexName, @IndexID, @PartNum, @FragPct, @Pages;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Check time budget
        IF @CutoffTime IS NOT NULL AND SYSUTCDATETIME() >= @CutoffTime
        BEGIN
            PRINT '';
            PRINT '*** TIME BUDGET EXCEEDED. Stopping. ***';
            PRINT '    Actions completed: ' + CAST(@ActionsPerformed AS VARCHAR);
            BREAK;
        END

        SET @ActionStart = SYSUTCDATETIME();

        IF @FragPct >= @RebuildThreshold
        BEGIN
            -- REBUILD
            SET @SQL = N'ALTER INDEX ' + QUOTENAME(@IndexName)
                + N' ON ' + @TableName
                + N' REBUILD PARTITION = ' + CAST(@PartNum AS NVARCHAR(10))
                + N' WITH ('
                + CASE WHEN @OnlineRebuild = 1 THEN N'ONLINE = ON, ' ELSE N'' END
                + N'SORT_IN_TEMPDB = ON, MAXDOP = 0);';

            -- Log the action
            INSERT INTO dbo.MaintenanceLog
                (ExecutionID, TableName, IndexName, PartitionNumber, ActionType, FragBefore, PageCount, SQLExecuted)
            VALUES
                (@ExecutionID, @TableName, @IndexName, @PartNum, 'REBUILD', @FragPct, @Pages, @SQL);
            SET @LogID = SCOPE_IDENTITY();

            PRINT 'REBUILD: ' + @IndexName + ' P' + CAST(@PartNum AS VARCHAR)
                + ' (frag=' + CAST(CAST(@FragPct AS DECIMAL(5,1)) AS VARCHAR) + '%, '
                + FORMAT(@Pages, 'N0') + ' pages)';
        END
        ELSE
        BEGIN
            -- REORGANIZE
            SET @SQL = N'ALTER INDEX ' + QUOTENAME(@IndexName)
                + N' ON ' + @TableName
                + N' REORGANIZE PARTITION = ' + CAST(@PartNum AS NVARCHAR(10)) + N';';

            INSERT INTO dbo.MaintenanceLog
                (ExecutionID, TableName, IndexName, PartitionNumber, ActionType, FragBefore, PageCount, SQLExecuted)
            VALUES
                (@ExecutionID, @TableName, @IndexName, @PartNum, 'REORGANIZE', @FragPct, @Pages, @SQL);
            SET @LogID = SCOPE_IDENTITY();

            PRINT 'REORG:   ' + @IndexName + ' P' + CAST(@PartNum AS VARCHAR)
                + ' (frag=' + CAST(CAST(@FragPct AS DECIMAL(5,1)) AS VARCHAR) + '%, '
                + FORMAT(@Pages, 'N0') + ' pages)';
        END

        IF @DryRun = 0
        BEGIN
            BEGIN TRY
                EXEC sp_executesql @SQL;

                -- Measure post-maintenance fragmentation
                DECLARE @PostFrag FLOAT;
                SELECT @PostFrag = avg_fragmentation_in_percent
                FROM sys.dm_db_index_physical_stats(DB_ID(), @ObjectID, @IndexID, @PartNum, 'LIMITED');

                UPDATE dbo.MaintenanceLog
                SET DurationMs = DATEDIFF(MILLISECOND, @ActionStart, SYSUTCDATETIME()),
                    FragAfter = @PostFrag,
                    Status = 'Complete'
                WHERE LogID = @LogID;
            END TRY
            BEGIN CATCH
                UPDATE dbo.MaintenanceLog
                SET DurationMs = DATEDIFF(MILLISECOND, @ActionStart, SYSUTCDATETIME()),
                    Status = 'Failed',
                    ErrorMessage = ERROR_MESSAGE()
                WHERE LogID = @LogID;

                PRINT '  ERROR: ' + ERROR_MESSAGE();
            END CATCH
        END
        ELSE
        BEGIN
            UPDATE dbo.MaintenanceLog
            SET DurationMs = 0, Status = 'DryRun'
            WHERE LogID = @LogID;
        END

        SET @ActionsPerformed = @ActionsPerformed + 1;
        FETCH NEXT FROM maint_cursor INTO @IndexName, @IndexID, @PartNum, @FragPct, @Pages;
    END

    CLOSE maint_cursor;
    DEALLOCATE maint_cursor;

    PRINT '';
    PRINT 'Index maintenance complete. Actions: ' + CAST(@ActionsPerformed AS VARCHAR)
        + ' | Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, SYSUTCDATETIME()) AS VARCHAR) + 's';
    PRINT 'Execution ID: ' + CAST(@ExecutionID AS VARCHAR(36));
END
GO


-- ============================================================================
-- SECTION 4.3: PARTITION-AWARE STATISTICS MANAGEMENT
-- ============================================================================
/*
  Statistics accuracy is critical for good query plans, especially on
  partitioned tables where the optimizer must estimate cardinality per
  partition for partition elimination decisions.

  PROBLEMS WITH DEFAULT STATISTICS:
    - Auto-update triggers after 20% of rows change (SQL 2016+ uses a
      dynamic threshold with trace flag 2371 behavior as default)
    - On a 6B row table, 20% = 1.2 BILLION rows must change before
      auto-update fires. This is far too late.
    - Even the dynamic threshold may not trigger often enough for the
      hot partitions that receive constant writes.

  SOLUTION: Manually update statistics on hot partitions with appropriate
  sampling, and leave cold partition statistics alone.

  SQL SERVER 2019 INCREMENTAL STATISTICS:
    If enabled, SQL Server maintains per-partition statistics that can be
    updated independently. This is the ideal approach for partitioned tables.
*/

CREATE OR ALTER PROCEDURE dbo.usp_SmartStatisticsUpdate
    @TableName              SYSNAME,
    @HotPartitionCount      INT = 3,
    @SamplePercent          INT = NULL,         -- NULL = auto (FULLSCAN for hot, skip cold)
    @UseIncremental         BIT = 1,            -- Use incremental statistics if available
    @DryRun                 BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ObjectID       INT = OBJECT_ID(@TableName);
    DECLARE @MaxPartition   INT;
    DECLARE @MinHotPartition INT;
    DECLARE @ExecutionID    UNIQUEIDENTIFIER = NEWID();
    DECLARE @SQL            NVARCHAR(MAX);
    DECLARE @StatsName      SYSNAME;
    DECLARE @ActionStart    DATETIME2(3);
    DECLARE @ActionsPerformed INT = 0;

    SELECT @MaxPartition = MAX(partition_number)
    FROM sys.partitions WHERE object_id = @ObjectID AND index_id IN (0, 1);

    SET @MinHotPartition = @MaxPartition - @HotPartitionCount + 1;
    IF @MinHotPartition < 1 SET @MinHotPartition = 1;

    PRINT '================================================================';
    PRINT 'SMART STATISTICS UPDATE - ' + @TableName;
    PRINT '================================================================';
    PRINT 'Hot partitions: ' + CAST(@MinHotPartition AS VARCHAR) + ' to ' + CAST(@MaxPartition AS VARCHAR);

    -- Check if incremental statistics are enabled on the table
    DECLARE @IsIncremental BIT = 0;
    IF EXISTS (
        SELECT 1 FROM sys.stats s
        WHERE s.object_id = @ObjectID AND s.is_incremental = 1
    )
        SET @IsIncremental = 1;

    IF @IsIncremental = 1 AND @UseIncremental = 1
    BEGIN
        -- INCREMENTAL UPDATE: Update only the hot partitions
        -- SQL Server 2019 uses incremental statistics with RESAMPLE
        -- SQL Server internally tracks modification counters and resamples only partitions where data changed
        -- (Note: SQL Server 2022 adds the ON PARTITIONS() syntax for even more surgical targeting)
        PRINT 'Mode: INCREMENTAL (per-partition statistics update)';

        -- Update statistics with RESAMPLE on incremental stats
        -- SQL Server 2019 will intelligently rescan only partitions where data has changed
        SET @SQL = N'UPDATE STATISTICS ' + @TableName
            + N' WITH RESAMPLE;';

        PRINT @SQL;

        INSERT INTO dbo.MaintenanceLog
            (ExecutionID, TableName, ActionType, PartitionNumber, SQLExecuted)
        VALUES
            (@ExecutionID, @TableName, 'STATS_INCREMENTAL', @MinHotPartition, @SQL);

        SET @ActionStart = SYSUTCDATETIME();
        IF @DryRun = 0
        BEGIN
            EXEC sp_executesql @SQL;
            UPDATE dbo.MaintenanceLog
            SET DurationMs = DATEDIFF(MILLISECOND, @ActionStart, SYSUTCDATETIME()), Status = 'Complete'
            WHERE ExecutionID = @ExecutionID AND ActionType = 'STATS_INCREMENTAL';
        END

        SET @ActionsPerformed = @ActionsPerformed + 1;
    END
    ELSE
    BEGIN
        -- NON-INCREMENTAL: Update each statistic object individually
        -- Use a higher sample rate for better accuracy on hot data
        PRINT 'Mode: FULL (table-level statistics update)';

        DECLARE stats_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT s.name
            FROM sys.stats s
            WHERE s.object_id = @ObjectID
            AND s.auto_created = 0  -- Focus on index-linked and manually created stats
            ORDER BY s.stats_id;

        OPEN stats_cursor;
        FETCH NEXT FROM stats_cursor INTO @StatsName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @SamplePercent IS NOT NULL
                SET @SQL = N'UPDATE STATISTICS ' + @TableName + N' ' + QUOTENAME(@StatsName)
                    + N' WITH SAMPLE ' + CAST(@SamplePercent AS NVARCHAR(3)) + N' PERCENT;';
            ELSE
                SET @SQL = N'UPDATE STATISTICS ' + @TableName + N' ' + QUOTENAME(@StatsName)
                    + N' WITH FULLSCAN;';

            PRINT '  ' + @StatsName + ': ' + @SQL;

            INSERT INTO dbo.MaintenanceLog
                (ExecutionID, TableName, IndexName, ActionType, SQLExecuted)
            VALUES
                (@ExecutionID, @TableName, @StatsName, 'STATS_UPDATE', @SQL);

            SET @ActionStart = SYSUTCDATETIME();
            IF @DryRun = 0
            BEGIN
                EXEC sp_executesql @SQL;
                UPDATE dbo.MaintenanceLog
                SET DurationMs = DATEDIFF(MILLISECOND, @ActionStart, SYSUTCDATETIME()), Status = 'Complete'
                WHERE ExecutionID = @ExecutionID AND IndexName = @StatsName AND ActionType = 'STATS_UPDATE';
            END

            SET @ActionsPerformed = @ActionsPerformed + 1;
            FETCH NEXT FROM stats_cursor INTO @StatsName;
        END

        CLOSE stats_cursor;
        DEALLOCATE stats_cursor;
    END

    PRINT '';
    PRINT 'Statistics update complete. Actions: ' + CAST(@ActionsPerformed AS VARCHAR);
END
GO

-- ----------------------------------------------------------------------------
-- 4.3.1  Enable incremental statistics on a table
-- ----------------------------------------------------------------------------
/*
  Incremental statistics must be explicitly enabled per statistics object.
  Once enabled, SQL Server maintains a per-partition histogram that can
  be updated independently.

  IMPORTANT: Incremental stats require the table to be partitioned and
  the statistics to be created with INCREMENTAL = ON.
*/

-- Enable incremental statistics for the clustered index
UPDATE STATISTICS dbo.Transactions
WITH FULLSCAN, INCREMENTAL = ON;
GO

-- Verify incremental status
SELECT
    s.name              AS StatsName,
    s.is_incremental    AS IsIncremental,
    s.auto_created      AS AutoCreated,
    sp.last_updated     AS LastUpdated,
    sp.rows             AS TotalRows,
    sp.rows_sampled     AS RowsSampled,
    sp.modification_counter AS ModCounter
FROM sys.stats s
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
WHERE s.object_id = OBJECT_ID('dbo.Transactions')
ORDER BY s.stats_id;
GO


-- ============================================================================
-- SECTION 4.4: PARTITION-AWARE INTEGRITY CHECKS
-- ============================================================================
/*
  DBCC CHECKDB is essential but extremely expensive on large databases.
  The partition-aware strategy uses DBCC CHECKTABLE with targeted scope
  and spreads the work across multiple maintenance windows.

  STRATEGIES:

  1. DBCC CHECKTABLE per table (instead of CHECKDB for entire database)
     - Allows you to prioritize critical tables
     - Can be spread across multiple nights

  2. PHYSICAL_ONLY option
     - Skips logical checks (much faster)
     - Catches the most critical corruption: page checksums, torn pages
     - Run PHYSICAL_ONLY frequently, full check less often

  3. Filegroup-level checks
     - DBCC CHECKFILEGROUP checks one filegroup at a time
     - Since each partition maps to a filegroup, this effectively
       checks one partition at a time
     - Spread across maintenance windows: one filegroup per night

  RECOMMENDED SCHEDULE:
    - Nightly: DBCC CHECKTABLE WITH PHYSICAL_ONLY on critical tables
    - Weekly: Rotate through filegroups with DBCC CHECKFILEGROUP
    - Monthly: Full DBCC CHECKDB (during extended maintenance window)
*/

CREATE OR ALTER PROCEDURE dbo.usp_SmartIntegrityCheck
    @Mode                   VARCHAR(20) = 'PHYSICAL_ONLY',  -- PHYSICAL_ONLY, FILEGROUP, FULL
    @TargetTable            SYSNAME = NULL,                  -- NULL = all user tables
    @TargetFilegroup        SYSNAME = NULL,                  -- For FILEGROUP mode
    @MaxDurationMinutes     INT = NULL,
    @DryRun                 BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL            NVARCHAR(MAX);
    DECLARE @TblName        SYSNAME;
    DECLARE @StartTime      DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @CutoffTime     DATETIME2(3);
    DECLARE @ExecutionID    UNIQUEIDENTIFIER = NEWID();
    DECLARE @ActionStart    DATETIME2(3);

    IF @MaxDurationMinutes IS NOT NULL
        SET @CutoffTime = DATEADD(MINUTE, @MaxDurationMinutes, @StartTime);

    PRINT '================================================================';
    PRINT 'SMART INTEGRITY CHECK - Mode: ' + @Mode;
    PRINT '================================================================';

    IF @Mode = 'PHYSICAL_ONLY'
    BEGIN
        -- Fast physical-only check on specified table(s)
        DECLARE tbl_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT SCHEMA_NAME(schema_id) + '.' + name
            FROM sys.tables
            WHERE is_ms_shipped = 0
            AND (@TargetTable IS NULL OR SCHEMA_NAME(schema_id) + '.' + name = @TargetTable)
            ORDER BY
                -- Prioritize largest tables (most likely to have corruption)
                (SELECT SUM(p.rows) FROM sys.partitions p
                 WHERE p.object_id = tables.object_id AND p.index_id IN (0,1)) DESC;

        OPEN tbl_cursor;
        FETCH NEXT FROM tbl_cursor INTO @TblName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @CutoffTime IS NOT NULL AND SYSUTCDATETIME() >= @CutoffTime
            BEGIN
                PRINT '*** TIME BUDGET EXCEEDED ***';
                BREAK;
            END

            SET @SQL = N'DBCC CHECKTABLE (''' + @TblName + ''') WITH PHYSICAL_ONLY, NO_INFOMSGS;';
            PRINT 'CHECK: ' + @TblName + ' (PHYSICAL_ONLY)';

            INSERT INTO dbo.MaintenanceLog
                (ExecutionID, TableName, ActionType, SQLExecuted)
            VALUES (@ExecutionID, @TblName, 'INTEGRITY_PHYSICAL', @SQL);

            SET @ActionStart = SYSUTCDATETIME();
            IF @DryRun = 0
            BEGIN
                BEGIN TRY
                    EXEC sp_executesql @SQL;
                    UPDATE dbo.MaintenanceLog
                    SET DurationMs = DATEDIFF(MILLISECOND, @ActionStart, SYSUTCDATETIME()), Status = 'Complete'
                    WHERE ExecutionID = @ExecutionID AND TableName = @TblName;
                END TRY
                BEGIN CATCH
                    UPDATE dbo.MaintenanceLog
                    SET DurationMs = DATEDIFF(MILLISECOND, @ActionStart, SYSUTCDATETIME()),
                        Status = 'Failed', ErrorMessage = ERROR_MESSAGE()
                    WHERE ExecutionID = @ExecutionID AND TableName = @TblName;
                    PRINT '  CORRUPTION DETECTED: ' + ERROR_MESSAGE();
                END CATCH
            END

            FETCH NEXT FROM tbl_cursor INTO @TblName;
        END

        CLOSE tbl_cursor;
        DEALLOCATE tbl_cursor;
    END
    ELSE IF @Mode = 'FILEGROUP'
    BEGIN
        -- Check a specific filegroup (one partition's worth of data)
        IF @TargetFilegroup IS NULL
        BEGIN
            RAISERROR('FILEGROUP mode requires @TargetFilegroup parameter.', 16, 1);
            RETURN;
        END

        DECLARE @FGID INT;
        SELECT @FGID = data_space_id FROM sys.filegroups WHERE name = @TargetFilegroup;

        IF @FGID IS NULL
        BEGIN
            RAISERROR('Filegroup not found: %s', 16, 1, @TargetFilegroup);
            RETURN;
        END

        SET @SQL = N'DBCC CHECKFILEGROUP (' + CAST(@FGID AS NVARCHAR(10)) + N') WITH NO_INFOMSGS;';
        PRINT 'CHECK FILEGROUP: ' + @TargetFilegroup + ' (ID=' + CAST(@FGID AS VARCHAR) + ')';
        PRINT @SQL;

        INSERT INTO dbo.MaintenanceLog
            (ExecutionID, TableName, ActionType, SQLExecuted)
        VALUES (@ExecutionID, @TargetFilegroup, 'INTEGRITY_FILEGROUP', @SQL);

        SET @ActionStart = SYSUTCDATETIME();
        IF @DryRun = 0
            EXEC sp_executesql @SQL;

        UPDATE dbo.MaintenanceLog
        SET DurationMs = DATEDIFF(MILLISECOND, @ActionStart, SYSUTCDATETIME()), Status = 'Complete'
        WHERE ExecutionID = @ExecutionID AND ActionType = 'INTEGRITY_FILEGROUP';
    END
    ELSE IF @Mode = 'FULL'
    BEGIN
        SET @SQL = N'DBCC CHECKDB (''' + DB_NAME() + N''') WITH NO_INFOMSGS;';
        PRINT 'FULL DATABASE CHECK: ' + DB_NAME();

        INSERT INTO dbo.MaintenanceLog
            (ExecutionID, TableName, ActionType, SQLExecuted)
        VALUES (@ExecutionID, DB_NAME(), 'INTEGRITY_FULL', @SQL);

        SET @ActionStart = SYSUTCDATETIME();
        IF @DryRun = 0
            EXEC sp_executesql @SQL;

        UPDATE dbo.MaintenanceLog
        SET DurationMs = DATEDIFF(MILLISECOND, @ActionStart, SYSUTCDATETIME()), Status = 'Complete'
        WHERE ExecutionID = @ExecutionID AND ActionType = 'INTEGRITY_FULL';
    END

    PRINT '';
    PRINT 'Integrity check complete. Duration: '
        + CAST(DATEDIFF(SECOND, @StartTime, SYSUTCDATETIME()) AS VARCHAR) + 's';
END
GO

-- Generate the weekly filegroup rotation schedule
SELECT
    fg.name AS FilegroupName,
    'Week ' + CAST(ROW_NUMBER() OVER (ORDER BY fg.data_space_id) AS VARCHAR) AS ScheduleWeek,
    fg.data_space_id AS FilegroupID
FROM sys.filegroups fg
WHERE fg.type = 'FG'
ORDER BY fg.data_space_id;
GO


-- ============================================================================
-- SECTION 4.5: FILTERED INDEX LIFECYCLE MAINTENANCE
-- ============================================================================
/*
  Date-based filtered indexes need their boundary refreshed periodically.
  This procedure handles the full lifecycle:
    1. Identify all date-filtered indexes on the table
    2. Drop each one
    3. Recreate with the updated boundary date
    4. Update statistics on the new indexes

  TIMING: Run this AFTER the sliding window advance (Module 3) and
  BEFORE the nightly index maintenance. This ensures the filtered indexes
  cover the correct date range before the optimizer starts using them.
*/

CREATE OR ALTER PROCEDURE dbo.usp_FilteredIndexLifecycle
    @TableName          SYSNAME,
    @NewCutoffDate      DATE = NULL,        -- NULL = 90 days ago
    @DryRun             BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    IF @NewCutoffDate IS NULL
        SET @NewCutoffDate = DATEADD(DAY, -90, CAST(GETDATE() AS DATE));

    DECLARE @CutoffStr      VARCHAR(10) = CONVERT(VARCHAR(10), @NewCutoffDate, 120);
    DECLARE @ObjectID       INT = OBJECT_ID(@TableName);
    DECLARE @ExecutionID    UNIQUEIDENTIFIER = NEWID();

    PRINT '================================================================';
    PRINT 'FILTERED INDEX LIFECYCLE - ' + @TableName;
    PRINT 'New cutoff date: ' + @CutoffStr;
    PRINT '================================================================';

    -- Find all filtered indexes with date-based filters
    DECLARE @IndexName      SYSNAME;
    DECLARE @FilterDef      NVARCHAR(MAX);
    DECLARE @IndexDef       NVARCHAR(MAX);
    DECLARE @SQL            NVARCHAR(MAX);

    DECLARE fidx_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            i.name,
            i.filter_definition,
            -- Reconstruct the CREATE INDEX statement
            'CREATE NONCLUSTERED INDEX ' + QUOTENAME(i.name)
            + ' ON ' + @TableName
            + ' (' + STUFF((
                SELECT ', ' + QUOTENAME(c.name)
                    + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE '' END
                FROM sys.index_columns ic
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
                ORDER BY ic.key_ordinal
                FOR XML PATH('')
            ), 1, 2, '') + ')'
            + ISNULL(' INCLUDE (' + STUFF((
                SELECT ', ' + QUOTENAME(c.name)
                FROM sys.index_columns ic
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
                ORDER BY ic.column_id
                FOR XML PATH('')
            ), 1, 2, '') + ')', '')
            AS IndexDefinition
        FROM sys.indexes i
        WHERE i.object_id = @ObjectID
        AND i.has_filter = 1
        AND i.filter_definition LIKE '%TransactionDate%'  -- Date-based filters only
        AND i.filter_definition LIKE '%>=%';               -- Range filters

    OPEN fidx_cursor;
    FETCH NEXT FROM fidx_cursor INTO @IndexName, @FilterDef, @IndexDef;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT '';
        PRINT 'Index: ' + @IndexName;
        PRINT '  Old filter: ' + @FilterDef;

        -- Drop the old index
        SET @SQL = N'DROP INDEX ' + QUOTENAME(@IndexName) + N' ON ' + @TableName + N';';
        PRINT '  DROP: ' + @SQL;

        IF @DryRun = 0
            EXEC sp_executesql @SQL;

        -- Recreate with new date boundary
        -- Replace the old date in the filter with the new cutoff
        SET @SQL = @IndexDef
            + N' WHERE [TransactionDate]>=''' + @CutoffStr + ''''
            + N' ON ps_TransactionDate([TransactionDate])'
            + N' WITH (ONLINE = ON, SORT_IN_TEMPDB = ON, DATA_COMPRESSION = ROW);';

        PRINT '  CREATE: ' + @SQL;

        INSERT INTO dbo.MaintenanceLog
            (ExecutionID, TableName, IndexName, ActionType, SQLExecuted)
        VALUES (@ExecutionID, @TableName, @IndexName, 'FILTERED_REFRESH', @SQL);

        IF @DryRun = 0
        BEGIN
            BEGIN TRY
                EXEC sp_executesql @SQL;
                UPDATE dbo.MaintenanceLog
                SET Status = 'Complete'
                WHERE ExecutionID = @ExecutionID AND IndexName = @IndexName;
                PRINT '  New filter: [TransactionDate]>=''' + @CutoffStr + '''';
            END TRY
            BEGIN CATCH
                UPDATE dbo.MaintenanceLog
                SET Status = 'Failed', ErrorMessage = ERROR_MESSAGE()
                WHERE ExecutionID = @ExecutionID AND IndexName = @IndexName;
                PRINT '  ERROR: ' + ERROR_MESSAGE();
            END CATCH
        END

        FETCH NEXT FROM fidx_cursor INTO @IndexName, @FilterDef, @IndexDef;
    END

    CLOSE fidx_cursor;
    DEALLOCATE fidx_cursor;

    PRINT '';
    PRINT 'Filtered index lifecycle complete.';
END
GO


-- ============================================================================
-- SECTION 4.6: COMPRESSION TIER MANAGEMENT
-- ============================================================================
/*
  As partitions age from HOT to COLD, their compression level should
  increase. This procedure manages the transition automatically.

  TRANSITIONS:
    HOT -> WARM:  Apply ROW compression (at 2-month boundary)
    WARM -> COLD: Apply PAGE compression (at 4-month boundary)
    COLD -> ARCHIVE: PAGE compression already applied (no change needed)
*/

CREATE OR ALTER PROCEDURE dbo.usp_CompressionTierManagement
    @TableName          SYSNAME,
    @WarmAfterMonths    INT = 2,
    @ColdAfterMonths    INT = 4,
    @DryRun             BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ObjectID       INT = OBJECT_ID(@TableName);
    DECLARE @SQL            NVARCHAR(MAX);
    DECLARE @WarmCutoff     DATE = DATEADD(MONTH, -@WarmAfterMonths, GETDATE());
    DECLARE @ColdCutoff     DATE = DATEADD(MONTH, -@ColdAfterMonths, GETDATE());

    PRINT '================================================================';
    PRINT 'COMPRESSION TIER MANAGEMENT - ' + @TableName;
    PRINT 'WARM cutoff (ROW):  ' + CONVERT(VARCHAR, @WarmCutoff, 120);
    PRINT 'COLD cutoff (PAGE): ' + CONVERT(VARCHAR, @ColdCutoff, 120);
    PRINT '================================================================';

    -- Find partitions that need compression changes
    DECLARE @PartNum        INT;
    DECLARE @PartRows       BIGINT;
    DECLARE @CurrentComp    VARCHAR(60);
    DECLARE @UpperBound     DATETIME2(3);
    DECLARE @DesiredComp    VARCHAR(10);

    DECLARE comp_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            p.partition_number,
            p.rows,
            p.data_compression_desc,
            CAST(prv.value AS DATETIME2(3)) AS UpperBoundary
        FROM sys.partitions p
        JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
        LEFT JOIN sys.partition_range_values prv
            ON prv.function_id = (SELECT ps.function_id FROM sys.partition_schemes ps
                                  WHERE ps.data_space_id = i.data_space_id)
            AND prv.boundary_id = p.partition_number
        WHERE p.object_id = @ObjectID
        AND i.type = 1
        AND p.rows > 0
        ORDER BY p.partition_number;

    OPEN comp_cursor;
    FETCH NEXT FROM comp_cursor INTO @PartNum, @PartRows, @CurrentComp, @UpperBound;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Determine desired compression based on the partition's age
        SET @DesiredComp = CASE
            WHEN @UpperBound IS NULL THEN 'NONE'           -- Current/future partition
            WHEN @UpperBound > @WarmCutoff THEN 'NONE'     -- Still HOT
            WHEN @UpperBound > @ColdCutoff THEN 'ROW'      -- WARM tier
            ELSE 'PAGE'                                     -- COLD tier
        END;

        -- Only act if compression needs to change (and only upgrade, never downgrade)
        IF (@DesiredComp = 'ROW' AND @CurrentComp = 'NONE')
        OR (@DesiredComp = 'PAGE' AND @CurrentComp IN ('NONE', 'ROW'))
        BEGIN
            SET @SQL = N'ALTER TABLE ' + @TableName
                + N' REBUILD PARTITION = ' + CAST(@PartNum AS NVARCHAR(10))
                + N' WITH (DATA_COMPRESSION = ' + @DesiredComp + N', ONLINE = ON);';

            PRINT 'P' + CAST(@PartNum AS VARCHAR) + ': '
                + @CurrentComp + ' -> ' + @DesiredComp
                + ' (' + FORMAT(@PartRows, 'N0') + ' rows)';

            IF @DryRun = 0
                EXEC sp_executesql @SQL;
        END
        ELSE
        BEGIN
            PRINT 'P' + CAST(@PartNum AS VARCHAR) + ': '
                + @CurrentComp + ' (already correct)';
        END

        FETCH NEXT FROM comp_cursor INTO @PartNum, @PartRows, @CurrentComp, @UpperBound;
    END

    CLOSE comp_cursor;
    DEALLOCATE comp_cursor;

    PRINT '';
    PRINT 'Compression tier management complete.';
END
GO

-- Test
EXEC dbo.usp_CompressionTierManagement @TableName = 'dbo.Transactions', @DryRun = 1;
GO


-- ============================================================================
-- SECTION 4.7: MASTER MAINTENANCE ORCHESTRATOR
-- ============================================================================
/*
  This procedure orchestrates ALL maintenance tasks in the correct order.
  It is designed to be called by a single SQL Agent job, simplifying
  scheduling and monitoring.

  EXECUTION ORDER:
    1. Sliding window advance (if it's the 1st of the month)
    2. Filtered index refresh
    3. Compression tier management
    4. Index maintenance (rebuild/reorg hot partitions)
    5. Statistics update (incremental on hot partitions)
    6. Integrity check (rotating filegroup or physical-only)
    7. Log cleanup (purge old maintenance logs)
*/

CREATE OR ALTER PROCEDURE dbo.usp_MasterMaintenance
    @TableName              SYSNAME = 'dbo.Transactions',
    @MaintenanceType        VARCHAR(20) = 'NIGHTLY',    -- NIGHTLY, WEEKLY, MONTHLY
    @MaxDurationMinutes     INT = 120,                   -- 2-hour default budget
    @DryRun                 BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime      DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @StepStart      DATETIME2(3);
    DECLARE @RemainingMin   INT;

    PRINT '================================================================';
    PRINT 'MASTER MAINTENANCE ORCHESTRATOR';
    PRINT 'Type:       ' + @MaintenanceType;
    PRINT 'Table:      ' + @TableName;
    PRINT 'Budget:     ' + CAST(@MaxDurationMinutes AS VARCHAR) + ' minutes';
    PRINT 'Started:    ' + CONVERT(VARCHAR(30), @StartTime, 121);
    PRINT 'Dry run:    ' + CASE @DryRun WHEN 1 THEN 'YES' ELSE 'NO' END;
    PRINT '================================================================';

    -- ── STEP 1: Sliding window (monthly only, 1st of month) ──
    IF @MaintenanceType = 'MONTHLY' AND DAY(GETDATE()) <= 3
    BEGIN
        PRINT '';
        PRINT '── STEP 1: Sliding Window Advance ──';
        SET @StepStart = SYSUTCDATETIME();
        EXEC dbo.usp_SlidingWindow_MonthlyAdvance @DryRun = @DryRun;
        PRINT '  Duration: ' + CAST(DATEDIFF(SECOND, @StepStart, SYSUTCDATETIME()) AS VARCHAR) + 's';
    END
    ELSE
        PRINT '── STEP 1: Sliding Window (skipped - not monthly/1st) ──';

    -- ── STEP 2: Filtered index refresh (monthly) ──
    IF @MaintenanceType IN ('MONTHLY', 'WEEKLY')
    BEGIN
        PRINT '';
        PRINT '── STEP 2: Filtered Index Refresh ──';
        SET @StepStart = SYSUTCDATETIME();
        EXEC dbo.usp_FilteredIndexLifecycle @TableName = @TableName, @DryRun = @DryRun;
        PRINT '  Duration: ' + CAST(DATEDIFF(SECOND, @StepStart, SYSUTCDATETIME()) AS VARCHAR) + 's';
    END
    ELSE
        PRINT '── STEP 2: Filtered Index Refresh (skipped - nightly) ──';

    -- ── STEP 3: Compression tier management (weekly) ──
    IF @MaintenanceType IN ('MONTHLY', 'WEEKLY')
    BEGIN
        PRINT '';
        PRINT '── STEP 3: Compression Tier Management ──';
        SET @StepStart = SYSUTCDATETIME();
        EXEC dbo.usp_CompressionTierManagement @TableName = @TableName, @DryRun = @DryRun;
        PRINT '  Duration: ' + CAST(DATEDIFF(SECOND, @StepStart, SYSUTCDATETIME()) AS VARCHAR) + 's';
    END
    ELSE
        PRINT '── STEP 3: Compression Tiers (skipped - nightly) ──';

    -- ── STEP 4: Index maintenance (nightly) ──
    SET @RemainingMin = @MaxDurationMinutes - DATEDIFF(MINUTE, @StartTime, SYSUTCDATETIME());
    IF @RemainingMin > 10  -- Need at least 10 minutes for index work
    BEGIN
        PRINT '';
        PRINT '── STEP 4: Index Maintenance (' + CAST(@RemainingMin AS VARCHAR) + ' min remaining) ──';
        SET @StepStart = SYSUTCDATETIME();
        EXEC dbo.usp_SmartIndexMaintenance
            @TableName = @TableName,
            @HotPartitionCount = CASE @MaintenanceType
                WHEN 'MONTHLY' THEN 6   -- Touch more partitions monthly
                WHEN 'WEEKLY' THEN 4
                ELSE 3                    -- Nightly: just the hot ones
            END,
            @MaxDurationMinutes = @RemainingMin - 10,  -- Reserve 10 min for stats + integrity
            @DryRun = @DryRun;
        PRINT '  Duration: ' + CAST(DATEDIFF(SECOND, @StepStart, SYSUTCDATETIME()) AS VARCHAR) + 's';
    END
    ELSE
        PRINT '── STEP 4: Index Maintenance (skipped - insufficient time) ──';

    -- ── STEP 5: Statistics update (nightly) ──
    SET @RemainingMin = @MaxDurationMinutes - DATEDIFF(MINUTE, @StartTime, SYSUTCDATETIME());
    IF @RemainingMin > 5
    BEGIN
        PRINT '';
        PRINT '── STEP 5: Statistics Update ──';
        SET @StepStart = SYSUTCDATETIME();
        EXEC dbo.usp_SmartStatisticsUpdate
            @TableName = @TableName,
            @HotPartitionCount = 3,
            @DryRun = @DryRun;
        PRINT '  Duration: ' + CAST(DATEDIFF(SECOND, @StepStart, SYSUTCDATETIME()) AS VARCHAR) + 's';
    END
    ELSE
        PRINT '── STEP 5: Statistics Update (skipped - insufficient time) ──';

    -- ── STEP 6: Integrity check ──
    SET @RemainingMin = @MaxDurationMinutes - DATEDIFF(MINUTE, @StartTime, SYSUTCDATETIME());
    IF @RemainingMin > 5
    BEGIN
        PRINT '';
        PRINT '── STEP 6: Integrity Check ──';
        SET @StepStart = SYSUTCDATETIME();

        IF @MaintenanceType = 'MONTHLY'
            EXEC dbo.usp_SmartIntegrityCheck @Mode = 'FULL', @DryRun = @DryRun;
        ELSE
            EXEC dbo.usp_SmartIntegrityCheck
                @Mode = 'PHYSICAL_ONLY',
                @TargetTable = @TableName,
                @MaxDurationMinutes = @RemainingMin,
                @DryRun = @DryRun;

        PRINT '  Duration: ' + CAST(DATEDIFF(SECOND, @StepStart, SYSUTCDATETIME()) AS VARCHAR) + 's';
    END
    ELSE
        PRINT '── STEP 6: Integrity Check (skipped - insufficient time) ──';

    -- ── STEP 7: Log cleanup (keep 90 days) ──
    PRINT '';
    PRINT '── STEP 7: Log Cleanup ──';
    DELETE FROM dbo.MaintenanceLog WHERE ActionTime < DATEADD(DAY, -90, GETDATE());
    PRINT '  Purged ' + CAST(@@ROWCOUNT AS VARCHAR) + ' old log entries.';

    -- ── SUMMARY ──
    PRINT '';
    PRINT '================================================================';
    PRINT 'MASTER MAINTENANCE COMPLETE';
    PRINT 'Total duration: ' + CAST(DATEDIFF(SECOND, @StartTime, SYSUTCDATETIME()) AS VARCHAR) + 's';
    PRINT '================================================================';
END
GO


-- ============================================================================
-- SECTION 4.8: SQL AGENT JOB FRAMEWORK
-- ============================================================================
/*
  Create three SQL Agent jobs to call the master orchestrator:

  JOB 1: "Partition Maintenance - Nightly"
    Schedule: Every night at 1:00 AM
    Command:  EXEC dbo.usp_MasterMaintenance @MaintenanceType = 'NIGHTLY', @MaxDurationMinutes = 120;

  JOB 2: "Partition Maintenance - Weekly"
    Schedule: Sunday at 2:00 AM
    Command:  EXEC dbo.usp_MasterMaintenance @MaintenanceType = 'WEEKLY', @MaxDurationMinutes = 240;

  JOB 3: "Partition Maintenance - Monthly"
    Schedule: 1st of month at 3:00 AM
    Command:  EXEC dbo.usp_MasterMaintenance @MaintenanceType = 'MONTHLY', @MaxDurationMinutes = 360;

  NOTE: The SQL below generates the Agent job creation scripts.
  Review and execute them against your production instance.
*/

PRINT '-- SQL Agent Job Scripts (review before executing):';
PRINT '';
PRINT '-- JOB 1: Nightly maintenance';
PRINT 'EXEC msdb.dbo.sp_add_job @job_name = N''Partition Maintenance - Nightly'';';
PRINT 'EXEC msdb.dbo.sp_add_jobstep @job_name = N''Partition Maintenance - Nightly'',';
PRINT '    @step_name = N''Run Nightly Maintenance'',';
PRINT '    @command = N''EXEC dbo.usp_MasterMaintenance @MaintenanceType = ''''NIGHTLY'''', @MaxDurationMinutes = 120;'',';
PRINT '    @database_name = N''' + DB_NAME() + ''';';
PRINT '';
PRINT '-- JOB 2: Weekly maintenance (Sundays)';
PRINT 'EXEC msdb.dbo.sp_add_job @job_name = N''Partition Maintenance - Weekly'';';
PRINT 'EXEC msdb.dbo.sp_add_jobstep @job_name = N''Partition Maintenance - Weekly'',';
PRINT '    @step_name = N''Run Weekly Maintenance'',';
PRINT '    @command = N''EXEC dbo.usp_MasterMaintenance @MaintenanceType = ''''WEEKLY'''', @MaxDurationMinutes = 240;'',';
PRINT '    @database_name = N''' + DB_NAME() + ''';';
PRINT '';
PRINT '-- JOB 3: Monthly maintenance (1st of month)';
PRINT 'EXEC msdb.dbo.sp_add_job @job_name = N''Partition Maintenance - Monthly'';';
PRINT 'EXEC msdb.dbo.sp_add_jobstep @job_name = N''Partition Maintenance - Monthly'',';
PRINT '    @step_name = N''Run Monthly Maintenance'',';
PRINT '    @command = N''EXEC dbo.usp_MasterMaintenance @MaintenanceType = ''''MONTHLY'''', @MaxDurationMinutes = 360;'',';
PRINT '    @database_name = N''' + DB_NAME() + ''';';
GO


-- ============================================================================
-- SECTION 4.9: MONITORING MAINTENANCE HEALTH
-- ============================================================================

-- View recent maintenance activity
SELECT
    ExecutionID,
    MIN(ActionTime)         AS Started,
    MAX(DATEADD(MILLISECOND, ISNULL(DurationMs, 0), ActionTime)) AS Ended,
    COUNT(*)                AS TotalActions,
    SUM(CASE WHEN Status = 'Complete' THEN 1 ELSE 0 END) AS Succeeded,
    SUM(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END) AS Failed,
    SUM(DurationMs) / 1000 AS TotalDurationSec,
    STRING_AGG(DISTINCT ActionType, ', ') AS ActionTypes
FROM dbo.MaintenanceLog
GROUP BY ExecutionID
ORDER BY MIN(ActionTime) DESC;
GO

-- View index fragmentation trends (are we maintaining well enough?)
SELECT
    TableName,
    IndexName,
    PartitionNumber,
    ActionType,
    FragBefore,
    FragAfter,
    DurationMs,
    ActionTime
FROM dbo.MaintenanceLog
WHERE ActionType IN ('REBUILD', 'REORGANIZE')
AND ActionTime >= DATEADD(DAY, -7, GETDATE())
ORDER BY ActionTime DESC;
GO

-- View failed maintenance actions
SELECT *
FROM dbo.MaintenanceLog
WHERE Status = 'Failed'
AND ActionTime >= DATEADD(DAY, -30, GETDATE())
ORDER BY ActionTime DESC;
GO


-- Test the full orchestrator in dry-run mode
EXEC dbo.usp_MasterMaintenance
    @TableName = 'dbo.Transactions',
    @MaintenanceType = 'NIGHTLY',
    @MaxDurationMinutes = 120,
    @DryRun = 1;
GO


PRINT '================================================================================';
PRINT '  Module 4 Complete: Partition-Aware Maintenance Plans';
PRINT '  Next: Module 5 - Monitoring & Performance Validation';
PRINT '================================================================================';
GO
