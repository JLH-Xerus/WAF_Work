/*
================================================================================
  SQL Server 2019 Table Partitioning POC - Module 3
  Partition SWITCH Operations & Data Lifecycle Management
================================================================================
  Author:       Justin Hunter
  Date:         2026-02-22
  SQL Server:   2019 (Enterprise Edition)
  Purpose:      Implement the sliding window pattern for data lifecycle
                management using partition SWITCH operations

  Module 3 covers:
    3.1  What is partition SWITCH and why is it transformative?
    3.2  Prerequisites and alignment requirements
    3.3  The sliding window pattern
    3.4  SWITCH OUT: Purging old data (milliseconds, not hours)
    3.5  SWITCH IN: Bulk loading new data
    3.6  SPLIT and MERGE: Adding and removing partition boundaries
    3.7  Automating the sliding window (monthly maintenance procedure)
    3.8  Edge cases, gotchas, and production considerations
================================================================================
*/

USE PartitioningPOC;
GO

-- ============================================================================
-- SECTION 3.1: WHAT IS PARTITION SWITCH?
-- ============================================================================
/*
  ALTER TABLE ... SWITCH is SQL Server's mechanism for instantly moving an
  entire partition's data between tables. It is a METADATA-ONLY operation:
  no data physically moves. SQL Server simply reassigns the pages from one
  table to another by updating internal allocation pointers.

  WHY THIS IS TRANSFORMATIVE:

  Consider your scenario: you have 6 billion rows and need to purge data
  older than 12 months. Without partitioning:

    DELETE FROM Transactions WHERE TransactionDate < '2025-02-01';

    This generates:
      - Billions of row-level delete operations
      - Massive transaction log (2-5x the data size)
      - Hours or days of execution time
      - Locks that block concurrent operations
      - Fragmentation requiring subsequent index rebuilds

  With partition SWITCH:

    ALTER TABLE Transactions SWITCH PARTITION 1 TO Transactions_Archive;
    TRUNCATE TABLE Transactions_Archive;

    This completes in:
      - MILLISECONDS (metadata-only)
      - Minimal transaction log (just the metadata change)
      - No blocking of concurrent operations
      - No fragmentation
      - Billions of rows gone instantly

  The same principle applies in reverse for BULK LOADING. Instead of
  inserting millions of rows into a live table (which fragments indexes
  and generates enormous log), you load into a staging table offline,
  then SWITCH the partition in.
*/


-- ============================================================================
-- SECTION 3.2: PREREQUISITES AND ALIGNMENT REQUIREMENTS
-- ============================================================================
/*
  SWITCH has strict requirements. ALL of these must be met or the operation
  fails with an error:

  1. IDENTICAL SCHEMAS:
     Source and target tables must have exactly the same columns, data types,
     nullability, and column order. Computed columns, defaults, and
     constraints must match.

  2. SAME FILEGROUP:
     The source partition and the target table/partition must reside on the
     same filegroup. (This is handled automatically if both tables use the
     same partition scheme.)

  3. INDEX ALIGNMENT:
     Every index on the source table must have a corresponding index on the
     target table with the same key columns, included columns, and filter.
     All indexes must be partition-aligned.

  4. CHECK CONSTRAINTS (for SWITCH IN):
     When switching data INTO a partitioned table, the target partition must
     have a CHECK constraint that guarantees the incoming data belongs in
     that partition. Without this, SQL Server can't verify the data fits.

  5. NO FOREIGN KEYS:
     Tables involved in SWITCH cannot have foreign key relationships.
     You must drop FKs before switching and recreate them after.
     (This is a common source of frustration in production.)

  6. NO FULL-TEXT INDEXES:
     Full-text indexes must be dropped before SWITCH.

  7. EMPTY TARGET:
     When switching a partition OUT, the target table/partition must be empty.
     When switching a partition IN, the target partition must be empty.
*/

-- Let's build the infrastructure to demonstrate all of this.

-- ----------------------------------------------------------------------------
-- 3.2.1  Create the staging/archive tables (SWITCH targets)
-- ----------------------------------------------------------------------------

-- ARCHIVE TABLE: Receives partitions switched out for purging
-- Must match the Transactions table schema exactly
-- Must be on the same filegroup as the partition being switched

-- For switching out the ARCHIVE partition (partition 1, on FG_Cold_Archive):
CREATE TABLE dbo.Transactions_SwitchOut (
    TransactionID       BIGINT          NOT NULL,
    TransactionDate     DATETIME2(3)    NOT NULL,
    AccountID           INT             NOT NULL,
    TransactionTypeID   TINYINT         NOT NULL,
    Amount              DECIMAL(18,2)   NOT NULL,
    StatusCode          CHAR(1)         NOT NULL DEFAULT 'A',
    ReferenceNumber     VARCHAR(50)     NULL,
    CreatedDate         DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate        DATETIME2(3)    NULL,

    CONSTRAINT PK_Transactions_SwitchOut
        PRIMARY KEY CLUSTERED (TransactionDate, TransactionID)
) ON FG_Cold_Archive;  -- MUST match the filegroup of the partition being switched
GO

-- The switch-out table needs matching indexes
CREATE NONCLUSTERED INDEX IX_SwitchOut_AccountID
ON dbo.Transactions_SwitchOut (AccountID, TransactionDate)
INCLUDE (Amount, StatusCode)
ON FG_Cold_Archive;
GO

-- STAGING TABLE: For loading new data before switching IN
-- For switching into the CURRENT partition (partition 7, on FG_Hot_Current):
CREATE TABLE dbo.Transactions_SwitchIn (
    TransactionID       BIGINT          NOT NULL,
    TransactionDate     DATETIME2(3)    NOT NULL,
    AccountID           INT             NOT NULL,
    TransactionTypeID   TINYINT         NOT NULL,
    Amount              DECIMAL(18,2)   NOT NULL,
    StatusCode          CHAR(1)         NOT NULL DEFAULT 'A',
    ReferenceNumber     VARCHAR(50)     NULL,
    CreatedDate         DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate        DATETIME2(3)    NULL,

    -- CHECK CONSTRAINT: Required for SWITCH IN
    -- Guarantees all data belongs in the target partition
    CONSTRAINT CK_SwitchIn_DateRange
        CHECK (TransactionDate >= '2026-02-01'),  -- Matches partition 7's boundary

    CONSTRAINT PK_Transactions_SwitchIn
        PRIMARY KEY CLUSTERED (TransactionDate, TransactionID)
) ON FG_Hot_Current;  -- MUST match the target partition's filegroup
GO

-- Matching index
CREATE NONCLUSTERED INDEX IX_SwitchIn_AccountID
ON dbo.Transactions_SwitchIn (AccountID, TransactionDate)
INCLUDE (Amount, StatusCode)
ON FG_Hot_Current;
GO


-- ============================================================================
-- SECTION 3.3: THE SLIDING WINDOW PATTERN
-- ============================================================================
/*
  The sliding window is the standard pattern for managing time-based
  partitioned tables. As time advances:

    1. A NEW partition is added at the "right" (future) end for incoming data
    2. The OLDEST partition is switched OUT at the "left" (past) end for purging

  This creates a window that slides forward in time, always containing
  a fixed number of months/periods of data.

  VISUAL REPRESENTATION:

  Month:    Aug   Sep   Oct   Nov   Dec   Jan   Feb   Mar
            ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┐
  Before:   │ P1  │ P2  │ P3  │ P4  │ P5  │ P6  │ P7  │
            │ARCH │COLD │COLD │COLD │WARM │WARM │ HOT │
            └─────┴─────┴─────┴─────┴─────┴─────┴─────┘
                                                      ↑ current

  After sliding window (entering March):
            ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┐
            │ P1  │ P2  │ P3  │ P4  │ P5  │ P6  │ P7  │
            │ARCH │COLD │COLD │WARM │WARM │ HOT │ NEW │
            └─────┴─────┴─────┴─────┴─────┴─────┴─────┘
                                                      ↑ current

  The steps for each window slide:
    1. SPLIT the rightmost partition to create a new month
    2. SWITCH OUT the leftmost partition to purge old data
    3. MERGE the empty leftmost boundary to clean up
    4. Update filtered indexes and compression levels
*/


-- ============================================================================
-- SECTION 3.4: SWITCH OUT - Purging Old Data
-- ============================================================================
/*
  Switching OUT removes a partition's data from the table by moving it
  to a staging table. The staging table can then be truncated (instant)
  or archived to another location.

  This replaces:
    DELETE FROM Transactions WHERE TransactionDate < '2025-09-01'
  Which would take hours and generate massive transaction log.
*/

-- First, let's verify what's in partition 1 (archive, < Sep 2025)
SELECT
    p.partition_number,
    p.rows,
    fg.name AS Filegroup
FROM sys.partitions p
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.destination_data_spaces dds
    ON i.data_space_id = dds.partition_scheme_id
    AND p.partition_number = dds.destination_id
JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE p.object_id = OBJECT_ID('dbo.Transactions')
AND i.type = 1
ORDER BY p.partition_number;
GO

-- Verify the switch-out table is empty (required)
SELECT COUNT(*) AS SwitchOutRowCount FROM dbo.Transactions_SwitchOut;
GO

-- THE SWITCH OUT (this is the magic moment)
-- This moves ALL rows from partition 1 to the staging table instantly
ALTER TABLE dbo.Transactions
SWITCH PARTITION 1 TO dbo.Transactions_SwitchOut;
GO

-- Verify: partition 1 is now empty
SELECT 'Transactions P1' AS Source, COUNT(*) AS Rows
FROM dbo.Transactions
WHERE $PARTITION.pf_TransactionDate(TransactionDate) = 1
UNION ALL
SELECT 'SwitchOut Table', COUNT(*)
FROM dbo.Transactions_SwitchOut;
GO

-- Now purge the data (instant truncate, not row-by-row delete)
TRUNCATE TABLE dbo.Transactions_SwitchOut;
GO

/*
  WHAT JUST HAPPENED:
    - Billions of rows (in production) moved out of the live table
    - The operation took MILLISECONDS
    - Zero transaction log (metadata only)
    - Zero blocking of concurrent queries
    - Zero fragmentation created
    - The data is gone
*/


-- ============================================================================
-- SECTION 3.5: SWITCH IN - Bulk Loading New Data
-- ============================================================================
/*
  Switching IN is the reverse: load data into a staging table offline
  (where you can index it, validate it, compress it) and then instantly
  move it into the partitioned table.

  This is useful for:
    - ETL processes that load large batches of historical data
    - Data migrations from other systems
    - Reloading a corrected partition after a data quality fix

  ADVANTAGES over direct INSERT:
    - Zero impact on the live table during the load phase
    - The staging table can be indexed and compressed offline
    - Validation happens before the data enters the live table
    - The SWITCH itself is instant and non-blocking
*/

-- Load data into the staging table (offline, no impact on live table)
-- In production, this would be your ETL/bulk insert process
INSERT INTO dbo.Transactions_SwitchIn
    (TransactionID, TransactionDate, AccountID, TransactionTypeID,
     Amount, StatusCode, ReferenceNumber, CreatedDate)
SELECT
    900000000 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
    DATEADD(SECOND,
        ABS(CHECKSUM(NEWID())) % (86400 * 20),  -- Spread across 20 days in Feb
        '2026-02-01'
    ),
    ABS(CHECKSUM(NEWID())) % 10000 + 1,
    ABS(CHECKSUM(NEWID())) % 5 + 1,
    CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) / 100,
    'A',
    'BULK-' + CAST(ABS(CHECKSUM(NEWID())) % 999999 AS VARCHAR(10)),
    SYSUTCDATETIME()
FROM (SELECT TOP 5000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) n
      FROM sys.all_objects a CROSS JOIN sys.all_objects b) nums;
GO

-- Verify the staging data passes the CHECK constraint
-- (If any row violates the constraint, the INSERT above would have failed)
SELECT
    MIN(TransactionDate) AS EarliestDate,
    MAX(TransactionDate) AS LatestDate,
    COUNT(*) AS RowCount
FROM dbo.Transactions_SwitchIn;
GO

-- Verify partition 7 is empty (required for SWITCH IN to a specific partition)
-- NOTE: In practice, the current partition is NOT empty -- it has live data.
-- So SWITCH IN is typically used for HISTORICAL data loading, not current.
-- For current data, you'd SPLIT to create a new empty partition first.

-- For demonstration, let's switch into a partition we know is empty (partition 1)
-- First, we need a staging table on the right filegroup with the right CHECK
DROP TABLE IF EXISTS dbo.Transactions_SwitchIn_Archive;
GO

CREATE TABLE dbo.Transactions_SwitchIn_Archive (
    TransactionID       BIGINT          NOT NULL,
    TransactionDate     DATETIME2(3)    NOT NULL,
    AccountID           INT             NOT NULL,
    TransactionTypeID   TINYINT         NOT NULL,
    Amount              DECIMAL(18,2)   NOT NULL,
    StatusCode          CHAR(1)         NOT NULL DEFAULT 'A',
    ReferenceNumber     VARCHAR(50)     NULL,
    CreatedDate         DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate        DATETIME2(3)    NULL,

    -- CHECK: Data must be < Sep 2025 (partition 1's range)
    CONSTRAINT CK_SwitchIn_Archive_DateRange
        CHECK (TransactionDate < '2025-09-01'),

    CONSTRAINT PK_SwitchIn_Archive
        PRIMARY KEY CLUSTERED (TransactionDate, TransactionID)
) ON FG_Cold_Archive;
GO

CREATE NONCLUSTERED INDEX IX_SwitchIn_Archive_AccountID
ON dbo.Transactions_SwitchIn_Archive (AccountID, TransactionDate)
INCLUDE (Amount, StatusCode)
ON FG_Cold_Archive;
GO

-- Load some archive-era data
INSERT INTO dbo.Transactions_SwitchIn_Archive
    (TransactionID, TransactionDate, AccountID, TransactionTypeID,
     Amount, StatusCode, ReferenceNumber, CreatedDate)
SELECT
    800000000 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
    DATEADD(DAY,
        -(ABS(CHECKSUM(NEWID())) % 180 + 180),  -- 6-12 months before Sep 2025
        '2025-09-01'
    ),
    ABS(CHECKSUM(NEWID())) % 10000 + 1,
    ABS(CHECKSUM(NEWID())) % 5 + 1,
    CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) / 100,
    'C',  -- Completed
    'ARCH-' + CAST(ABS(CHECKSUM(NEWID())) % 999999 AS VARCHAR(10)),
    SYSUTCDATETIME()
FROM (SELECT TOP 5000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) n
      FROM sys.all_objects a CROSS JOIN sys.all_objects b) nums;
GO

-- SWITCH IN: Instantly load the archive data into partition 1
ALTER TABLE dbo.Transactions_SwitchIn_Archive
SWITCH TO dbo.Transactions PARTITION 1;
GO

-- Verify
SELECT 'Partition 1' AS Source, COUNT(*) AS Rows
FROM dbo.Transactions
WHERE $PARTITION.pf_TransactionDate(TransactionDate) = 1
UNION ALL
SELECT 'Staging Table', COUNT(*)
FROM dbo.Transactions_SwitchIn_Archive;
GO
-- Partition 1 now has the data; staging table is empty


-- ============================================================================
-- SECTION 3.6: SPLIT AND MERGE - Managing Partition Boundaries
-- ============================================================================
/*
  As the sliding window advances, you need to:
    - SPLIT: Add a new boundary to create a partition for the new month
    - MERGE: Remove an old boundary to consolidate after purging

  SPLIT creates an additional partition by adding a boundary value.
  MERGE removes a partition by removing a boundary value.

  CRITICAL: SPLIT and MERGE are NOT metadata-only operations if the
  partition being split/merged contains data. Data must physically move
  to the correct side of the new/removed boundary. For this reason:

    - Always SPLIT an EMPTY partition (the rightmost one, which is the
      "future" partition that holds no or minimal data)
    - Always MERGE an EMPTY partition (the one you just switched out)

  If you follow this rule, SPLIT and MERGE are instant.
*/

-- ----------------------------------------------------------------------------
-- 3.6.1  SPLIT: Add a new partition for March 2026
-- ----------------------------------------------------------------------------

-- First, designate which filegroup the new partition will use.
-- The NEXT USED filegroup tells SQL Server where to put the new partition.
ALTER PARTITION SCHEME ps_TransactionDate
NEXT USED FG_Hot_Current;  -- New month goes on the hot tier
GO

-- Now SPLIT the rightmost boundary to create the March partition.
-- Before: Partition 7 holds everything >= '2026-02-01' (Feb + future)
-- After:  Partition 7 holds Feb, Partition 8 holds everything >= '2026-03-01'
ALTER PARTITION FUNCTION pf_TransactionDate()
SPLIT RANGE ('2026-03-01');
GO

-- Verify: We now have 8 partitions
SELECT
    p.partition_number,
    p.rows,
    fg.name AS Filegroup,
    prv.value AS LowerBoundary
FROM sys.partitions p
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.destination_data_spaces dds
    ON i.data_space_id = dds.partition_scheme_id
    AND p.partition_number = dds.destination_id
JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
LEFT JOIN sys.partition_range_values prv
    ON prv.function_id = (SELECT function_id FROM sys.partition_schemes
                          WHERE data_space_id = i.data_space_id)
    AND prv.boundary_id = p.partition_number - 1
WHERE p.object_id = OBJECT_ID('dbo.Transactions')
AND i.type = 1
ORDER BY p.partition_number;
GO


-- ----------------------------------------------------------------------------
-- 3.6.2  MERGE: Remove the oldest boundary after purging
-- ----------------------------------------------------------------------------
/*
  After switching out and purging partition 1 (archive), and if partition 2
  (Sep 2025) is also switched out, you can MERGE the Sep 2025 boundary
  to consolidate the empty partitions.

  IMPORTANT: Only merge EMPTY partitions. Merging non-empty partitions
  causes data movement, which is slow and generates transaction log.
*/

-- For demonstration: switch out partition 2 (Sep 2025) first
-- (We need a staging table on the right filegroup)
DROP TABLE IF EXISTS dbo.Transactions_SwitchOut_Sep;
GO

CREATE TABLE dbo.Transactions_SwitchOut_Sep (
    TransactionID       BIGINT          NOT NULL,
    TransactionDate     DATETIME2(3)    NOT NULL,
    AccountID           INT             NOT NULL,
    TransactionTypeID   TINYINT         NOT NULL,
    Amount              DECIMAL(18,2)   NOT NULL,
    StatusCode          CHAR(1)         NOT NULL DEFAULT 'A',
    ReferenceNumber     VARCHAR(50)     NULL,
    CreatedDate         DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate        DATETIME2(3)    NULL,
    CONSTRAINT PK_SwitchOut_Sep
        PRIMARY KEY CLUSTERED (TransactionDate, TransactionID)
) ON FG_Cold_Month5;
GO

CREATE NONCLUSTERED INDEX IX_SwitchOut_Sep_AccountID
ON dbo.Transactions_SwitchOut_Sep (AccountID, TransactionDate)
INCLUDE (Amount, StatusCode)
ON FG_Cold_Month5;
GO

-- Switch out partition 2
ALTER TABLE dbo.Transactions
SWITCH PARTITION 2 TO dbo.Transactions_SwitchOut_Sep;
GO

-- Purge
TRUNCATE TABLE dbo.Transactions_SwitchOut_Sep;
GO

-- Now both partition 1 and 2 are empty. MERGE the Sep 2025 boundary.
ALTER PARTITION FUNCTION pf_TransactionDate()
MERGE RANGE ('2025-09-01');
GO

-- Verify: Back to 7 partitions, with the archive partition now covering
-- everything before Oct 2025
SELECT
    p.partition_number,
    p.rows,
    fg.name AS Filegroup,
    prv.value AS LowerBoundary
FROM sys.partitions p
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.destination_data_spaces dds
    ON i.data_space_id = dds.partition_scheme_id
    AND p.partition_number = dds.destination_id
JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
LEFT JOIN sys.partition_range_values prv
    ON prv.function_id = (SELECT function_id FROM sys.partition_schemes
                          WHERE data_space_id = i.data_space_id)
    AND prv.boundary_id = p.partition_number - 1
WHERE p.object_id = OBJECT_ID('dbo.Transactions')
AND i.type = 1
ORDER BY p.partition_number;
GO


-- ============================================================================
-- SECTION 3.7: AUTOMATING THE SLIDING WINDOW
-- ============================================================================
/*
  This procedure automates the complete monthly sliding window operation:
    1. Create new month partition (SPLIT)
    2. Switch out oldest data partition
    3. Purge or archive the switched-out data
    4. Merge the empty boundary (MERGE)
    5. Update compression on the transitioning partition
    6. Refresh filtered indexes

  Schedule this as a SQL Agent job to run on the 1st of each month.
*/

CREATE OR ALTER PROCEDURE dbo.usp_SlidingWindow_MonthlyAdvance
    @NewMonthDate       DATE = NULL,        -- First day of the new month (NULL = next month)
    @PurgeOldest        BIT = 1,            -- 1 = purge after switch out, 0 = keep for archival
    @RefreshFiltered    BIT = 1,            -- 1 = refresh date-based filtered indexes
    @DryRun             BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @NewBoundary DATE;
    DECLARE @OldestBoundary DATE;
    DECLARE @OldestPartition INT;
    DECLARE @MaxPartition INT;
    DECLARE @OldestFilegroup SYSNAME;
    DECLARE @FunctionID INT;

    -- Determine the new month boundary
    IF @NewMonthDate IS NULL
        SET @NewBoundary = DATEADD(MONTH, 1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1));
    ELSE
        SET @NewBoundary = @NewMonthDate;

    -- Get partition function info
    SELECT @FunctionID = ps.function_id
    FROM sys.partition_schemes ps
    WHERE ps.name = 'ps_TransactionDate';

    -- Find the oldest non-archive boundary
    SELECT @OldestBoundary = MIN(CAST(value AS DATE))
    FROM sys.partition_range_values
    WHERE function_id = @FunctionID;

    -- Find the oldest partition with data
    SELECT TOP 1
        @OldestPartition = p.partition_number,
        @OldestFilegroup = fg.name
    FROM sys.partitions p
    JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
    JOIN sys.destination_data_spaces dds
        ON i.data_space_id = dds.partition_scheme_id
        AND p.partition_number = dds.destination_id
    JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
    WHERE p.object_id = OBJECT_ID('dbo.Transactions')
    AND i.type = 1
    AND p.rows > 0
    ORDER BY p.partition_number;

    SELECT @MaxPartition = MAX(partition_number)
    FROM sys.partitions
    WHERE object_id = OBJECT_ID('dbo.Transactions')
    AND index_id IN (0, 1);

    PRINT '================================================================';
    PRINT 'SLIDING WINDOW MONTHLY ADVANCE';
    PRINT '================================================================';
    PRINT 'New boundary:     ' + CONVERT(VARCHAR, @NewBoundary, 120);
    PRINT 'Oldest boundary:  ' + ISNULL(CONVERT(VARCHAR, @OldestBoundary, 120), 'N/A');
    PRINT 'Oldest partition:  ' + ISNULL(CAST(@OldestPartition AS VARCHAR), 'N/A')
        + ' on ' + ISNULL(@OldestFilegroup, 'N/A');
    PRINT 'Current partitions: ' + CAST(@MaxPartition AS VARCHAR);
    PRINT 'Dry run:          ' + CASE @DryRun WHEN 1 THEN 'YES' ELSE 'NO' END;
    PRINT '================================================================';

    -- STEP 1: Set NEXT USED filegroup for the new partition
    SET @SQL = N'ALTER PARTITION SCHEME ps_TransactionDate NEXT USED FG_Hot_Current;';
    PRINT 'STEP 1 - Set NEXT USED: ' + @SQL;
    IF @DryRun = 0 EXEC sp_executesql @SQL;

    -- STEP 2: SPLIT to create new partition
    SET @SQL = N'ALTER PARTITION FUNCTION pf_TransactionDate() SPLIT RANGE ('''
        + CONVERT(VARCHAR, @NewBoundary, 120) + N''');';
    PRINT 'STEP 2 - SPLIT: ' + @SQL;
    IF @DryRun = 0 EXEC sp_executesql @SQL;

    -- STEP 3: Switch out the oldest partition (if it has data)
    IF @OldestPartition IS NOT NULL AND @PurgeOldest = 1
    BEGIN
        -- Create a temporary switch-out table on the correct filegroup
        -- In production, you'd have a pre-existing staging table
        PRINT 'STEP 3 - SWITCH OUT partition ' + CAST(@OldestPartition AS VARCHAR);
        PRINT '         (In production, execute SWITCH to pre-existing staging table)';
        PRINT '         ALTER TABLE dbo.Transactions SWITCH PARTITION '
            + CAST(@OldestPartition AS VARCHAR)
            + ' TO dbo.Transactions_StagingPurge;';

        -- STEP 4: Purge
        PRINT 'STEP 4 - TRUNCATE staging table (purge)';
    END

    -- STEP 5: MERGE the oldest empty boundary (if we purged)
    IF @OldestBoundary IS NOT NULL AND @PurgeOldest = 1
    BEGIN
        SET @SQL = N'ALTER PARTITION FUNCTION pf_TransactionDate() MERGE RANGE ('''
            + CONVERT(VARCHAR, @OldestBoundary, 120) + N''');';
        PRINT 'STEP 5 - MERGE: ' + @SQL;
        IF @DryRun = 0 AND @OldestPartition IS NOT NULL
            PRINT '         (Skipping in demo - would need staging tables created first)';
    END

    -- STEP 6: Apply compression to the partition transitioning from WARM to COLD
    PRINT 'STEP 6 - Update compression on transitioning partitions';
    PRINT '         ALTER TABLE dbo.Transactions REBUILD PARTITION = <cold_partition>';
    PRINT '         WITH (DATA_COMPRESSION = PAGE, ONLINE = ON);';

    -- STEP 7: Refresh filtered indexes
    IF @RefreshFiltered = 1
    BEGIN
        PRINT 'STEP 7 - Refresh filtered indexes';
        IF @DryRun = 0
            EXEC dbo.usp_RefreshFilteredIndexes @DaysBack = 90, @DryRun = @DryRun;
        ELSE
            PRINT '         EXEC dbo.usp_RefreshFilteredIndexes @DaysBack = 90;';
    END

    PRINT '';
    PRINT '================================================================';
    PRINT 'SLIDING WINDOW ADVANCE COMPLETE';
    PRINT 'New partition count: ' + CAST(@MaxPartition + 1 AS VARCHAR);
    PRINT '================================================================';
END
GO

-- Test the procedure in dry-run mode
EXEC dbo.usp_SlidingWindow_MonthlyAdvance
    @NewMonthDate = '2026-04-01',
    @PurgeOldest = 1,
    @RefreshFiltered = 1,
    @DryRun = 1;
GO


-- ============================================================================
-- SECTION 3.8: EDGE CASES, GOTCHAS, AND PRODUCTION CONSIDERATIONS
-- ============================================================================
/*
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  GOTCHA #1: FILTERED INDEXES BLOCK SWITCH                              │
  ├──────────────────────────────────────────────────────────────────────────┤
  │  If the source table has a filtered index that the target table does    │
  │  NOT have (or vice versa), SWITCH fails. The target table must have     │
  │  EXACTLY matching filtered indexes.                                    │
  │                                                                        │
  │  SOLUTION: Include all filtered indexes on the staging tables, OR      │
  │  drop filtered indexes before SWITCH and recreate after.               │
  │  Since filtered indexes are small, the drop/create is fast.            │
  └──────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────┐
  │  GOTCHA #2: FOREIGN KEYS BLOCK SWITCH                                  │
  ├──────────────────────────────────────────────────────────────────────────┤
  │  Any foreign key referencing the partitioned table (or referenced BY    │
  │  it) prevents SWITCH. You must drop FKs before switching.              │
  │                                                                        │
  │  SOLUTION: Use a pre/post SWITCH procedure that scripts out all FKs,   │
  │  drops them, performs the SWITCH, then recreates them.                 │
  │                                                                        │
  │  ALTERNATIVE: Replace FKs with CHECK constraints or application-level  │
  │  validation. Many high-volume OLTP systems do this for performance     │
  │  reasons anyway.                                                       │
  └──────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────┐
  │  GOTCHA #3: STATISTICS AFTER SWITCH                                    │
  ├──────────────────────────────────────────────────────────────────────────┤
  │  After a SWITCH operation, the statistics on the partitioned table are  │
  │  NOT automatically updated. The optimizer may still think the partition │
  │  has data (or is empty) based on stale stats.                          │
  │                                                                        │
  │  SOLUTION: Always UPDATE STATISTICS after a SWITCH operation.          │
  │  At minimum: UPDATE STATISTICS dbo.Transactions WITH RESAMPLE;         │
  │  Ideally per-partition: UPDATE STATISTICS on the affected partition.    │
  └──────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────┐
  │  GOTCHA #4: IDENTITY COLUMNS AND SWITCH                                │
  ├──────────────────────────────────────────────────────────────────────────┤
  │  If the table has an IDENTITY column, the IDENTITY seed is NOT         │
  │  adjusted after SWITCH. The live table may have gaps or start          │
  │  generating IDs that conflict with archived data if reloaded.          │
  │                                                                        │
  │  SOLUTION: Use BIGINT IDENTITY with sufficient headroom (start at 1,   │
  │  increment by 1, BIGINT max = 9.2 quintillion).                        │
  │  OR use SEQUENCE objects for more control.                             │
  └──────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────┐
  │  GOTCHA #5: SPLIT ON A NON-EMPTY PARTITION                             │
  ├──────────────────────────────────────────────────────────────────────────┤
  │  If you SPLIT a partition that contains data, SQL Server must           │
  │  physically move rows to the correct side of the new boundary.         │
  │  For billions of rows, this takes hours and generates massive log.     │
  │                                                                        │
  │  SOLUTION: ALWAYS split the rightmost (empty/nearly empty) partition.  │
  │  The "future" partition should only contain the current month's data   │
  │  at most. Schedule the SPLIT before the month boundary.               │
  └──────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────┐
  │  GOTCHA #6: CHECK CONSTRAINT REQUIREMENTS FOR SWITCH IN                │
  ├──────────────────────────────────────────────────────────────────────────┤
  │  When switching INTO a partition, the source table MUST have a CHECK   │
  │  constraint that guarantees all rows fit in the target partition.       │
  │  Without this, SQL Server returns error 4972.                          │
  │                                                                        │
  │  The CHECK must be TRUSTED (not WITH NOCHECK). If you add a CHECK     │
  │  with NOCHECK, you must then run:                                      │
  │  ALTER TABLE ... WITH CHECK CHECK CONSTRAINT ...                       │
  │  to make it trusted.                                                   │
  └──────────────────────────────────────────────────────────────────────────┘
*/


-- ============================================================================
-- HELPER: Script to generate SWITCH staging tables automatically
-- ============================================================================
/*
  In production, you'll need staging tables for each filegroup that
  participates in SWITCH operations. This utility generates the CREATE
  TABLE statements for you.
*/

CREATE OR ALTER PROCEDURE dbo.usp_GenerateSwitchStagingDDL
    @SourceTable SYSNAME,
    @PartitionNumber INT,
    @StagingTableName SYSNAME = NULL  -- NULL = auto-generate name
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ObjectID       INT = OBJECT_ID(@SourceTable);
    DECLARE @Filegroup      SYSNAME;
    DECLARE @LowerBound     SQL_VARIANT;
    DECLARE @UpperBound     SQL_VARIANT;
    DECLARE @SchemaID       INT;
    DECLARE @SchemaName     SYSNAME;

    IF @ObjectID IS NULL
    BEGIN
        RAISERROR('Table not found: %s', 16, 1, @SourceTable);
        RETURN;
    END

    -- Get schema
    SELECT @SchemaID = schema_id, @SchemaName = SCHEMA_NAME(schema_id)
    FROM sys.tables WHERE object_id = @ObjectID;

    -- Get filegroup for this partition
    SELECT @Filegroup = fg.name
    FROM sys.indexes i
    JOIN sys.destination_data_spaces dds
        ON i.data_space_id = dds.partition_scheme_id
        AND dds.destination_id = @PartitionNumber
    JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
    WHERE i.object_id = @ObjectID AND i.type = 1;

    -- Get boundaries
    SELECT @LowerBound = prv1.value, @UpperBound = prv2.value
    FROM sys.indexes i
    JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    LEFT JOIN sys.partition_range_values prv1
        ON ps.function_id = prv1.function_id AND prv1.boundary_id = @PartitionNumber - 1
    LEFT JOIN sys.partition_range_values prv2
        ON ps.function_id = prv2.function_id AND prv2.boundary_id = @PartitionNumber
    WHERE i.object_id = @ObjectID AND i.type = 1;

    IF @StagingTableName IS NULL
        SET @StagingTableName = OBJECT_NAME(@ObjectID) + '_Switch_P' + CAST(@PartitionNumber AS VARCHAR);

    PRINT '-- Staging table for ' + @SourceTable + ' partition ' + CAST(@PartitionNumber AS VARCHAR);
    PRINT '-- Filegroup: ' + @Filegroup;
    PRINT '-- Lower bound: ' + ISNULL(CAST(CAST(@LowerBound AS DATETIME2(3)) AS VARCHAR(30)), 'OPEN');
    PRINT '-- Upper bound: ' + ISNULL(CAST(CAST(@UpperBound AS DATETIME2(3)) AS VARCHAR(30)), 'OPEN');
    PRINT '';

    -- Generate column list
    PRINT 'CREATE TABLE ' + @SchemaName + '.' + @StagingTableName + ' (';

    DECLARE @Cols NVARCHAR(MAX) = '';
    SELECT @Cols = @Cols + '    '
        + QUOTENAME(c.name) + ' '
        + tp.name
        + CASE
            WHEN tp.name IN ('varchar','nvarchar','char','nchar','binary','varbinary')
                THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END + ')'
            WHEN tp.name IN ('decimal','numeric')
                THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
            WHEN tp.name IN ('datetime2','datetimeoffset','time')
                THEN '(' + CAST(c.scale AS VARCHAR) + ')'
            ELSE ''
          END
        + CASE WHEN c.is_nullable = 0 THEN ' NOT NULL' ELSE ' NULL' END
        + ',' + CHAR(13) + CHAR(10)
    FROM sys.columns c
    JOIN sys.types tp ON c.system_type_id = tp.system_type_id AND c.user_type_id = tp.user_type_id
    WHERE c.object_id = @ObjectID
    ORDER BY c.column_id;

    PRINT @Cols;

    -- Add CHECK constraint
    IF @LowerBound IS NOT NULL AND @UpperBound IS NOT NULL
        PRINT '    CONSTRAINT CK_' + @StagingTableName + '_Range CHECK ('
            + 'TransactionDate >= ''' + CAST(CAST(@LowerBound AS DATETIME2(3)) AS VARCHAR(30)) + ''''
            + ' AND TransactionDate < ''' + CAST(CAST(@UpperBound AS DATETIME2(3)) AS VARCHAR(30)) + ''')';
    ELSE IF @UpperBound IS NOT NULL
        PRINT '    CONSTRAINT CK_' + @StagingTableName + '_Range CHECK ('
            + 'TransactionDate < ''' + CAST(CAST(@UpperBound AS DATETIME2(3)) AS VARCHAR(30)) + ''')';
    ELSE IF @LowerBound IS NOT NULL
        PRINT '    CONSTRAINT CK_' + @StagingTableName + '_Range CHECK ('
            + 'TransactionDate >= ''' + CAST(CAST(@LowerBound AS DATETIME2(3)) AS VARCHAR(30)) + ''')';

    PRINT ') ON ' + QUOTENAME(@Filegroup) + ';';
    PRINT '';

    -- Generate matching index DDL
    PRINT '-- Matching indexes:';
    DECLARE @IdxSQL NVARCHAR(MAX);
    DECLARE idx_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            'CREATE ' + CASE WHEN i.is_unique = 1 THEN 'UNIQUE ' ELSE '' END
            + i.type_desc COLLATE SQL_Latin1_General_CP1_CI_AS + ' INDEX '
            + QUOTENAME(i.name + '_Switch')
            + ' ON ' + @SchemaName + '.' + @StagingTableName
            + ' (' + STUFF((
                SELECT ', ' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE '' END
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
            + ' ON ' + QUOTENAME(@Filegroup) + ';'
        FROM sys.indexes i
        WHERE i.object_id = @ObjectID
        AND i.type IN (1, 2)
        AND i.is_primary_key = 0;

    OPEN idx_cursor;
    FETCH NEXT FROM idx_cursor INTO @IdxSQL;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT @IdxSQL;
        FETCH NEXT FROM idx_cursor INTO @IdxSQL;
    END
    CLOSE idx_cursor;
    DEALLOCATE idx_cursor;
END
GO

-- Generate staging table DDL for partition 3
EXEC dbo.usp_GenerateSwitchStagingDDL
    @SourceTable = 'dbo.Transactions',
    @PartitionNumber = 3;
GO


-- ============================================================================
-- SUMMARY
-- ============================================================================
/*
  PARTITION SWITCH OPERATIONS CHEAT SHEET:

  SWITCH OUT (purge old data):
    ALTER TABLE dbo.Transactions SWITCH PARTITION <n> TO dbo.StagingTable;
    TRUNCATE TABLE dbo.StagingTable;

  SWITCH IN (bulk load):
    -- Staging table needs CHECK constraint matching target partition bounds
    ALTER TABLE dbo.StagingTable SWITCH TO dbo.Transactions PARTITION <n>;

  SPLIT (add new partition):
    ALTER PARTITION SCHEME ps_TransactionDate NEXT USED <filegroup>;
    ALTER PARTITION FUNCTION pf_TransactionDate() SPLIT RANGE ('<date>');

  MERGE (remove empty partition):
    ALTER PARTITION FUNCTION pf_TransactionDate() MERGE RANGE ('<date>');

  MONTHLY MAINTENANCE SEQUENCE:
    1. SPLIT rightmost partition (create new month)
    2. SWITCH OUT oldest partition
    3. TRUNCATE staging table
    4. MERGE oldest boundary
    5. UPDATE STATISTICS
    6. Refresh filtered indexes
    7. Apply compression to transitioning partition

  NEXT MODULE:
    Module 4 - Partition-Aware Maintenance Plans
*/

PRINT '================================================================================';
PRINT '  Module 3 Complete: Partition SWITCH Operations & Data Lifecycle';
PRINT '  Next: Module 4 - Partition-Aware Maintenance Plans';
PRINT '================================================================================';
GO
