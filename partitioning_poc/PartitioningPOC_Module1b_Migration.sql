/*
================================================================================
  SQL Server 2019 Table Partitioning POC - Module 1b
  Migrating a 6-Billion-Row Table to a Partitioned State
================================================================================
  Author:       Justin Hunter
  Date:         2026-02-22
  SQL Server:   2019 (Enterprise Edition)
  Purpose:      Real-world migration strategies for massive existing tables

  This module covers two approaches:
    APPROACH A: In-Place Rebuild (rebuild clustered index ON partition scheme)
    APPROACH B: Side-by-Side Migration (new table, batch copy, name swap)

  RECOMMENDATION: For a 6-billion-row table, Approach B (Side-by-Side) is
  almost always the right answer. This script explains why and provides
  production-ready implementation patterns for both.
================================================================================
*/

USE PartitioningPOC;
GO

-- ============================================================================
-- DECISION FRAMEWORK: WHICH APPROACH FOR 6 BILLION ROWS?
-- ============================================================================
/*
  ┌──────────────────────┬──────────────────────────┬──────────────────────────┐
  │  Factor              │  Approach A: In-Place    │  Approach B: Side-by-Side│
  ├──────────────────────┼──────────────────────────┼──────────────────────────┤
  │  Downtime Required   │  ONLINE rebuild reduces  │  Near-zero. Old table    │
  │                      │  it, but 6B rows still   │  stays live until the    │
  │                      │  takes HOURS. The final  │  final sp_rename swap    │
  │                      │  phase takes a Sch-M     │  (seconds).              │
  │                      │  lock that blocks ALL    │                          │
  │                      │  DML.                    │                          │
  ├──────────────────────┼──────────────────────────┼──────────────────────────┤
  │  Transaction Log     │  MASSIVE. The entire     │  Controllable. You batch │
  │                      │  rebuild is one implicit │  the inserts and can     │
  │                      │  transaction. For 6B     │  checkpoint between      │
  │                      │  rows expect 2-5x the    │  batches. Log stays      │
  │                      │  table's data size in    │  manageable.             │
  │                      │  log space.              │                          │
  ├──────────────────────┼──────────────────────────┼──────────────────────────┤
  │  Disk Space          │  2x table size (old +    │  2x table size (both     │
  │                      │  new copy during         │  tables coexist until    │
  │                      │  rebuild). SAN must      │  swap). Same space       │
  │                      │  absorb this.            │  requirement, but more   │
  │                      │                          │  predictable.            │
  ├──────────────────────┼──────────────────────────┼──────────────────────────┤
  │  Rollback Safety     │  DANGEROUS. If the       │  SAFE. Old table is      │
  │                      │  rebuild fails 8 hours   │  untouched. If migration │
  │                      │  in, the rollback takes  │  fails, drop the new     │
  │                      │  just as long. You are   │  table and try again.    │
  │                      │  stuck.                  │                          │
  ├──────────────────────┼──────────────────────────┼──────────────────────────┤
  │  Resumability        │  SQL 2017+ supports      │  Inherently resumable.   │
  │  (SQL 2017+)         │  RESUMABLE index         │  Track your watermark    │
  │                      │  rebuilds, but NOT for   │  and restart from where  │
  │                      │  partition scheme        │  you left off.           │
  │                      │  changes.                │                          │
  ├──────────────────────┼──────────────────────────┼──────────────────────────┤
  │  Complexity          │  LOW. One DDL statement. │  MEDIUM. Requires batch  │
  │                      │                          │  copy logic, watermark   │
  │                      │                          │  tracking, delta sync,   │
  │                      │                          │  and a cutover plan.     │
  ├──────────────────────┼──────────────────────────┼──────────────────────────┤
  │  VERDICT (6B rows)   │  TOO RISKY. The log      │  RECOMMENDED. Full       │
  │                      │  blast, lock duration,   │  control over pacing,    │
  │                      │  and rollback risk make  │  resumability, and       │
  │                      │  this unsuitable for     │  near-zero downtime      │
  │                      │  production-scale        │  cutover.                │
  │                      │  tables.                 │                          │
  └──────────────────────┴──────────────────────────┴──────────────────────────┘

  BOTTOM LINE: Approach A is fine for tables under ~100M rows. For 6 billion
  rows, Approach B is the only responsible choice in a production OLTP system.
*/


-- ============================================================================
-- APPROACH A: IN-PLACE REBUILD (shown for completeness, NOT recommended at 6B)
-- ============================================================================
/*
  This approach rebuilds the clustered index ON the partition scheme, which
  physically redistributes all rows into the correct partitions. It's a single
  DDL statement, which makes it attractive -- but at 6B rows the operational
  risks are severe.

  WHEN TO USE THIS:
    - Table has < 100M rows
    - You have a maintenance window measured in hours
    - Your transaction log can handle 2-5x the data size
    - You can tolerate a full rollback if it fails

  HOW IT WORKS:
    1. Ensure the partition function, scheme, and filegroups exist
    2. Drop any non-aligned unique constraints/indexes
    3. Rebuild the clustered index ON the partition scheme
    4. Recreate non-clustered indexes ON the partition scheme
*/

-- Demo: Create a non-partitioned table to show the in-place approach
CREATE TABLE dbo.SmallLookupTable (
    LookupID        INT             IDENTITY(1,1)   NOT NULL,
    LookupDate      DATETIME2(3)    NOT NULL,
    LookupValue     VARCHAR(100)    NOT NULL,
    CONSTRAINT PK_SmallLookup PRIMARY KEY CLUSTERED (LookupDate, LookupID)
);
GO

-- Insert a small amount of data for demonstration
INSERT INTO dbo.SmallLookupTable (LookupDate, LookupValue)
SELECT
    DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 180, '2025-08-15'),
    'Value-' + CAST(ABS(CHECKSUM(NEWID())) % 99999 AS VARCHAR(10))
FROM (SELECT TOP 10000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) n
      FROM sys.all_objects a CROSS JOIN sys.all_objects b) nums;
GO

-- Verify: currently NOT partitioned (single partition)
SELECT
    OBJECT_NAME(p.object_id) AS TableName,
    p.partition_number,
    p.rows,
    ds.name AS DataSpace,
    ds.type_desc
FROM sys.partitions p
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
WHERE p.object_id = OBJECT_ID('dbo.SmallLookupTable')
AND i.type = 1;
GO

-- IN-PLACE PARTITION: Rebuild the clustered index ON the partition scheme
-- This is the actual partitioning step -- one statement
ALTER TABLE dbo.SmallLookupTable
DROP CONSTRAINT PK_SmallLookup;
GO

ALTER TABLE dbo.SmallLookupTable
ADD CONSTRAINT PK_SmallLookup PRIMARY KEY CLUSTERED (LookupDate, LookupID)
ON ps_TransactionDate(LookupDate);  -- <-- Now partitioned!
GO

-- With ONLINE support (reduces blocking, Enterprise Edition):
-- CREATE CLUSTERED INDEX PK_SmallLookup
-- ON dbo.SmallLookupTable (LookupDate, LookupID)
-- WITH (DROP_EXISTING = ON, ONLINE = ON, SORT_IN_TEMPDB = ON)
-- ON ps_TransactionDate(LookupDate);
-- GO

-- Verify: now partitioned across multiple partitions
SELECT
    OBJECT_NAME(p.object_id) AS TableName,
    p.partition_number,
    p.rows,
    fg.name AS Filegroup
FROM sys.partitions p
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.destination_data_spaces dds
    ON i.data_space_id = dds.partition_scheme_id
    AND p.partition_number = dds.destination_id
JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE p.object_id = OBJECT_ID('dbo.SmallLookupTable')
AND i.type = 1
ORDER BY p.partition_number;
GO


-- ============================================================================
-- APPROACH B: SIDE-BY-SIDE MIGRATION (RECOMMENDED FOR 6 BILLION ROWS)
-- ============================================================================
/*
  This is the production-grade approach for massive tables. The strategy:

    1. CREATE the new partitioned table (empty)
    2. BATCH-COPY data from old to new in controlled chunks
    3. TRACK progress with a watermark so you can resume
    4. DELTA-SYNC any rows that changed during migration
    5. CUTOVER: brief lock, final delta, sp_rename swap

  Key advantages:
    - The old table stays fully operational throughout
    - Transaction log stays manageable (batch commits)
    - Fully resumable (restart from the last watermark)
    - Rollback = DROP the new table (instant)
    - Cutover downtime is seconds, not hours
*/

-- ----------------------------------------------------------------------------
-- STEP 1: Create the new partitioned table (identical schema)
-- ----------------------------------------------------------------------------
/*
  The new table must have the exact same columns, data types, and defaults.
  The only difference is that it's created ON the partition scheme.

  IMPORTANT: Do NOT add foreign key constraints yet. FKs will be added
  after the migration is complete and validated. This avoids FK validation
  overhead during the batch copy.
*/

-- For this demo, we'll simulate migrating our existing Transactions table.
-- In production, you'd create the new table alongside the existing one.

CREATE TABLE dbo.Transactions_Partitioned (
    TransactionID       BIGINT          NOT NULL,  -- No IDENTITY; we're copying values
    TransactionDate     DATETIME2(3)    NOT NULL,
    AccountID           INT             NOT NULL,
    TransactionTypeID   TINYINT         NOT NULL,
    Amount              DECIMAL(18,2)   NOT NULL,
    StatusCode          CHAR(1)         NOT NULL DEFAULT 'A',
    ReferenceNumber     VARCHAR(50)     NULL,
    CreatedDate         DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate        DATETIME2(3)    NULL,

    CONSTRAINT PK_Transactions_Part
        PRIMARY KEY CLUSTERED (TransactionDate, TransactionID)
) ON ps_TransactionDate(TransactionDate);
GO

-- Add the same nonclustered indexes
CREATE NONCLUSTERED INDEX IX_TransPart_AccountID
ON dbo.Transactions_Partitioned (AccountID, TransactionDate)
INCLUDE (Amount, StatusCode)
ON ps_TransactionDate(TransactionDate);
GO


-- ----------------------------------------------------------------------------
-- STEP 2: Create the migration tracking table
-- ----------------------------------------------------------------------------
/*
  This table tracks batch progress so you can:
    - Monitor migration speed and ETA
    - Resume from exactly where you left off
    - Audit the migration after completion
*/

CREATE TABLE dbo.MigrationLog (
    BatchID             INT             IDENTITY(1,1) PRIMARY KEY,
    BatchStartTime      DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    BatchEndTime        DATETIME2(3)    NULL,
    RowsCopied          BIGINT          NOT NULL DEFAULT 0,
    WatermarkLow        BIGINT          NOT NULL,   -- TransactionID range start
    WatermarkHigh       BIGINT          NOT NULL,   -- TransactionID range end
    DateRangeLow        DATETIME2(3)    NULL,        -- Date range for this batch
    DateRangeHigh       DATETIME2(3)    NULL,
    DurationSeconds     AS DATEDIFF(SECOND, BatchStartTime, BatchEndTime),
    RowsPerSecond       AS CASE
                            WHEN DATEDIFF(SECOND, BatchStartTime, BatchEndTime) > 0
                            THEN RowsCopied / DATEDIFF(SECOND, BatchStartTime, BatchEndTime)
                            ELSE 0
                         END,
    Status              VARCHAR(20)     NOT NULL DEFAULT 'Running'  -- Running, Complete, Failed
);
GO


-- ----------------------------------------------------------------------------
-- STEP 3: Batch migration procedure
-- ----------------------------------------------------------------------------
/*
  This procedure copies data in controlled batches. Key design decisions:

  BATCH STRATEGY: Copy by TransactionID ranges (not date ranges).
    - TransactionID is sequential and evenly distributed
    - Each batch touches a predictable number of rows
    - The watermark is a simple integer comparison (fast)

  BATCH SIZE: Start with 500,000 rows per batch and tune from there.
    - Too small = excessive overhead from commits and log records
    - Too large = transaction log pressure and longer lock durations
    - 500K is a good starting point for a SAN-attached system

  WHY NOT PARTITION SWITCH FOR LOADING?
    You might think: "Load data into a staging table, then SWITCH into the
    partitioned table." That works for NEW data, but for migration you'd
    need to:
      a) Sort all 6B rows by partition key
      b) Split them into per-partition staging tables
      c) SWITCH each one
    This is actually MORE complex and slower than batched INSERT...SELECT
    because the sorting step alone is massive. Batch copy is simpler and
    the optimizer handles the partition routing automatically.

  TABLOCK HINT:
    Using TABLOCK on the INSERT destination enables minimal logging in
    BULK_LOGGED recovery model, which dramatically reduces log volume.
    Discuss with your DBA whether temporarily switching to BULK_LOGGED
    is acceptable during the migration window.
*/

CREATE OR ALTER PROCEDURE dbo.usp_MigrateTransactions_Batch
    @BatchSize          INT = 500000,
    @MaxBatches         INT = NULL,         -- NULL = run until complete
    @ThrottleSeconds    INT = 0,            -- Pause between batches (reduce I/O pressure)
    @DryRun             BIT = 0             -- 1 = log what would happen, don't copy
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @MinID          BIGINT;
    DECLARE @MaxID          BIGINT;
    DECLARE @CurrentLow     BIGINT;
    DECLARE @CurrentHigh    BIGINT;
    DECLARE @RowsCopied     BIGINT;
    DECLARE @BatchCount     INT = 0;
    DECLARE @TotalCopied    BIGINT = 0;
    DECLARE @BatchID        INT;
    DECLARE @StartTime      DATETIME2(3);

    -- Determine the full ID range in the source table
    SELECT @MinID = MIN(TransactionID), @MaxID = MAX(TransactionID)
    FROM dbo.Transactions;

    -- Find where we left off (resume capability)
    SELECT @CurrentLow = ISNULL(MAX(WatermarkHigh), @MinID - 1) + 1
    FROM dbo.MigrationLog
    WHERE Status = 'Complete';

    IF @CurrentLow > @MaxID
    BEGIN
        PRINT 'Migration already complete. All rows have been copied.';
        PRINT 'Source range: ' + CAST(@MinID AS VARCHAR) + ' to ' + CAST(@MaxID AS VARCHAR);
        PRINT 'Last completed watermark: ' + CAST(@CurrentLow - 1 AS VARCHAR);
        RETURN;
    END

    PRINT '================================================================';
    PRINT 'Starting batch migration';
    PRINT 'Source range:    ' + CAST(@MinID AS VARCHAR) + ' to ' + CAST(@MaxID AS VARCHAR);
    PRINT 'Resuming from:   ' + CAST(@CurrentLow AS VARCHAR);
    PRINT 'Remaining rows:  ~' + CAST(@MaxID - @CurrentLow + 1 AS VARCHAR);
    PRINT 'Batch size:      ' + CAST(@BatchSize AS VARCHAR);
    PRINT 'Max batches:     ' + ISNULL(CAST(@MaxBatches AS VARCHAR), 'Unlimited');
    PRINT 'Throttle:        ' + CAST(@ThrottleSeconds AS VARCHAR) + ' seconds';
    PRINT 'Dry run:         ' + CASE @DryRun WHEN 1 THEN 'YES' ELSE 'NO' END;
    PRINT '================================================================';

    WHILE @CurrentLow <= @MaxID
    BEGIN
        SET @CurrentHigh = @CurrentLow + @BatchSize - 1;
        SET @BatchCount = @BatchCount + 1;
        SET @StartTime = SYSUTCDATETIME();

        -- Check batch limit
        IF @MaxBatches IS NOT NULL AND @BatchCount > @MaxBatches
        BEGIN
            PRINT 'Reached max batch limit (' + CAST(@MaxBatches AS VARCHAR) + '). Stopping.';
            BREAK;
        END

        -- Log batch start
        INSERT INTO dbo.MigrationLog (WatermarkLow, WatermarkHigh, Status)
        VALUES (@CurrentLow, @CurrentHigh, 'Running');
        SET @BatchID = SCOPE_IDENTITY();

        IF @DryRun = 0
        BEGIN
            BEGIN TRY
                -- The actual copy
                -- IDENTITY_INSERT is ON because we're preserving original IDs
                SET IDENTITY_INSERT dbo.Transactions_Partitioned ON;

                INSERT INTO dbo.Transactions_Partitioned WITH (TABLOCK)
                    (TransactionID, TransactionDate, AccountID, TransactionTypeID,
                     Amount, StatusCode, ReferenceNumber, CreatedDate, ModifiedDate)
                SELECT
                    TransactionID, TransactionDate, AccountID, TransactionTypeID,
                    Amount, StatusCode, ReferenceNumber, CreatedDate, ModifiedDate
                FROM dbo.Transactions
                WHERE TransactionID BETWEEN @CurrentLow AND @CurrentHigh;

                SET @RowsCopied = @@ROWCOUNT;

                SET IDENTITY_INSERT dbo.Transactions_Partitioned OFF;

                -- Log batch completion
                UPDATE dbo.MigrationLog
                SET BatchEndTime = SYSUTCDATETIME(),
                    RowsCopied = @RowsCopied,
                    DateRangeLow = (SELECT MIN(TransactionDate) FROM dbo.Transactions
                                    WHERE TransactionID BETWEEN @CurrentLow AND @CurrentHigh),
                    DateRangeHigh = (SELECT MAX(TransactionDate) FROM dbo.Transactions
                                     WHERE TransactionID BETWEEN @CurrentLow AND @CurrentHigh),
                    Status = 'Complete'
                WHERE BatchID = @BatchID;

                SET @TotalCopied = @TotalCopied + @RowsCopied;

            END TRY
            BEGIN CATCH
                -- Log failure and stop
                UPDATE dbo.MigrationLog
                SET BatchEndTime = SYSUTCDATETIME(),
                    Status = 'Failed'
                WHERE BatchID = @BatchID;

                SET IDENTITY_INSERT dbo.Transactions_Partitioned OFF;

                PRINT 'ERROR in batch ' + CAST(@BatchCount AS VARCHAR) + ': ' + ERROR_MESSAGE();
                THROW;
            END CATCH
        END
        ELSE
        BEGIN
            -- Dry run: just log what would happen
            SET @RowsCopied = (SELECT COUNT(*) FROM dbo.Transactions
                               WHERE TransactionID BETWEEN @CurrentLow AND @CurrentHigh);

            UPDATE dbo.MigrationLog
            SET BatchEndTime = SYSUTCDATETIME(),
                RowsCopied = @RowsCopied,
                Status = 'DryRun'
            WHERE BatchID = @BatchID;

            SET @TotalCopied = @TotalCopied + @RowsCopied;
        END

        -- Progress report
        IF @BatchCount % 10 = 0 OR @CurrentHigh >= @MaxID
        BEGIN
            DECLARE @PctComplete DECIMAL(5,2) =
                CAST((@CurrentHigh - @MinID + 1) * 100.0 / (@MaxID - @MinID + 1) AS DECIMAL(5,2));

            PRINT 'Batch ' + CAST(@BatchCount AS VARCHAR)
                + ' | Rows: ' + CAST(@TotalCopied AS VARCHAR)
                + ' | Progress: ' + CAST(@PctComplete AS VARCHAR) + '%'
                + ' | ID range: ' + CAST(@CurrentLow AS VARCHAR)
                + '-' + CAST(@CurrentHigh AS VARCHAR);
        END

        -- Move watermark forward
        SET @CurrentLow = @CurrentHigh + 1;

        -- Throttle if requested (reduce I/O pressure on the SAN)
        IF @ThrottleSeconds > 0
            WAITFOR DELAY @ThrottleSeconds;  -- Simplified; use proper time string in production
    END

    PRINT '================================================================';
    PRINT 'Migration batch run complete.';
    PRINT 'Total batches:   ' + CAST(@BatchCount AS VARCHAR);
    PRINT 'Total rows:      ' + CAST(@TotalCopied AS VARCHAR);
    PRINT '================================================================';
END
GO


-- ----------------------------------------------------------------------------
-- STEP 4: Run the migration (example)
-- ----------------------------------------------------------------------------

-- Dry run first to validate
EXEC dbo.usp_MigrateTransactions_Batch
    @BatchSize = 25000,
    @MaxBatches = 3,
    @DryRun = 1;
GO

-- Check the migration log
SELECT * FROM dbo.MigrationLog ORDER BY BatchID;
GO

-- Clear dry run data and do the real migration
DELETE FROM dbo.MigrationLog;
GO

-- Real migration (small batches for POC; use 500K in production)
EXEC dbo.usp_MigrateTransactions_Batch
    @BatchSize = 25000,
    @MaxBatches = NULL,  -- Run to completion
    @ThrottleSeconds = 0;
GO

-- Verify migration progress
SELECT
    COUNT(*)            AS TotalBatches,
    SUM(RowsCopied)     AS TotalRowsCopied,
    MIN(WatermarkLow)   AS FirstID,
    MAX(WatermarkHigh)  AS LastID,
    SUM(DurationSeconds) AS TotalSeconds,
    CASE WHEN SUM(DurationSeconds) > 0
        THEN SUM(RowsCopied) / SUM(DurationSeconds)
        ELSE 0
    END                 AS OverallRowsPerSecond
FROM dbo.MigrationLog
WHERE Status = 'Complete';
GO


-- ----------------------------------------------------------------------------
-- STEP 5: Validate data integrity before cutover
-- ----------------------------------------------------------------------------
/*
  CRITICAL: Never proceed to cutover without validating:
    1. Row counts match
    2. Spot-check specific rows
    3. Verify partition distribution makes sense
*/

-- Row count comparison
SELECT
    'Source' AS [Table],
    COUNT(*) AS RowCount
FROM dbo.Transactions
UNION ALL
SELECT
    'Destination' AS [Table],
    COUNT(*) AS RowCount
FROM dbo.Transactions_Partitioned;
GO

-- Checksum comparison (sample-based for 6B rows)
-- Compare a random sample of rows by key columns
SELECT
    'Source' AS [Table],
    SUM(CAST(CHECKSUM(TransactionID, TransactionDate, AccountID, Amount) AS BIGINT)) AS CheckVal
FROM dbo.Transactions
WHERE TransactionID % 1000 = 0  -- Sample every 1000th row
UNION ALL
SELECT
    'Destination' AS [Table],
    SUM(CAST(CHECKSUM(TransactionID, TransactionDate, AccountID, Amount) AS BIGINT)) AS CheckVal
FROM dbo.Transactions_Partitioned
WHERE TransactionID % 1000 = 0;
GO

-- Verify partition distribution
SELECT
    p.partition_number,
    fg.name AS Filegroup,
    p.rows,
    FORMAT(p.rows, 'N0') AS RowCount_Formatted
FROM sys.partitions p
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.destination_data_spaces dds
    ON i.data_space_id = dds.partition_scheme_id
    AND p.partition_number = dds.destination_id
JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE p.object_id = OBJECT_ID('dbo.Transactions_Partitioned')
AND i.type = 1
ORDER BY p.partition_number;
GO


-- ----------------------------------------------------------------------------
-- STEP 6: Delta sync (capture changes made during migration)
-- ----------------------------------------------------------------------------
/*
  During the hours/days the batch migration runs, the source table is still
  live. New inserts, updates, and deletes happen. You need to capture these
  deltas before cutover.

  STRATEGIES FOR DELTA CAPTURE:

  Option A: ModifiedDate Column (simplest if you have one)
    - Query source for rows WHERE ModifiedDate > migration_start_time
    - MERGE into the destination
    - Repeat until delta is small enough for the cutover window

  Option B: Change Tracking (built into SQL Server)
    - Enable Change Tracking on the source table BEFORE migration starts
    - Query CHANGETABLE() for all changes since last sync version
    - Apply changes to destination

  Option C: CDC (Change Data Capture)
    - More overhead but captures full change history
    - Best if you need audit trail during migration

  Option D: Trigger-Based (last resort)
    - Add a trigger to the source that logs changes to a staging table
    - Process the staging table during delta sync

  RECOMMENDATION: Option A (ModifiedDate) if the column exists and is
  reliably updated. Option B (Change Tracking) if it doesn't.

  Below is the ModifiedDate approach:
*/

CREATE OR ALTER PROCEDURE dbo.usp_MigrateTransactions_DeltaSync
    @SyncFromTime DATETIME2(3) = NULL   -- NULL = use migration start time
AS
BEGIN
    SET NOCOUNT ON;

    -- Default to when the migration started
    IF @SyncFromTime IS NULL
        SELECT @SyncFromTime = MIN(BatchStartTime) FROM dbo.MigrationLog;

    PRINT 'Delta sync: capturing changes since ' + CONVERT(VARCHAR(30), @SyncFromTime, 121);

    -- MERGE handles inserts, updates, and (optionally) deletes
    SET IDENTITY_INSERT dbo.Transactions_Partitioned ON;

    MERGE dbo.Transactions_Partitioned AS tgt
    USING (
        SELECT TransactionID, TransactionDate, AccountID, TransactionTypeID,
               Amount, StatusCode, ReferenceNumber, CreatedDate, ModifiedDate
        FROM dbo.Transactions
        WHERE ModifiedDate >= @SyncFromTime
           OR CreatedDate >= @SyncFromTime
    ) AS src
    ON tgt.TransactionID = src.TransactionID
       AND tgt.TransactionDate = src.TransactionDate
    WHEN MATCHED AND (
        tgt.Amount <> src.Amount
        OR tgt.StatusCode <> src.StatusCode
        OR ISNULL(tgt.ModifiedDate, '1900-01-01') <> ISNULL(src.ModifiedDate, '1900-01-01')
    ) THEN
        UPDATE SET
            tgt.AccountID = src.AccountID,
            tgt.TransactionTypeID = src.TransactionTypeID,
            tgt.Amount = src.Amount,
            tgt.StatusCode = src.StatusCode,
            tgt.ReferenceNumber = src.ReferenceNumber,
            tgt.ModifiedDate = src.ModifiedDate
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (TransactionID, TransactionDate, AccountID, TransactionTypeID,
                Amount, StatusCode, ReferenceNumber, CreatedDate, ModifiedDate)
        VALUES (src.TransactionID, src.TransactionDate, src.AccountID, src.TransactionTypeID,
                src.Amount, src.StatusCode, src.ReferenceNumber, src.CreatedDate, src.ModifiedDate);

    SET IDENTITY_INSERT dbo.Transactions_Partitioned OFF;

    PRINT 'Delta sync complete. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR);
END
GO


-- ----------------------------------------------------------------------------
-- STEP 7: CUTOVER (the moment of truth)
-- ----------------------------------------------------------------------------
/*
  The cutover is the only moment of real downtime. The goal is to make it
  as short as possible. The sequence:

    1. Stop application traffic (or put in read-only mode)
    2. Run final delta sync
    3. Validate final row counts
    4. sp_rename the tables (metadata-only, milliseconds)
    5. Resume application traffic
    6. Add foreign keys, constraints, synonyms, etc.

  TOTAL DOWNTIME: The time for the final delta sync + validation.
  If deltas are small (hundreds/thousands of rows), this is seconds.

  CRITICAL: sp_rename acquires a schema modification (Sch-M) lock. This
  is instantaneous but will block if any other transaction holds any
  lock on the table. Ensure all connections are drained first.
*/

-- Final delta sync
EXEC dbo.usp_MigrateTransactions_DeltaSync;
GO

-- Final validation
SELECT 'Source' AS [Table], COUNT(*) AS RowCount FROM dbo.Transactions
UNION ALL
SELECT 'Destination', COUNT(*) FROM dbo.Transactions_Partitioned;
GO

-- THE SWAP (milliseconds, metadata only)
BEGIN TRANSACTION;

    -- Rename old table to archive name
    EXEC sp_rename 'dbo.Transactions', 'Transactions_OLD';

    -- Rename new partitioned table to the original name
    EXEC sp_rename 'dbo.Transactions_Partitioned', 'Transactions';

    -- Rename the PK constraint to match the original name
    EXEC sp_rename 'dbo.Transactions.PK_Transactions_Part', 'PK_Transactions', 'INDEX';

COMMIT TRANSACTION;
GO

/*
  POST-CUTOVER TASKS:
    1. Rename indexes on the new table to match original names
    2. Add foreign key constraints
    3. Update any synonyms, views, or stored procedures that reference the table
    4. Update statistics on the new table
    5. Verify application connectivity
    6. Keep the _OLD table for a safety period (1-2 weeks)
    7. DROP the _OLD table when confident

  ROLLBACK PLAN (if something goes wrong):
    BEGIN TRANSACTION;
        EXEC sp_rename 'dbo.Transactions', 'Transactions_Partitioned';
        EXEC sp_rename 'dbo.Transactions_OLD', 'Transactions';
    COMMIT;
*/


-- ============================================================================
-- PRODUCTION CONSIDERATIONS FOR 6 BILLION ROWS
-- ============================================================================
/*
  TIMELINE ESTIMATE:
    At 500K rows/batch with ~2 seconds per batch, you process ~250K rows/sec.
    6 billion rows / 250K per second = ~24,000 seconds = ~6.7 hours.
    Add 50% buffer for I/O contention, index overhead: ~10 hours.
    Plan for a weekend or multi-day migration with throttling.

  TRANSACTION LOG MANAGEMENT:
    - Consider switching to BULK_LOGGED recovery during migration
    - Take log backups every 15-30 minutes during the migration
    - Monitor log space: DBCC SQLPERF(LOGSPACE)
    - Size the log to handle at least 500K rows worth of log data

  TEMPDB:
    - Batch inserts with SORT_IN_TEMPDB can spill if tempdb is undersized
    - Monitor tempdb usage during initial batches and adjust

  SAN I/O:
    - Use @ThrottleSeconds to pace the migration during business hours
    - Run at full speed during off-hours
    - Monitor SAN queue depths and latency

  NONCLUSTERED INDEXES:
    - Option 1: Create NCI on the new table BEFORE migration (each batch
      maintains the indexes during insert -- slower but table is ready)
    - Option 2: Omit NCIs during migration, create them AFTER (faster
      migration but requires an index build phase after)
    - For 6B rows, Option 2 is usually faster overall because a single
      index build is more efficient than incremental maintenance

  STATISTICS:
    - After migration, update statistics with FULLSCAN on the new table
    - This is critical for the optimizer to generate good plans
    - UPDATE STATISTICS dbo.Transactions WITH FULLSCAN;

  COMPRESSION:
    - Consider applying PAGE compression to cold partitions during or
      after migration. This reduces storage and can improve read
      performance for range scans.
    - ALTER TABLE dbo.Transactions
      REBUILD PARTITION = 1  -- archive partition
      WITH (DATA_COMPRESSION = PAGE);
*/


PRINT '================================================================================';
PRINT '  Module 1b Complete: 6-Billion-Row Migration Strategy';
PRINT '  Approach B (Side-by-Side) recommended for production';
PRINT '================================================================================';
GO
