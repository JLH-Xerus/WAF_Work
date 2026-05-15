/*
================================================================================
  SQL Server 2019 Table Partitioning POC - Module 1
  What Is Partitioning & Identifying Candidate Tables
================================================================================
  Author:       Justin Hunter
  Date:         2026-02-22
  SQL Server:   2019 (Enterprise / Developer Edition required for partitioning)
  Purpose:      Proof of Concept for partitioning high-volume OLTP tables

  Module 1 covers:
    1.1  What is partitioning?
    1.2  Required components (partition function, partition scheme, filegroups)
    1.3  Rules and constraints
    1.4  Identifying candidate tables for partitioning
================================================================================
*/

USE master;
GO

-- ============================================================================
-- SECTION 1.1: WHAT IS TABLE PARTITIONING?
-- ============================================================================
/*
  Table partitioning divides a single logical table into multiple physical
  segments (partitions) based on the values of a designated column called the
  "partition key." Each partition is stored as an independent B-tree structure
  on its own filegroup, but the table remains a single logical entity to the
  application layer -- no application code changes required.

  WHY IT MATTERS FOR HIGH-VOLUME OLTP:

  In your scenario, hundreds of millions of rows accumulate over time. Records
  are "active" for roughly 60 days, then become stale but remain in the table.
  Without partitioning, every query -- whether targeting today's hot data or
  last year's cold data -- must contend with the entire B-tree. Partitioning
  addresses this by:

    - PARTITION ELIMINATION: When queries include the partition key in their
      WHERE clause, the optimizer skips irrelevant partitions entirely. A query
      for the last 60 days of data only touches the partitions that contain
      those dates, ignoring the hundreds of millions of archived rows.

    - EFFICIENT DATA LIFECYCLE: Entire partitions can be switched in/out of
      the table in milliseconds via ALTER TABLE ... SWITCH. This is a metadata-
      only operation -- no data movement. This enables:
        * Near-instant purging of old data (switch partition to staging table,
          then truncate)
        * Near-instant loading of bulk data (load into staging table, then
          switch into the partitioned table)

    - TARGETED MAINTENANCE: Index rebuilds, statistics updates, and integrity
      checks can be performed per-partition rather than on the entire table.
      Rebuilding the index on a single month's partition takes a fraction of
      the time (and I/O) compared to the whole table.

    - FILEGROUP ISOLATION: Each partition maps to a filegroup, and each
      filegroup maps to one or more data files on disk. This means:
        * Hot partitions (recent data) can live on fast SAN tiers / SSDs
        * Cold partitions (archived data) can live on slower, cheaper storage
        * Backup/restore can target individual filegroups (piecemeal restore)
*/


-- ============================================================================
-- SECTION 1.2: REQUIRED COMPONENTS OF PARTITIONING
-- ============================================================================
/*
  There are exactly THREE components required to partition a table in SQL Server:

    1. PARTITION FUNCTION   - Defines the boundary values and how rows are
                              mapped to partitions
    2. PARTITION SCHEME     - Maps each partition to a filegroup
    3. FILEGROUPS (& FILES) - The physical storage containers

  These components form a chain:

    Column Value --> Partition Function --> Partition Number --> Partition Scheme --> Filegroup --> File(s)

  Let's walk through each one.
*/

-- ----------------------------------------------------------------------------
-- 1.2.1  FILEGROUPS AND DATA FILES
-- ----------------------------------------------------------------------------
/*
  Filegroups are logical containers for data files (.ndf). Each partition will
  be mapped to a filegroup via the partition scheme. You can:

    - Use one filegroup per partition (maximum flexibility for storage tiering,
      piecemeal backup/restore)
    - Use a single filegroup for all partitions (simpler, but loses the
      storage tiering and piecemeal restore benefits)

  RECOMMENDATION for your SAN environment: Start with dedicated filegroups
  for at least the "hot" vs "cold" tiers. You can always consolidate later.

  IMPORTANT: In production, you would create these on specific SAN LUNs.
  For this POC, we'll use local paths.
*/

-- Create a POC database to demonstrate
IF DB_ID('PartitioningPOC') IS NOT NULL
BEGIN
    ALTER DATABASE PartitioningPOC SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE PartitioningPOC;
END
GO

CREATE DATABASE PartitioningPOC
ON PRIMARY (
    NAME = N'PartitioningPOC_Primary',
    FILENAME = N'/var/opt/mssql/data/PartitioningPOC_Primary.mdf',
    SIZE = 64MB, FILEGROWTH = 64MB
)
LOG ON (
    NAME = N'PartitioningPOC_Log',
    FILENAME = N'/var/opt/mssql/data/PartitioningPOC_Log.ldf',
    SIZE = 64MB, FILEGROWTH = 64MB
);
GO

USE PartitioningPOC;
GO

-- Add filegroups for each monthly partition
-- In production, each filegroup would map to files on specific SAN LUNs
-- Hot tier (recent months): fast SSDs / Tier 1 storage
-- Cold tier (older months): slower / Tier 2 storage

-- Hot tier filegroups (recent 3 months)
ALTER DATABASE PartitioningPOC ADD FILEGROUP FG_Hot_Current;
ALTER DATABASE PartitioningPOC ADD FILEGROUP FG_Hot_Month1;
ALTER DATABASE PartitioningPOC ADD FILEGROUP FG_Hot_Month2;

-- Cold tier filegroups (older months)
ALTER DATABASE PartitioningPOC ADD FILEGROUP FG_Cold_Month3;
ALTER DATABASE PartitioningPOC ADD FILEGROUP FG_Cold_Month4;
ALTER DATABASE PartitioningPOC ADD FILEGROUP FG_Cold_Month5;
ALTER DATABASE PartitioningPOC ADD FILEGROUP FG_Cold_Archive;
GO

-- Add data files to each filegroup
-- In production, these paths would point to specific SAN mount points
ALTER DATABASE PartitioningPOC ADD FILE
    (NAME = N'Hot_Current', FILENAME = N'/var/opt/mssql/data/Hot_Current.ndf', SIZE = 64MB, FILEGROWTH = 64MB)
    TO FILEGROUP FG_Hot_Current;

ALTER DATABASE PartitioningPOC ADD FILE
    (NAME = N'Hot_Month1', FILENAME = N'/var/opt/mssql/data/Hot_Month1.ndf', SIZE = 64MB, FILEGROWTH = 64MB)
    TO FILEGROUP FG_Hot_Month1;

ALTER DATABASE PartitioningPOC ADD FILE
    (NAME = N'Hot_Month2', FILENAME = N'/var/opt/mssql/data/Hot_Month2.ndf', SIZE = 64MB, FILEGROWTH = 64MB)
    TO FILEGROUP FG_Hot_Month2;

ALTER DATABASE PartitioningPOC ADD FILE
    (NAME = N'Cold_Month3', FILENAME = N'/var/opt/mssql/data/Cold_Month3.ndf', SIZE = 64MB, FILEGROWTH = 64MB)
    TO FILEGROUP FG_Cold_Month3;

ALTER DATABASE PartitioningPOC ADD FILE
    (NAME = N'Cold_Month4', FILENAME = N'/var/opt/mssql/data/Cold_Month4.ndf', SIZE = 64MB, FILEGROWTH = 64MB)
    TO FILEGROUP FG_Cold_Month4;

ALTER DATABASE PartitioningPOC ADD FILE
    (NAME = N'Cold_Month5', FILENAME = N'/var/opt/mssql/data/Cold_Month5.ndf', SIZE = 64MB, FILEGROWTH = 64MB)
    TO FILEGROUP FG_Cold_Month5;

ALTER DATABASE PartitioningPOC ADD FILE
    (NAME = N'Cold_Archive', FILENAME = N'/var/opt/mssql/data/Cold_Archive.ndf', SIZE = 64MB, FILEGROWTH = 64MB)
    TO FILEGROUP FG_Cold_Archive;
GO


-- ----------------------------------------------------------------------------
-- 1.2.2  PARTITION FUNCTION
-- ----------------------------------------------------------------------------
/*
  The partition function defines:
    1. The data type of the partition key column
    2. The boundary values that divide the data into partitions
    3. Whether boundary values belong to the LEFT or RIGHT partition

  RANGE LEFT vs RANGE RIGHT:

    RANGE LEFT:  Boundary value belongs to the LEFT partition
                 Partition N holds values <= boundary N
                 Common for DATE-based partitioning (each boundary is the
                 last moment of a period)

    RANGE RIGHT: Boundary value belongs to the RIGHT partition
                 Partition N holds values >= boundary N
                 Also common for DATE-based partitioning (each boundary is
                 the first moment of a period)

  RECOMMENDATION: Use RANGE RIGHT with date boundaries set to the first day
  of each month. This is the most intuitive approach:

    Partition 1: everything < '2025-09-01'    (archive)
    Partition 2: '2025-09-01' to '2025-09-30' (September)
    Partition 3: '2025-10-01' to '2025-10-31' (October)
    ... and so on
    Last Partition: everything >= last boundary (current/future data)

  NOTE: N boundary values create N+1 partitions.
*/

CREATE PARTITION FUNCTION pf_TransactionDate (DATETIME2(3))
AS RANGE RIGHT FOR VALUES (
    '2025-09-01',   -- Partition 1: < Sep 2025 (archive)
    '2025-10-01',   -- Partition 2: Sep 2025
    '2025-11-01',   -- Partition 3: Oct 2025
    '2025-12-01',   -- Partition 4: Nov 2025
    '2026-01-01',   -- Partition 5: Dec 2025
    '2026-02-01'    -- Partition 6: Jan 2026
                    -- Partition 7: Feb 2026+ (current/future)
);
GO

-- Verify the partition function
SELECT
    pf.name                     AS PartitionFunction,
    pf.type_desc                AS FunctionType,
    pf.fanout                   AS PartitionCount,
    prv.boundary_id             AS BoundaryID,
    prv.value                   AS BoundaryValue,
    CASE pf.boundary_value_on_right
        WHEN 1 THEN 'RIGHT'
        ELSE 'LEFT'
    END                         AS RangeDirection
FROM sys.partition_functions pf
LEFT JOIN sys.partition_range_values prv
    ON pf.function_id = prv.function_id
WHERE pf.name = 'pf_TransactionDate'
ORDER BY prv.boundary_id;
GO


-- ----------------------------------------------------------------------------
-- 1.2.3  PARTITION SCHEME
-- ----------------------------------------------------------------------------
/*
  The partition scheme maps each partition (from the partition function) to a
  filegroup. The mapping is positional:

    Partition 1 (archive)     --> FG_Cold_Archive
    Partition 2 (Sep 2025)    --> FG_Cold_Month5
    Partition 3 (Oct 2025)    --> FG_Cold_Month4
    Partition 4 (Nov 2025)    --> FG_Cold_Month3
    Partition 5 (Dec 2025)    --> FG_Hot_Month2
    Partition 6 (Jan 2026)    --> FG_Hot_Month1
    Partition 7 (Feb 2026+)   --> FG_Hot_Current

  The scheme also designates a NEXT USED filegroup for when new partitions
  are added via SPLIT.
*/

CREATE PARTITION SCHEME ps_TransactionDate
AS PARTITION pf_TransactionDate
TO (
    FG_Cold_Archive,    -- Partition 1: < Sep 2025
    FG_Cold_Month5,     -- Partition 2: Sep 2025
    FG_Cold_Month4,     -- Partition 3: Oct 2025
    FG_Cold_Month3,     -- Partition 4: Nov 2025
    FG_Hot_Month2,      -- Partition 5: Dec 2025
    FG_Hot_Month1,      -- Partition 6: Jan 2026
    FG_Hot_Current      -- Partition 7: Feb 2026+
);
GO

-- Verify the partition scheme mapping
SELECT
    ps.name                     AS PartitionScheme,
    pf.name                     AS PartitionFunction,
    p.partition_number          AS PartitionNumber,
    fg.name                     AS FilegroupName,
    prv.value                   AS LowerBoundary,
    prv2.value                  AS UpperBoundary,
    CASE
        WHEN fg.name LIKE 'FG_Hot%' THEN 'HOT (Tier 1 / SSD)'
        WHEN fg.name LIKE 'FG_Cold%' THEN 'COLD (Tier 2 / HDD)'
        ELSE 'PRIMARY'
    END                         AS StorageTier
FROM sys.partition_schemes ps
JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id
JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
JOIN sys.partitions p ON p.partition_number = dds.destination_id
    AND p.object_id = (SELECT OBJECT_ID('dbo.Transactions'))
LEFT JOIN sys.partition_range_values prv
    ON pf.function_id = prv.function_id AND prv.boundary_id = p.partition_number - 1
LEFT JOIN sys.partition_range_values prv2
    ON pf.function_id = prv2.function_id AND prv2.boundary_id = p.partition_number
WHERE ps.name = 'ps_TransactionDate';
GO


-- ----------------------------------------------------------------------------
-- 1.2.4  CREATE A PARTITIONED TABLE (Demonstration)
-- ----------------------------------------------------------------------------
/*
  To partition a table, you create (or alter) it so the clustered index is
  built ON the partition scheme rather than on a filegroup.

  The partition key column MUST be part of:
    - The clustered index (always)
    - Every unique index/constraint on the table (including PKs)
*/

CREATE TABLE dbo.Transactions (
    TransactionID       BIGINT          IDENTITY(1,1)   NOT NULL,
    TransactionDate     DATETIME2(3)    NOT NULL,
    AccountID           INT             NOT NULL,
    TransactionTypeID   TINYINT         NOT NULL,
    Amount              DECIMAL(18,2)   NOT NULL,
    StatusCode          CHAR(1)         NOT NULL DEFAULT 'A',  -- A=Active, C=Completed, X=Cancelled
    ReferenceNumber     VARCHAR(50)     NULL,
    CreatedDate         DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate        DATETIME2(3)    NULL,

    -- The PK must include the partition key column (TransactionDate)
    CONSTRAINT PK_Transactions
        PRIMARY KEY CLUSTERED (TransactionDate, TransactionID)
) ON ps_TransactionDate(TransactionDate);  -- <-- This is what makes it partitioned
GO

-- Add a nonclustered index (also must be partition-aligned or explicitly non-aligned)
CREATE NONCLUSTERED INDEX IX_Transactions_AccountID
ON dbo.Transactions (AccountID, TransactionDate)
INCLUDE (Amount, StatusCode)
ON ps_TransactionDate(TransactionDate);  -- Aligned with the partition scheme
GO


-- ============================================================================
-- SECTION 1.3: RULES AND CONSTRAINTS OF PARTITIONING
-- ============================================================================
/*
  SQL Server partitioning has specific rules that must be understood and
  respected. Violating these will result in errors or suboptimal performance.
*/

-- ----------------------------------------------------------------------------
-- RULE 1: PARTITION KEY IN EVERY UNIQUE INDEX
-- ----------------------------------------------------------------------------
/*
  Every unique index (including the primary key) MUST include the partition
  key column. This is because SQL Server cannot enforce uniqueness across
  partitions without including the partition key.

  This has design implications: if your current PK is just an IDENTITY column,
  you'll need to make it a composite key that includes the partition key.

  EXAMPLE OF WHAT FAILS:
*/
-- This would FAIL:
-- CREATE TABLE dbo.BadExample (
--     ID INT IDENTITY PRIMARY KEY,           -- PK without partition key
--     TransactionDate DATETIME2(3) NOT NULL
-- ) ON ps_TransactionDate(TransactionDate);
-- Error: Partition columns for a unique index must be a subset of the index key.

/*
  WORKAROUND APPROACHES:
    a) Make the PK a composite: (TransactionDate, ID)  <-- recommended
    b) Drop the uniqueness constraint and enforce via application logic
    c) Use a non-partitioned unique index (loses partition alignment)
*/


-- ----------------------------------------------------------------------------
-- RULE 2: PARTITION KEY DATA TYPE MUST MATCH THE FUNCTION
-- ----------------------------------------------------------------------------
/*
  The column used as the partition key must have the exact same data type
  as the partition function parameter. Implicit conversions are NOT allowed.

  If your partition function uses DATETIME2(3), the column must be DATETIME2(3).
  DATETIME, DATE, DATETIME2(7), etc. will NOT work.
*/


-- ----------------------------------------------------------------------------
-- RULE 3: MAXIMUM 15,000 PARTITIONS (SQL Server 2012+)
-- ----------------------------------------------------------------------------
/*
  SQL Server supports up to 15,000 partitions per table. For monthly
  partitioning, this gives you 1,250 years of partitions -- more than enough.

  For daily partitioning, you get about 41 years. Still plenty, but be
  mindful if you choose sub-daily boundaries.
*/


-- ----------------------------------------------------------------------------
-- RULE 4: PARTITION ALIGNMENT FOR SWITCH OPERATIONS
-- ----------------------------------------------------------------------------
/*
  The SWITCH operation (the primary tool for fast data lifecycle management)
  requires strict alignment:

    - Source and target tables must have identical schemas
    - Both must use the same partition function boundary for the partition
      being switched
    - Both must live on the same filegroup for the partition being switched
    - CHECK constraints on the target must guarantee data fits in the partition
    - All indexes must be aligned (same partition scheme) or the switch fails
    - Foreign key constraints must be dropped before switching (no FK on
      partitioned tables that participate in SWITCH)

  We'll demonstrate SWITCH operations in a later module.
*/


-- ----------------------------------------------------------------------------
-- RULE 5: PARTITION ELIMINATION REQUIRES THE PARTITION KEY IN THE WHERE CLAUSE
-- ----------------------------------------------------------------------------
/*
  Partitioning only improves query performance if the query optimizer can
  eliminate partitions. This happens when the partition key appears in the
  WHERE clause with a sargable predicate.

  GOOD (partition elimination occurs):
    WHERE TransactionDate >= '2026-01-01' AND TransactionDate < '2026-02-01'
    WHERE TransactionDate BETWEEN '2026-01-15' AND '2026-02-15'

  BAD (no partition elimination -- scans all partitions):
    WHERE YEAR(TransactionDate) = 2026
    WHERE DATEADD(DAY, -30, TransactionDate) > GETDATE()
    WHERE CAST(TransactionDate AS DATE) = '2026-01-15'

  This is the same principle as index sargability, applied to partitions.
*/


-- ----------------------------------------------------------------------------
-- RULE 6: EDITION REQUIREMENT
-- ----------------------------------------------------------------------------
/*
  Table partitioning requires SQL Server Enterprise Edition (or Developer
  Edition for non-production). Standard Edition does NOT support partitioning.

  Starting with SQL Server 2016 SP1, some previously Enterprise-only features
  were added to Standard Edition, but partitioning was NOT one of them.

  SQL Server maintains this requirement.
*/


-- ----------------------------------------------------------------------------
-- RULE 7: PARTITION FUNCTION AND SCHEME ARE DATABASE-SCOPED
-- ----------------------------------------------------------------------------
/*
  Partition functions and schemes are created at the database level. Multiple
  tables can share the same partition function and/or scheme, which is actually
  recommended when tables share the same partitioning strategy. This also
  enables partition-aligned JOINs (collocated joins) where the optimizer can
  join matching partitions in parallel.
*/


-- ============================================================================
-- SECTION 1.4: IDENTIFYING CANDIDATE TABLES FOR PARTITIONING
-- ============================================================================
/*
  Not every large table benefits from partitioning. The best candidates have:

    1. HIGH ROW COUNT     - Hundreds of millions of rows
    2. DATE-BASED ACCESS  - Queries naturally filter by a date/time column
    3. HOT/COLD PATTERN   - Recent data is accessed frequently, old data rarely
    4. DATA LIFECYCLE     - Need to purge or archive old data efficiently
    5. MAINTENANCE PAIN   - Index rebuilds and DBCC checks are taking too long

  The following queries help you identify candidate tables in your actual
  production database. Run these against your target database.
*/

-- ============================================================================
-- QUERY 1: Large tables by row count and size
-- ============================================================================
/*
  This is the starting point. Find tables with the most rows and the largest
  physical footprint. Small tables gain nothing from partitioning.
*/
SELECT TOP 50
    s.name + '.' + t.name                           AS TableName,
    p.rows                                          AS RowCount,
    FORMAT(p.rows, 'N0')                            AS RowCount_Formatted,

    -- Size breakdown
    SUM(a.total_pages) * 8 / 1024                   AS TotalSize_MB,
    SUM(a.used_pages) * 8 / 1024                    AS UsedSize_MB,
    SUM(a.data_pages) * 8 / 1024                    AS DataSize_MB,
    (SUM(a.total_pages) - SUM(a.data_pages))
        * 8 / 1024                                  AS IndexSize_MB,

    -- Index count (relevant for maintenance impact)
    (SELECT COUNT(*) FROM sys.indexes i
     WHERE i.object_id = t.object_id
     AND i.type > 0)                                AS IndexCount,

    -- Current partitioning status
    CASE WHEN EXISTS (
        SELECT 1 FROM sys.indexes i
        WHERE i.object_id = t.object_id
        AND i.type IN (0,1)
        AND i.data_space_id IN (
            SELECT data_space_id FROM sys.partition_schemes
        )
    ) THEN 'YES' ELSE 'NO' END                     AS AlreadyPartitioned

FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE t.is_ms_shipped = 0
GROUP BY s.name, t.name, t.object_id, p.rows
HAVING p.rows > 1000000   -- Focus on tables with > 1M rows (adjust threshold as needed)
ORDER BY p.rows DESC;
GO


-- ============================================================================
-- QUERY 2: Identify date/datetime columns that could serve as partition keys
-- ============================================================================
/*
  For each large table, find columns with date/datetime types. The ideal
  partition key is:
    - A datetime column used in most WHERE clauses
    - Monotonically increasing (new data goes to the latest partition)
    - NOT frequently updated (partition key changes cause row movement)
*/
SELECT
    s.name + '.' + t.name                           AS TableName,
    c.name                                          AS ColumnName,
    ty.name                                         AS DataType,
    c.max_length                                    AS MaxLength,
    c.precision                                     AS Precision,
    c.scale                                         AS Scale,
    c.is_nullable                                   AS IsNullable,

    -- Check if column is part of the PK (relevant for Rule 1)
    CASE WHEN EXISTS (
        SELECT 1 FROM sys.index_columns ic
        JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE i.is_primary_key = 1
        AND ic.object_id = t.object_id
        AND ic.column_id = c.column_id
    ) THEN 'YES' ELSE 'NO' END                     AS IsInPrimaryKey,

    -- Check if column is in the clustered index
    CASE WHEN EXISTS (
        SELECT 1 FROM sys.index_columns ic
        JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE i.type = 1  -- Clustered
        AND ic.object_id = t.object_id
        AND ic.column_id = c.column_id
    ) THEN 'YES' ELSE 'NO' END                     AS IsInClusteredIndex

FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.columns c ON t.object_id = c.object_id
JOIN sys.types ty ON c.system_type_id = ty.system_type_id AND c.user_type_id = ty.user_type_id
WHERE t.is_ms_shipped = 0
AND ty.name IN ('date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset')
AND t.object_id IN (
    -- Only look at large tables (> 1M rows)
    SELECT p.object_id
    FROM sys.partitions p
    WHERE p.index_id IN (0, 1)
    GROUP BY p.object_id
    HAVING SUM(p.rows) > 1000000
)
ORDER BY s.name, t.name, c.column_id;
GO


-- ============================================================================
-- QUERY 3: Analyze date column distribution (run per candidate table/column)
-- ============================================================================
/*
  Once you've identified candidate tables and date columns, check the data
  distribution. An ideal partition key has a relatively even spread across
  time periods, with most queries targeting recent data.

  IMPORTANT: This query uses the POC Transactions table as an example.
  Replace the table and column names with your actual production table.

  For your production database, run this against each candidate table
  identified in Query 2.
*/

-- First, let's insert some sample data into our POC table to demonstrate
-- (In production, you'd run Query 3 against your real data)
SET NOCOUNT ON;
DECLARE @i INT = 0;
DECLARE @batchSize INT = 10000;
DECLARE @totalRows INT = 100000; -- Small for POC; your tables have 100M+

WHILE @i < @totalRows
BEGIN
    INSERT INTO dbo.Transactions (TransactionDate, AccountID, TransactionTypeID, Amount, StatusCode, ReferenceNumber)
    SELECT
        DATEADD(SECOND,
            ABS(CHECKSUM(NEWID())) % (86400 * 180),  -- Spread across 180 days
            '2025-08-15'
        ),
        ABS(CHECKSUM(NEWID())) % 10000 + 1,
        ABS(CHECKSUM(NEWID())) % 5 + 1,
        CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) / 100,
        CASE ABS(CHECKSUM(NEWID())) % 3
            WHEN 0 THEN 'A'
            WHEN 1 THEN 'C'
            ELSE 'X'
        END,
        'REF-' + CAST(ABS(CHECKSUM(NEWID())) % 999999 AS VARCHAR(10))
    FROM (SELECT TOP (@batchSize) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
          FROM sys.all_objects a CROSS JOIN sys.all_objects b) AS nums;

    SET @i = @i + @batchSize;
END
GO

-- Now analyze the distribution
SELECT
    YEAR(TransactionDate)                           AS [Year],
    MONTH(TransactionDate)                          AS [Month],
    FORMAT(MIN(TransactionDate), 'yyyy-MM-dd')      AS EarliestDate,
    FORMAT(MAX(TransactionDate), 'yyyy-MM-dd')      AS LatestDate,
    COUNT(*)                                        AS RowCount,
    FORMAT(COUNT(*), 'N0')                          AS RowCount_Formatted,

    -- Show which partition this data lives in
    $PARTITION.pf_TransactionDate(MIN(TransactionDate)) AS PartitionNumber,

    -- Percentage of total
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS PctOfTotal

FROM dbo.Transactions
GROUP BY YEAR(TransactionDate), MONTH(TransactionDate)
ORDER BY YEAR(TransactionDate), MONTH(TransactionDate);
GO


-- ============================================================================
-- QUERY 4: Verify partition data distribution on the POC table
-- ============================================================================
/*
  After loading data (or on your existing partitioned tables), verify how
  rows are distributed across partitions.
*/
SELECT
    p.partition_number                              AS PartitionNumber,
    fg.name                                         AS FilegroupName,
    prv.value                                       AS BoundaryValue,
    p.rows                                          AS RowCount,
    FORMAT(p.rows, 'N0')                            AS RowCount_Formatted,
    CAST(p.rows * 100.0 / NULLIF(SUM(p.rows) OVER(), 0)
        AS DECIMAL(5,2))                            AS PctOfTotal,

    -- Size per partition
    SUM(a.total_pages) * 8 / 1024                   AS TotalSize_MB,

    -- Data compression status
    p.data_compression_desc                         AS Compression

FROM sys.partitions p
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.destination_data_spaces dds
    ON i.data_space_id = dds.partition_scheme_id
    AND p.partition_number = dds.destination_id
JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
JOIN sys.allocation_units a ON p.partition_id = a.container_id
LEFT JOIN sys.partition_range_values prv
    ON i.data_space_id IN (SELECT ps.data_space_id FROM sys.partition_schemes ps
                           WHERE ps.function_id = prv.function_id)
    AND prv.boundary_id = p.partition_number
JOIN sys.partition_functions pf
    ON pf.function_id = (SELECT ps.function_id FROM sys.partition_schemes ps
                         WHERE ps.data_space_id = i.data_space_id)
LEFT JOIN sys.partition_range_values prv2
    ON pf.function_id = prv2.function_id
    AND prv2.boundary_id = p.partition_number - 1
WHERE p.object_id = OBJECT_ID('dbo.Transactions')
AND i.type IN (0, 1)  -- Heap or clustered
GROUP BY p.partition_number, fg.name, prv.value, p.rows, p.data_compression_desc
ORDER BY p.partition_number;
GO


-- ============================================================================
-- QUERY 5: Analyze query patterns (index usage stats)
-- ============================================================================
/*
  Index usage stats reveal how the table is being queried. Tables with high
  seek/scan counts on date-related indexes are strong candidates.

  Look for:
    - High user_scans on the clustered index = range scans = partitioning helps
    - High user_seeks on date columns = partition elimination will benefit
    - Very low usage on certain indexes = possible dead indexes to clean up

  NOTE: These stats reset when the SQL Server service restarts. Collect these
  over a representative period (ideally a full business cycle).
*/
SELECT
    s.name + '.' + t.name                           AS TableName,
    i.name                                          AS IndexName,
    i.type_desc                                     AS IndexType,
    ius.user_seeks                                  AS UserSeeks,
    ius.user_scans                                  AS UserScans,
    ius.user_lookups                                AS UserLookups,
    ius.user_updates                                AS UserUpdates,

    -- Read-to-Write ratio (higher = more read-heavy = better partition candidate)
    CASE WHEN ius.user_updates > 0
        THEN CAST((ius.user_seeks + ius.user_scans + ius.user_lookups) * 1.0
             / ius.user_updates AS DECIMAL(10,2))
        ELSE NULL
    END                                             AS ReadToWriteRatio,

    ius.last_user_seek                              AS LastSeek,
    ius.last_user_scan                              AS LastScan,

    -- Index key columns
    STUFF((
        SELECT ', ' + c.name
        FROM sys.index_columns ic
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH('')
    ), 1, 2, '')                                    AS KeyColumns

FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.indexes i ON t.object_id = i.object_id
LEFT JOIN sys.dm_db_index_usage_stats ius
    ON i.object_id = ius.object_id
    AND i.index_id = ius.index_id
    AND ius.database_id = DB_ID()
WHERE t.is_ms_shipped = 0
AND i.type > 0
ORDER BY (ISNULL(ius.user_scans, 0) + ISNULL(ius.user_seeks, 0)) DESC;
GO


-- ============================================================================
-- QUERY 6: Current index maintenance overhead
-- ============================================================================
/*
  Large tables with high fragmentation and long rebuild times are prime
  candidates for partitioning because you can rebuild indexes per-partition.

  Check current fragmentation levels. Tables with consistently high
  fragmentation benefit most from partition-level maintenance.
*/
SELECT
    s.name + '.' + t.name                           AS TableName,
    i.name                                          AS IndexName,
    i.type_desc                                     AS IndexType,
    ips.partition_number                            AS PartitionNumber,
    ips.alloc_unit_type_desc                        AS AllocUnitType,

    FORMAT(ips.page_count, 'N0')                    AS PageCount,
    CAST(ips.avg_fragmentation_in_percent
        AS DECIMAL(5,2))                            AS AvgFragPct,
    CAST(ips.avg_page_space_used_in_percent
        AS DECIMAL(5,2))                            AS AvgPageDensityPct,
    ips.record_count                                AS RecordCount,

    -- Maintenance recommendation
    CASE
        WHEN ips.avg_fragmentation_in_percent > 30 THEN 'REBUILD'
        WHEN ips.avg_fragmentation_in_percent > 10 THEN 'REORGANIZE'
        ELSE 'OK'
    END                                             AS MaintenanceAction

FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.indexes i ON t.object_id = i.object_id
CROSS APPLY sys.dm_db_index_physical_stats(
    DB_ID(), t.object_id, i.index_id, NULL, 'LIMITED'
) ips
WHERE t.is_ms_shipped = 0
AND i.type > 0
AND ips.page_count > 1000  -- Only look at indexes with meaningful size
ORDER BY ips.avg_fragmentation_in_percent DESC;
GO


-- ============================================================================
-- QUERY 7: Comprehensive candidate scoring matrix
-- ============================================================================
/*
  This query combines the insights from queries 1-6 into a single scoring
  matrix to help prioritize which tables to partition first.

  SCORING CRITERIA (higher = better candidate):
    - Row count:           10M+ = 3pts, 1M+ = 2pts, 100K+ = 1pt
    - Has datetime column: Yes = 2pts
    - Table size:          10GB+ = 3pts, 1GB+ = 2pts, 100MB+ = 1pt
    - Index count:         5+ = 2pts (more maintenance benefit)
    - Not yet partitioned: Yes = 1pt
*/
;WITH TableMetrics AS (
    SELECT
        t.object_id,
        s.name + '.' + t.name                       AS TableName,
        SUM(p.rows)                                  AS TotalRows,
        SUM(a.total_pages) * 8 / 1024                AS TotalSize_MB,
        (SELECT COUNT(*) FROM sys.indexes i2
         WHERE i2.object_id = t.object_id
         AND i2.type > 0)                            AS IndexCount,

        -- Has suitable datetime column
        CASE WHEN EXISTS (
            SELECT 1 FROM sys.columns c
            JOIN sys.types ty ON c.system_type_id = ty.system_type_id
                AND c.user_type_id = ty.user_type_id
            WHERE c.object_id = t.object_id
            AND ty.name IN ('date','datetime','datetime2','smalldatetime','datetimeoffset')
        ) THEN 1 ELSE 0 END                         AS HasDateColumn,

        -- Already partitioned
        CASE WHEN EXISTS (
            SELECT 1 FROM sys.indexes i
            WHERE i.object_id = t.object_id
            AND i.type IN (0,1)
            AND i.data_space_id IN (
                SELECT data_space_id FROM sys.partition_schemes
            )
        ) THEN 1 ELSE 0 END                         AS IsPartitioned

    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
    JOIN sys.allocation_units a ON p.partition_id = a.container_id
    WHERE t.is_ms_shipped = 0
    GROUP BY t.object_id, s.name, t.name
)
SELECT
    TableName,
    FORMAT(TotalRows, 'N0')                          AS TotalRows,
    TotalSize_MB,
    IndexCount,
    CASE HasDateColumn WHEN 1 THEN 'YES' ELSE 'NO' END AS HasDateColumn,
    CASE IsPartitioned WHEN 1 THEN 'YES' ELSE 'NO' END AS IsPartitioned,

    -- Calculate candidate score
    CASE
        WHEN TotalRows >= 10000000 THEN 3
        WHEN TotalRows >= 1000000  THEN 2
        WHEN TotalRows >= 100000   THEN 1
        ELSE 0
    END
    + HasDateColumn * 2
    + CASE
        WHEN TotalSize_MB >= 10240 THEN 3
        WHEN TotalSize_MB >= 1024  THEN 2
        WHEN TotalSize_MB >= 100   THEN 1
        ELSE 0
    END
    + CASE WHEN IndexCount >= 5 THEN 2 ELSE 0 END
    + CASE WHEN IsPartitioned = 0 THEN 1 ELSE 0 END AS CandidateScore,

    -- Human-readable recommendation
    CASE
        WHEN TotalRows >= 10000000 AND HasDateColumn = 1 AND IsPartitioned = 0
            THEN '*** STRONG CANDIDATE - Partition immediately'
        WHEN TotalRows >= 1000000 AND HasDateColumn = 1 AND IsPartitioned = 0
            THEN '** GOOD CANDIDATE - Evaluate query patterns'
        WHEN TotalRows >= 100000 AND HasDateColumn = 1 AND IsPartitioned = 0
            THEN '* POSSIBLE CANDIDATE - Monitor growth'
        WHEN IsPartitioned = 1
            THEN 'Already partitioned'
        ELSE 'Not recommended for partitioning'
    END                                              AS Recommendation

FROM TableMetrics
WHERE TotalRows > 0
ORDER BY
    CASE
        WHEN TotalRows >= 10000000 THEN 3
        WHEN TotalRows >= 1000000  THEN 2
        WHEN TotalRows >= 100000   THEN 1
        ELSE 0
    END
    + HasDateColumn * 2
    + CASE
        WHEN TotalSize_MB >= 10240 THEN 3
        WHEN TotalSize_MB >= 1024  THEN 2
        WHEN TotalSize_MB >= 100   THEN 1
        ELSE 0
    END
    + CASE WHEN IndexCount >= 5 THEN 2 ELSE 0 END
    + CASE WHEN IsPartitioned = 0 THEN 1 ELSE 0 END
    DESC;
GO


-- ============================================================================
-- SUMMARY: CANDIDATE IDENTIFICATION CHECKLIST
-- ============================================================================
/*
  For each candidate table, verify:

  [  ] Row count exceeds 10 million (or growing toward it)
  [  ] Has a datetime column used in most query WHERE clauses
  [  ] Data follows a hot/cold access pattern (recent = active, old = archive)
  [  ] Index maintenance (rebuilds, reorgs) is consuming significant time
  [  ] Data lifecycle management (purging old data) is a current or future need
  [  ] No existing partitioning strategy in place
  [  ] The PK can accommodate adding the datetime column (composite key)
  [  ] Queries predominantly use sargable predicates on the datetime column

  Tables meeting 5+ of these criteria are strong candidates for partitioning.

  NEXT MODULES (coming soon):
    Module 2: Implementing partitioning on existing tables (online operations)
    Module 3: Partition SWITCH operations for data lifecycle management
    Module 4: Partition-aligned indexing strategy
    Module 5: Partition-aware maintenance plans
    Module 6: Monitoring and performance validation
*/

PRINT '================================================================================';
PRINT '  Module 1 Complete: Partitioning Fundamentals & Candidate Identification';
PRINT '  Next: Module 2 - Implementing Partitioning on Existing Tables';
PRINT '================================================================================';
GO
