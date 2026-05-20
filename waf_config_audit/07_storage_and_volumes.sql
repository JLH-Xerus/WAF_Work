/* ============================================================================
   07_storage_and_volumes.sql
   ----------------------------------------------------------------------------
   Captures: storage view from inside the engine. On SAN-attached FCI we
            cannot see the underlying array, but we can see what Windows
            presents - block size, free space, mount points - plus the
            engine's measured I/O stall behavior per drive.

   Target  : SQL Server 2019, physical host, SAN, A-P cluster
   Safety  : Read-only.
   Output  : 4 result sets.
   ============================================================================ */
SET NOCOUNT ON;

------------------------------------------------------------------------------
-- 1. Volumes hosting SQL files - block size, free space, mount points
--    sys.dm_os_volume_stats walks each database file and asks Windows about
--    the volume it lives on.
------------------------------------------------------------------------------
;WITH volumes AS (
    SELECT DISTINCT
        vs.volume_mount_point,
        vs.file_system_type,
        vs.logical_volume_name,
        vs.total_bytes,
        vs.available_bytes,
        vs.supports_compression,
        vs.supports_alternate_streams,
        vs.supports_sparse_files,
        vs.is_read_only,
        vs.is_compressed
    FROM sys.master_files mf
    CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) vs
)
SELECT
    [section]                  = N'01 - Volumes hosting SQL files',
    [volume_mount_point]       = volume_mount_point,
    [logical_volume_name]      = logical_volume_name,
    [file_system_type]         = file_system_type,
    [total_gb]                 = CAST(total_bytes     / 1024.0 / 1024.0 / 1024.0 AS decimal(18,2)),
    [available_gb]             = CAST(available_bytes / 1024.0 / 1024.0 / 1024.0 AS decimal(18,2)),
    [pct_free]                 = CASE WHEN total_bytes = 0 THEN 0
                                      ELSE CAST(100.0 * available_bytes / total_bytes AS decimal(5,2)) END,
    [is_read_only]             = is_read_only,
    [is_compressed]            = is_compressed,
    [supports_sparse_files]    = supports_sparse_files,
    [supports_compression]     = supports_compression
FROM volumes
ORDER BY volume_mount_point;

------------------------------------------------------------------------------
-- 2. Which files live on which volume
------------------------------------------------------------------------------
SELECT
    [section]                  = N'02 - File-to-volume mapping',
    [database_name]            = DB_NAME(mf.database_id),
    [logical_name]             = mf.name,
    [type_desc]                = mf.type_desc,
    [physical_name]            = mf.physical_name,
    [volume_mount_point]       = vs.volume_mount_point,
    [logical_volume_name]      = vs.logical_volume_name,
    [size_mb]                  = CAST(mf.size * 8.0 / 1024 AS decimal(18,2)),
    [volume_total_gb]          = CAST(vs.total_bytes     / 1024.0 / 1024.0 / 1024.0 AS decimal(18,2)),
    [volume_free_gb]           = CAST(vs.available_bytes / 1024.0 / 1024.0 / 1024.0 AS decimal(18,2))
FROM sys.master_files mf
CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) vs
ORDER BY vs.volume_mount_point, mf.database_id, mf.file_id;

------------------------------------------------------------------------------
-- 3. I/O stalls aggregated by drive (since SQL startup)
------------------------------------------------------------------------------
;WITH io AS (
    SELECT
        LEFT(mf.physical_name, 2) AS drive,
        mf.type_desc,
        vfs.num_of_reads,
        vfs.num_of_writes,
        vfs.io_stall_read_ms,
        vfs.io_stall_write_ms,
        vfs.num_of_bytes_read,
        vfs.num_of_bytes_written
    FROM sys.master_files mf
    JOIN sys.dm_io_virtual_file_stats(NULL, NULL) vfs
      ON mf.database_id = vfs.database_id
     AND mf.file_id     = vfs.file_id
)
SELECT
    [section]                  = N'03 - I/O stalls by drive and file type',
    [drive]                    = drive,
    [type_desc]                = type_desc,
    [reads]                    = SUM(num_of_reads),
    [writes]                   = SUM(num_of_writes),
    [io_stall_read_ms]         = SUM(io_stall_read_ms),
    [io_stall_write_ms]        = SUM(io_stall_write_ms),
    [avg_read_stall_ms]        = CASE WHEN SUM(num_of_reads)  > 0
                                      THEN SUM(io_stall_read_ms)  / SUM(num_of_reads)  ELSE 0 END,
    [avg_write_stall_ms]       = CASE WHEN SUM(num_of_writes) > 0
                                      THEN SUM(io_stall_write_ms) / SUM(num_of_writes) ELSE 0 END,
    [gb_read]                  = CAST(SUM(num_of_bytes_read)    / 1024.0 / 1024.0 / 1024.0 AS decimal(18,2)),
    [gb_written]               = CAST(SUM(num_of_bytes_written) / 1024.0 / 1024.0 / 1024.0 AS decimal(18,2)),
    [io_latency_health]        = CASE
        WHEN SUM(num_of_reads) + SUM(num_of_writes) = 0 THEN N'NO ACTIVITY'
        WHEN ((CASE WHEN SUM(num_of_reads)  > 0 THEN SUM(io_stall_read_ms)  / SUM(num_of_reads)  ELSE 0 END) > 20
           OR (CASE WHEN SUM(num_of_writes) > 0 THEN SUM(io_stall_write_ms) / SUM(num_of_writes) ELSE 0 END) > 20)
            THEN N'INVESTIGATE'
        WHEN ((CASE WHEN SUM(num_of_reads)  > 0 THEN SUM(io_stall_read_ms)  / SUM(num_of_reads)  ELSE 0 END) > 10
           OR (CASE WHEN SUM(num_of_writes) > 0 THEN SUM(io_stall_write_ms) / SUM(num_of_writes) ELSE 0 END) > 10)
            THEN N'WATCH'
        ELSE N'OK'
    END
FROM io
GROUP BY drive, type_desc
ORDER BY drive, type_desc;

------------------------------------------------------------------------------
-- 4. Heuristic check: NTFS allocation unit
--    SQL Server data files prefer 64 KB NTFS allocation unit. We cannot read
--    NTFS block size from inside SQL, so we emit a reminder row pointing at
--    the recommended verification command (run from Windows).
------------------------------------------------------------------------------
SELECT
    [section]      = N'04 - NTFS allocation unit reminder',
    [host_check]   = N'Run from an elevated Windows cmd on the active node:  fsutil fsinfo ntfsinfo <drive>:',
    [recommendation] = N'Expect "Bytes Per Cluster" = 65536 (64 KB) for drives that hold SQL data and log files.',
    [why]          = N'A 4 KB allocation unit can cause extra I/Os per 8 KB page read - the difference shows up as elevated read latency under load.';
