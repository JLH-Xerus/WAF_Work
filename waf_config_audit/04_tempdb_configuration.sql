SET NOCOUNT ON;

SELECT
    [section]                          = N'01 - tempdb database options',
    [name]                             = d.name,
    [database_id]                      = d.database_id,
    [recovery_model_desc]              = d.recovery_model_desc,
    [collation_name]                   = d.collation_name,
    [user_access_desc]                 = d.user_access_desc,
    [is_read_only]                     = d.is_read_only,
    [snapshot_isolation_state_desc]    = d.snapshot_isolation_state_desc,
    [is_read_committed_snapshot_on]    = d.is_read_committed_snapshot_on,
    [is_auto_create_stats_on]          = d.is_auto_create_stats_on,
    [is_auto_update_stats_on]          = d.is_auto_update_stats_on,
    [is_auto_update_stats_async_on]    = d.is_auto_update_stats_async_on,
    [is_memory_optimized_elevate_to_snapshot_on] = d.is_memory_optimized_elevate_to_snapshot_on,
    [delayed_durability_desc]          = d.delayed_durability_desc,
    [compatibility_level]              = d.compatibility_level,
    [page_verify_option_desc]          = d.page_verify_option_desc
FROM sys.databases d
WHERE d.database_id = 2;

DECLARE @scheduler_count int =
    (SELECT COUNT(*) FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE');

;WITH files AS (
    SELECT
        mf.file_id,
        mf.name                                       AS logical_name,
        mf.type_desc,
        mf.physical_name,
        LEFT(mf.physical_name, 2)                     AS drive,
        CAST(mf.size  * 8.0 / 1024 AS decimal(12,2))  AS size_mb,
        mf.is_percent_growth,
        CASE WHEN mf.is_percent_growth = 1
             THEN CAST(mf.growth AS varchar(20)) + N'%'
             ELSE CAST(CAST(mf.growth * 8.0 / 1024 AS decimal(12,2)) AS varchar(20)) + N' MB'
        END                                           AS growth_setting,
        mf.max_size,
        mf.state_desc
    FROM tempdb.sys.database_files mf
)
SELECT
    [section]                  = N'02 - tempdb files',
    [file_id]                  = file_id,
    [logical_name]             = logical_name,
    [type_desc]                = type_desc,
    [drive]                    = drive,
    [physical_name]            = physical_name,
    [size_mb]                  = size_mb,
    [growth_setting]           = growth_setting,
    [is_percent_growth]        = is_percent_growth,
    [max_size]                 = max_size,
    [state_desc]               = state_desc
FROM files
ORDER BY type_desc, file_id;

;WITH data_files AS (
    SELECT
        size       AS pages,
        CAST(size * 8.0 / 1024 AS decimal(12,2)) AS size_mb,
        is_percent_growth,
        growth
    FROM tempdb.sys.database_files
    WHERE type_desc = 'ROWS'
)
SELECT
    [section]                          = N'03 - tempdb data file sanity',
    [data_file_count]                  = COUNT(*),
    [scheduler_count]                  = @scheduler_count,
    [recommended_file_count_min]       = CASE WHEN @scheduler_count <= 8 THEN @scheduler_count ELSE 8 END,
    [recommended_file_count_max]       = CASE WHEN @scheduler_count <= 8 THEN @scheduler_count ELSE @scheduler_count END,
    [min_size_mb]                      = MIN(size_mb),
    [max_size_mb]                      = MAX(size_mb),
    [files_are_equal_size]             = CASE WHEN MIN(size_mb) = MAX(size_mb) THEN 1 ELSE 0 END,
    [any_percent_growth]               = MAX(CAST(is_percent_growth AS int)),
    [growth_settings_distinct_count]   = COUNT(DISTINCT CAST(is_percent_growth AS varchar(2)) + N':' + CAST(growth AS varchar(20))),
    [comment] = N'MS guidance: equal-sized data files. Count = #schedulers (cap 8); add by 4 if PAGELATCH contention persists.'
FROM data_files;

SELECT
    [section]                          = N'04 - tempdb allocation latch waits (current)',
    [session_id]                       = r.session_id,
    [wait_type]                        = r.wait_type,
    [wait_resource]                    = r.wait_resource,
    [wait_time_ms]                     = r.wait_time,
    [blocking_session_id]              = r.blocking_session_id,
    [database_name]                    = DB_NAME(r.database_id),
    [command]                          = r.command,
    [status]                           = r.status
FROM sys.dm_exec_requests r
WHERE r.wait_type LIKE 'PAGELATCH%'
  AND r.wait_resource LIKE '2:%'
  AND r.session_id <> @@SPID;

SELECT
    [section]                          = N'05 - tempdb metadata memory-optimized (configured)',
    [sp_configure_value]               = (SELECT value_in_use
                                             FROM sys.configurations
                                            WHERE name = N'tempdb metadata memory-optimized'),
    [notes] = N'1 = configured ON, 0 = OFF. Requires restart to apply. Helps when sysschobjs / sysobjvalues PAGELATCH contention is high in tempdb.';

IF EXISTS (SELECT 1 FROM sys.all_objects WHERE name = 'dm_xtp_system_memory_consumers')
BEGIN
    SELECT
        [section]                  = N'05b - tempdb XTP memory consumers (runtime indicator)',
        [memory_consumer_type]     = memory_consumer_type_desc,
        [allocated_bytes]          = allocated_bytes,
        [used_bytes]                = used_bytes
    FROM tempdb.sys.dm_xtp_system_memory_consumers
    WHERE allocated_bytes > 0;
END

SELECT
    [section]                  = N'06 - tempdb I/O stats',
    [file_id]                  = vfs.file_id,
    [logical_name]             = mf.name,
    [type_desc]                = mf.type_desc,
    [drive]                    = LEFT(mf.physical_name, 2),
    [num_of_reads]             = vfs.num_of_reads,
    [num_of_writes]            = vfs.num_of_writes,
    [io_stall_read_ms]         = vfs.io_stall_read_ms,
    [io_stall_write_ms]        = vfs.io_stall_write_ms,
    [avg_read_stall_ms]        = CASE WHEN vfs.num_of_reads > 0
                                      THEN vfs.io_stall_read_ms / vfs.num_of_reads ELSE 0 END,
    [avg_write_stall_ms]       = CASE WHEN vfs.num_of_writes > 0
                                      THEN vfs.io_stall_write_ms / vfs.num_of_writes ELSE 0 END,
    [num_of_bytes_read_mb]     = CAST(vfs.num_of_bytes_read  / 1024.0 / 1024.0 AS decimal(18,2)),
    [num_of_bytes_written_mb]  = CAST(vfs.num_of_bytes_written / 1024.0 / 1024.0 AS decimal(18,2))
FROM sys.dm_io_virtual_file_stats(2, NULL) vfs
JOIN tempdb.sys.database_files mf
  ON vfs.file_id = mf.file_id
ORDER BY mf.type_desc, vfs.file_id;
