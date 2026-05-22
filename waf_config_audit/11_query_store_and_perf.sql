SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#qs') IS NOT NULL DROP TABLE #qs;
CREATE TABLE #qs (
    database_name                  sysname,
    actual_state_desc              nvarchar(60),
    desired_state_desc             nvarchar(60),
    readonly_reason                int,
    readonly_reason_desc           nvarchar(120),
    current_storage_size_mb        bigint,
    max_storage_size_mb            bigint,
    query_capture_mode_desc        nvarchar(60),
    flush_interval_seconds         bigint,
    interval_length_minutes        bigint,
    stale_query_threshold_days     bigint,
    max_plans_per_query            bigint,
    size_based_cleanup_mode_desc   nvarchar(60),
    wait_stats_capture_mode_desc   nvarchar(60)
);

DECLARE @sql nvarchar(max) = N'';
SELECT @sql = @sql +
    N'BEGIN TRY
        USE ' + QUOTENAME(name) + N';
        INSERT INTO #qs
        SELECT ''' + REPLACE(name, '''', '''''') + N''',
               actual_state_desc, desired_state_desc,
               readonly_reason,
               CASE readonly_reason
                  WHEN 1   THEN N''Database is in single-user mode''
                  WHEN 2   THEN N''Database is in emergency mode''
                  WHEN 4   THEN N''Database is read-only''
                  WHEN 8   THEN N''Database is in transition''
                  WHEN 65536 THEN N''Reached MAX_STORAGE_SIZE_MB''
                  WHEN 131072 THEN N''Number of different statements exceeded''
                  WHEN 262144 THEN N''Size of in-memory items exceeds limit''
                  WHEN 524288 THEN N''DB has reached read-only file group''
                  WHEN 1048576 THEN N''DB has reached free disk space limit''
                  ELSE NULL
               END,
               current_storage_size_mb, max_storage_size_mb,
               query_capture_mode_desc, flush_interval_seconds,
               interval_length_minutes, stale_query_threshold_days,
               max_plans_per_query, size_based_cleanup_mode_desc,
               wait_stats_capture_mode_desc
          FROM sys.database_query_store_options;
      END TRY
      BEGIN CATCH
        PRINT N''QS skipped ' + REPLACE(name, '''', '''''') + N': '' + ERROR_MESSAGE();
      END CATCH;' + CHAR(10)
FROM sys.databases
WHERE state_desc = 'ONLINE'
  AND database_id > 4
  AND HAS_DBACCESS(name) = 1;

IF LEN(@sql) > 0
    EXEC sys.sp_executesql @sql;

SELECT
    [section]                       = N'01 - Query Store per database',
    [database_name]                 = database_name,
    [actual_state_desc]             = actual_state_desc,
    [desired_state_desc]            = desired_state_desc,
    [readonly_reason]               = readonly_reason,
    [readonly_reason_desc]          = readonly_reason_desc,
    [current_storage_size_mb]       = current_storage_size_mb,
    [max_storage_size_mb]           = max_storage_size_mb,
    [pct_used]                      = CASE WHEN max_storage_size_mb = 0 THEN 0
                                            ELSE CAST(100.0 * current_storage_size_mb / max_storage_size_mb
                                                      AS decimal(5,2)) END,
    [query_capture_mode_desc]       = query_capture_mode_desc,
    [flush_interval_seconds]        = flush_interval_seconds,
    [interval_length_minutes]       = interval_length_minutes,
    [stale_query_threshold_days]    = stale_query_threshold_days,
    [max_plans_per_query]           = max_plans_per_query,
    [size_based_cleanup_mode_desc]  = size_based_cleanup_mode_desc,
    [wait_stats_capture_mode_desc]  = wait_stats_capture_mode_desc,
    [posture] = CASE
        WHEN actual_state_desc IS NULL OR actual_state_desc = 'OFF' THEN N'NOT IN USE'
        WHEN actual_state_desc = 'READ_ONLY' THEN N'READ_ONLY (' + ISNULL(readonly_reason_desc, N'investigate') + N')'
        WHEN query_capture_mode_desc = 'ALL' THEN N'ALL capture mode - can be noisy on busy OLTP, consider AUTO'
        WHEN max_storage_size_mb < 1000 THEN N'Storage cap < 1 GB - small for busy DBs'
        ELSE N'OK'
    END
FROM #qs
ORDER BY database_name;

SELECT
    [section]                = N'02 - Query/perf toggles',
    [setting]                = c.name,
    [value_in_use]           = c.value_in_use,
    [recommendation]         = x.rec
FROM sys.configurations c
JOIN (VALUES
    ('optimize for ad hoc workloads', N'Enable (1) on most OLTP. Reduces single-use plan bloat.'),
    ('cost threshold for parallelism', N'25-50 typical. Default 5 is too low for modern hardware.'),
    ('max degree of parallelism', N'Set per cores-per-NUMA. See 03 script for derived value.'),
    ('priority boost', N'Must be 0.'),
    ('lightweight pooling', N'Must be 0.'),
    ('blocked process threshold (s)', N'5-15 to surface blocked-process reports.'),
    ('cursor threshold', N'-1 (default).'),
    ('query wait (s)', N'-1 (default).'),
    ('default trace enabled', N'1 - leave on.')
) x(name, rec) ON LOWER(c.name) = LOWER(x.name)
ORDER BY c.name;

SELECT
    [section]                = N'03 - Parameterization per database',
    [database_name]          = name,
    [is_parameterization_forced] = is_parameterization_forced,
    [comment] = N'Most OLTP should be SIMPLE; FORCED can help apps that send unparameterized SQL but watch for plan reuse bugs.'
FROM sys.databases
WHERE database_id > 4
ORDER BY name;
