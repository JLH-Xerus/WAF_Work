SET NOCOUNT ON;

DECLARE @sql nvarchar(max) = N'';

SELECT @sql = @sql +
    N'BEGIN TRY
        USE ' + QUOTENAME(d.name) + N';
        INSERT INTO @t (database_id, database_name, file_id, file_type, logical_name,
                        physical_name, drive, size_mb, used_mb, free_mb, pct_used,
                        is_percent_growth, growth_setting, max_size_setting,
                        state_desc, is_read_only)
        SELECT
            DB_ID(),
            DB_NAME(),
            df.file_id,
            df.type_desc,
            df.name,
            df.physical_name,
            LEFT(df.physical_name, 2),
            CAST(df.size * 8.0 / 1024 AS decimal(18,2)),
            CAST(FILEPROPERTY(df.name, ''SpaceUsed'') * 8.0 / 1024 AS decimal(18,2)),
            CAST(df.size * 8.0 / 1024 AS decimal(18,2)) -
                CAST(FILEPROPERTY(df.name, ''SpaceUsed'') * 8.0 / 1024 AS decimal(18,2)),
            CASE WHEN df.size = 0 THEN 0
                 ELSE CAST(100.0 * FILEPROPERTY(df.name, ''SpaceUsed'') / df.size AS decimal(5,2))
            END,
            df.is_percent_growth,
            CASE WHEN df.is_percent_growth = 1
                 THEN CAST(df.growth AS varchar(10)) + N''%''
                 ELSE CAST(CAST(df.growth * 8.0 / 1024 AS decimal(18,2)) AS varchar(20)) + N'' MB''
            END,
            CASE WHEN df.max_size = -1 THEN N''UNLIMITED''
                 WHEN df.max_size =  0 THEN N''NOGROW''
                 WHEN df.max_size = 268435456 THEN N''2 TB''
                 ELSE CAST(CAST(df.max_size * 8.0 / 1024 AS decimal(18,2)) AS varchar(20)) + N'' MB''
            END,
            df.state_desc,
            df.is_read_only
        FROM sys.database_files df;
    END TRY
    BEGIN CATCH
        PRINT N''Skipped ' + REPLACE(d.name, '''', '''''') + N': '' + ERROR_MESSAGE();
    END CATCH;
    '
FROM sys.databases d
WHERE d.state_desc = 'ONLINE'
  AND HAS_DBACCESS(d.name) = 1;

IF OBJECT_ID('tempdb..#file_layout') IS NOT NULL DROP TABLE #file_layout;
CREATE TABLE #file_layout (
    database_id        int,
    database_name      sysname,
    file_id            int,
    file_type          nvarchar(60),
    logical_name       sysname,
    physical_name      nvarchar(520),
    drive              nvarchar(10),
    size_mb            decimal(18,2),
    used_mb            decimal(18,2),
    free_mb            decimal(18,2),
    pct_used           decimal(5,2),
    is_percent_growth  bit,
    growth_setting     nvarchar(40),
    max_size_setting   nvarchar(40),
    state_desc         nvarchar(60),
    is_read_only       bit
);

SET @sql = REPLACE(@sql, N'@t', N'#file_layout');
EXEC sys.sp_executesql @sql;

SELECT
    [section]            = N'01 - Database file layout',
    [database_name]      = database_name,
    [file_id]            = file_id,
    [file_type]          = file_type,
    [logical_name]       = logical_name,
    [drive]              = drive,
    [physical_name]      = physical_name,
    [size_mb]            = size_mb,
    [used_mb]            = used_mb,
    [free_mb]            = free_mb,
    [pct_used]           = pct_used,
    [growth_setting]     = growth_setting,
    [is_percent_growth]  = is_percent_growth,
    [max_size_setting]   = max_size_setting,
    [state_desc]         = state_desc,
    [is_read_only]       = is_read_only
FROM #file_layout
ORDER BY database_name, file_type DESC, file_id;

;WITH per_db AS (
    SELECT
        database_name,
        COUNT(DISTINCT drive)                                              AS distinct_drives,
        SUM(CASE WHEN file_type = 'ROWS' THEN 1 ELSE 0 END)                AS data_file_count,
        SUM(CASE WHEN file_type = 'LOG'  THEN 1 ELSE 0 END)                AS log_file_count,
        SUM(CASE WHEN is_percent_growth = 1 THEN 1 ELSE 0 END)             AS pct_growth_files,
        MIN(CASE WHEN file_type = 'ROWS' AND is_percent_growth = 0
                 THEN size_mb END)                                         AS smallest_data_mb,
        MAX(CASE WHEN file_type = 'ROWS' AND is_percent_growth = 0
                 THEN size_mb END)                                         AS largest_data_mb,
        SUM(CASE WHEN file_type = 'ROWS' AND drive IN (
            SELECT DISTINCT drive FROM #file_layout fl
            WHERE fl.database_name = f.database_name AND fl.file_type = 'LOG'
        ) THEN 1 ELSE 0 END)                                               AS data_on_log_drive
    FROM #file_layout f
    GROUP BY database_name
)
SELECT
    [section]               = N'02 - File-layout findings',
    [database_name]         = database_name,
    [data_file_count]       = data_file_count,
    [log_file_count]        = log_file_count,
    [distinct_drives_used]  = distinct_drives,
    [pct_growth_files]      = pct_growth_files,
    [data_files_on_log_drive] = data_on_log_drive,
    [finding] = CONCAT(
        CASE WHEN log_file_count > 1 THEN N'[multiple log files] ' ELSE N'' END,
        CASE WHEN pct_growth_files > 0 THEN N'[percent autogrowth set - prefer fixed MB] ' ELSE N'' END,
        CASE WHEN data_on_log_drive > 0 THEN N'[data and log on same drive] ' ELSE N'' END,
        CASE WHEN data_file_count > 1 AND smallest_data_mb <> largest_data_mb
             THEN N'[user-DB data files are not equal-sized] ' ELSE N'' END
    )
FROM per_db
WHERE database_name NOT IN ('master','model','msdb')
ORDER BY database_name;

DECLARE @vlf_sql nvarchar(max) = N'';
IF OBJECT_ID('tempdb..#vlfs') IS NOT NULL DROP TABLE #vlfs;
CREATE TABLE #vlfs (
    database_name  sysname,
    vlf_count      int,
    active_vlfs    int,
    log_size_mb    decimal(18,2),
    avg_vlf_mb     decimal(18,2)
);

SELECT @vlf_sql = @vlf_sql +
    N'BEGIN TRY
        INSERT INTO #vlfs (database_name, vlf_count, active_vlfs, log_size_mb, avg_vlf_mb)
        SELECT ''' + REPLACE(name, '''', '''''') + N''',
               COUNT(*),
               SUM(CASE WHEN vlf_active = 1 THEN 1 ELSE 0 END),
               CAST(SUM(vlf_size_mb) AS decimal(18,2)),
               CAST(AVG(vlf_size_mb) AS decimal(18,2))
          FROM sys.dm_db_log_info(DB_ID(''' + REPLACE(name, '''', '''''') + N'''));
      END TRY
      BEGIN CATCH
        PRINT N''VLF skipped ' + REPLACE(name, '''', '''''') + N': '' + ERROR_MESSAGE();
      END CATCH;' + CHAR(10)
FROM sys.databases
WHERE state_desc = 'ONLINE'
  AND source_database_id IS NULL
  AND HAS_DBACCESS(name) = 1;

EXEC sys.sp_executesql @vlf_sql;

SELECT
    [section]            = N'03 - VLF counts',
    [database_name]      = database_name,
    [vlf_count]          = vlf_count,
    [active_vlfs]        = active_vlfs,
    [log_size_mb]        = log_size_mb,
    [avg_vlf_mb]         = avg_vlf_mb,
    [vlf_health]         = CASE
                              WHEN vlf_count < 100 THEN N'OK'
                              WHEN vlf_count < 500 THEN N'WATCH'
                              ELSE N'INVESTIGATE'
                           END,
    [comment]            = CASE
                              WHEN vlf_count >= 500 THEN
                                N'Consider shrinking log to a small size, then growing in 8 GB chunks to right-size VLFs.'
                              WHEN avg_vlf_mb < 64 AND vlf_count > 100 THEN
                                N'Lots of small VLFs - log was probably grown by small autogrowth increments.'
                              ELSE N''
                           END
FROM #vlfs
ORDER BY vlf_count DESC, database_name;
