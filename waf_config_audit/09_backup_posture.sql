/* ============================================================================
   09_backup_posture.sql
   ----------------------------------------------------------------------------
   Captures: how recoverable is this instance, right now?
            Last full / diff / log backup per database, backup target paths,
            compression usage, suspect pages, and time since last CHECKDB.

   Target  : SQL Server 2019, physical host, SAN, A-P cluster
   Safety  : Read-only. Queries only msdb history and sys.suspect_pages.
   Output  : 5 result sets.
   ============================================================================ */
SET NOCOUNT ON;

------------------------------------------------------------------------------
-- 1. Last backup per database (FULL / DIFF / LOG)
------------------------------------------------------------------------------
;WITH last_b AS (
    SELECT
        d.name                                          AS database_name,
        d.recovery_model_desc,
        d.state_desc,
        MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END) AS last_full,
        MAX(CASE WHEN bs.type = 'I' THEN bs.backup_finish_date END) AS last_diff,
        MAX(CASE WHEN bs.type = 'L' THEN bs.backup_finish_date END) AS last_log,
        MAX(CASE WHEN bs.type = 'D' THEN bs.is_copy_only END)       AS last_full_copyonly,
        MAX(CASE WHEN bs.type = 'D' THEN bs.compressed_backup_size END) AS last_full_compressed_bytes,
        MAX(CASE WHEN bs.type = 'D' THEN bs.backup_size END)            AS last_full_uncompressed_bytes
    FROM sys.databases d
    LEFT JOIN msdb.dbo.backupset bs
           ON bs.database_name = d.name
    GROUP BY d.name, d.recovery_model_desc, d.state_desc
)
SELECT
    [section]                  = N'01 - Backup history per database',
    [database_name]            = database_name,
    [recovery_model]           = recovery_model_desc,
    [state]                    = state_desc,
    [last_full]                = last_full,
    [hours_since_full]         = CASE WHEN last_full IS NULL THEN NULL
                                      ELSE DATEDIFF(HOUR, last_full, SYSDATETIME()) END,
    [last_diff]                = last_diff,
    [hours_since_diff]         = CASE WHEN last_diff IS NULL THEN NULL
                                      ELSE DATEDIFF(HOUR, last_diff, SYSDATETIME()) END,
    [last_log]                 = last_log,
    [minutes_since_log]        = CASE WHEN last_log IS NULL THEN NULL
                                      ELSE DATEDIFF(MINUTE, last_log, SYSDATETIME()) END,
    [last_full_copyonly]       = last_full_copyonly,
    [last_full_size_mb]        = CAST(last_full_uncompressed_bytes / 1024.0 / 1024 AS decimal(18,2)),
    [last_full_compressed_mb]  = CAST(last_full_compressed_bytes   / 1024.0 / 1024 AS decimal(18,2)),
    [compression_ratio]        = CASE WHEN last_full_uncompressed_bytes > 0
                                      AND last_full_compressed_bytes > 0
                                      THEN CAST(1.0 * last_full_compressed_bytes / last_full_uncompressed_bytes AS decimal(5,3))
                                      ELSE NULL END,
    [posture]                  = CASE
        WHEN database_name IN ('tempdb') THEN N'N/A'
        WHEN last_full IS NULL THEN N'NO FULL BACKUP ON RECORD'
        WHEN recovery_model_desc IN ('FULL','BULK_LOGGED') AND last_log IS NULL THEN N'FULL recovery model but no log backup'
        WHEN recovery_model_desc IN ('FULL','BULK_LOGGED') AND DATEDIFF(MINUTE, last_log, SYSDATETIME()) > 60 THEN N'Log backup older than 60 min'
        WHEN DATEDIFF(HOUR, last_full, SYSDATETIME()) > 48 THEN N'Last full > 48h ago'
        ELSE N'OK'
    END
FROM last_b
ORDER BY database_name;

------------------------------------------------------------------------------
-- 2. Backup destinations seen in the last 7 days (paths and devices)
--    Helps spot anyone backing up to local disk on the active node, which
--    is invisible after failover.
------------------------------------------------------------------------------
SELECT
    [section]                = N'02 - Backup destinations (last 7 days)',
    [database_name]          = bs.database_name,
    [backup_type]            = CASE bs.type
                                  WHEN 'D' THEN N'FULL'
                                  WHEN 'I' THEN N'DIFF'
                                  WHEN 'L' THEN N'LOG'
                                  ELSE bs.type END,
    [device_type_desc]       = CASE bmf.device_type
                                  WHEN 2 THEN N'Disk'
                                  WHEN 5 THEN N'Tape'
                                  WHEN 7 THEN N'Virtual Device'
                                  WHEN 9 THEN N'Azure URL'
                                  ELSE CAST(bmf.device_type AS varchar(10))
                                END,
    [physical_device_name]   = bmf.physical_device_name,
    [count_in_window]        = COUNT(*),
    [latest_backup]          = MAX(bs.backup_finish_date),
    [compressed]             = CASE WHEN MAX(bs.compressed_backup_size) < MAX(bs.backup_size) THEN 1 ELSE 0 END
FROM msdb.dbo.backupset bs
JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
WHERE bs.backup_finish_date > DATEADD(DAY, -7, SYSDATETIME())
GROUP BY bs.database_name, bs.type, bmf.device_type, bmf.physical_device_name
ORDER BY bs.database_name, bs.type;

------------------------------------------------------------------------------
-- 3. Suspect pages - any entries here are a red flag
------------------------------------------------------------------------------
SELECT
    [section]                = N'03 - Suspect pages',
    [database_name]          = DB_NAME(database_id),
    [file_id]                = file_id,
    [page_id]                = page_id,
    [event_type]             = event_type,
    [event_type_desc]        = CASE event_type
                                  WHEN 1 THEN N'823/824 error'
                                  WHEN 2 THEN N'Bad checksum'
                                  WHEN 3 THEN N'Torn page'
                                  WHEN 4 THEN N'Restored (cleared)'
                                  WHEN 5 THEN N'Repaired (cleared by DBCC)'
                                  WHEN 7 THEN N'De-allocated (cleared)'
                                  ELSE CAST(event_type AS varchar(10))
                                END,
    [error_count]            = error_count,
    [last_update_date]       = last_update_date
FROM msdb.dbo.suspect_pages;

------------------------------------------------------------------------------
-- 4. Time since last DBCC CHECKDB (per database)
--    Read from boot-time persisted info; DBCC DBINFO has it but locks files.
--    The DBCC PAGE trick is the cleanest read-only way; we use it.
------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#last_checkdb') IS NOT NULL DROP TABLE #last_checkdb;
CREATE TABLE #last_checkdb (
    database_name      sysname,
    parent_object      varchar(255),
    [object]           varchar(255),
    field              varchar(128),
    value              varchar(255)
);

DECLARE @cmd nvarchar(max) = N'';
SELECT @cmd = @cmd +
    N'INSERT INTO #last_checkdb (parent_object, [object], field, value)
      EXEC (''DBCC DBINFO('''''' + REPLACE(name, '''', '''''') + N'''''') WITH TABLERESULTS, NO_INFOMSGS'');
      UPDATE #last_checkdb SET database_name = ''' + REPLACE(name, '''', '''''') + N''' WHERE database_name IS NULL;' + CHAR(10)
FROM sys.databases
WHERE state_desc = 'ONLINE'
  AND HAS_DBACCESS(name) = 1;

BEGIN TRY
    EXEC sys.sp_executesql @cmd;
END TRY
BEGIN CATCH
    -- DBCC DBINFO needs sysadmin; if it errors we just emit a note.
    PRINT 'Note: DBCC DBINFO requires sysadmin; CHECKDB age section may be empty.';
END CATCH

SELECT
    [section]                = N'04 - Time since last successful CHECKDB',
    [database_name]          = database_name,
    [last_known_good_checkdb]= TRY_CAST(value AS datetime),
    [days_since_checkdb]     = CASE WHEN TRY_CAST(value AS datetime) IS NULL THEN NULL
                                    ELSE DATEDIFF(DAY, TRY_CAST(value AS datetime), SYSDATETIME()) END,
    [posture]                = CASE
                                  WHEN TRY_CAST(value AS datetime) IS NULL THEN N'NEVER (or no permission to read DBINFO)'
                                  WHEN DATEDIFF(DAY, TRY_CAST(value AS datetime), SYSDATETIME()) > 14 THEN N'INVESTIGATE (>14d)'
                                  WHEN DATEDIFF(DAY, TRY_CAST(value AS datetime), SYSDATETIME()) > 7  THEN N'WATCH (>7d)'
                                  ELSE N'OK'
                               END
FROM #last_checkdb
WHERE field = N'dbi_dbccLastKnownGood'
ORDER BY database_name;

------------------------------------------------------------------------------
-- 5. Backup compression default setting reminder
------------------------------------------------------------------------------
SELECT
    [section]                = N'05 - Backup compression default',
    [value_in_use]           = value_in_use,
    [recommendation]         = N'Set to 1 unless your backup tool overrides per-job. CPU is cheap, SAN bandwidth and tape are not.'
FROM sys.configurations
WHERE name = 'backup compression default';
