/* ============================================================================
   05_database_configuration.sql
   ----------------------------------------------------------------------------
   Captures: every per-database setting that affects performance, recoverability,
            or security. One row per database, plus a set of "deviation"
            highlights for quick scanning.

   Target  : SQL Server 2019, physical host, SAN, A-P cluster
   Safety  : Read-only.
   Output  : 3 result sets.
   ============================================================================ */
SET NOCOUNT ON;

------------------------------------------------------------------------------
-- 1. Per-database configuration matrix
------------------------------------------------------------------------------
SELECT
    [section]                              = N'01 - Database configuration',
    [database_id]                          = d.database_id,
    [name]                                 = d.name,
    [create_date]                          = d.create_date,
    [owner_name]                           = SUSER_SNAME(d.owner_sid),
    [state_desc]                           = d.state_desc,
    [user_access_desc]                     = d.user_access_desc,
    [is_read_only]                         = d.is_read_only,
    [is_auto_close_on]                     = d.is_auto_close_on,
    [is_auto_shrink_on]                    = d.is_auto_shrink_on,
    [is_auto_create_stats_on]              = d.is_auto_create_stats_on,
    [is_auto_create_stats_incremental_on]  = d.is_auto_create_stats_incremental_on,
    [is_auto_update_stats_on]              = d.is_auto_update_stats_on,
    [is_auto_update_stats_async_on]        = d.is_auto_update_stats_async_on,
    [recovery_model_desc]                  = d.recovery_model_desc,
    [page_verify_option_desc]              = d.page_verify_option_desc,
    [compatibility_level]                  = d.compatibility_level,
    [collation_name]                       = d.collation_name,
    [snapshot_isolation_state_desc]        = d.snapshot_isolation_state_desc,
    [is_read_committed_snapshot_on]        = d.is_read_committed_snapshot_on,
    [is_trustworthy_on]                    = d.is_trustworthy_on,
    [is_db_chaining_on]                    = d.is_db_chaining_on,
    [is_broker_enabled]                    = d.is_broker_enabled,
    [is_published]                         = d.is_published,
    [is_subscribed]                        = d.is_subscribed,
    [is_merge_published]                   = d.is_merge_published,
    [is_distributor]                       = d.is_distributor,
    [is_encrypted]                         = d.is_encrypted,
    [is_query_store_on]                    = d.is_query_store_on,
    [is_cdc_enabled]                       = d.is_cdc_enabled,
    [containment_desc]                     = d.containment_desc,
    [target_recovery_time_in_seconds]      = d.target_recovery_time_in_seconds,
    [delayed_durability_desc]              = d.delayed_durability_desc,
    [log_reuse_wait_desc]                  = d.log_reuse_wait_desc,
    [is_parameterization_forced]           = d.is_parameterization_forced,
    [is_supplemental_logging_enabled]      = d.is_supplemental_logging_enabled,
    [is_date_correlation_on]               = d.is_date_correlation_on,
    [is_ansi_null_default_on]              = d.is_ansi_null_default_on,
    [is_ansi_nulls_on]                     = d.is_ansi_nulls_on,
    [is_ansi_padding_on]                   = d.is_ansi_padding_on,
    [is_ansi_warnings_on]                  = d.is_ansi_warnings_on,
    [is_arithabort_on]                     = d.is_arithabort_on,
    [is_concat_null_yields_null_on]        = d.is_concat_null_yields_null_on,
    [is_numeric_roundabort_on]             = d.is_numeric_roundabort_on,
    [is_quoted_identifier_on]              = d.is_quoted_identifier_on,
    [is_recursive_triggers_on]             = d.is_recursive_triggers_on,
    [is_cursor_close_on_commit_on]         = d.is_cursor_close_on_commit_on,
    [is_local_cursor_default]              = d.is_local_cursor_default,
    [is_fulltext_enabled]                  = d.is_fulltext_enabled,
    [is_honor_broker_priority_on]          = d.is_honor_broker_priority_on
FROM sys.databases d
ORDER BY d.database_id;

------------------------------------------------------------------------------
-- 2. Deviations from common best-practice defaults
--    One row per "finding" so it sorts well and exports cleanly.
------------------------------------------------------------------------------
;WITH findings AS (
    SELECT name, N'AUTO_CLOSE is ON'          AS finding,
           N'Disable. Causes connection setup overhead and prevents AG/log shipping.' AS rationale
    FROM sys.databases WHERE database_id > 4 AND is_auto_close_on = 1
    UNION ALL
    SELECT name, N'AUTO_SHRINK is ON',
           N'Disable. Causes fragmentation and IO spikes; explicit DBCC SHRINKFILE is the documented path.'
    FROM sys.databases WHERE database_id > 4 AND is_auto_shrink_on = 1
    UNION ALL
    SELECT name, N'PAGE_VERIFY <> CHECKSUM',
           N'Set to CHECKSUM. Detects torn pages and bit rot - essential on SAN storage where the array does its own caching.'
    FROM sys.databases WHERE database_id > 4 AND page_verify_option_desc <> 'CHECKSUM'
    UNION ALL
    SELECT name, N'AUTO_UPDATE_STATISTICS is OFF',
           N'Enable unless you maintain stats with a dedicated job. Even then, consider leaving it on for safety.'
    FROM sys.databases WHERE database_id > 4 AND is_auto_update_stats_on = 0
    UNION ALL
    SELECT name, N'AUTO_CREATE_STATISTICS is OFF',
           N'Enable. The optimizer needs this for accurate row estimates.'
    FROM sys.databases WHERE database_id > 4 AND is_auto_create_stats_on = 0
    UNION ALL
    SELECT name, N'TRUSTWORTHY is ON',
           N'Disable unless you actively need it. Combined with a privileged owner, it is a privilege-escalation vector.'
    FROM sys.databases WHERE database_id > 4 AND is_trustworthy_on = 1
    UNION ALL
    SELECT name, N'DB owner is not sa',
           N'Set owner to sa (or a dedicated low-privilege login) to avoid escalation surprises after staff changes.'
    FROM sys.databases WHERE database_id > 4
      AND SUSER_SNAME(owner_sid) <> N'sa'
    UNION ALL
    SELECT name, N'Compatibility level is below 150',
           N'Compat 150 (SQL 2019) enables Intelligent QP features. Hold back only if validated regressions.'
    FROM sys.databases WHERE database_id > 4 AND compatibility_level < 150
    UNION ALL
    SELECT name, N'recovery_model is SIMPLE',
           N'SIMPLE prevents PITR and breaks Always On AG eligibility. Confirm this is intentional for the workload.'
    FROM sys.databases WHERE database_id > 4 AND recovery_model_desc = 'SIMPLE'
    UNION ALL
    SELECT name, N'RCSI is OFF',
           N'Many modern workloads benefit from READ_COMMITTED_SNAPSHOT. Verify isolation expectations before enabling.'
    FROM sys.databases WHERE database_id > 4 AND is_read_committed_snapshot_on = 0
    UNION ALL
    SELECT name, N'DB_CHAINING (cross-DB ownership chaining) ON',
           N'Disable unless explicitly required. Combined with TRUSTWORTHY this is dangerous.'
    FROM sys.databases WHERE database_id > 4 AND is_db_chaining_on = 1
    UNION ALL
    SELECT name, N'target_recovery_time_in_seconds is 0 (legacy checkpointing)',
           N'Set to 60. Indirect checkpoint smooths I/O bursts; 0 falls back to recovery interval (recovery time can spike).'
    FROM sys.databases WHERE database_id > 4 AND target_recovery_time_in_seconds = 0
    UNION ALL
    SELECT name, N'DELAYED_DURABILITY is FORCED',
           N'Forced delayed durability trades durability for throughput. Confirm intentional for the workload.'
    FROM sys.databases WHERE database_id > 4 AND delayed_durability_desc = 'FORCED'
)
SELECT
    [section] = N'02 - Database deviations from best practice',
    [database_name] = name,
    [finding] = finding,
    [rationale] = rationale
FROM findings
ORDER BY name, finding;

------------------------------------------------------------------------------
-- 3. Database owners (helps identify "orphan SID" owners)
------------------------------------------------------------------------------
SELECT
    [section]                = N'03 - Database owners',
    [database_id]            = d.database_id,
    [name]                   = d.name,
    [owner_sid_hex]          = CONVERT(varchar(2000), d.owner_sid, 1),
    [owner_name]             = SUSER_SNAME(d.owner_sid),
    [owner_is_orphan]        = CASE WHEN SUSER_SNAME(d.owner_sid) IS NULL THEN 1 ELSE 0 END
FROM sys.databases d
ORDER BY d.database_id;
