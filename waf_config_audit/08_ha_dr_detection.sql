/* ============================================================================
   08_ha_dr_detection.sql
   ----------------------------------------------------------------------------
   Captures: which HA/DR technologies are actually in use, then dumps the
            relevant configuration for each. Auto-detects FCI, Always On AG,
            log shipping, and database mirroring (legacy, still seen).

   Why auto-detect? In the user's environment "Active-Passive cluster with
   a listener" can be either:
       - FCI (the listener is the WSFC virtual network name), or
       - AG (a single-secondary AG with an AG listener), or
       - both (FCI for local HA + AG for DR).
   The script lets the data tell us which.

   Target  : SQL Server 2019, physical host, SAN, A-P cluster
   Safety  : Read-only.
   Output  : 8 result sets (sections that don't apply emit an explanatory row).
   ============================================================================ */
SET NOCOUNT ON;

DECLARE @is_clustered     int = ISNULL(CAST(SERVERPROPERTY('IsClustered')     AS int), 0);
DECLARE @is_hadr_enabled  int = ISNULL(CAST(SERVERPROPERTY('IsHadrEnabled')   AS int), 0);
DECLARE @hadr_manager     int = ISNULL(CAST(SERVERPROPERTY('HadrManagerStatus') AS int), -1);

------------------------------------------------------------------------------
-- 1. HA/DR posture summary
------------------------------------------------------------------------------
SELECT
    [section]                   = N'01 - HA/DR posture summary',
    [IsClustered_FCI]           = @is_clustered,                 -- 1 = FCI
    [IsHadrEnabled_AG]          = @is_hadr_enabled,              -- 1 = AG feature enabled
    [HadrManagerStatus]         = @hadr_manager,                 -- 1 = started, 0 = not started
    [HadrManagerStatus_desc]    = CASE @hadr_manager
                                      WHEN  1 THEN N'Started and running'
                                      WHEN  0 THEN N'Not started (AG feature on but Hadr_manager off)'
                                      WHEN  2 THEN N'Not started, pending communication'
                                      ELSE N'Unknown / not enabled'
                                  END,
    [ComputerNamePhysicalNetBIOS] = CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS sysname),
    [MachineName]               = CAST(SERVERPROPERTY('MachineName')               AS sysname),
    [ServerName]                = CAST(SERVERPROPERTY('ServerName')                AS sysname),
    [@@SERVERNAME]              = @@SERVERNAME,
    [interpretation] = CASE
        WHEN @is_clustered = 1 AND @is_hadr_enabled = 1 THEN N'FCI + AG hybrid'
        WHEN @is_clustered = 1 AND @is_hadr_enabled = 0 THEN N'Failover Cluster Instance (FCI) only'
        WHEN @is_clustered = 0 AND @is_hadr_enabled = 1 THEN N'Always On Availability Group (AG) only'
        ELSE N'Standalone instance - no FCI, no AG'
    END;

------------------------------------------------------------------------------
-- 2. Failover Cluster Instance - nodes and ownership
------------------------------------------------------------------------------
IF @is_clustered = 1
BEGIN
    SELECT
        [section]            = N'02 - FCI cluster nodes',
        [NodeName]           = NodeName,
        [status]             = status,
        [status_description] = status_description,
        [is_current_owner]   = is_current_owner
    FROM sys.dm_os_cluster_nodes
    ORDER BY NodeName;

    SELECT
        [section]                = N'02b - FCI cluster properties',
        [VerboseLogging]         = VerboseLogging,
        [SqlDumperDumpFlags]     = SqlDumperDumpFlags,
        [SqlDumperDumpPath]      = SqlDumperDumpPath,
        [SqlDumperDumpTimeOut]   = SqlDumperDumpTimeOut,
        [FailureConditionLevel]  = FailureConditionLevel,        -- 0-5, default 3
        [HealthCheckTimeout]     = HealthCheckTimeout
    FROM sys.dm_os_cluster_properties;

    SELECT
        [section]                = N'02c - FCI shared drives (drives visible to all cluster nodes via VSS)',
        [DriveName]              = DriveName
    FROM sys.dm_io_cluster_shared_drives;
END
ELSE
BEGIN
    SELECT [section] = N'02 - FCI cluster nodes', [note] = N'IsClustered = 0. This is not a Failover Cluster Instance.';
END

------------------------------------------------------------------------------
-- 3. Always On - feature enabled? endpoint?
------------------------------------------------------------------------------
SELECT
    [section]                = N'03 - Always On endpoints',
    [name]                   = e.name,
    [endpoint_id]            = e.endpoint_id,
    [protocol_desc]          = e.protocol_desc,
    [type_desc]              = e.type_desc,
    [role_desc]              = mre.role_desc,
    [is_encryption_enabled]  = mre.is_encryption_enabled,
    [connection_auth_desc]   = mre.connection_auth_desc,
    [encryption_algorithm_desc] = mre.encryption_algorithm_desc,
    [state_desc]             = e.state_desc,
    [port]                   = tep.port
FROM sys.endpoints e
LEFT JOIN sys.database_mirroring_endpoints mre ON e.endpoint_id = mre.endpoint_id
LEFT JOIN sys.tcp_endpoints tep ON e.endpoint_id = tep.endpoint_id
WHERE e.type_desc = 'DATABASE_MIRRORING';

------------------------------------------------------------------------------
-- 4. Always On - Availability Groups
------------------------------------------------------------------------------
IF @is_hadr_enabled = 1
BEGIN
    SELECT
        [section]                                 = N'04 - Availability Groups',
        [ag_name]                                 = ag.name,
        [ag_id]                                   = ag.group_id,
        [resource_id]                             = ag.resource_id,
        [resource_group_id]                       = ag.resource_group_id,
        [failure_condition_level]                 = ag.failure_condition_level,
        [health_check_timeout]                    = ag.health_check_timeout,
        [automated_backup_preference_desc]        = ag.automated_backup_preference_desc,
        [version]                                 = ag.version,
        [basic_features]                          = ag.basic_features,
        [dtc_support]                             = ag.dtc_support,
        [db_failover]                             = ag.db_failover,
        [is_distributed]                          = ag.is_distributed,
        [cluster_type_desc]                       = ag.cluster_type_desc,
        [required_synchronized_secondaries_to_commit] = ag.required_synchronized_secondaries_to_commit,
        [sequence_number]                         = ag.sequence_number
    FROM sys.availability_groups ag;

    SELECT
        [section]                     = N'04b - AG replicas',
        [ag_name]                     = ag.name,
        [replica_server_name]         = ar.replica_server_name,
        [endpoint_url]                = ar.endpoint_url,
        [availability_mode_desc]      = ar.availability_mode_desc,    -- SYNC / ASYNC
        [failover_mode_desc]          = ar.failover_mode_desc,        -- AUTOMATIC / MANUAL
        [session_timeout]             = ar.session_timeout,
        [primary_role_allow_connections_desc] = ar.primary_role_allow_connections_desc,
        [secondary_role_allow_connections_desc] = ar.secondary_role_allow_connections_desc,
        [backup_priority]             = ar.backup_priority,
        [seeding_mode_desc]           = ar.seeding_mode_desc,         -- AUTOMATIC / MANUAL
        [read_only_routing_url]       = ar.read_only_routing_url,
        [create_date]                 = ar.create_date,
        [modify_date]                 = ar.modify_date
    FROM sys.availability_groups ag
    JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
    ORDER BY ag.name, ar.replica_server_name;

    SELECT
        [section]                = N'04c - AG listeners',
        [ag_name]                = ag.name,
        [listener_name]          = agl.dns_name,
        [port]                   = agl.port,
        [is_conformant]          = agl.is_conformant,
        [ip_configuration_string_from_cluster] = agl.ip_configuration_string_from_cluster
    FROM sys.availability_group_listeners agl
    JOIN sys.availability_groups ag ON agl.group_id = ag.group_id;

    SELECT
        [section]                = N'04d - AG replica current state',
        [replica_server_name]    = ar.replica_server_name,
        [role_desc]              = ars.role_desc,                  -- PRIMARY / SECONDARY
        [operational_state_desc] = ars.operational_state_desc,
        [connected_state_desc]   = ars.connected_state_desc,
        [recovery_health_desc]   = ars.recovery_health_desc,
        [synchronization_health_desc] = ars.synchronization_health_desc,
        [last_connect_error_number]   = ars.last_connect_error_number,
        [last_connect_error_description] = ars.last_connect_error_description,
        [last_connect_error_timestamp]   = ars.last_connect_error_timestamp
    FROM sys.dm_hadr_availability_replica_states ars
    JOIN sys.availability_replicas ar ON ars.replica_id = ar.replica_id;

    SELECT
        [section]                          = N'04e - AG database synchronization',
        [database_name]                    = DB_NAME(drs.database_id),
        [replica_server_name]              = ar.replica_server_name,
        [synchronization_state_desc]       = drs.synchronization_state_desc,
        [synchronization_health_desc]      = drs.synchronization_health_desc,
        [is_suspended]                     = drs.is_suspended,
        [suspend_reason_desc]              = drs.suspend_reason_desc,
        [last_sent_time]                   = drs.last_sent_time,
        [last_received_time]               = drs.last_received_time,
        [last_hardened_time]               = drs.last_hardened_time,
        [last_redone_time]                 = drs.last_redone_time,
        [log_send_queue_size_kb]           = drs.log_send_queue_size,
        [log_send_rate_kb_per_sec]         = drs.log_send_rate,
        [redo_queue_size_kb]               = drs.redo_queue_size,
        [redo_rate_kb_per_sec]             = drs.redo_rate,
        [end_of_log_lsn]                   = drs.end_of_log_lsn,
        [last_commit_time]                 = drs.last_commit_time
    FROM sys.dm_hadr_database_replica_states drs
    JOIN sys.availability_replicas ar ON drs.replica_id = ar.replica_id
    ORDER BY DB_NAME(drs.database_id), ar.replica_server_name;
END
ELSE
BEGIN
    SELECT [section] = N'04 - Availability Groups', [note] = N'IsHadrEnabled = 0. Always On feature is not enabled on this instance.';
END

------------------------------------------------------------------------------
-- 5. Database mirroring (legacy - still seen, deprecated since 2012)
------------------------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.database_mirroring WHERE mirroring_state IS NOT NULL)
BEGIN
    SELECT
        [section]                       = N'05 - Database mirroring',
        [database_name]                 = DB_NAME(database_id),
        [mirroring_state_desc]          = mirroring_state_desc,
        [mirroring_role_desc]           = mirroring_role_desc,
        [mirroring_partner_name]        = mirroring_partner_name,
        [mirroring_partner_instance]    = mirroring_partner_instance,
        [mirroring_witness_name]        = mirroring_witness_name,
        [mirroring_witness_state_desc]  = mirroring_witness_state_desc,
        [mirroring_safety_level_desc]   = mirroring_safety_level_desc,   -- FULL / OFF
        [mirroring_redo_queue]          = mirroring_redo_queue,
        [mirroring_redo_queue_type]     = mirroring_redo_queue_type,
        [mirroring_failover_lsn]        = mirroring_failover_lsn
    FROM sys.database_mirroring
    WHERE mirroring_state IS NOT NULL;
END
ELSE
BEGIN
    SELECT [section] = N'05 - Database mirroring', [note] = N'No databases participating in mirroring.';
END

------------------------------------------------------------------------------
-- 6. Log shipping (primary side)
------------------------------------------------------------------------------
IF DB_ID('msdb') IS NOT NULL
BEGIN
    SELECT
        [section]                   = N'06 - Log shipping (primary)',
        [primary_id]                = p.primary_id,
        [primary_database]          = p.primary_database,
        [backup_directory]          = p.backup_directory,
        [backup_share]              = p.backup_share,
        [backup_retention_period]   = p.backup_retention_period,
        [backup_job_id]             = p.backup_job_id,
        [last_backup_file]          = p.last_backup_file,
        [last_backup_date]          = p.last_backup_date,
        [history_retention_period]  = p.history_retention_period
    FROM msdb.dbo.log_shipping_primary_databases p;

    SELECT
        [section]                   = N'06b - Log shipping (secondary)',
        [secondary_id]              = s.secondary_id,
        [secondary_database]        = sd.secondary_database,
        [primary_server]            = s.primary_server,
        [primary_database]          = s.primary_database,
        [backup_source_directory]   = s.backup_source_directory,
        [backup_destination_directory] = s.backup_destination_directory,
        [restore_delay]             = sd.restore_delay,
        [restore_all]               = sd.restore_all,
        [disconnect_users]          = sd.disconnect_users,
        [block_size]                = sd.block_size,
        [restore_mode]              = sd.restore_mode,
        [last_copied_file]          = sd.last_copied_file,
        [last_copied_date]          = sd.last_copied_date,
        [last_restored_file]        = sd.last_restored_file,
        [last_restored_date]        = sd.last_restored_date,
        [last_restored_latency]     = sd.last_restored_latency
    FROM msdb.dbo.log_shipping_secondary s
    JOIN msdb.dbo.log_shipping_secondary_databases sd ON s.secondary_id = sd.secondary_id;
END

------------------------------------------------------------------------------
-- 7. WSFC quorum / cluster membership (visible from SQL since 2012)
------------------------------------------------------------------------------
IF @is_clustered = 1 OR @is_hadr_enabled = 1
BEGIN
    IF OBJECT_ID('sys.dm_hadr_cluster') IS NOT NULL
    BEGIN
        SELECT
            [section]                = N'07 - WSFC cluster',
            [cluster_name]           = cluster_name,
            [quorum_type_desc]       = quorum_type_desc,
            [quorum_state_desc]      = quorum_state_desc
        FROM sys.dm_hadr_cluster;

        SELECT
            [section]                = N'07b - WSFC members',
            [member_name]            = member_name,
            [member_type_desc]       = member_type_desc,
            [member_state_desc]      = member_state_desc,
            [number_of_quorum_votes] = number_of_quorum_votes
        FROM sys.dm_hadr_cluster_members;

        SELECT
            [section]                = N'07c - WSFC network subnets',
            [member_name]            = member_name,
            [network_subnet_ip]      = network_subnet_ip,
            [network_subnet_ipv4_mask] = network_subnet_ipv4_mask,
            [network_subnet_prefix_length] = network_subnet_prefix_length,
            [is_public]              = is_public,
            [is_ipv4]                = is_ipv4
        FROM sys.dm_hadr_cluster_networks;
    END
END

------------------------------------------------------------------------------
-- 8. Recent automatic failover events (from default trace - last 7 days)
--    Useful to spot recent failovers without going to the Windows event log.
------------------------------------------------------------------------------
DECLARE @trace_path nvarchar(520) =
    (SELECT REVERSE(SUBSTRING(REVERSE([path]), CHARINDEX(N'\', REVERSE([path])), 520)) + N'log.trc'
       FROM sys.traces WHERE is_default = 1);

IF @trace_path IS NOT NULL
BEGIN
    SELECT TOP (50)
        [section]      = N'08 - Recent failover/cluster mentions in default trace',
        [StartTime]    = StartTime,
        [EventClass]   = EventClass,
        [TextData]     = CAST(TextData AS nvarchar(1000)),
        [DatabaseName] = DatabaseName,
        [LoginName]    = LoginName,
        [HostName]     = HostName,
        [ApplicationName] = ApplicationName
    FROM sys.fn_trace_gettable(@trace_path, DEFAULT)
    WHERE TextData LIKE N'%failover%'
       OR TextData LIKE N'%cluster%'
       OR TextData LIKE N'%role change%'
       OR EventClass = 148          -- Deadlock Graph - only documented class we want unconditionally
    ORDER BY StartTime DESC;

    -- AG state changes go to the AlwaysOn_health Extended Events session, not
    -- the default trace. The query below pulls the most recent role-change
    -- events from that session. Will be empty if AG is not in use.
    IF EXISTS (SELECT 1 FROM sys.dm_xe_sessions WHERE name = N'AlwaysOn_health')
    BEGIN
        SELECT TOP (25)
            [section]     = N'08b - AlwaysOn_health recent role/state changes',
            [event_time]  = DATEADD(MS, ca.[xml].value('(/event/@timestamp)[1]', 'datetime2'), 0),
            [event_name]  = ca.[xml].value('(/event/@name)[1]', 'nvarchar(128)'),
            [event_xml]   = ca.[xml]
        FROM (
            SELECT CAST(event_data AS xml) AS [xml]
            FROM sys.fn_xe_file_target_read_file(
                (SELECT TOP (1) CAST(target_data AS xml).value('(/EventFileTarget/File/@name)[1]', 'nvarchar(260)')
                   FROM sys.dm_xe_session_targets st
                   JOIN sys.dm_xe_sessions s ON s.address = st.event_session_address
                  WHERE s.name = N'AlwaysOn_health' AND st.target_name = N'event_file'),
                NULL, NULL, NULL)
        ) ca
        WHERE ca.[xml].value('(/event/@name)[1]', 'nvarchar(128)') IN (
                N'availability_replica_state_change',
                N'availability_replica_manager_state_change',
                N'availability_group_lease_expired',
                N'lock_redo_blocked')
        ORDER BY ca.[xml].value('(/event/@timestamp)[1]', 'datetime2') DESC;
    END
END
