/* ============================================================================
   01_instance_identity_and_hardware.sql
   ----------------------------------------------------------------------------
   Captures: who/what this instance is and what hardware it sits on.
            Version, edition, build, host, services, startup parameters,
            CPU/memory layout, hyperthreading ratio, and active trace flags.

   Target  : SQL Server 2019, physical host, SAN, A-P cluster (FCI or AG)
   Safety  : Read-only. Requires VIEW SERVER STATE for most DMVs.
   Output  : 8 labeled result sets.
   ============================================================================ */
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

------------------------------------------------------------------------------
-- 1. Version and edition
------------------------------------------------------------------------------
SELECT
    [section]                = N'01 - Version and edition',
    [@@SERVERNAME]           = @@SERVERNAME,
    [MachineName]            = CAST(SERVERPROPERTY('MachineName')         AS sysname),
    [ServerName]             = CAST(SERVERPROPERTY('ServerName')          AS sysname),
    [ComputerNamePhysicalNetBIOS] = CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS sysname),
    [InstanceName]           = ISNULL(CAST(SERVERPROPERTY('InstanceName') AS sysname), N'MSSQLSERVER'),
    [Edition]                = CAST(SERVERPROPERTY('Edition')             AS nvarchar(256)),
    [ProductVersion]         = CAST(SERVERPROPERTY('ProductVersion')      AS nvarchar(64)),
    [ProductLevel]           = CAST(SERVERPROPERTY('ProductLevel')        AS nvarchar(64)),
    [ProductUpdateLevel]     = CAST(SERVERPROPERTY('ProductUpdateLevel')  AS nvarchar(64)),
    [ProductUpdateReference] = CAST(SERVERPROPERTY('ProductUpdateReference') AS nvarchar(64)),
    [ProductBuild]           = CAST(SERVERPROPERTY('ProductBuild')        AS nvarchar(64)),
    [ProductMajorVersion]    = CAST(SERVERPROPERTY('ProductMajorVersion') AS nvarchar(64)),
    [ProductMinorVersion]    = CAST(SERVERPROPERTY('ProductMinorVersion') AS nvarchar(64)),
    [EngineEdition]          = CAST(SERVERPROPERTY('EngineEdition')       AS int),
    [Collation]              = CAST(SERVERPROPERTY('Collation')           AS sysname),
    [IsIntegratedSecurityOnly] = CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS int),
    [IsClustered]            = CAST(SERVERPROPERTY('IsClustered')         AS int),
    [IsHadrEnabled]          = CAST(SERVERPROPERTY('IsHadrEnabled')       AS int),
    [HadrManagerStatus]      = CAST(SERVERPROPERTY('HadrManagerStatus')   AS int),
    [IsFullTextInstalled]    = CAST(SERVERPROPERTY('IsFullTextInstalled') AS int),
    [IsPolyBaseInstalled]    = CAST(SERVERPROPERTY('IsPolyBaseInstalled') AS int),
    [FilestreamConfiguredLevel] = CAST(SERVERPROPERTY('FilestreamConfiguredLevel') AS int),
    [FilestreamEffectiveLevel] = CAST(SERVERPROPERTY('FilestreamEffectiveLevel') AS int),
    [@@VERSION]              = @@VERSION;

------------------------------------------------------------------------------
-- 2. Host / OS info (from sys.dm_os_sys_info and sys.dm_os_host_info)
------------------------------------------------------------------------------
SELECT
    [section]                          = N'02 - Host and OS',
    [host_platform]                    = h.host_platform,
    [host_distribution]                = h.host_distribution,
    [host_release]                     = h.host_release,
    [host_service_pack_level]          = h.host_service_pack_level,
    [host_sku]                         = h.host_sku,
    [os_language_version]              = h.os_language_version,
    [sqlserver_start_time]             = s.sqlserver_start_time,
    [uptime_days]                      = DATEDIFF(DAY, s.sqlserver_start_time, SYSDATETIME()),
    [process_kernel_time_ms]           = s.process_kernel_time_ms,
    [process_user_time_ms]             = s.process_user_time_ms,
    [time_source]                      = s.time_source,
    [virtual_machine_type_desc]        = s.virtual_machine_type_desc,
    [container_type_desc]              = s.container_type_desc,
    [socket_count]                     = s.socket_count,
    [cores_per_socket]                 = s.cores_per_socket,
    [numa_node_count]                  = s.numa_node_count,
    [cpu_count]                        = s.cpu_count,
    [hyperthread_ratio]                = s.hyperthread_ratio,
    [softnuma_configuration_desc]      = s.softnuma_configuration_desc,
    [physical_memory_kb]               = s.physical_memory_kb,
    [physical_memory_gb]               = CAST(s.physical_memory_kb / 1024.0 / 1024.0 AS decimal(10,2)),
    [committed_kb]                     = s.committed_kb,
    [committed_target_kb]              = s.committed_target_kb,
    [committed_target_gb]              = CAST(s.committed_target_kb / 1024.0 / 1024.0 AS decimal(10,2)),
    [max_workers_count]                = s.max_workers_count,
    [scheduler_count]                  = s.scheduler_count,
    [affinity_type_desc]               = s.affinity_type_desc,
    [process_physical_affinity]        = s.process_physical_affinity,
    [sql_memory_model_desc]            = s.sql_memory_model_desc      -- LPIM is "LOCK_PAGES" or "LARGE_PAGES"
FROM sys.dm_os_sys_info s
CROSS APPLY (SELECT TOP (1) * FROM sys.dm_os_host_info) h;

------------------------------------------------------------------------------
-- 3. Services - account, start type, cluster awareness
------------------------------------------------------------------------------
SELECT
    [section]                = N'03 - SQL services',
    [servicename]            = servicename,
    [startup_type_desc]      = startup_type_desc,
    [status_desc]            = status_desc,
    [process_id]             = process_id,
    [last_startup_time]      = last_startup_time,
    [service_account]        = service_account,
    [is_clustered]           = is_clustered,
    [cluster_nodename]       = cluster_nodename,
    [filename]               = [filename],
    [instant_file_initialization_enabled] =
        TRY_CAST(instant_file_initialization_enabled AS char(1))  -- 'Y' / 'N' on supported builds
FROM sys.dm_server_services;

------------------------------------------------------------------------------
-- 4. Registered startup parameters and trace flags from -T
------------------------------------------------------------------------------
SELECT
    [section]                = N'04 - Registry startup parameters',
    [registry_key]           = registry_key,
    [value_name]             = value_name,
    [value_data]             = value_data
FROM sys.dm_server_registry
WHERE registry_key LIKE N'%MSSQLServer\Parameters%';

------------------------------------------------------------------------------
-- 5. Active trace flags (session-wide and global)
------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#trace') IS NOT NULL DROP TABLE #trace;
CREATE TABLE #trace (TraceFlag int, [Status] tinyint, [Global] tinyint, [Session] tinyint);
INSERT INTO #trace EXEC ('DBCC TRACESTATUS() WITH NO_INFOMSGS');

SELECT
    [section]                = N'05 - Active trace flags',
    [TraceFlag]              = TraceFlag,
    [Status]                 = [Status],
    [Global]                 = [Global],
    [Session]                = [Session],
    [Notes]                  = CASE TraceFlag
        WHEN 1117 THEN N'Pre-2016: grow all files in filegroup together. Default behavior in 2016+.'
        WHEN 1118 THEN N'Pre-2016: uniform extent allocation (reduces SGAM contention). Default in 2016+.'
        WHEN 1204 THEN N'Deadlock graph to error log.'
        WHEN 1222 THEN N'Verbose deadlock graph to error log.'
        WHEN 3226 THEN N'Suppress successful backup messages in error log. Often desired.'
        WHEN 4199 THEN N'Enable post-RTM query optimizer hotfixes (consider compat level instead in 2016+).'
        WHEN 7412 THEN N'Lightweight query execution statistics profile (default-on in 2019).'
        WHEN 8048 THEN N'Reduce NUMA partitioning of memory objects. Largely superseded by CU fixes.'
        WHEN 9024 THEN N'Reduce LOGCACHE_ACCESS spinlock contention.'
        ELSE N''
    END
FROM #trace;

------------------------------------------------------------------------------
-- 6. SQL Server process (memory, CPU usage right now)
------------------------------------------------------------------------------
SELECT
    [section]                = N'06 - Process memory',
    [physical_memory_in_use_kb]    = physical_memory_in_use_kb,
    [physical_memory_in_use_gb]    = CAST(physical_memory_in_use_kb / 1024.0 / 1024.0 AS decimal(10,2)),
    [large_page_allocations_kb]    = large_page_allocations_kb,
    [locked_page_allocations_kb]   = locked_page_allocations_kb,
    [page_fault_count]             = page_fault_count,
    [memory_utilization_percentage] = memory_utilization_percentage,
    [available_commit_limit_kb]    = available_commit_limit_kb,
    [process_physical_memory_low]  = process_physical_memory_low,
    [process_virtual_memory_low]   = process_virtual_memory_low
FROM sys.dm_os_process_memory;

------------------------------------------------------------------------------
-- 7. Schedulers (one row per CPU plus hidden ones)
------------------------------------------------------------------------------
SELECT
    [section]                = N'07 - Scheduler summary',
    [status]                 = status,
    [scheduler_count]        = COUNT(*),
    [is_online_true]         = SUM(CASE WHEN is_online = 1 THEN 1 ELSE 0 END),
    [is_idle_true]           = SUM(CASE WHEN is_idle  = 1 THEN 1 ELSE 0 END),
    [avg_load_factor]        = AVG(load_factor),
    [avg_current_tasks_count] = AVG(current_tasks_count),
    [avg_runnable_tasks_count]= AVG(runnable_tasks_count),
    [avg_active_workers_count]= AVG(active_workers_count)
FROM sys.dm_os_schedulers
GROUP BY status
ORDER BY status;

------------------------------------------------------------------------------
-- 8. NUMA node detail (memory side)
--    sys.dm_os_memory_nodes does NOT have node_state_desc - that is on
--    sys.dm_os_nodes (the SQLOS/scheduler-node view). We join the two so
--    you see both the memory accounting and the node state.
------------------------------------------------------------------------------
SELECT
    [section]                            = N'08 - NUMA nodes (memory side)',
    [memory_node_id]                     = mn.memory_node_id,
    [virtual_address_space_reserved_kb]  = mn.virtual_address_space_reserved_kb,
    [virtual_address_space_committed_kb] = mn.virtual_address_space_committed_kb,
    [foreign_committed_kb]               = mn.foreign_committed_kb,
    [pages_kb]                           = mn.pages_kb,
    [target_kb]                          = mn.target_kb,
    [shared_memory_reserved_kb]          = mn.shared_memory_reserved_kb,
    [shared_memory_committed_kb]         = mn.shared_memory_committed_kb,
    [processor_group]                    = mn.processor_group,
    -- Aggregate SQLOS node info per memory node so we don't fan out on
    -- soft-NUMA (where one memory node can have multiple SQLOS nodes).
    [sqlos_nodes_in_this_memory_node]    = n.node_count,
    [first_node_state_desc]              = n.first_node_state_desc,
    [total_online_schedulers]            = n.total_online_schedulers,
    [combined_cpu_affinity_mask]         = n.combined_cpu_affinity_mask
FROM sys.dm_os_memory_nodes mn
OUTER APPLY (
    SELECT  node_count               = COUNT(*),
            first_node_state_desc    = MIN(node_state_desc),
            total_online_schedulers  = SUM(online_scheduler_count),
            combined_cpu_affinity_mask = SUM(cpu_affinity_mask)
      FROM  sys.dm_os_nodes
     WHERE  memory_node_id = mn.memory_node_id
) n
WHERE mn.memory_node_id <> 64;   -- 64 = DAC node, not interesting here

------------------------------------------------------------------------------
-- 8b. SQLOS nodes detail (separate row per SQLOS node, before aggregation)
------------------------------------------------------------------------------
SELECT
    [section]                = N'08b - SQLOS nodes (one row per SQLOS node)',
    [node_id]                = node_id,
    [node_state_desc]        = node_state_desc,
    [memory_node_id]         = memory_node_id,
    [cpu_affinity_mask]      = cpu_affinity_mask,
    [online_scheduler_count] = online_scheduler_count,
    [active_worker_count]    = active_worker_count,
    [avg_load_balance]       = avg_load_balance,
    [resource_monitor_state] = resource_monitor_state
FROM sys.dm_os_nodes
WHERE node_state_desc NOT LIKE '%DAC%'
ORDER BY node_id;
