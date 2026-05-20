/* ============================================================================
   03_memory_cpu_parallelism.sql
   ----------------------------------------------------------------------------
   Captures: memory and CPU posture - the levers that hurt most when wrong.
            Min/max server memory, MAXDOP (global + Resource Governor),
            cost threshold for parallelism, NUMA layout, LPIM/IFI privilege,
            optimize-for-ad-hoc, large pages, plan cache, and target ratios.

   Target  : SQL Server 2019, physical host, SAN, A-P cluster
   Safety  : Read-only.
   Output  : 6 result sets.
   ============================================================================ */
SET NOCOUNT ON;

------------------------------------------------------------------------------
-- 1. Memory and parallelism settings vs derived recommendations
------------------------------------------------------------------------------
DECLARE @physical_gb decimal(10,2) =
    (SELECT CAST(physical_memory_kb / 1024.0 / 1024.0 AS decimal(10,2)) FROM sys.dm_os_sys_info);

DECLARE @numa_nodes int =
    (SELECT numa_node_count FROM sys.dm_os_sys_info);

DECLARE @cpu_count int =
    (SELECT cpu_count FROM sys.dm_os_sys_info);

DECLARE @cores_per_socket int =
    (SELECT cores_per_socket FROM sys.dm_os_sys_info);

DECLARE @max_mem_mb bigint =
    CAST((SELECT value_in_use FROM sys.configurations WHERE name = 'max server memory (MB)') AS bigint);

DECLARE @min_mem_mb bigint =
    CAST((SELECT value_in_use FROM sys.configurations WHERE name = 'min server memory (MB)') AS bigint);

DECLARE @maxdop int =
    CAST((SELECT value_in_use FROM sys.configurations WHERE name = 'max degree of parallelism') AS int);

DECLARE @ctfp int =
    CAST((SELECT value_in_use FROM sys.configurations WHERE name = 'cost threshold for parallelism') AS int);

DECLARE @opt_adhoc int =
    CAST((SELECT value_in_use FROM sys.configurations WHERE name = 'optimize for ad hoc workloads') AS int);

-- Recommended MAXDOP per MS guidance (KB 2806535 / 2019 docs):
--   * Multiple NUMA nodes: <= logical cores per NUMA node, max 16
--   * Single NUMA, <=8 logical cores: keep at or below logical core count
--   * Single NUMA, >8 logical cores: 8
DECLARE @rec_maxdop int =
    CASE
        WHEN @numa_nodes > 1 THEN
            CASE WHEN (@cpu_count / NULLIF(@numa_nodes,0)) > 16 THEN 16
                 ELSE (@cpu_count / NULLIF(@numa_nodes,0)) END
        WHEN @cpu_count <= 8 THEN @cpu_count
        ELSE 8
    END;

SELECT
    [section]                = N'01 - Memory and parallelism',
    [physical_memory_gb]     = @physical_gb,
    [min_server_memory_mb]   = @min_mem_mb,
    [max_server_memory_mb]   = @max_mem_mb,
    [max_server_memory_gb]   = CAST(@max_mem_mb / 1024.0 AS decimal(10,2)),
    [max_is_default]         = CASE WHEN @max_mem_mb = 2147483647 THEN 1 ELSE 0 END,
    [rec_max_server_memory_gb] =
        CASE
            WHEN @physical_gb <= 16 THEN @physical_gb - 4
            WHEN @physical_gb <= 64 THEN @physical_gb - 8
            WHEN @physical_gb <= 256 THEN @physical_gb - 16
            ELSE @physical_gb - 32
        END,
    [maxdop]                 = @maxdop,
    [recommended_maxdop]     = @rec_maxdop,
    [cost_threshold]         = @ctfp,
    [rec_cost_threshold]     = 50,            -- common OLTP starting point
    [optimize_for_ad_hoc]    = @opt_adhoc,
    [numa_node_count]        = @numa_nodes,
    [cpu_count]              = @cpu_count,
    [cores_per_socket]       = @cores_per_socket;

------------------------------------------------------------------------------
-- 2. Resource Governor workload group MAXDOP overrides (if any)
------------------------------------------------------------------------------
SELECT
    [section]                = N'02 - Resource Governor groups',
    rgrp.name                AS group_name,
    rgrp.is_system_group,
    rgrp.importance,
    rgrp.request_max_memory_grant_percent,
    rgrp.request_max_cpu_time_sec,
    rgrp.request_memory_grant_timeout_sec,
    rgrp.max_dop,
    rgrp.group_max_requests,
    pool.name                AS pool_name,
    pool.min_cpu_percent,
    pool.max_cpu_percent,
    pool.min_memory_percent,
    pool.max_memory_percent
FROM sys.dm_resource_governor_workload_groups rgrp
JOIN sys.dm_resource_governor_resource_pools  pool
  ON rgrp.pool_id = pool.pool_id
ORDER BY pool.name, rgrp.name;

------------------------------------------------------------------------------
-- 3. Database-scoped MAXDOP overrides (introduced in 2016+)
------------------------------------------------------------------------------
DECLARE @sql nvarchar(max) = N'';
-- Use a LEADING separator pattern so the trim math is exact and not
-- dependent on LEN()'s trailing-space behavior.
SELECT @sql = @sql +
    N' UNION ALL
       SELECT ''' + REPLACE(name, '''', '''''') + N''' AS database_name,
              configuration_id, name, value, value_for_secondary
         FROM ' + QUOTENAME(name) + N'.sys.database_scoped_configurations'
FROM sys.databases
WHERE state_desc = 'ONLINE'
  AND database_id > 4
  AND HAS_DBACCESS(name) = 1;

IF LEN(@sql) > 0
BEGIN
    -- Strip the leading " UNION ALL" (10 chars including the leading space).
    SET @sql = STUFF(@sql, 1, 10, N'');
    SET @sql = N';WITH dsc AS (' + @sql + N')
    SELECT [section] = N''03 - Database scoped configurations'', *
      FROM dsc
     WHERE name IN (
        N''MAXDOP'',
        N''LEGACY_CARDINALITY_ESTIMATION'',
        N''PARAMETER_SNIFFING'',
        N''QUERY_OPTIMIZER_HOTFIXES'',
        N''ACCELERATED_DATABASE_RECOVERY'',
        N''LIGHTWEIGHT_QUERY_PROFILING'',
        N''ELEVATE_ONLINE'',
        N''ELEVATE_RESUMABLE'',
        N''ASYNC_STATS_UPDATE_WAIT_AT_LOW_PRIORITY''
     )
     ORDER BY database_name, name;';
    EXEC sys.sp_executesql @sql;
END
ELSE
BEGIN
    SELECT [section] = N'03 - Database scoped configurations', [note] = N'No user databases found / no access.';
END

------------------------------------------------------------------------------
-- 4. LPIM and Instant File Initialization privileges
------------------------------------------------------------------------------
SELECT
    [section]                = N'04 - LPIM and IFI',
    [sql_memory_model_desc]  = sql_memory_model_desc,   -- "LOCK_PAGES" or "CONVENTIONAL"
    [lpim_enabled]           = CASE WHEN sql_memory_model_desc IN ('LOCK_PAGES', 'LARGE_PAGES') THEN 1 ELSE 0 END,
    [virtual_machine_type_desc] = virtual_machine_type_desc
FROM sys.dm_os_sys_info;

SELECT
    [section]                = N'04b - IFI per service',
    [servicename]            = servicename,
    [service_account]        = service_account,
    [ifi_enabled]            = instant_file_initialization_enabled
FROM sys.dm_server_services
WHERE instant_file_initialization_enabled IS NOT NULL;

------------------------------------------------------------------------------
-- 5. Plan cache pressure indicators (ad-hoc bloat)
------------------------------------------------------------------------------
SELECT
    [section]                = N'05 - Plan cache by objtype',
    [objtype]                = objtype,
    [cached_plans]           = COUNT_BIG(*),
    [total_mb]               = CAST(SUM(size_in_bytes) / 1024.0 / 1024.0 AS decimal(12,2)),
    [single_use_plans]       = SUM(CASE WHEN usecounts = 1 THEN 1 ELSE 0 END),
    [single_use_mb]          = CAST(SUM(CASE WHEN usecounts = 1 THEN size_in_bytes ELSE 0 END)
                                    / 1024.0 / 1024.0 AS decimal(12,2))
FROM sys.dm_exec_cached_plans
GROUP BY objtype
ORDER BY total_mb DESC;

------------------------------------------------------------------------------
-- 6. Memory clerks - biggest consumers
------------------------------------------------------------------------------
SELECT TOP (25)
    [section]                = N'06 - Top memory clerks',
    [type]                   = [type],
    [name]                   = [name],
    [memory_node_id]         = memory_node_id,
    [pages_kb]               = SUM(pages_kb),
    [pages_mb]               = CAST(SUM(pages_kb) / 1024.0 AS decimal(12,2)),
    [virtual_memory_committed_kb] = SUM(virtual_memory_committed_kb)
FROM sys.dm_os_memory_clerks
GROUP BY [type], [name], memory_node_id
ORDER BY SUM(pages_kb) DESC;
