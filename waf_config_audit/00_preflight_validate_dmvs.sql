SET NOCOUNT ON;

DECLARE @inventory TABLE (
    object_name  sysname,
    needed_cols  nvarchar(max)
);

INSERT INTO @inventory (object_name, needed_cols) VALUES

('sys.dm_os_sys_info',
 N'cpu_count,hyperthread_ratio,physical_memory_kb,committed_kb,committed_target_kb,'
+ N'max_workers_count,scheduler_count,affinity_type_desc,process_physical_affinity,'
+ N'sqlserver_start_time,process_kernel_time_ms,process_user_time_ms,time_source,'
+ N'virtual_machine_type_desc,container_type_desc,socket_count,cores_per_socket,'
+ N'numa_node_count,softnuma_configuration_desc,sql_memory_model_desc'),

('sys.dm_os_host_info',
 N'host_platform,host_distribution,host_release,host_service_pack_level,host_sku,os_language_version'),

('sys.dm_os_process_memory',
 N'physical_memory_in_use_kb,large_page_allocations_kb,locked_page_allocations_kb,'
+ N'page_fault_count,memory_utilization_percentage,available_commit_limit_kb,'
+ N'process_physical_memory_low,process_virtual_memory_low'),

('sys.dm_os_schedulers',
 N'status,is_online,is_idle,load_factor,current_tasks_count,runnable_tasks_count,active_workers_count'),

('sys.dm_os_memory_nodes',
 N'memory_node_id,virtual_address_space_reserved_kb,virtual_address_space_committed_kb,'
+ N'foreign_committed_kb,pages_kb,target_kb,shared_memory_reserved_kb,'
+ N'shared_memory_committed_kb,processor_group'),

('sys.dm_os_nodes',
 N'node_id,node_state_desc,memory_node_id,cpu_affinity_mask,online_scheduler_count,'
+ N'active_worker_count,avg_load_balance,resource_monitor_state'),

('sys.dm_os_memory_clerks',
 N'type,name,memory_node_id,pages_kb,virtual_memory_committed_kb'),

('sys.dm_os_volume_stats',
 N'volume_mount_point,file_system_type,logical_volume_name,total_bytes,available_bytes,'
+ N'supports_compression,supports_alternate_streams,supports_sparse_files,'
+ N'is_read_only,is_compressed'),

('sys.dm_os_wait_stats',
 N'wait_type,wait_time_ms,signal_wait_time_ms,waiting_tasks_count,max_wait_time_ms'),

('sys.dm_os_latch_stats',
 N'latch_class,waiting_requests_count,wait_time_ms,max_wait_time_ms'),

('sys.dm_server_services',
 N'servicename,startup_type_desc,status_desc,process_id,last_startup_time,service_account,'
+ N'is_clustered,cluster_nodename,filename,instant_file_initialization_enabled'),

('sys.dm_server_registry',
 N'registry_key,value_name,value_data'),

('sys.dm_server_audit_status',
 N'audit_id,name,status,status_desc,status_time,audit_file_path,event_session_address'),

('sys.dm_exec_cached_plans',
 N'objtype,size_in_bytes,usecounts'),

('sys.dm_exec_requests',
 N'session_id,wait_type,wait_resource,wait_time,blocking_session_id,database_id,command,status'),

('sys.dm_io_virtual_file_stats',
 N'database_id,file_id,num_of_reads,num_of_writes,io_stall_read_ms,io_stall_write_ms,'
+ N'num_of_bytes_read,num_of_bytes_written'),

('sys.dm_db_log_info',
 N'file_id,vlf_size_mb,vlf_active'),

('sys.dm_xtp_system_memory_consumers',
 N'memory_consumer_type_desc,allocated_bytes,used_bytes'),

('sys.dm_resource_governor_workload_groups',
 N'name,group_id,importance,request_max_memory_grant_percent,request_max_cpu_time_sec,'
+ N'request_memory_grant_timeout_sec,max_dop,group_max_requests,pool_id'),

('sys.resource_governor_workload_groups',
 N'group_id,name,importance'),

('sys.dm_resource_governor_resource_pools',
 N'pool_id,name,min_cpu_percent,max_cpu_percent,min_memory_percent,max_memory_percent'),

('sys.dm_os_cluster_nodes',
 N'NodeName,status,status_description,is_current_owner'),
('sys.dm_os_cluster_properties',
 N'VerboseLogging,SqlDumperDumpFlags,SqlDumperDumpPath,SqlDumperDumpTimeOut,FailureConditionLevel,HealthCheckTimeout'),
('sys.dm_io_cluster_shared_drives',
 N'DriveName'),
('sys.dm_hadr_availability_replica_states',
 N'replica_id,role_desc,operational_state_desc,connected_state_desc,recovery_health_desc,'
+ N'synchronization_health_desc,last_connect_error_number,last_connect_error_description,last_connect_error_timestamp'),
('sys.dm_hadr_database_replica_states',
 N'database_id,replica_id,synchronization_state_desc,synchronization_health_desc,'
+ N'is_suspended,suspend_reason_desc,last_sent_time,last_received_time,last_hardened_time,'
+ N'last_redone_time,log_send_queue_size,log_send_rate,redo_queue_size,redo_rate,'
+ N'end_of_log_lsn,last_commit_time'),
('sys.dm_hadr_cluster',
 N'cluster_name,quorum_type_desc,quorum_state_desc'),
('sys.dm_hadr_cluster_members',
 N'member_name,member_type_desc,member_state_desc,number_of_quorum_votes'),
('sys.dm_hadr_cluster_networks',
 N'member_name,network_subnet_ip,network_subnet_ipv4_mask,network_subnet_prefix_length,is_public,is_ipv4'),

('sys.configurations',
 N'configuration_id,name,value,value_in_use,minimum,maximum,is_dynamic,is_advanced,description'),

('sys.databases',
 N'database_id,name,create_date,owner_sid,state_desc,user_access_desc,is_read_only,'
+ N'is_auto_close_on,is_auto_shrink_on,is_auto_create_stats_on,is_auto_create_stats_incremental_on,'
+ N'is_auto_update_stats_on,is_auto_update_stats_async_on,recovery_model_desc,page_verify_option_desc,'
+ N'compatibility_level,collation_name,snapshot_isolation_state_desc,is_read_committed_snapshot_on,'
+ N'is_trustworthy_on,is_db_chaining_on,is_broker_enabled,is_published,is_subscribed,'
+ N'is_merge_published,is_distributor,is_encrypted,is_query_store_on,is_cdc_enabled,'
+ N'containment_desc,target_recovery_time_in_seconds,delayed_durability_desc,log_reuse_wait_desc,'
+ N'is_parameterization_forced,is_supplemental_logging_enabled,is_memory_optimized_elevate_to_snapshot_on,'
+ N'source_database_id,is_honor_broker_priority_on,is_fulltext_enabled'),

('sys.database_files',
 N'file_id,name,type_desc,physical_name,size,is_percent_growth,growth,max_size,state_desc,is_read_only'),

('sys.master_files',
 N'database_id,file_id,name,type_desc,physical_name,size'),

('sys.database_scoped_configurations',
 N'configuration_id,name,value,value_for_secondary'),

('sys.database_query_store_options',
 N'actual_state_desc,desired_state_desc,readonly_reason,current_storage_size_mb,'
+ N'max_storage_size_mb,query_capture_mode_desc,flush_interval_seconds,'
+ N'interval_length_minutes,stale_query_threshold_days,max_plans_per_query,'
+ N'size_based_cleanup_mode_desc,wait_stats_capture_mode_desc'),

('sys.availability_groups',
 N'name,group_id,resource_id,resource_group_id,failure_condition_level,health_check_timeout,'
+ N'automated_backup_preference_desc,version,basic_features,dtc_support,db_failover,'
+ N'is_distributed,cluster_type_desc,required_synchronized_secondaries_to_commit,sequence_number'),

('sys.availability_replicas',
 N'replica_id,group_id,replica_server_name,endpoint_url,availability_mode_desc,failover_mode_desc,'
+ N'session_timeout,primary_role_allow_connections_desc,secondary_role_allow_connections_desc,'
+ N'backup_priority,seeding_mode_desc,read_only_routing_url,create_date,modify_date'),

('sys.availability_group_listeners',
 N'group_id,dns_name,port,is_conformant,ip_configuration_string_from_cluster'),

('sys.database_mirroring',
 N'database_id,mirroring_state_desc,mirroring_role_desc,mirroring_partner_name,'
+ N'mirroring_partner_instance,mirroring_witness_name,mirroring_witness_state_desc,'
+ N'mirroring_safety_level_desc,mirroring_redo_queue,mirroring_redo_queue_type,mirroring_failover_lsn'),

('sys.database_mirroring_endpoints',
 N'endpoint_id,role_desc,is_encryption_enabled,connection_auth_desc,encryption_algorithm_desc'),

('sys.endpoints',
 N'name,endpoint_id,protocol_desc,type_desc,state_desc'),

('sys.tcp_endpoints',
 N'endpoint_id,port'),

('sys.server_audits',
 N'name,audit_guid,audit_id,type,on_failure_desc,queue_delay,predicate,create_date,modify_date'),

('sys.server_audit_specifications',
 N'server_specification_id,name,audit_guid,is_state_enabled'),

('sys.server_audit_specification_details',
 N'server_specification_id,audit_action_name,class_desc'),

('sys.server_role_members',
 N'role_principal_id,member_principal_id'),

('sys.server_principals',
 N'principal_id,name,type_desc,is_disabled,create_date,modify_date'),

('sys.sql_logins',
 N'name,principal_id,is_disabled,is_policy_checked,is_expiration_checked,password_hash,'
+ N'create_date,modify_date,default_database_name'),

('sys.server_permissions',
 N'grantee_principal_id,permission_name,state_desc,class_desc'),

('sys.servers',
 N'server_id,name,product,provider,data_source,is_remote_login_enabled,is_rpc_out_enabled,'
+ N'is_data_access_enabled,is_collation_compatible,is_remote_proc_transaction_promotion_enabled,'
+ N'modify_date,lazy_schema_validation'),

('sys.dm_database_encryption_keys',
 N'database_id,encryption_state,key_algorithm,key_length,encryptor_thumbprint,encryptor_type,percent_complete'),

('sys.certificates',
 N'name,pvt_key_encryption_type_desc,issuer_name,subject,expiry_date,start_date,thumbprint'),

('sys.traces',                  N'is_default,path'),
('sys.dm_xe_sessions',          N'name,address'),
('sys.dm_xe_session_targets',   N'target_name,event_session_address,target_data'),

('msdb.dbo.backupset',
 N'database_name,backup_finish_date,type,is_copy_only,compressed_backup_size,backup_size,media_set_id'),
('msdb.dbo.backupmediafamily',  N'media_set_id,device_type,physical_device_name'),
('msdb.dbo.suspect_pages',      N'database_id,file_id,page_id,event_type,error_count,last_update_date'),
('msdb.dbo.sysjobs',
 N'job_id,name,enabled,owner_sid,category_id,description,date_created,date_modified,'
+ N'notify_level_email,notify_email_operator_id,delete_level'),
('msdb.dbo.sysjobhistory',
 N'job_id,step_id,step_name,run_status,run_date,run_time,run_duration,server,sql_message_id,sql_severity,message'),
('msdb.dbo.syscategories',      N'category_id,name'),
('msdb.dbo.sysoperators',
 N'id,name,enabled,email_address,last_email_date,last_email_time,'
+ N'weekday_pager_start_time,weekday_pager_end_time,pager_days,category_id'),
('msdb.dbo.sysalerts',
 N'id,name,enabled,message_id,severity,database_name,performance_condition,'
+ N'event_description_keyword,delay_between_responses,occurrence_count,'
+ N'last_occurrence_date,last_occurrence_time,count_reset_date,count_reset_time,'
+ N'include_event_description,notification_message'),
('msdb.dbo.sysnotifications',   N'alert_id,operator_id'),
('msdb.dbo.sysmail_profile',    N'profile_id,name,description'),
('msdb.dbo.sysmail_profileaccount', N'profile_id,account_id,is_default,sequence_number'),
('msdb.dbo.sysmail_account',    N'account_id,name,email_address,display_name,replyto_address'),
('msdb.dbo.sysmail_server',     N'account_id,servername,servertype,port,enable_ssl'),
('msdb.dbo.log_shipping_primary_databases',
 N'primary_id,primary_database,backup_directory,backup_share,backup_retention_period,'
+ N'backup_job_id,last_backup_file,last_backup_date,history_retention_period'),
('msdb.dbo.log_shipping_secondary',
 N'secondary_id,primary_server,primary_database,backup_source_directory,backup_destination_directory'),
('msdb.dbo.log_shipping_secondary_databases',
 N'secondary_id,secondary_database,restore_delay,restore_all,disconnect_users,block_size,'
+ N'restore_mode,last_copied_file,last_copied_date,last_restored_file,last_restored_date,last_restored_latency');

IF OBJECT_ID('tempdb..#actual') IS NOT NULL DROP TABLE #actual;
CREATE TABLE #actual (
    object_full sysname,
    col_name    sysname,
    PRIMARY KEY (object_full, col_name)
);

INSERT INTO #actual (object_full, col_name)
SELECT
    N'sys.' + o.name,
    c.name
FROM master.sys.all_objects o
JOIN master.sys.all_columns c ON o.object_id = c.object_id
WHERE SCHEMA_NAME(o.schema_id) = N'sys';

INSERT INTO #actual (object_full, col_name)
SELECT
    N'msdb.dbo.' + o.name,
    c.name
FROM msdb.sys.all_objects o
JOIN msdb.sys.all_columns c ON o.object_id = c.object_id
WHERE SCHEMA_NAME(o.schema_id) = N'dbo';

;WITH expected AS (
    SELECT
        i.object_name,
        LTRIM(RTRIM(s.value)) AS needed_col
    FROM @inventory i
    CROSS APPLY STRING_SPLIT(i.needed_cols, N',') s
)
SELECT
    [section]      = N'01 - Missing columns (any row here is a real problem)',
    [object]       = e.object_name,
    [missing_col]  = e.needed_col
FROM expected e
WHERE NOT EXISTS (
    SELECT 1 FROM #actual a
     WHERE a.object_full = e.object_name
       AND a.col_name    = e.needed_col
)
ORDER BY e.object_name, e.needed_col;

;WITH expected AS (
    SELECT
        i.object_name,
        LTRIM(RTRIM(s.value)) AS needed_col
    FROM @inventory i
    CROSS APPLY STRING_SPLIT(i.needed_cols, N',') s
),
agg AS (
    SELECT
        e.object_name,
        needed_count = COUNT(*),
        missing_count = SUM(CASE WHEN a.col_name IS NULL THEN 1 ELSE 0 END)
    FROM expected e
    LEFT JOIN #actual a
        ON a.object_full = e.object_name
       AND a.col_name    = e.needed_col
    GROUP BY e.object_name
)
SELECT
    [section]      = N'02 - Per-object summary',
    [object]       = agg.object_name,
    [object_exists] = CASE WHEN EXISTS (SELECT 1 FROM #actual a WHERE a.object_full = agg.object_name)
                          THEN 1 ELSE 0 END,
    [needed_cols]  = agg.needed_count,
    [missing_cols] = agg.missing_count,
    [status]       = CASE
                        WHEN NOT EXISTS (SELECT 1 FROM #actual a WHERE a.object_full = agg.object_name)
                            THEN N'OBJECT NOT FOUND'
                        WHEN agg.missing_count > 0 THEN N'MISSING COLUMNS'
                        ELSE N'OK'
                     END
FROM agg
ORDER BY [status] DESC, agg.object_name;

SELECT
    [section]                = N'03 - Current login permissions',
    [login]                  = SUSER_SNAME(),
    [is_sysadmin]            = IS_SRVROLEMEMBER('sysadmin'),
    [is_serveradmin]         = IS_SRVROLEMEMBER('serveradmin'),
    [is_securityadmin]       = IS_SRVROLEMEMBER('securityadmin'),
    [has_view_server_state]  = HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE'),
    [has_view_any_definition]= HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW ANY DEFINITION'),
    [has_view_server_perf_state] = HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER PERFORMANCE STATE');

IF DB_ID('msdb') IS NOT NULL
BEGIN
    DECLARE @msdb_perms TABLE (role_name sysname);
    INSERT INTO @msdb_perms (role_name)
    EXEC ('USE msdb;
           SELECT r.name
             FROM sys.database_role_members rm
             JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
             JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
            WHERE m.name = ORIGINAL_LOGIN()
               OR m.sid = SUSER_SID();');

    SELECT
        [section]            = N'03b - msdb role membership for current login',
        [role_name]          = role_name
    FROM @msdb_perms
    ORDER BY role_name;

    IF NOT EXISTS (SELECT 1 FROM @msdb_perms)
        SELECT [section] = N'03b - msdb role membership for current login',
               [role_name] = N'(none beyond public; this explains the OBJECT NOT FOUND rows above)';
END
