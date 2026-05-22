SET NOCOUNT ON;

SELECT
    [section]                = N'01 - SQL Agent service',
    [servicename]            = servicename,
    [startup_type_desc]      = startup_type_desc,
    [status_desc]            = status_desc,
    [process_id]             = process_id,
    [last_startup_time]      = last_startup_time,
    [service_account]        = service_account,
    [is_clustered]           = is_clustered,
    [cluster_nodename]       = cluster_nodename
FROM sys.dm_server_services
WHERE servicename LIKE N'%Agent%';

SELECT
    [section]                = N'02 - Agent-related sp_configure',
    [name]                   = name,
    [value_in_use]           = value_in_use,
    [recommendation] = CASE name
        WHEN N'Agent XPs' THEN N'1 - required for SQL Agent to function'
        WHEN N'Database Mail XPs' THEN N'1 - required for Database Mail (which Agent uses for notifications)'
        WHEN N'SQL Mail XPs' THEN N'0 - deprecated, do not enable'
        WHEN N'Replication XPs' THEN N'1 if replication is in use, 0 otherwise'
    END
FROM sys.configurations
WHERE name IN (N'Agent XPs', N'Database Mail XPs', N'SQL Mail XPs', N'Replication XPs');

SELECT
    [section]                = N'03 - SQL Agent properties',
    [registry_key]           = registry_key,
    [value_name]             = value_name,
    [value_data]             = value_data
FROM sys.dm_server_registry
WHERE registry_key LIKE N'%SQLServerAgent%'
ORDER BY registry_key, value_name;

;WITH last_run AS (
    SELECT
        jh.job_id,
        MAX(msdb.dbo.agent_datetime(jh.run_date, jh.run_time)) AS last_run_dt,
        MAX(jh.run_status) AS last_run_status
    FROM msdb.dbo.sysjobhistory jh
    WHERE jh.step_id = 0
    GROUP BY jh.job_id
)
SELECT
    [section]                = N'04 - SQL Agent jobs',
    [job_id]                 = j.job_id,
    [name]                   = j.name,
    [enabled]                = j.enabled,
    [owner_name]             = SUSER_SNAME(j.owner_sid),
    [owner_is_orphan]        = CASE WHEN SUSER_SNAME(j.owner_sid) IS NULL THEN 1 ELSE 0 END,
    [category]               = c.name,
    [description]            = j.description,
    [date_created]           = j.date_created,
    [date_modified]          = j.date_modified,
    [last_run_dt]            = lr.last_run_dt,
    [last_run_status]        = lr.last_run_status,
    [last_run_status_desc]   = CASE lr.last_run_status
                                  WHEN 0 THEN N'Failed'
                                  WHEN 1 THEN N'Succeeded'
                                  WHEN 2 THEN N'Retry'
                                  WHEN 3 THEN N'Canceled'
                                  WHEN 4 THEN N'In progress'
                               END,
    [notify_level_email]     = j.notify_level_email,
    [notify_email_operator]  = op.name,
    [delete_level]           = j.delete_level
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
LEFT JOIN last_run lr              ON j.job_id = lr.job_id
LEFT JOIN msdb.dbo.sysoperators op ON j.notify_email_operator_id = op.id
ORDER BY j.name;

SELECT TOP (200)
    [section]                = N'05 - Failed job steps (last 7 days)',
    [job_name]               = j.name,
    [step_id]                = jh.step_id,
    [step_name]              = jh.step_name,
    [run_dt]                 = msdb.dbo.agent_datetime(jh.run_date, jh.run_time),
    [run_duration_hhmmss]    = STUFF(STUFF(RIGHT('000000' + CAST(jh.run_duration AS varchar(6)), 6), 5, 0, ':'), 3, 0, ':'),
    [server]                 = jh.server,
    [sql_message_id]         = jh.sql_message_id,
    [sql_severity]           = jh.sql_severity,
    [message]                = LEFT(jh.message, 4000)
FROM msdb.dbo.sysjobhistory jh
JOIN msdb.dbo.sysjobs j ON jh.job_id = j.job_id
WHERE jh.run_status = 0
  AND CONVERT(date, CAST(jh.run_date AS varchar(8)), 112) > DATEADD(DAY, -7, SYSDATETIME())
ORDER BY run_dt DESC;

SELECT
    [section]                = N'06 - Operators',
    [name]                   = name,
    [enabled]                = enabled,
    [email_address]          = email_address,
    [last_email_date]        = last_email_date,
    [last_email_time]        = last_email_time,
    [weekday_pager_start_time] = weekday_pager_start_time,
    [weekday_pager_end_time]   = weekday_pager_end_time,
    [pager_days]             = pager_days,
    [category_id]            = category_id
FROM msdb.dbo.sysoperators;

SELECT
    [section]                = N'07 - Alerts',
    [name]                   = a.name,
    [enabled]                = a.enabled,
    [message_id]             = a.message_id,
    [severity]               = a.severity,
    [database_name]          = a.database_name,
    [performance_condition]  = a.performance_condition,
    [event_description_keyword] = a.event_description_keyword,
    [delay_between_responses] = a.delay_between_responses,
    [occurrence_count]       = a.occurrence_count,
    [last_occurrence_date]   = a.last_occurrence_date,
    [last_occurrence_time]   = a.last_occurrence_time,
    [count_reset_date]       = a.count_reset_date,
    [count_reset_time]       = a.count_reset_time,
    [include_event_description] = a.include_event_description,
    [notification_message]   = a.notification_message,
    [notify_to_operator]     = op.name
FROM msdb.dbo.sysalerts a
LEFT JOIN msdb.dbo.sysnotifications n ON a.id = n.alert_id
LEFT JOIN msdb.dbo.sysoperators op    ON n.operator_id = op.id;

SELECT
    [section]                = N'08 - Database Mail profiles',
    [profile_id]             = p.profile_id,
    [profile_name]           = p.name,
    [description]            = p.description,
    [account_id]             = a.account_id,
    [account_name]           = a.name,
    [email_address]          = a.email_address,
    [display_name]           = a.display_name,
    [replyto_address]        = a.replyto_address,
    [mailserver_name]        = s.servername,
    [mailserver_type]        = s.servertype,
    [port]                   = s.port,
    [enable_ssl]             = s.enable_ssl,
    [is_default]             = pa.is_default,
    [sequence_number]        = pa.sequence_number
FROM msdb.dbo.sysmail_profile p
LEFT JOIN msdb.dbo.sysmail_profileaccount pa ON p.profile_id = pa.profile_id
LEFT JOIN msdb.dbo.sysmail_account a         ON pa.account_id = a.account_id
LEFT JOIN msdb.dbo.sysmail_server s          ON a.account_id = s.account_id
ORDER BY p.name, pa.sequence_number;
