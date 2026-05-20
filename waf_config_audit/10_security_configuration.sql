/* ============================================================================
   10_security_configuration.sql
   ----------------------------------------------------------------------------
   Captures: the security surface of the instance.
            Authentication mode, sa login state, sysadmin membership, weak
            password policies, server-level audits, force encryption,
            transparent data encryption status per DB, server permissions.

   Target  : SQL Server 2019, physical host, SAN, A-P cluster
   Safety  : Read-only.
   Output  : 9 result sets.
   ============================================================================ */
SET NOCOUNT ON;

------------------------------------------------------------------------------
-- 1. Authentication mode and force-encryption
------------------------------------------------------------------------------
SELECT
    [section]                          = N'01 - Authentication and encryption',
    [IsIntegratedSecurityOnly]         = CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS int),  -- 1 = Windows only, 0 = Mixed
    [auth_mode]                        = CASE CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS int)
                                              WHEN 1 THEN N'Windows Authentication only'
                                              WHEN 0 THEN N'Mixed Mode (Windows + SQL)'
                                              ELSE N'unknown'
                                         END,
    [force_encryption]                 = (SELECT TOP (1) value_data
                                            FROM sys.dm_server_registry
                                           WHERE value_name = 'ForceEncryption'
                                           ORDER BY registry_key),
    [hide_instance]                    = (SELECT TOP (1) value_data
                                            FROM sys.dm_server_registry
                                           WHERE value_name = 'HideInstance'
                                           ORDER BY registry_key),
    [tcp_dynamic_ports]                = (SELECT TOP (1) value_data
                                            FROM sys.dm_server_registry
                                           WHERE value_name = 'TcpDynamicPorts'
                                           ORDER BY registry_key),
    [tcp_static_port]                  = (SELECT TOP (1) value_data
                                            FROM sys.dm_server_registry
                                           WHERE value_name = 'TcpPort'
                                           ORDER BY registry_key);

------------------------------------------------------------------------------
-- 2. sa login state and recent activity
------------------------------------------------------------------------------
SELECT
    [section]                = N'02 - sa login',
    [name]                   = name,
    [is_disabled]            = is_disabled,
    [is_policy_checked]      = is_policy_checked,
    [is_expiration_checked]  = is_expiration_checked,
    [create_date]            = create_date,
    [modify_date]            = modify_date,
    [default_database_name]  = default_database_name,
    [posture] = CASE
                  WHEN is_disabled = 0 AND principal_id = 1 THEN N'sa is ENABLED - consider disabling and using a renamed sysadmin'
                  ELSE N'OK'
                END
FROM sys.sql_logins
WHERE principal_id = 1;       -- sa is always principal_id = 1

------------------------------------------------------------------------------
-- 3. sysadmin and other powerful server-role members
------------------------------------------------------------------------------
SELECT
    [section]                = N'03 - Server-role membership',
    [role_name]              = role.name,
    [member_name]            = member.name,
    [member_type_desc]       = member.type_desc,
    [is_disabled]            = member.is_disabled,
    [create_date]            = member.create_date,
    [modify_date]            = member.modify_date
FROM sys.server_role_members rm
JOIN sys.server_principals role   ON rm.role_principal_id   = role.principal_id
JOIN sys.server_principals member ON rm.member_principal_id = member.principal_id
WHERE role.name IN ('sysadmin','securityadmin','serveradmin','setupadmin','processadmin','diskadmin','dbcreator','bulkadmin')
ORDER BY role.name, member.name;

------------------------------------------------------------------------------
-- 4. SQL logins with weak password policy / expiration not enforced
------------------------------------------------------------------------------
SELECT
    [section]                = N'04 - SQL logins with weak password policy',
    [name]                   = name,
    [is_disabled]            = is_disabled,
    [is_policy_checked]      = is_policy_checked,
    [is_expiration_checked]  = is_expiration_checked,
    [create_date]            = create_date,
    [modify_date]            = modify_date,
    [findings] = CONCAT(
        CASE WHEN is_policy_checked = 0     THEN N'[CHECK_POLICY OFF] '     ELSE N'' END,
        CASE WHEN is_expiration_checked = 0 THEN N'[CHECK_EXPIRATION OFF] ' ELSE N'' END,
        CASE WHEN PWDCOMPARE(N'', password_hash) = 1 THEN N'[blank password] ' ELSE N'' END,
        CASE WHEN PWDCOMPARE(name, password_hash) = 1 THEN N'[password = login name] ' ELSE N'' END,
        CASE WHEN PWDCOMPARE(N'password', password_hash) = 1 THEN N'[password = "password"] ' ELSE N'' END
    )
FROM sys.sql_logins
WHERE name NOT LIKE '##%'
ORDER BY name;

------------------------------------------------------------------------------
-- 5. Server-level audits (SQL Server Audit objects)
------------------------------------------------------------------------------
SELECT
    [section]                = N'05 - SQL Server Audits',
    [audit_name]             = a.name,
    [audit_id]               = a.audit_id,
    [type]                   = a.type,
    [on_failure_desc]        = a.on_failure_desc,
    [queue_delay]            = a.queue_delay,
    [predicate]              = a.predicate,
    [is_state_enabled]       = sa.is_state_enabled,
    [create_date]            = a.create_date,
    [modify_date]            = a.modify_date
FROM sys.server_audits a
LEFT JOIN sys.dm_server_audit_status sa ON a.audit_id = sa.audit_id;

SELECT
    [section]                = N'05b - Server Audit specifications',
    [audit_name]             = a.name,
    [spec_name]              = sas.name,
    [is_state_enabled]       = sas.is_state_enabled,
    [audit_action_name]      = sasd.audit_action_name,
    [class_desc]             = sasd.class_desc
FROM sys.server_audit_specifications sas
JOIN sys.server_audits a
  ON sas.audit_guid = a.audit_guid
LEFT JOIN sys.server_audit_specification_details sasd
  ON sas.server_specification_id = sasd.server_specification_id;

------------------------------------------------------------------------------
-- 6. TDE - certificates and encrypted databases
------------------------------------------------------------------------------
SELECT
    [section]                = N'06 - TDE encrypted databases',
    [database_name]          = DB_NAME(database_id),
    [encryption_state]       = encryption_state,
    [encryption_state_desc]  = CASE encryption_state
                                  WHEN 0 THEN N'No DEK'
                                  WHEN 1 THEN N'Unencrypted'
                                  WHEN 2 THEN N'Encryption in progress'
                                  WHEN 3 THEN N'Encrypted'
                                  WHEN 4 THEN N'Key change in progress'
                                  WHEN 5 THEN N'Decryption in progress'
                                  WHEN 6 THEN N'Protection change in progress'
                               END,
    [key_algorithm]          = key_algorithm,
    [key_length]             = key_length,
    [encryptor_thumbprint]   = encryptor_thumbprint,
    [encryptor_type]         = encryptor_type,
    [percent_complete]       = percent_complete
FROM sys.dm_database_encryption_keys;

SELECT
    [section]                = N'06b - Certificates in master',
    [name]                   = name,
    [pvt_key_encryption_type_desc] = pvt_key_encryption_type_desc,
    [issuer_name]            = issuer_name,
    [subject]                = subject,
    [expiry_date]            = expiry_date,
    [start_date]             = start_date,
    [thumbprint]             = thumbprint
FROM master.sys.certificates;

------------------------------------------------------------------------------
-- 7. Cross-DB ownership chaining (per-DB level)
------------------------------------------------------------------------------
SELECT
    [section]                = N'07 - Cross-DB ownership chaining enabled',
    [database_name]          = name,
    [is_db_chaining_on]      = is_db_chaining_on
FROM sys.databases
WHERE is_db_chaining_on = 1;

------------------------------------------------------------------------------
-- 8. Server permissions granted to non-default principals
------------------------------------------------------------------------------
SELECT
    [section]                = N'08 - Notable server-level permissions',
    [grantee_name]           = p.name,
    [grantee_type_desc]      = p.type_desc,
    [permission_name]        = sp.permission_name,
    [state_desc]             = sp.state_desc,
    [class_desc]             = sp.class_desc
FROM sys.server_permissions sp
JOIN sys.server_principals p ON sp.grantee_principal_id = p.principal_id
WHERE sp.permission_name IN (
        'CONTROL SERVER','ALTER ANY LOGIN','ALTER ANY SERVER ROLE','ALTER SETTINGS',
        'ALTER TRACE','UNSAFE ASSEMBLY','EXTERNAL ACCESS ASSEMBLY','ALTER ANY ENDPOINT',
        'ALTER ANY AVAILABILITY GROUP','VIEW ANY DEFINITION','VIEW SERVER STATE',
        'IMPERSONATE ANY LOGIN','CONNECT SQL'
  )
  AND p.name NOT IN ('public','sa','NT SERVICE\MSSQLSERVER','NT SERVICE\SQLSERVERAGENT',
                     'NT AUTHORITY\SYSTEM','##MS_PolicyEventProcessingLogin##','##MS_PolicyTsqlExecutionLogin##')
  AND p.name NOT LIKE '##%'
ORDER BY p.name, sp.permission_name;

------------------------------------------------------------------------------
-- 9. Linked servers - often a forgotten attack surface
------------------------------------------------------------------------------
SELECT
    [section]                = N'09 - Linked servers',
    [name]                   = s.name,
    [product]                = s.product,
    [provider]               = s.provider,
    [data_source]            = s.data_source,
    [is_remote_login_enabled]= s.is_remote_login_enabled,
    [is_rpc_out_enabled]     = s.is_rpc_out_enabled,
    [is_data_access_enabled] = s.is_data_access_enabled,
    [is_collation_compatible]= s.is_collation_compatible,
    [is_promotion_of_distributed_txns_enabled] = s.is_promotion_of_distributed_transactions_enabled,
    [modify_date]            = s.modify_date,
    [lazy_schema_validation] = s.lazy_schema_validation
FROM sys.servers s
WHERE s.server_id > 0;        -- 0 = the local server, not a linked server
