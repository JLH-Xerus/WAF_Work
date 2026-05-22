:setvar AuditFolder "."

PRINT N'==================================================================';
PRINT N'01 - Instance identity and hardware';
PRINT N'==================================================================';
:r $(AuditFolder)\01_instance_identity_and_hardware.sql

PRINT N'==================================================================';
PRINT N'02 - sp_configure and advanced options';
PRINT N'==================================================================';
:r $(AuditFolder)\02_sp_configure_and_advanced.sql

PRINT N'==================================================================';
PRINT N'03 - Memory, CPU, parallelism';
PRINT N'==================================================================';
:r $(AuditFolder)\03_memory_cpu_parallelism.sql

PRINT N'==================================================================';
PRINT N'04 - tempdb configuration';
PRINT N'==================================================================';
:r $(AuditFolder)\04_tempdb_configuration.sql

PRINT N'==================================================================';
PRINT N'05 - Database configuration';
PRINT N'==================================================================';
:r $(AuditFolder)\05_database_configuration.sql

PRINT N'==================================================================';
PRINT N'06 - Database files and VLFs';
PRINT N'==================================================================';
:r $(AuditFolder)\06_database_files_and_vlfs.sql

PRINT N'==================================================================';
PRINT N'07 - Storage and volumes';
PRINT N'==================================================================';
:r $(AuditFolder)\07_storage_and_volumes.sql

PRINT N'==================================================================';
PRINT N'08 - HA/DR detection';
PRINT N'==================================================================';
:r $(AuditFolder)\08_ha_dr_detection.sql

PRINT N'==================================================================';
PRINT N'09 - Backup posture';
PRINT N'==================================================================';
:r $(AuditFolder)\09_backup_posture.sql

PRINT N'==================================================================';
PRINT N'10 - Security configuration';
PRINT N'==================================================================';
:r $(AuditFolder)\10_security_configuration.sql

PRINT N'==================================================================';
PRINT N'11 - Query Store and perf settings';
PRINT N'==================================================================';
:r $(AuditFolder)\11_query_store_and_perf.sql

PRINT N'==================================================================';
PRINT N'12 - SQL Agent configuration';
PRINT N'==================================================================';
:r $(AuditFolder)\12_sql_agent_configuration.sql

PRINT N'==================================================================';
PRINT N'13 - Wait stats baseline';
PRINT N'==================================================================';
:r $(AuditFolder)\13_wait_stats_baseline.sql
