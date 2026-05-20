/* ============================================================================
   02_sp_configure_and_advanced.sql
   ----------------------------------------------------------------------------
   Captures: full sys.configurations output with non-default flags and brief
            best-practice annotations for the most consequential settings.

   Target  : SQL Server 2019, physical host, SAN, A-P cluster
   Safety  : Read-only. No call to sp_configure, no RECONFIGURE.
   Output  : 2 result sets:
              (a) every configurable option with current/run/default values
              (b) curated "consequential settings" view with guidance text
   ============================================================================ */
SET NOCOUNT ON;

------------------------------------------------------------------------------
-- 1. Full sys.configurations with non-default flag
--    is_value_default = 1 only when value AND value_in_use match the engine
--    default. We compute it from the documented default list below.
------------------------------------------------------------------------------
;WITH defaults AS (
    -- SQL Server 2019 documented defaults. (Curated, not exhaustive; any
    -- option not listed will simply show as default_value = NULL.)
    SELECT * FROM (VALUES
        ('access check cache bucket count',              0),
        ('access check cache quota',                     0),
        ('Ad Hoc Distributed Queries',                   0),
        ('ADR cleaner retry timeout (min)',             15),
        ('ADR Preallocation Factor',                     4),
        ('affinity I/O mask',                            0),
        ('affinity mask',                                0),
        ('affinity64 I/O mask',                          0),
        ('affinity64 mask',                              0),
        ('Agent XPs',                                    0),
        ('allow filesystem enumeration',                 1),
        ('allow polybase export',                        0),
        ('allow updates',                                0),
        ('automatic soft-NUMA disabled',                 0),
        ('backup checksum default',                      0),
        ('backup compression default',                   0),
        ('blocked process threshold (s)',                0),
        ('c2 audit mode',                                0),
        ('clr enabled',                                  0),
        ('clr strict security',                          1),
        ('common criteria compliance enabled',           0),
        ('contained database authentication',            0),
        ('cost threshold for parallelism',               5),
        ('cross db ownership chaining',                  0),
        ('cursor threshold',                            -1),
        ('Database Mail XPs',                            0),
        ('default full-text language',                1033),
        ('default language',                             0),
        ('default trace enabled',                        1),
        ('disallow results from triggers',               0),
        ('EKM provider enabled',                         0),
        ('external scripts enabled',                     0),
        ('filestream access level',                      0),
        ('fill factor (%)',                              0),
        ('ft crawl bandwidth (max)',                   100),
        ('ft crawl bandwidth (min)',                     0),
        ('ft notify bandwidth (max)',                  100),
        ('ft notify bandwidth (min)',                    0),
        ('hadoop connectivity',                          0),
        ('index create memory (KB)',                     0),
        ('in-doubt xact resolution',                     0),
        ('lightweight pooling',                          0),
        ('locks',                                        0),
        ('max degree of parallelism',                    0),
        ('max full-text crawl range',                    4),
        ('max server memory (MB)',                  2147483647),
        ('max text repl size (B)',                65536),
        ('max worker threads',                           0),
        ('media retention',                              0),
        ('min memory per query (KB)',                 1024),
        ('min server memory (MB)',                       0),
        ('nested triggers',                              1),
        ('network packet size (B)',                   4096),
        ('Ole Automation Procedures',                    0),
        ('open objects',                                 0),
        ('optimize for ad hoc workloads',                0),
        ('PH timeout (s)',                              60),
        ('polybase enabled',                             0),
        ('polybase network encryption',                  1),
        ('precompute rank',                              0),
        ('priority boost',                               0),
        ('query governor cost limit',                    0),
        ('query wait (s)',                              -1),
        ('recovery interval (min)',                      0),
        ('remote access',                                1),
        ('remote admin connections',                     0),
        ('remote data archive',                          0),
        ('remote login timeout (s)',                    10),
        ('remote proc trans',                            0),
        ('remote query timeout (s)',                   600),
        ('Replication XPs',                              0),
        ('scan for startup procs',                       0),
        ('server trigger recursion',                     1),
        ('set working set size',                         0),
        ('show advanced options',                        0),
        ('SMO and DMO XPs',                              1),
        ('SQL Mail XPs',                                 0),
        ('tempdb metadata memory-optimized',             0),
        ('transform noise words',                        0),
        ('two digit year cutoff',                     2049),
        ('user connections',                             0),
        ('user options',                                 0),
        ('xp_cmdshell',                                  0)
    ) d(name, default_value)
)
SELECT
    [section]            = N'01 - sys.configurations',
    c.configuration_id,
    c.name,
    c.value,
    c.value_in_use,
    [default_value]      = d.default_value,
    [is_default]         = CASE
                              WHEN d.default_value IS NULL THEN NULL
                              WHEN c.value_in_use = d.default_value THEN 1
                              ELSE 0
                           END,
    [pending_reconfigure]= CASE WHEN c.value <> c.value_in_use THEN 1 ELSE 0 END,
    c.minimum,
    c.maximum,
    c.is_dynamic,
    c.is_advanced,
    c.description
FROM sys.configurations c
LEFT JOIN defaults d
    ON LOWER(c.name) = LOWER(d.name)
ORDER BY c.name;

------------------------------------------------------------------------------
-- 2. Curated "consequential settings" view with guidance
--    These are the settings that, in our FCI-on-SAN-physical context, most
--    often drive performance, stability, or security outcomes.
------------------------------------------------------------------------------
;WITH cfg AS (
    SELECT name, value_in_use FROM sys.configurations
)
SELECT
    [section]    = N'02 - Consequential settings',
    [setting]    = s.name,
    [value_in_use] = c.value_in_use,
    [recommended_for_this_topology] = s.recommended,
    [why_it_matters] = s.why
FROM (VALUES
    ('max server memory (MB)',
     N'Leave headroom for OS + plan cache; on FCI leave enough so the passive node still runs the OS when the instance fails over.',
     N'Set to total RAM minus ~4-8 GB for OS and SQL components. Default 2147483647 means "take it all".'),
    ('min server memory (MB)',
     N'Often 0 is fine on dedicated boxes; raise it when other services compete or to avoid trimming on memory pressure.',
     N'Floor SQL will not release memory below.'),
    ('max degree of parallelism',
     N'Set to logical-cores-per-NUMA-node, capped at 8 for OLTP, 16 for mixed. Never leave at 0 on multi-socket physical hardware.',
     N'0 = unlimited; can cause CXPACKET storms on multi-NUMA hosts.'),
    ('cost threshold for parallelism',
     N'Raise from 5 to 25-50 on OLTP. Reduces parallelism for trivial queries.',
     N'Threshold below which plans stay serial.'),
    ('optimize for ad hoc workloads',
     N'Enable (1). Avoids bloating plan cache with single-use plans.',
     N'Stores only a stub on first compile, full plan on second.'),
    ('backup compression default',
     N'Enable (1) for FCI backups - CPU is cheap, SAN bandwidth and storage are not.',
     N'Default for newly created backups.'),
    ('remote admin connections',
     N'Enable (1). Lets you reach the DAC remotely from the listener when SQL is wedged.',
     N'DAC-over-the-network switch.'),
    ('default trace enabled',
     N'Leave on (1). Many tools depend on it; the overhead is negligible.',
     N'Always-on lightweight server-side trace.'),
    ('clr enabled',
     N'Disable (0) unless you actively use SQLCLR. If on, "clr strict security" must be 1.',
     N'Allows execution of managed assemblies.'),
    ('clr strict security',
     N'Must be 1 if CLR is enabled in 2017+.',
     N'Enforces that all assemblies be signed.'),
    ('Ad Hoc Distributed Queries',
     N'Disable (0) unless OPENROWSET / OPENDATASOURCE are needed.',
     N'Enables ad hoc OPENROWSET.'),
    ('Ole Automation Procedures',
     N'Disable (0). Legacy attack surface.',
     N'Enables sp_OACreate etc.'),
    ('xp_cmdshell',
     N'Disable (0). Re-enable only briefly for specific maintenance and audit it.',
     N'Shell escape from T-SQL.'),
    ('Agent XPs',
     N'1 on instances where SQL Agent is used (almost always).',
     N'Enables Agent extended procs.'),
    ('Database Mail XPs',
     N'1 if Database Mail is configured (recommended for alerting).',
     N'Enables Database Mail XPs.'),
    ('blocked process threshold (s)',
     N'Set to 5 (or 15) so SQL emits blocked-process reports. Captured by default trace and Extended Events.',
     N'Seconds after which a blocked session triggers a report.'),
    ('priority boost',
     N'Must be 0. Deprecated and harmful.',
     N'Raises SQL process priority above Windows kernel threads.'),
    ('lightweight pooling',
     N'Must be 0. Fiber mode is incompatible with most modern features.',
     N'Switches scheduler to fibers.'),
    ('contained database authentication',
     N'Enable only if you use contained DBs (handy for AG portability).',
     N'Required for partial containment.'),
    ('cross db ownership chaining',
     N'Disable (0). Enable per-DB only when truly required.',
     N'Allows ownership chains to cross DB boundaries.'),
    ('automatic soft-NUMA disabled',
     N'Leave 0. Auto-soft-NUMA helps on high-core-count physical hosts.',
     N'1 disables the automatic soft-NUMA partitioning introduced in 2016.'),
    ('tempdb metadata memory-optimized',
     N'2019+: enable (1) if your workload sees PAGELATCH on tempdb system tables. Requires restart.',
     N'Moves tempdb system catalog into Hekaton memory-optimized tables.'),
    ('show advanced options',
     N'Operational toggle - 1 while configuring, but value at audit time does not matter.',
     N'Gates visibility of advanced settings to sp_configure.')
) s(name, recommended, why)
JOIN cfg c ON LOWER(c.name) = LOWER(s.name)
ORDER BY s.name;
