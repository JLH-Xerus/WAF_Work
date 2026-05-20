/* ============================================================================
   13_wait_stats_baseline.sql
   ----------------------------------------------------------------------------
   Captures: a one-shot wait-stats baseline from instance start. This is NOT
            a delta capture - it's the cumulative shape since restart, which
            is enough to identify the dominant wait categories on an
            instance that has been up for a while.

   Use it to:
       - Spot dominant waits (CPU? IO? lock? AG sync? memory grants?)
       - Identify NUMA imbalance or scheduler pressure (SOS_SCHEDULER_YIELD)
       - Confirm storage stalls (PAGEIOLATCH_* and WRITELOG)

   Target  : SQL Server 2019, physical host, SAN, A-P cluster
   Safety  : Read-only.
   Output  : 4 result sets.
   ============================================================================ */
SET NOCOUNT ON;

------------------------------------------------------------------------------
-- 1. Instance uptime context (so the wait totals can be interpreted)
------------------------------------------------------------------------------
SELECT
    [section]                = N'01 - Wait stats window',
    [sqlserver_start_time]   = sqlserver_start_time,
    [uptime_days]            = DATEDIFF(DAY, sqlserver_start_time, SYSDATETIME()),
    [uptime_hours]           = DATEDIFF(HOUR, sqlserver_start_time, SYSDATETIME()),
    [comment]                = N'All wait totals below are cumulative since this time.'
FROM sys.dm_os_sys_info;

------------------------------------------------------------------------------
-- 2. Top waits (filtering out benign/idle waits)
------------------------------------------------------------------------------
;WITH waits AS (
    SELECT
        wait_type,
        wait_time_ms,
        signal_wait_time_ms,
        waiting_tasks_count,
        max_wait_time_ms,
        100.0 * wait_time_ms /
            NULLIF(SUM(wait_time_ms) OVER (), 0) AS pct_of_total_waits
    FROM sys.dm_os_wait_stats
    WHERE waiting_tasks_count > 0
      AND wait_type NOT IN (
        -- Benign/idle waits to exclude (Paul Randal's list, trimmed)
        N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP',
        N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE',
        N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE',
        N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE',
        N'DBMIRRORING_CMD', N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE',
        N'EXECSYNC', N'FSAGENT', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX',
        N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT',
        N'HADR_NOTIFICATION_DEQUEUE', N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE',
        N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', N'MEMORY_ALLOCATION_EXT',
        N'ONDEMAND_TASK_QUEUE', N'PARALLEL_REDO_WORKER_WAIT_WORK',
        N'PREEMPTIVE_HADR_LEASE_MECHANISM', N'PREEMPTIVE_SP_SERVER_DIAGNOSTICS',
        N'PREEMPTIVE_OS_LIBRARYOPS', N'PREEMPTIVE_OS_COMOPS', N'PREEMPTIVE_OS_CRYPTOPS',
        N'PREEMPTIVE_OS_PIPEOPS', N'PREEMPTIVE_OS_AUTHENTICATIONOPS',
        N'PREEMPTIVE_OS_GENERICOPS', N'PREEMPTIVE_OS_VERIFYTRUST', N'PREEMPTIVE_OS_FILEOPS',
        N'PREEMPTIVE_OS_DEVICEOPS', N'PREEMPTIVE_OS_QUERYREGISTRY', N'PREEMPTIVE_OS_WRITEFILE',
        N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PWAIT_DIRECTLOGCONSUMER_GETNEXT',
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_ASYNC_QUEUE',
        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'QDS_SHUTDOWN_QUEUE',
        N'REDO_THREAD_PENDING_WORK', N'REQUEST_FOR_DEADLOCK_SEARCH', N'RESOURCE_QUEUE',
        N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP', N'SLEEP_DCOMSTARTUP',
        N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY', N'SLEEP_MASTERUPGRADED',
        N'SLEEP_MSDBSTARTUP', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK', N'SLEEP_TEMPDBSTARTUP',
        N'SNI_HTTP_ACCEPT', N'SOS_WORK_DISPATCHER', N'SP_SERVER_DIAGNOSTICS_SLEEP',
        N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
        N'SQLTRACE_WAIT_ENTRIES', N'WAIT_FOR_RESULTS', N'WAITFOR', N'WAITFOR_TASKSHUTDOWN',
        N'WAIT_XTP_RECOVERY', N'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
        N'WAIT_XTP_CKPT_CLOSE', N'XE_DISPATCHER_JOIN', N'XE_DISPATCHER_WAIT',
        N'XE_TIMER_EVENT'
      )
)
SELECT TOP (40)
    [section]                = N'02 - Top non-idle waits',
    [wait_type]              = wait_type,
    [waiting_tasks_count]    = waiting_tasks_count,
    [wait_time_seconds]      = wait_time_ms / 1000,
    [signal_wait_seconds]    = signal_wait_time_ms / 1000,
    [resource_wait_seconds]  = (wait_time_ms - signal_wait_time_ms) / 1000,
    [signal_pct]             = CASE WHEN wait_time_ms = 0 THEN 0
                                    ELSE CAST(100.0 * signal_wait_time_ms / wait_time_ms AS decimal(5,2)) END,
    [pct_of_total_waits]     = CAST(pct_of_total_waits AS decimal(5,2)),
    [avg_wait_ms_per_task]   = CASE WHEN waiting_tasks_count = 0 THEN 0
                                    ELSE wait_time_ms / waiting_tasks_count END,
    [max_wait_ms]            = max_wait_time_ms,
    [hint] = CASE
        WHEN wait_type LIKE 'PAGEIOLATCH%' THEN N'Buffer pool waiting on disk I/O - check storage latency'
        WHEN wait_type = 'WRITELOG' THEN N'Log write latency - check log drive, autogrowth, and SAN cache'
        WHEN wait_type LIKE 'PAGELATCH%' THEN N'Memory-page latch contention - often tempdb allocation pages'
        WHEN wait_type LIKE 'LCK_%' THEN N'Lock contention'
        WHEN wait_type = 'CXPACKET' THEN N'Parallel waits - tune MAXDOP/CTFP if dominant'
        WHEN wait_type = 'CXCONSUMER' THEN N'Benign parallel consumer wait - usually safe to ignore'
        WHEN wait_type = 'SOS_SCHEDULER_YIELD' THEN N'CPU pressure - check scheduler load and parallelism'
        WHEN wait_type LIKE 'ASYNC_NETWORK_IO%' THEN N'Client not consuming results fast enough'
        WHEN wait_type LIKE 'RESOURCE_SEMAPHORE%' THEN N'Memory grant contention - look at large memory consumers'
        WHEN wait_type LIKE 'HADR_SYNC_COMMIT' THEN N'AG sync-commit replica is slow on log hardening'
        WHEN wait_type LIKE 'THREADPOOL' THEN N'Out of worker threads - blocking storm or thread starvation'
        WHEN wait_type LIKE 'LATCH_%' THEN N'In-memory structure latch contention - investigate specific class'
        ELSE N''
    END
FROM waits
ORDER BY wait_time_ms DESC;

------------------------------------------------------------------------------
-- 3. Top latch waits (excluding buffer latches, which show as PAGELATCH)
------------------------------------------------------------------------------
SELECT TOP (20)
    [section]                = N'03 - Top latch waits',
    [latch_class]            = latch_class,
    [waiting_requests_count] = waiting_requests_count,
    [wait_time_ms]           = wait_time_ms,
    [max_wait_time_ms]       = max_wait_time_ms,
    [avg_wait_ms]            = CASE WHEN waiting_requests_count = 0 THEN 0
                                    ELSE wait_time_ms / waiting_requests_count END
FROM sys.dm_os_latch_stats
WHERE waiting_requests_count > 0
ORDER BY wait_time_ms DESC;

------------------------------------------------------------------------------
-- 4. Signal-wait ratio (CPU pressure indicator)
--    Signal waits = time spent waiting for a CPU after the resource became
--    available. > 25% sustained = likely CPU pressure.
------------------------------------------------------------------------------
SELECT
    [section]                = N'04 - Signal wait ratio',
    [total_wait_seconds]     = SUM(wait_time_ms) / 1000,
    [signal_wait_seconds]    = SUM(signal_wait_time_ms) / 1000,
    [resource_wait_seconds]  = SUM(wait_time_ms - signal_wait_time_ms) / 1000,
    [signal_pct]             = CASE WHEN SUM(wait_time_ms) = 0 THEN 0
                                    ELSE CAST(100.0 * SUM(signal_wait_time_ms) / SUM(wait_time_ms) AS decimal(5,2)) END,
    [posture]                = CASE
        WHEN SUM(wait_time_ms) = 0 THEN N'NO DATA'
        WHEN 100.0 * SUM(signal_wait_time_ms) / SUM(wait_time_ms) > 25 THEN N'INVESTIGATE - sustained CPU pressure'
        WHEN 100.0 * SUM(signal_wait_time_ms) / SUM(wait_time_ms) > 15 THEN N'WATCH'
        ELSE N'OK'
    END
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    N'CLR_SEMAPHORE', N'LAZYWRITER_SLEEP', N'RESOURCE_QUEUE', N'SLEEP_TASK',
    N'SLEEP_SYSTEMTASK', N'SQLTRACE_BUFFER_FLUSH', N'WAITFOR', N'LOGMGR_QUEUE',
    N'CHECKPOINT_QUEUE', N'REQUEST_FOR_DEADLOCK_SEARCH', N'XE_TIMER_EVENT',
    N'BROKER_TO_FLUSH', N'BROKER_TASK_STOP', N'CLR_MANUAL_EVENT', N'CLR_AUTO_EVENT',
    N'DISPATCHER_QUEUE_SEMAPHORE', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'XE_DISPATCHER_WAIT',
    N'XE_DISPATCHER_JOIN', N'BROKER_EVENTHANDLER', N'TRACEWRITE',
    N'FT_IFTSHC_MUTEX', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'BROKER_RECEIVE_WAITFOR',
    N'ONDEMAND_TASK_QUEUE', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRRORING_CMD',
    N'BROKER_TRANSMITTER', N'SQLTRACE_WAIT_ENTRIES', N'SLEEP_BPOOL_FLUSH',
    N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT',
    N'HADR_NOTIFICATION_DEQUEUE', N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE',
    N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'
);
