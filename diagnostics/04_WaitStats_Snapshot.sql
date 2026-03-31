-----------------------------------------------------------------------
-- Diagnostic: Wait Statistics Snapshot
--
-- Purpose:
--     Captures the current cumulative wait stats, ranked by total
--     wait time. This tells you WHERE the server is spending time
--     waiting — I/O, locks, memory, CPU scheduling, network, etc.
--
-- Usage:
--     Run against master or any database. Read-only.
--     For delta analysis: run once, wait N minutes, run again, and
--     compare. The columns are cumulative since last server restart
--     or stats reset.
--
-- Filtered waits:
--     Benign/idle waits are excluded (WAITFOR, SLEEP, BROKER_*,
--     CLR_AUTO_EVENT, etc.) to surface only actionable waits.
--
-- What to look for:
--     - PAGEIOLATCH_SH/EX: Physical I/O waits. Disk is slow or
--       buffer pool is too small. High reads from bad query plans
--       make this worse.
--     - LCK_M_S, LCK_M_X, LCK_M_IX: Lock contention. Queries are
--       blocking each other. Consider NOLOCK for read-only display
--       procs or RCSI at the database level.
--     - CXPACKET / CXCONSUMER: Parallelism waits. Normal in moderate
--       amounts; excessive means unbalanced parallel plans.
--     - SOS_SCHEDULER_YIELD: CPU pressure. Queries are compute-bound
--       and waiting for scheduler time.
--     - WRITELOG: Transaction log write waits. Log disk is slow.
--     - MEMORY_ALLOCATION_EXT: Memory pressure.
--     - ASYNC_NETWORK_IO: Client is slow consuming results. Not a
--       server problem — look at the application.
-----------------------------------------------------------------------

;With FilteredWaits As
(
    Select
          wait_type
        , waiting_tasks_count           As WaitCount
        , wait_time_ms                  As TotalWaitMs
        , wait_time_ms - signal_wait_time_ms As ResourceWaitMs
        , signal_wait_time_ms           As SignalWaitMs
        , max_wait_time_ms              As MaxWaitMs
    From
        sys.dm_os_wait_stats
    Where
        wait_type Not In (
            -- Idle / benign waits to exclude
            'BROKER_EVENTHANDLER', 'BROKER_RECEIVE_WAITFOR', 'BROKER_TASK_STOP',
            'BROKER_TO_FLUSH', 'BROKER_TRANSMITTER', 'CHECKPOINT_QUEUE',
            'CHKPT', 'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT', 'CLR_SEMAPHORE',
            'DBMIRROR_DBM_EVENT', 'DBMIRROR_EVENTS_QUEUE', 'DBMIRROR_WORKER_QUEUE',
            'DBMIRRORING_CMD', 'DIRTY_PAGE_POLL', 'DISPATCHER_QUEUE_SEMAPHORE',
            'EXECSYNC', 'FSAGENT', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
            'FT_IFTSHC_MUTEX', 'HADR_CLUSAPI_CALL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
            'HADR_LOGCAPTURE_WAIT', 'HADR_NOTIFICATION_DEQUEUE',
            'HADR_TIMER_TASK', 'HADR_WORK_QUEUE', 'KSOURCE_WAKEUP',
            'LAZYWRITER_SLEEP', 'LOGMGR_QUEUE', 'MEMORY_ALLOCATION_EXT',
            'ONDEMAND_TASK_QUEUE', 'PARALLEL_REDO_DRAIN_WORKER',
            'PARALLEL_REDO_LOG_CACHE', 'PARALLEL_REDO_TRAN_LIST',
            'PARALLEL_REDO_WORKER_SYNC', 'PARALLEL_REDO_WORKER_WAIT_WORK',
            'PREEMPTIVE_OS_FLUSHFILEBUFFERS', 'PREEMPTIVE_XE_GETTARGETSTATE',
            'PVS_PREALLOCATE', 'PWAIT_ALL_COMPONENTS_INITIALIZED',
            'PWAIT_DIRECTLOGCONSUMER_GETNEXT', 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
            'QDS_ASYNC_QUEUE', 'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
            'QDS_SHUTDOWN_QUEUE', 'REDO_THREAD_PENDING_WORK',
            'REQUEST_FOR_DEADLOCK_SEARCH', 'RESOURCE_QUEUE',
            'SERVER_IDLE_CHECK', 'SLEEP_BPOOL_FLUSH', 'SLEEP_DBSTARTUP',
            'SLEEP_DCOMSTARTUP', 'SLEEP_MASTERDBREADY', 'SLEEP_MASTERMDREADY',
            'SLEEP_MASTERUPGRADED', 'SLEEP_MSDBSTARTUP', 'SLEEP_SYSTEMTASK',
            'SLEEP_TASK', 'SLEEP_TEMPDBSTARTUP', 'SNI_HTTP_ACCEPT',
            'SOS_WORK_DISPATCHER', 'SP_SERVER_DIAGNOSTICS_SLEEP',
            'SQLTRACE_BUFFER_FLUSH', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
            'SQLTRACE_WAIT_ENTRIES', 'UCS_SESSION_REGISTRATION',
            'WAIT_FOR_RESULTS', 'WAIT_XTP_CKPT_CLOSE', 'WAIT_XTP_HOST_WAIT',
            'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', 'WAIT_XTP_RECOVERY',
            'WAITFOR', 'XE_BUFFERMGR_ALLPROCESSED_EVENT', 'XE_DISPATCHER_JOIN',
            'XE_DISPATCHER_WAIT', 'XE_LIVE_TARGET_TVF', 'XE_TIMER_EVENT'
        )
        And waiting_tasks_count > 0
)
Select Top 30
      wait_type                                                     As WaitType
    , WaitCount
    , Cast(TotalWaitMs / 1000.0 As Decimal(18,2))                  As TotalWaitSec
    , Cast(ResourceWaitMs / 1000.0 As Decimal(18,2))                As ResourceWaitSec
    , Cast(SignalWaitMs / 1000.0 As Decimal(18,2))                  As SignalWaitSec
    , Cast(MaxWaitMs / 1000.0 As Decimal(18,2))                     As MaxSingleWaitSec
    , Cast(TotalWaitMs * 100.0 / Sum(TotalWaitMs) Over()
           As Decimal(5,2))                                         As PctOfTotal
    , Cast(Case When WaitCount > 0
           Then TotalWaitMs * 1.0 / WaitCount
           Else 0 End As Decimal(12,2))                             As AvgWaitMs
From
    FilteredWaits
Order By
    TotalWaitMs Desc
