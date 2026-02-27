/*
================================================================================
  Purge Optimization — Metadata Collection Queries
  Run against: PharmAssist database
  Date:        2026-02-26
  Purpose:     Gather row counts, space usage, foreign keys, fragmentation,
               purge execution logs, and t-log info for all 63 purge tables.

  NOTE: Query 4 (fragmentation) can be slow on large tables.
        Consider running it during off-peak hours.
        All other queries are lightweight metadata reads.
================================================================================
*/


-- ============================================================================
-- QUERY 1: Row Counts + Space Usage for All 63 Purge Tables
-- Output: TableName, RowCount, TotalSpaceMB, DataSpaceMB, IndexSpaceMB
-- Expected runtime: < 5 seconds
-- ============================================================================
SELECT
    s.name                                          AS SchemaName
  , t.name                                          AS TableName
  , SUM(p.rows)                                     AS RowCount
  , CAST(SUM(au.total_pages) * 8.0 / 1024 AS DECIMAL(18,2))  AS TotalSpaceMB
  , CAST(SUM(CASE WHEN au.type = 1 THEN au.used_pages ELSE 0 END) * 8.0 / 1024 AS DECIMAL(18,2)) AS DataSpaceMB
  , CAST(SUM(CASE WHEN au.type = 2 THEN au.used_pages ELSE 0 END) * 8.0 / 1024 AS DECIMAL(18,2)) AS IndexSpaceMB
FROM
    sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    JOIN sys.indexes i ON i.object_id = t.object_id
    JOIN sys.partitions p ON p.object_id = i.object_id AND p.index_id = i.index_id
    JOIN sys.allocation_units au ON au.container_id = p.partition_id
WHERE
    t.name IN (
        -- Tier 1: Direct HistoryDtTm tables (20)
        'OeOrderHistory', 'OeOrderSecondaryData', 'OeOrderCurrHistoryDtTm',
        'CaAudit', 'CvyRxRoute', 'OeDurData', 'OeDurFreeFormText',
        'OeFlaggedRxs', 'OeLotCode', 'OeOrderAuxLabelFile', 'OeOrderAuxLabelText',
        'OeOrderExtSysDocument', 'OeOrderExtUserDef', 'OeOrderTextDocument',
        'OeOrderThirdPartyPlan', 'OeRxBagAssoc', 'OeRxDoseSched',
        'OeRxItemHistory', 'OeRxPouchDispenserAssoc', 'OeRxPrefLangData',
        -- Tier 2: Association / cascade tables
        'OeOrderCanReplenAssoc', 'OeOrderShipmentAssoc', 'OeOrderTcdAssoc',
        'OeOrderPoNumAssoc', 'ImgRxImgAssoc', 'ImgCanImgAssoc',
        'PwkPaperworkSetOrderAssoc', 'OeRxBagAssoc',
        'DsRxDoseSchedDose', 'DsRxDoseSchedDoseByDayOfWeek', 'DsRxDoseSched',
        'CanCanisterHistory', 'CanLotCodeHistory',
        'CvyPackageRoute', 'PwkPrinterTray',
        'ShpShipment', 'ShpShipmentPkgLabelData', 'ShpShipmentSpecialServiceAssoc',
        'ShpShipmentPackageLabelInstance', 'ShpManifest', 'ShpManifestShipmentAssoc',
        'ShpPallet', 'ShpPalletManifestAssoc', 'ShpLoad', 'ShpLoadPalletAssoc',
        'SrtShipToteShipmentAssoc',
        -- Tier 2: Group cascade
        'OeGroup', 'OeGroupCvyFillData', 'OeGroupOrderData',
        'OeGroupPayment', 'OeGroupUserDef',
        'GovOnHoldGroups', 'OeExceptionGroups',
        -- Tier 3: Shared entity tables
        'OePatientCust', 'OePatientCustSecondaryData', 'PrivacySignature',
        'OePrescriber',
        -- Tier 3: OrderId singleton tables
        'OeComments', 'OeProblemRxs', 'OeRequireCorrespondenceRxs',
        'OeStatusTrail',
        -- Tier 3: Other
        'OeOrder', 'OeRxRequest', 'OeOrderAcceptReject',
        'ImgImage', 'ImgImage_IntId', 'PwkPaperworkSet'
    )
GROUP BY
    s.name, t.name
ORDER BY
    SUM(p.rows) DESC;


-- ============================================================================
-- QUERY 2: Foreign Key Constraints Involving Purge Tables
-- Output: FK name, parent table, parent column(s), referenced table, referenced column(s)
-- This is CRITICAL for partition switch feasibility.
-- Expected runtime: < 2 seconds
-- ============================================================================
SELECT
    fk.name                                         AS ForeignKeyName
  , OBJECT_SCHEMA_NAME(fk.parent_object_id)         AS ChildSchema
  , OBJECT_NAME(fk.parent_object_id)                AS ChildTable
  , COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS ChildColumn
  , OBJECT_SCHEMA_NAME(fk.referenced_object_id)     AS ParentSchema
  , OBJECT_NAME(fk.referenced_object_id)            AS ParentTable
  , COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ParentColumn
  , fk.is_disabled                                  AS IsDisabled
  , fk.delete_referential_action_desc               AS OnDeleteAction
  , fk.update_referential_action_desc               AS OnUpdateAction
FROM
    sys.foreign_keys fk
    JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
WHERE
    OBJECT_NAME(fk.parent_object_id) IN (
        'OeOrderHistory', 'OeOrderSecondaryData', 'OeOrderCurrHistoryDtTm',
        'CaAudit', 'CvyRxRoute', 'OeDurData', 'OeDurFreeFormText',
        'OeFlaggedRxs', 'OeLotCode', 'OeOrderAuxLabelFile', 'OeOrderAuxLabelText',
        'OeOrderExtSysDocument', 'OeOrderExtUserDef', 'OeOrderTextDocument',
        'OeOrderThirdPartyPlan', 'OeRxBagAssoc', 'OeRxDoseSched',
        'OeRxItemHistory', 'OeRxPouchDispenserAssoc', 'OeRxPrefLangData',
        'OeOrderCanReplenAssoc', 'OeOrderShipmentAssoc', 'OeOrderTcdAssoc',
        'OeOrderPoNumAssoc', 'ImgRxImgAssoc', 'ImgCanImgAssoc',
        'PwkPaperworkSetOrderAssoc',
        'DsRxDoseSchedDose', 'DsRxDoseSchedDoseByDayOfWeek', 'DsRxDoseSched',
        'CanCanisterHistory', 'CanLotCodeHistory',
        'CvyPackageRoute', 'PwkPrinterTray',
        'ShpShipment', 'ShpShipmentPkgLabelData', 'ShpShipmentSpecialServiceAssoc',
        'ShpShipmentPackageLabelInstance', 'ShpManifest', 'ShpManifestShipmentAssoc',
        'ShpPallet', 'ShpPalletManifestAssoc', 'ShpLoad', 'ShpLoadPalletAssoc',
        'SrtShipToteShipmentAssoc',
        'OeGroup', 'OeGroupCvyFillData', 'OeGroupOrderData',
        'OeGroupPayment', 'OeGroupUserDef',
        'GovOnHoldGroups', 'OeExceptionGroups',
        'OePatientCust', 'OePatientCustSecondaryData', 'PrivacySignature',
        'OePrescriber',
        'OeComments', 'OeProblemRxs', 'OeRequireCorrespondenceRxs',
        'OeStatusTrail',
        'OeOrder', 'OeRxRequest', 'OeOrderAcceptReject',
        'ImgImage', 'ImgImage_IntId', 'PwkPaperworkSet'
    )
    OR OBJECT_NAME(fk.referenced_object_id) IN (
        'OeOrderHistory', 'OeOrderSecondaryData', 'OeOrderCurrHistoryDtTm',
        'CaAudit', 'CvyRxRoute', 'OeDurData', 'OeDurFreeFormText',
        'OeFlaggedRxs', 'OeLotCode', 'OeOrderAuxLabelFile', 'OeOrderAuxLabelText',
        'OeOrderExtSysDocument', 'OeOrderExtUserDef', 'OeOrderTextDocument',
        'OeOrderThirdPartyPlan', 'OeRxBagAssoc', 'OeRxDoseSched',
        'OeRxItemHistory', 'OeRxPouchDispenserAssoc', 'OeRxPrefLangData',
        'OeOrderCanReplenAssoc', 'OeOrderShipmentAssoc', 'OeOrderTcdAssoc',
        'OeOrderPoNumAssoc', 'ImgRxImgAssoc', 'ImgCanImgAssoc',
        'PwkPaperworkSetOrderAssoc',
        'DsRxDoseSchedDose', 'DsRxDoseSchedDoseByDayOfWeek', 'DsRxDoseSched',
        'CanCanisterHistory', 'CanLotCodeHistory',
        'CvyPackageRoute', 'PwkPrinterTray',
        'ShpShipment', 'ShpShipmentPkgLabelData', 'ShpShipmentSpecialServiceAssoc',
        'ShpShipmentPackageLabelInstance', 'ShpManifest', 'ShpManifestShipmentAssoc',
        'ShpPallet', 'ShpPalletManifestAssoc', 'ShpLoad', 'ShpLoadPalletAssoc',
        'SrtShipToteShipmentAssoc',
        'OeGroup', 'OeGroupCvyFillData', 'OeGroupOrderData',
        'OeGroupPayment', 'OeGroupUserDef',
        'GovOnHoldGroups', 'OeExceptionGroups',
        'OePatientCust', 'OePatientCustSecondaryData', 'PrivacySignature',
        'OePrescriber',
        'OeComments', 'OeProblemRxs', 'OeRequireCorrespondenceRxs',
        'OeStatusTrail',
        'OeOrder', 'OeRxRequest', 'OeOrderAcceptReject',
        'ImgImage', 'ImgImage_IntId', 'PwkPaperworkSet'
    )
ORDER BY
    ParentTable, ChildTable, fkc.constraint_column_id;


-- ============================================================================
-- QUERY 3: Purge Execution Logs from EvtEvent
-- Schema: TrxId (bigint PK), Module (varchar 4), Event (varchar 20),
--         OprId, EventDtTm, Results, RxNum, Ndc, Qty, TcdSn,
--         Notes (varchar 255), SubRoutine (varchar 50), ComputerName,
--         ExeCode, CanisterSn
-- The purge proc calls lsp_DbLogSqlEvent with module 'A' and event
-- descriptions like 'PurgeOldData Beg' / 'PurgeOldData End'.
-- Expected runtime: < 5 seconds
-- ============================================================================
SELECT
    TrxId
  , Module
  , Event
  , SubRoutine
  , Notes
  , Results
  , EventDtTm
  , ComputerName
FROM
    dbo.EvtEvent
WHERE
    (   Event LIKE '%PurgeOldData%'
     OR Event LIKE '%Purge%'
     OR Notes LIKE '%lsp_DbPurgeHistoryData%'
     OR Notes LIKE '%PurgeOldData%'
     OR SubRoutine LIKE '%lsp_DbPurge%'
    )
    AND EventDtTm >= DATEADD(MONTH, -3, GETDATE())
ORDER BY
    EventDtTm DESC;


-- ============================================================================
-- QUERY 4: Index Fragmentation for Purge Tables
-- *** WARNING: This query can be SLOW on large tables. ***
-- Uses LIMITED mode for speed. Run during off-peak if possible.
-- For the very largest tables (OeOrderHistory, EvtEvent), this may take
-- a few minutes. Consider running just those separately if needed.
-- Expected runtime: 2-15 minutes depending on table sizes
-- ============================================================================
SELECT
    OBJECT_SCHEMA_NAME(ips.object_id)               AS SchemaName
  , OBJECT_NAME(ips.object_id)                      AS TableName
  , i.name                                          AS IndexName
  , i.type_desc                                     AS IndexType
  , ips.partition_number                            AS PartitionNumber
  , ips.index_depth                                 AS IndexDepth
  , ips.index_level                                 AS IndexLevel
  , CAST(ips.avg_fragmentation_in_percent AS DECIMAL(5,2)) AS FragPct
  , ips.fragment_count                              AS FragmentCount
  , ips.page_count                                  AS PageCount
  , CAST(ips.avg_page_space_used_in_percent AS DECIMAL(5,2)) AS AvgPageDensityPct
  , ips.record_count                                AS RecordCount
FROM
    sys.dm_db_index_physical_stats(
        DB_ID(), NULL, NULL, NULL, 'LIMITED'
    ) ips
    JOIN sys.indexes i ON i.object_id = ips.object_id AND i.index_id = ips.index_id
WHERE
    OBJECT_NAME(ips.object_id) IN (
        'OeOrderHistory', 'OeOrderSecondaryData', 'OeOrderCurrHistoryDtTm',
        'CaAudit', 'CvyRxRoute', 'OeDurData', 'OeDurFreeFormText',
        'OeFlaggedRxs', 'OeLotCode', 'OeOrderAuxLabelFile', 'OeOrderAuxLabelText',
        'OeOrderExtSysDocument', 'OeOrderExtUserDef', 'OeOrderTextDocument',
        'OeOrderThirdPartyPlan', 'OeRxBagAssoc', 'OeRxDoseSched',
        'OeRxItemHistory', 'OeRxPouchDispenserAssoc', 'OeRxPrefLangData',
        'OeOrderCanReplenAssoc', 'OeOrderShipmentAssoc', 'OeOrderTcdAssoc',
        'OeOrderPoNumAssoc', 'ImgRxImgAssoc', 'ImgCanImgAssoc',
        'PwkPaperworkSetOrderAssoc',
        'DsRxDoseSchedDose', 'DsRxDoseSchedDoseByDayOfWeek', 'DsRxDoseSched',
        'CanCanisterHistory', 'CanLotCodeHistory',
        'CvyPackageRoute', 'PwkPrinterTray',
        'ShpShipment', 'ShpShipmentPkgLabelData', 'ShpShipmentSpecialServiceAssoc',
        'ShpShipmentPackageLabelInstance', 'ShpManifest', 'ShpManifestShipmentAssoc',
        'ShpPallet', 'ShpPalletManifestAssoc', 'ShpLoad', 'ShpLoadPalletAssoc',
        'SrtShipToteShipmentAssoc',
        'OeGroup', 'OeGroupCvyFillData', 'OeGroupOrderData',
        'OeGroupPayment', 'OeGroupUserDef',
        'GovOnHoldGroups', 'OeExceptionGroups',
        'OePatientCust', 'OePatientCustSecondaryData', 'PrivacySignature',
        'OePrescriber',
        'OeComments', 'OeProblemRxs', 'OeRequireCorrespondenceRxs',
        'OeStatusTrail',
        'OeOrder', 'OeRxRequest', 'OeOrderAcceptReject',
        'ImgImage', 'ImgImage_IntId', 'PwkPaperworkSet'
    )
    AND ips.index_level = 0          -- leaf level only
    AND ips.page_count > 100         -- skip tiny tables
ORDER BY
    ips.page_count DESC, ips.avg_fragmentation_in_percent DESC;


-- ============================================================================
-- QUERY 5: Transaction Log File Size and Usage
-- Output: Log file size, space used, growth settings
-- Expected runtime: instant
-- ============================================================================
SELECT
    DB_NAME()                                        AS DatabaseName
  , f.name                                          AS LogFileName
  , f.physical_name                                 AS LogFilePath
  , CAST(f.size * 8.0 / 1024 AS DECIMAL(18,2))     AS LogFileSizeMB
  , CAST(FILEPROPERTY(f.name, 'SpaceUsed') * 8.0 / 1024 AS DECIMAL(18,2)) AS LogSpaceUsedMB
  , CAST((f.size - FILEPROPERTY(f.name, 'SpaceUsed')) * 8.0 / 1024 AS DECIMAL(18,2)) AS LogSpaceFreeMB
  , CASE f.max_size
        WHEN -1 THEN 'UNLIMITED'
        WHEN 0  THEN 'NO GROWTH'
        ELSE CAST(CAST(f.max_size * 8.0 / 1024 AS DECIMAL(18,2)) AS VARCHAR(20)) + ' MB'
    END                                             AS MaxSizeMB
  , CASE f.is_percent_growth
        WHEN 1 THEN CAST(f.growth AS VARCHAR(10)) + '%'
        ELSE CAST(CAST(f.growth * 8.0 / 1024 AS DECIMAL(18,2)) AS VARCHAR(20)) + ' MB'
    END                                             AS GrowthIncrement
  , d.recovery_model_desc                           AS RecoveryModel
  , d.log_reuse_wait_desc                           AS LogReuseWait
FROM
    sys.database_files f
    CROSS JOIN sys.databases d
WHERE
    f.type = 1  -- log files
    AND d.database_id = DB_ID();


/*
================================================================================
  EXPORT INSTRUCTIONS
================================================================================

  Query 1 → Save as:  PurgeTableSizes_20260226.xlsx
  Query 2 → Save as:  PurgeForeignKeys_20260226.xlsx
  Query 3 → Save as:  PurgeExecLogs_20260226.xlsx  (if table exists)
  Query 4 → Save as:  PurgeFragmentation_20260226.xlsx
  Query 5 → Save as:  TLogStatus_20260226.xlsx

  Or combine all into a single workbook with one tab per query —
  whatever is easiest for you.

  Priority order if you're short on time:
    1. Query 1 (row counts)     — instant, highest value
    2. Query 2 (foreign keys)   — instant, critical for partitioning
    5. Query 5 (t-log)          — instant, good context
    3. Query 3 (purge logs)     — instant if table exists
    4. Query 4 (fragmentation)  — may be slow, run last
================================================================================
*/
