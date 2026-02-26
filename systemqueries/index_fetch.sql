SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPK,
    i.filter_definition AS FilterDef,
    ic.key_ordinal AS KeyOrdinal,
    ic.is_included_column AS IsIncluded,
    c.name AS ColumnName,
    ty.name AS DataType
FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    JOIN sys.tables t ON t.object_id = i.object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    JOIN sys.types ty ON ty.user_type_id = c.user_type_id
WHERE t.name IN (
    -- Tier 2: Association/bridge tables
    'OeOrderShipmentAssoc', 'ImgRxImgAssoc', 'ImgCanImgAssoc',
    'OeOrderCanReplenAssoc', 'OeRxDoseSched', 'PwkPaperworkSetOrderAssoc',
    -- Tier 2: Relationship-derived tables
    'ShpShipment', 'ShpManifest', 'ShpPallet', 'ShpLoad',
    'ShpManifestShipmentAssoc', 'ShpPalletManifestAssoc', 'ShpLoadPalletAssoc',
    'ShpShipmentPkgLabelData', 'ShpShipmentSpecialServiceAssoc',
    'ShpShipmentPackageLabelInstance', 'SrtShipToteShipmentAssoc',
    'CvyPackageRoute', 'PwkPrinterTray',
    'ImgImage', 'ImgImage_IntId',
    'DsRxDoseSched', 'DsRxDoseSchedDose', 'DsRxDoseSchedDoseByDayOfWeek',
    'PwkPaperworkSet',
    'CanCanisterHistory', 'CanLotCodeHistory',
    -- Tier 3: Shared entity tables
    'OeGroup', 'OeGroupCvyFillData', 'OeGroupOrderData',
    'OeGroupPayment', 'OeGroupUserDef',
    'GovOnHoldGroups', 'OeExceptionGroups',
    'OePatientCust', 'OePatientCustSecondaryData', 'PrivacySignature',
    'OePrescriber',
    -- Tier 3: OrderId-only tables
    'OeComments', 'OeOrderPoNumAssoc', 'OeProblemRxs',
    'OeRequireCorrespondenceRxs', 'OeStatusTrail',
    -- Parent tables used in orphan detection joins
    'OeOrder', 'OeOrderHistory', 'OeRxRequest',
    -- Also include OeOrderAcceptReject
    'OeOrderAcceptReject',
    -- Tier 1 tables (to confirm partition-readiness)
    'OeOrderSecondaryData', 'OeOrderCurrHistoryDtTm', 'CaAudit',
    'CvyRxRoute', 'OeDurData', 'OeDurFreeFormText', 'OeFlaggedRxs',
    'OeLotCode', 'OeOrderAuxLabelFile', 'OeOrderAuxLabelText',
    'OeOrderExtSysDocument', 'OeOrderExtUserDef', 'OeOrderTcdAssoc',
    'OeOrderTextDocument', 'OeOrderThirdPartyPlan', 'OeRxBagAssoc',
    'OeRxItemHistory', 'OeRxPouchDispenserAssoc', 'OeRxPrefLangData'
)
ORDER BY s.name, t.name, i.name, ic.key_ordinal
