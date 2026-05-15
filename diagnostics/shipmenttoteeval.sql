-----------------------------------------------------------------------
-- Diagnostic: Data Skew Analysis for lsp_SrtGetShipToteIfExists
--
-- Purpose:
--     Exposes the distribution of key relationships the proc traverses.
--     If the density vector (avg rows per distinct value) is hiding
--     heavy skew, the optimizer will build a plan for the "average"
--     tote — which may be terrible for outliers.
--
-- Usage:
--     Run against the target database. Read-only, uses NoLock.
--     Results are four sections:
--       1a. Shipments-per-tote summary stats (SrtShipToteShipmentAssoc)
--       1b. Shipments-per-tote bucket histogram
--       2.  Orders-per-shipment summary stats (OeOrderShipmentAssoc)
--       3.  Manifest-records-per-tote summary stats (ShpManifest)
--
-- What to look for:
--     Compare the Mean to the P50 (median). If they diverge
--     significantly, you have skew. Compare P95/P99/Max to the Mean —
--     if Max is 10x+ the Mean, you have outlier totes that will get
--     plans optimized for the average and suffer badly.
--
--     The density vector the optimizer uses is essentially 1/CountDistinct,
--     which gives you the Mean. If Mean = 3 but Max = 400, every query
--     against a 400-shipment tote is planned as if it had 3 rows.
-----------------------------------------------------------------------

-----------------------------------------------------------------------
-- Section 1a: Shipments per Tote (SrtShipToteShipmentAssoc)
--
-- This is the primary fan-out. The proc joins through this table
-- for the exception check and the final shipment ID result set.
-----------------------------------------------------------------------
;With ToteCounts As
(
    Select
        ShipToteId,
        Count(*) As ShipmentCount
    From
        SrtShipToteShipmentAssoc With (NoLock)
    Group By
        ShipToteId
),
Aggs As
(
    Select
        Count(*)              As DistinctTotes,
        Min(ShipmentCount)    As [Min],
        Max(ShipmentCount)    As [Max],
        Avg(ShipmentCount)    As [Mean],
        Stdev(ShipmentCount)  As [StdDev]
    From
        ToteCounts
),
Pctls As
(
    Select Distinct
        Cast(Percentile_Cont(0.50) Within Group (Order By ShipmentCount) Over() As Decimal(10,2)) As [P50_Median],
        Cast(Percentile_Cont(0.90) Within Group (Order By ShipmentCount) Over() As Decimal(10,2)) As [P90],
        Cast(Percentile_Cont(0.95) Within Group (Order By ShipmentCount) Over() As Decimal(10,2)) As [P95],
        Cast(Percentile_Cont(0.99) Within Group (Order By ShipmentCount) Over() As Decimal(10,2)) As [P99]
    From
        ToteCounts
)
Select
    'SrtShipToteShipmentAssoc — Shipments per Tote' As Analysis,
    A.DistinctTotes,
    A.[Min],
    A.[Max],
    A.[Mean],
    A.[StdDev],
    P.[P50_Median],
    P.[P90],
    P.[P95],
    P.[P99],
    -- Skew ratio: how far the max deviates from the mean
    -- Values close to 1.0 = uniform, values >> 1.0 = heavy right skew
    Case
        When A.[Mean] > 0
        Then Cast(A.[Max] As Decimal(10,2)) / Cast(A.[Mean] As Decimal(10,2))
        Else 0
    End As [Max_to_Mean_Ratio]
From
    Aggs A
    Cross Join Pctls P

-----------------------------------------------------------------------
-- Section 1b: Bucket histogram — shipments per tote
-- Shows the shape of the distribution at a glance.
-----------------------------------------------------------------------
;With ToteCounts As
(
    Select
        ShipToteId,
        Count(*) As ShipmentCount
    From
        SrtShipToteShipmentAssoc With (NoLock)
    Group By
        ShipToteId
),
Buckets As
(
    Select
        Case
            When ShipmentCount = 1       Then '1'
            When ShipmentCount = 2       Then '2'
            When ShipmentCount = 3       Then '3'
            When ShipmentCount Between 4 And 5   Then '4-5'
            When ShipmentCount Between 6 And 10  Then '6-10'
            When ShipmentCount Between 11 And 25 Then '11-25'
            When ShipmentCount Between 26 And 50 Then '26-50'
            When ShipmentCount Between 51 And 100 Then '51-100'
            When ShipmentCount > 100     Then '100+'
        End As Bucket,
        Case
            When ShipmentCount = 1       Then 1
            When ShipmentCount = 2       Then 2
            When ShipmentCount = 3       Then 3
            When ShipmentCount Between 4 And 5   Then 4
            When ShipmentCount Between 6 And 10  Then 5
            When ShipmentCount Between 11 And 25 Then 6
            When ShipmentCount Between 26 And 50 Then 7
            When ShipmentCount Between 51 And 100 Then 8
            When ShipmentCount > 100     Then 9
        End As BucketSort
    From
        ToteCounts
)
Select
    'Shipments-per-Tote Histogram' As Analysis,
    Bucket,
    Count(*) As ToteCount,
    Cast(Count(*) * 100.0 / Sum(Count(*)) Over() As Decimal(5,2)) As PctOfTotal
From
    Buckets
Group By
    Bucket, BucketSort
Order By
    BucketSort


-----------------------------------------------------------------------
-- Section 2: Orders per Shipment (OeOrderShipmentAssoc)
--
-- The exception check joins through this table to reach OeOrder
-- and OeOrderHistory. If some shipments fan out to many orders,
-- those totes will be expensive to check for exceptions.
-----------------------------------------------------------------------
;With ShipmentCounts As
(
    Select
        ShipmentId,
        Count(*) As OrderCount
    From
        OeOrderShipmentAssoc With (NoLock)
    Group By
        ShipmentId
),
Aggs As
(
    Select
        Count(*)             As DistinctShipments,
        Min(OrderCount)      As [Min],
        Max(OrderCount)      As [Max],
        Avg(OrderCount)      As [Mean],
        Stdev(OrderCount)    As [StdDev]
    From
        ShipmentCounts
),
Pctls As
(
    Select Distinct
        Cast(Percentile_Cont(0.50) Within Group (Order By OrderCount) Over() As Decimal(10,2)) As [P50_Median],
        Cast(Percentile_Cont(0.90) Within Group (Order By OrderCount) Over() As Decimal(10,2)) As [P90],
        Cast(Percentile_Cont(0.95) Within Group (Order By OrderCount) Over() As Decimal(10,2)) As [P95],
        Cast(Percentile_Cont(0.99) Within Group (Order By OrderCount) Over() As Decimal(10,2)) As [P99]
    From
        ShipmentCounts
)
Select
    'OeOrderShipmentAssoc — Orders per Shipment' As Analysis,
    A.DistinctShipments,
    A.[Min],
    A.[Max],
    A.[Mean],
    A.[StdDev],
    P.[P50_Median],
    P.[P90],
    P.[P95],
    P.[P99],
    Case
        When A.[Mean] > 0
        Then Cast(A.[Max] As Decimal(10,2)) / Cast(A.[Mean] As Decimal(10,2))
        Else 0
    End As [Max_to_Mean_Ratio]
From
    Aggs A
    Cross Join Pctls P


-----------------------------------------------------------------------
-- Section 3: Manifest Records per Tote (ShpManifest)
--
-- The fallback path. If a tote isn't in SrtShipTote, the proc
-- queries ShpManifest and uses Count(*) Over() for NumOfPkgs.
-- Skew here affects the fallback path only.
-----------------------------------------------------------------------
;With ManifestCounts As
(
    Select
        ShipToteId,
        Count(*) As ManifestCount
    From
        ShpManifest With (NoLock)
    Group By
        ShipToteId
),
Aggs As
(
    Select
        Count(*)               As DistinctTotes,
        Min(ManifestCount)     As [Min],
        Max(ManifestCount)     As [Max],
        Avg(ManifestCount)     As [Mean],
        Stdev(ManifestCount)   As [StdDev]
    From
        ManifestCounts
),
Pctls As
(
    Select Distinct
        Cast(Percentile_Cont(0.50) Within Group (Order By ManifestCount) Over() As Decimal(10,2)) As [P50_Median],
        Cast(Percentile_Cont(0.90) Within Group (Order By ManifestCount) Over() As Decimal(10,2)) As [P90],
        Cast(Percentile_Cont(0.95) Within Group (Order By ManifestCount) Over() As Decimal(10,2)) As [P95],
        Cast(Percentile_Cont(0.99) Within Group (Order By ManifestCount) Over() As Decimal(10,2)) As [P99]
    From
        ManifestCounts
)
Select
    'ShpManifest — Records per Tote' As Analysis,
    A.DistinctTotes,
    A.[Min],
    A.[Max],
    A.[Mean],
    A.[StdDev],
    P.[P50_Median],
    P.[P90],
    P.[P95],
    P.[P99],
    Case
        When A.[Mean] > 0
        Then Cast(A.[Max] As Decimal(10,2)) / Cast(A.[Mean] As Decimal(10,2))
        Else 0
    End As [Max_to_Mean_Ratio]
From
    Aggs A
    Cross Join Pctls P


-----------------------------------------------------------------------
-- Section 4: What the optimizer actually thinks
--
-- Pull the density vectors SQL Server is using right now.
-- Compare "All density" (which is 1/distinct_values = the mean
-- the optimizer uses) against the actual distribution above.
-----------------------------------------------------------------------
-- Uncomment and adjust the index/stats names for your environment:
--
-- DBCC SHOW_STATISTICS('SrtShipToteShipmentAssoc', 'IX_SrtShipToteShipmentAssoc_ShipToteId') WITH DENSITY_VECTOR
-- DBCC SHOW_STATISTICS('OeOrderShipmentAssoc', 'IX_OeOrderShipmentAssoc_ShipmentId') WITH DENSITY_VECTOR
-- DBCC SHOW_STATISTICS('ShpManifest', 'IX_ShpManifest_ShipToteId') WITH DENSITY_VECTOR
--
-- The "All density" value * total rows = estimated rows per seek.
-- If that number diverges from your P95/P99 above, you've found
-- the gap between what the optimizer believes and reality.
-----------------------------------------------------------------------
