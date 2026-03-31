-----------------------------------------------------------------------
-- Diagnostic: Generalized Data Skew Analysis
--
-- Purpose:
--     Analyzes the distribution of a grouping column in any table
--     to determine whether the density vector (1/CountDistinct) is
--     a safe estimate for the optimizer. If data is heavily skewed,
--     density-based plans (from local variable mitigation) will
--     under- or over-estimate rows for outlier values.
--
-- Usage:
--     SET THE TWO VARIABLES BELOW to your target table and column.
--     This uses dynamic SQL so it works on any table/column pair.
--     Run against the target database. Read-only, uses NoLock.
--
-- Output:
--     Section A: Summary statistics (count, min, max, mean, stdev,
--                percentiles, Max-to-Mean ratio)
--     Section B: Bucket histogram showing distribution shape
--
-- What to look for:
--     - Max_to_Mean < 5x:  Density estimate is safe. Local variable
--       mitigation will produce a "good enough" plan.
--     - Max_to_Mean 5-20x: Moderate skew. Monitor after deploying
--       local variable mitigation.
--     - Max_to_Mean > 20x: Heavy skew. Density estimate is unreliable.
--       Consider OPTION(RECOMPILE), plan guides, or query redesign.
--     - P99/P50 ratio: How much the tail diverges from the median.
--       > 10x means the top 1% of values have dramatically different
--       row counts from the typical value.
-----------------------------------------------------------------------

-- *** CONFIGURE THESE TWO VARIABLES ***
Declare @TableName  NVarchar(256) = N'SrtShipToteShipmentAssoc'   -- Schema.Table or just Table
Declare @ColumnName NVarchar(256) = N'ShipToteId'                 -- The grouping column to analyze

-----------------------------------------------------------------------
-- Section A: Summary Statistics + Percentiles
-----------------------------------------------------------------------
Declare @SqlA NVarchar(Max) = N'
;With GroupCounts As
(
    Select
        ' + QuoteName(@ColumnName) + N' As GroupVal,
        Count(*) As Cnt
    From
        ' + QuoteName(@TableName) + N' With (NoLock)
    Group By
        ' + QuoteName(@ColumnName) + N'
),
Aggs As
(
    Select
        Count(*)            As DistinctValues,
        Sum(Cnt)            As TotalRows,
        Min(Cnt)            As [Min],
        Max(Cnt)            As [Max],
        Avg(Cnt)            As [Mean],
        Stdev(Cnt)          As [StdDev]
    From GroupCounts
),
Pctls As
(
    Select Distinct
        Cast(Percentile_Cont(0.50) Within Group (Order By Cnt) Over() As Decimal(12,2)) As P50,
        Cast(Percentile_Cont(0.90) Within Group (Order By Cnt) Over() As Decimal(12,2)) As P90,
        Cast(Percentile_Cont(0.95) Within Group (Order By Cnt) Over() As Decimal(12,2)) As P95,
        Cast(Percentile_Cont(0.99) Within Group (Order By Cnt) Over() As Decimal(12,2)) As P99
    From GroupCounts
)
Select
    ' + QuoteName(@TableName + '.' + @ColumnName, '''') + N' As Analysis,
    A.DistinctValues,
    A.TotalRows,
    A.[Min], A.[Max], A.[Mean], A.[StdDev],
    P.P50, P.P90, P.P95, P.P99,
    Cast(Case When A.[Mean] > 0
         Then Cast(A.[Max] As Float) / A.[Mean]
         Else 0 End As Decimal(10,2))    As Max_to_Mean,
    Cast(Case When P.P50 > 0
         Then P.P99 / P.P50
         Else 0 End As Decimal(10,2))    As P99_to_P50,
    Cast(Case When A.[Mean] > 0
         Then A.[StdDev] / A.[Mean]
         Else 0 End As Decimal(10,4))    As CoeffOfVariation
From Aggs A Cross Join Pctls P'

Exec sp_executesql @SqlA


-----------------------------------------------------------------------
-- Section B: Bucket Histogram
-----------------------------------------------------------------------
Declare @SqlB NVarchar(Max) = N'
;With GroupCounts As
(
    Select
        ' + QuoteName(@ColumnName) + N' As GroupVal,
        Count(*) As Cnt
    From
        ' + QuoteName(@TableName) + N' With (NoLock)
    Group By
        ' + QuoteName(@ColumnName) + N'
),
Buckets As
(
    Select
        Case
            When Cnt = 1                   Then ''1''
            When Cnt Between 2 And 3       Then ''2-3''
            When Cnt Between 4 And 5       Then ''4-5''
            When Cnt Between 6 And 10      Then ''6-10''
            When Cnt Between 11 And 25     Then ''11-25''
            When Cnt Between 26 And 50     Then ''26-50''
            When Cnt Between 51 And 100    Then ''51-100''
            When Cnt Between 101 And 500   Then ''101-500''
            When Cnt Between 501 And 1000  Then ''501-1K''
            When Cnt > 1000                Then ''1K+''
        End As Bucket,
        Case
            When Cnt = 1                   Then 1
            When Cnt Between 2 And 3       Then 2
            When Cnt Between 4 And 5       Then 3
            When Cnt Between 6 And 10      Then 4
            When Cnt Between 11 And 25     Then 5
            When Cnt Between 26 And 50     Then 6
            When Cnt Between 51 And 100    Then 7
            When Cnt Between 101 And 500   Then 8
            When Cnt Between 501 And 1000  Then 9
            When Cnt > 1000                Then 10
        End As BucketSort
    From GroupCounts
)
Select
    ' + QuoteName(@TableName + '.' + @ColumnName, '''') + N' As Analysis,
    Bucket,
    Count(*) As ValueCount,
    Cast(Count(*) * 100.0 / Sum(Count(*)) Over() As Decimal(5,2)) As PctOfTotal
From Buckets
Group By Bucket, BucketSort
Order By BucketSort'

Exec sp_executesql @SqlB
