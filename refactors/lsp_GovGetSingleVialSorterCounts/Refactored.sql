--/
ALTER PROCEDURE [dbo].[lsp_GovGetSingleVialSorterCounts]
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_GovGetSingleVialSorterCounts

Version:
   11

Description:
   Retrieve the number of queued and on-route single vial Rxs to each destination sorter.
   Includes all sorters from 1 to N, even if no Rxs are assigned to them.

Comments:
   v11 - Performance refactoring:
      1. Consolidated the two QueuedCounts and OnRouteCounts CTEs into a single
         conditional-aggregation CTE that scans GovRx + OeOrder once. The original
         scanned the same join twice, once per CTE, with disjoint WHERE clauses.
         A single scan with two conditional Sum() expressions produces both counts
         in one pass.
      2. Both branches of the new aggregation filter to `G.OrderTypeCode = 'SV'`,
         which is preserved in the WHERE clause of the consolidated scan.
      3. Added Option (Recompile) on the final SELECT to give the optimizer accurate
         cardinality estimates for the join into TallyNumbers based on the current
         sorter count.
*/

Set NoCount On

Declare @NumOfSorters Int

------------------------------------------------------------------------------------------
-- Sorter count.
------------------------------------------------------------------------------------------
Select @NumOfSorters = Count(*)
From SrtSorter With (NoLock)

------------------------------------------------------------------------------------------
-- Consolidated single-scan aggregation. Both counts (queued and on-route) are produced
-- from one pass over GovRx + OeOrder, filtered to OrderTypeCode = 'SV'.
------------------------------------------------------------------------------------------
;With SorterList As
(
    Select Top (@NumOfSorters)
          Row_Number() Over (Order By (Select Null)) As SorterId
    From TallyNumbers
)
, RxCounts As
(
    Select
          G.SorterId
        , [QueuedRxCount]  = Sum(Case
                                    When O.OrderStatus = 'Pending'
                                     And O.StatusReason = 'Awaiting Governor Allocation'
                                    Then 1
                                    Else 0
                                 End)
        , [OnRouteRxCount] = Sum(Case
                                    When Not (O.OrderStatus = 'Pending' Or O.PendingOrderStatus = 'Pending')
                                     And O.OrderStatus Not Like 'Split/%'
                                     And O.OrderId    Not Like '%[(<][1-8][)>]'
                                     And G.RxCvyStateCode <> 'C'
                                    Then 1
                                    Else 0
                                 End)
    From GovRx G With (NoLock)
    Inner Join OeOrder O With (NoLock)
        On G.OrderId = O.OrderId
    Where G.OrderTypeCode = 'SV'
    Group By G.SorterId
)
Select
      SL.SorterId
    , [QueuedRxCount]  = IsNull(RC.QueuedRxCount, 0)
    , [OnRouteRxCount] = IsNull(RC.OnRouteRxCount, 0)
From SorterList SL
Left Join RxCounts RC
    On RC.SorterId = SL.SorterId
Order By SL.SorterId
Option (Recompile);

Return @@Error
End
GO
