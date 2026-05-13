/****** Object:  StoredProcedure [dbo].[lsp_GovGetSingleVialSorterCounts]    Script Date: 4/5/2026 8:50:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO
Create   Procedure [dbo].[lsp_GovGetSingleVialSorterCounts]
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_GovGetSingleVialSorterCounts

Version:
   10

Description:
   Retrieve the number of queued & on-route single vial Rxs to each destination sorter.
   Includes all sorters from 1 to N, even if no Rxs are assigned to them.

Return Value:
   0     = Successfully returned queued counts for sorters
   < 0   = Value of @@Error
*/
Set NoCount On

Declare @NumOfSorters Int

-- Get number of sorters
Select
   @NumOfSorters = Count(*)
From
   SrtSorter With (NoLock)

-- Build list of all sorter IDs
; With 
     SorterList As 
        (Select 
            Top (@NumOfSorters) Row_Number() Over (Order By (Select Null)) As SorterId
         From TallyNumbers -- sufficient rows for up to ~2048 sorters
        )
   , QueuedCounts As 
        (Select 
              G.SorterId
            , [QueuedRxCount] = Count(*)
         From GovRx As G With (NoLock)
         Inner Join OeOrder As O With (NoLock) 
         On G.OrderId = O.OrderId
         Where O.OrderStatus = 'Pending'
               And
               O.StatusReason = 'Awaiting Governor Allocation'
               And
               G.OrderTypeCode = 'SV'
         Group By G.SorterId
        )
   , OnRouteCounts As 
        (Select
              G.SorterId
            , [OnRouteRxCount] = Count(*)
         From GovRx As G With (NoLock)
         Inner Join OeOrder As O With (NoLock) On G.OrderId = O.OrderId
         Where
             NOT (O.OrderStatus = 'Pending' Or O.PendingOrderStatus = 'Pending')
             And
             O.OrderStatus NOT LIKE 'Split/%'
             And
             O.OrderId NOT LIKE '%[(<][1-8][)>]'
             And
             G.RxCvyStateCode <> 'C'
             And
             G.OrderTypeCode = 'SV'
         Group By G.SorterId
        )
Select
      SL.SorterId
    , [QueuedRxCount] = IsNull(QC.QueuedRxCount, 0)
    , [OnRouteRxCount] = IsNull(OC.OnRouteRxCount, 0)
From SorterList SL
Left Join QueuedCounts QC On SL.SorterId = QC.SorterId
Left Join OnRouteCounts OC On SL.SorterId = OC.SorterId
Order By SL.SorterId

Return @@Error
End

GO
