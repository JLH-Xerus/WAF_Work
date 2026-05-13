--/
ALTER PROCEDURE [dbo].[lsp_SrtGenerateAndReturnSorterToteLocationStats]
    @NumAssignedTotes                 Int Output,
    @NumUnassignedTotes               Int Output,
    @NumFullTotes                     Int Output,
    @NumOfExceptPkgs                  Int Output,
    @NumOfPackagesInductedAndUnsorted Int Output
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_SrtGenerateAndReturnSorterToteLocationStats

Version:
   (next version)

Comments:
   v_next - Performance refactoring:
      1. Consolidated the eight `If Object_Id ... Drop Table` blocks into a single
         `Drop Table If Exists` statement at entry and a matching one at exit.
      2. Replaced the UNION-with-placeholder-columns aggregation pattern at the end
         of the procedure with five direct assignment SELECTs into the OUTPUT
         parameters. The original procedure built a 5-row, 5-column intermediate
         where each row contributed one value and four empty strings, then summed
         them at the outer level. The new form runs five focused SELECTs that each
         assign directly to the corresponding OUTPUT variable.
      3. Replaced the `Not In (Select ...)` subquery with `Not Exists` for safer
         NULL handling.
      4. Preserved the `Option (MaxDop 1)` hint on the SorterTotePackageRxs scan.
         The original procedure carries this hint with no comment explaining the
         rationale; the conservative choice is to preserve runtime behavior on a
         refactor pass and to flag the hint for re-evaluation in Section 11 of the
         analysis.
      5. Added supporting indexes on the temp tables that participate in downstream
         joins.
      6. Added Option (Recompile) on the cross-temp-table aggregates. The OUTPUT
         parameter signature does not have parameter-sniffing exposure, but the
         intermediate temp-table row counts vary materially across sites, so
         Recompile gives the optimizer accurate cardinality estimates per call.
*/

Set NoCount On

Declare @ToteFullPackageCount SmallInt

Drop Table If Exists
      #SorterTotePackageRxs
    , #ActiveExceptionRxPackageShipmentIds
    , #HistoryExceptionRxPackageShipmentIds
    , #AllShipmentIds
    , #NumOfPackagesInducted

------------------------------------------------------------------------------------------
-- Rxs in Packages residing in Sorter Totes.
------------------------------------------------------------------------------------------
Select
      OS.OrderId
    , OS.HistoryDtTm
    , OS.ShipmentId
Into #SorterTotePackageRxs
From SrtSorterLoc L With (NoLock)
Inner Join SrtShipToteShipmentAssoc TS With (NoLock)
    On TS.ShipToteId = L.ShipToteId
Inner Join OeOrderShipmentAssoc OS With (NoLock)
    On OS.ShipmentId = TS.ShipmentId
Option (MaxDop 1);

Create Index IX_STPR_Order On #SorterTotePackageRxs(OrderId, HistoryDtTm)

------------------------------------------------------------------------------------------
-- Active exception packages.
------------------------------------------------------------------------------------------
Select Distinct
      SR.ShipmentId
Into #ActiveExceptionRxPackageShipmentIds
From OeOrder O With (NoLock)
Inner Join #SorterTotePackageRxs SR
    On SR.OrderId = O.OrderId And SR.HistoryDtTm = O.HistoryDtTm
Where O.OrderStatus = 'Problem'
Option (Recompile);

------------------------------------------------------------------------------------------
-- History exception packages (Canceled only).
------------------------------------------------------------------------------------------
Select Distinct
      SR.ShipmentId
Into #HistoryExceptionRxPackageShipmentIds
From #SorterTotePackageRxs SR
Inner Join OeOrderHistory O With (NoLock)
    On O.OrderId = SR.OrderId And O.HistoryDtTm = SR.HistoryDtTm
Where SR.HistoryDtTm <> '12/31/9999'
  And O.OrderStatus = 'Canceled'
Option (Recompile);

------------------------------------------------------------------------------------------
-- Inducted-but-not-sorted packages.
------------------------------------------------------------------------------------------
Select
      OSA.ShipmentId
    , CRR.CurrLoc
    , CRR.NextLoc
    , CRR.LastLocChgDtTm
Into #AllShipmentIds
From OeOrderShipmentAssoc OSA With (NoLock)
Inner Join OeOrder O With (NoLock)
    On OSA.OrderId = O.OrderId And OSA.HistoryDtTm = O.HistoryDtTm
Inner Join ShpShipment SP With (NoLock)
    On OSA.ShipmentId = SP.Id
Inner Join CvyRxRoute CRR With (NoLock)
    On O.OrderId = CRR.OrderId And O.HistoryDtTm = CRR.HistoryDtTm
Option (Recompile);

Create Index IX_ASI_Shipment On #AllShipmentIds(ShipmentId) Include (CurrLoc, NextLoc, LastLocChgDtTm)

Select
      Count(*) As NumOfPackagesInducted
    , SI.LastLocChgDtTm As InductedDtTm
    , SI.ShipmentId
Into #NumOfPackagesInducted
From #AllShipmentIds SI
Where Left(SI.CurrLoc, 4) = 'SORT'
  And Left(SI.NextLoc, 2) = 'DR'
Group By SI.LastLocChgDtTm, SI.ShipmentId

Create Index IX_NOP_Shipment On #NumOfPackagesInducted(ShipmentId)

------------------------------------------------------------------------------------------
-- Read the ShippingToteFullPackageCount config value.
------------------------------------------------------------------------------------------
Select @ToteFullPackageCount = Cast(Value As SmallInt)
From CfgSystemParamVal With (NoLock)
Where Section = 'PackageSorter'
  And Parameter = 'ShippingToteFullPackageCount'

------------------------------------------------------------------------------------------
-- Five direct assignments into the OUTPUT variables. The original procedure used a
-- UNION-with-placeholder-columns aggregation that built a 5-row 5-column intermediate
-- and summed it; the focused form below is one statement per output value.
------------------------------------------------------------------------------------------
Set NoCount Off

Select @NumFullTotes = Count(Id)
From SrtShipTote With (NoLock)
Where NumOfPkgs >= @ToteFullPackageCount
  And Len(Id) = 22
  And Status <> 'Ready For Manifest'

Select @NumAssignedTotes = Count(SL.ShipToteId)
From SrtSorterLoc SL With (NoLock)
Inner Join SrtShipTote ST With (NoLock)
    On SL.ShipToteId = ST.Id
Where ST.Status In ('In Sorter Loc')
  And Len(ST.Id) = 22

Select @NumUnassignedTotes = Count(Distinct SL.Id)
From SrtSorterLoc SL With (NoLock)
Where SL.CarrierCode Is Not Null
  And SL.HasTote = 0

Select @NumOfExceptPkgs = Count(*)
From
(
    Select ShipmentId From #ActiveExceptionRxPackageShipmentIds
    Union
    Select ShipmentId From #HistoryExceptionRxPackageShipmentIds
) AS ExceptShipments

Select @NumOfPackagesInductedAndUnsorted = Count(*)
From #NumOfPackagesInducted PI
Where Not Exists
(
    Select 1
    From SrtShipToteShipmentAssoc TSA With (NoLock)
    Where TSA.ShipmentId = PI.ShipmentId
)
  And DateDiff(SECOND, PI.InductedDtTm, GetDate()) > 60

Drop Table If Exists
      #SorterTotePackageRxs
    , #ActiveExceptionRxPackageShipmentIds
    , #HistoryExceptionRxPackageShipmentIds
    , #AllShipmentIds
    , #NumOfPackagesInducted

End
GO
