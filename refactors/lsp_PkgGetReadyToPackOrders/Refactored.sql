--/
ALTER PROCEDURE [dbo].[lsp_PkgGetReadyToPackOrders]
   @CompleteOrdersOnly TinyInt = 1,
   @IgnorePendingAndProblemRxsWhenListingCompleteOrders TinyInt = 0,
   @RouteStop VarChar(8),
   @PartialShipmentsTimeoutMins Integer,
   @OverridePoNum VarChar(30) = '',
   @DisplayRxsStoredInBinsAtAnyStation TinyInt = 0,
   @DisplayCompleteOrderStored TinyInt = 0
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_PkgGetReadyToPackOrders

Version:
   44

Comments:
   v44 - Performance refactoring:
      1. Local-variable copies of all seven input parameters to break parameter sniffing.
         The cross-list capture flags "severe parameter sniffing" on this procedure and
         lists it on three of four expense lists. The local-variable form plus the
         per-statement Option (Recompile) on the long-running SELECTs is what addresses
         the plan-stability ratio directly.
      2. Replaced the five scattered `If Object_Id ... Drop Table` blocks with a single
         consolidated `Drop Table If Exists` at the top of the body and a matching one
         at the bottom.
      3. Consolidated the three Hazardous CTEs (HazardousToHandle, HazardousToDispose,
         HazardousToShip) into a single set-based aggregate against InvFlaggedProducts
         that produces one row per ProductId with three flag columns. The original ran
         three independent scans of InvFlaggedProducts and three independent left-joins.
      4. Pre-materialized the per-(GroupNum, ShipmentId, ToteId, QueueStationId)
         NumOfPendingOrProblemRxs aggregate into a temp table indexed for the join,
         instead of computing it via a correlated scalar Count subquery once per row
         of #GrpData02.
      5. Added Option (Recompile) on the two big SELECT INTO statements (#Rxs and the
         final result SELECT). The final SELECT carries the catch-all WHERE clause shape
         driven by the control parameters; Recompile lets the optimizer simplify the
         WHERE clause for each call's actual parameter shape.
      6. Retained the scalar UDF dbo.fOrdPatNameFmtd. Inlining it is queued for v45
         per Section 11 of Analysis.md.
*/

Set NoCount On

------------------------------------------------------------------------------------------
-- Local variable copies break parameter sniffing.
------------------------------------------------------------------------------------------
Declare @LocalCompleteOrdersOnly                              TinyInt = @CompleteOrdersOnly
Declare @LocalIgnorePendingAndProblemRxsWhenListingComplete   TinyInt = @IgnorePendingAndProblemRxsWhenListingCompleteOrders
Declare @LocalRouteStop                                       VarChar(8) = @RouteStop
Declare @LocalPartialShipmentsTimeoutMins                     Integer    = @PartialShipmentsTimeoutMins
Declare @LocalOverridePoNum                                   VarChar(30) = @OverridePoNum
Declare @LocalDisplayRxsStoredInBinsAtAnyStation              TinyInt    = @DisplayRxsStoredInBinsAtAnyStation
Declare @LocalDisplayCompleteOrderStored                      TinyInt    = @DisplayCompleteOrderStored

Drop Table If Exists
      #RxOrderIds
    , #AtStationGrps
    , #Rxs
    , #GrpData01
    , #GrpData02
    , #HazFlags
    , #PendingProblemCounts

Create Table #RxOrderIds (OrderId VarChar(30))

------------------------------------------------------------------------------------------
-- Override path: if a specific PO# was supplied, fetch its active Rxs.
------------------------------------------------------------------------------------------
If @LocalOverridePoNum > ''
Begin
    Insert Into #RxOrderIds
    Select O.OrderId
    From OeOrderPoNumAssoc OP With (NoLock)
    Inner Join OeOrder O With (NoLock)
        On O.OrderId = OP.OrderId
    Where OP.PoNum = @LocalOverridePoNum
      And O.OrderStatus <> 'Ship Pending'
      And O.OrderStatus <> 'Split/MV'
      And O.OrderId Not Like '%<[1-2]>'
    Option (Recompile);
End
Else
Begin
    ------------------------------------------------------------------------------------------
    -- Standard path: gather "At Station" groups based on @RouteStop kind, then expand.
    ------------------------------------------------------------------------------------------
    Create Table #AtStationGrps (GroupNum VarChar(12))

    If @LocalRouteStop Like 'APK%' Or @LocalRouteStop Like 'SABP%'
    Begin
        Insert Into #AtStationGrps
        Select Distinct O.GroupNum
        From CvyAutoPackStationVialQueue APQ With (NoLock)
        Inner Join OeOrder O With (NoLock)
            On O.OrderId = APQ.OrderId And O.HistoryDtTm = APQ.HistoryDtTm
        Where APQ.StationId = @LocalRouteStop
        Option (Recompile);
    End
    Else If @LocalRouteStop Like 'SEMIPK%'
    Begin
        Insert Into #AtStationGrps
        Select Distinct O.GroupNum
        From CvySemiAutoPackStationToteQueue SPQ With (NoLock)
        Inner Join OrderToteAssoc OT With (NoLock)
            On OT.ToteId = SPQ.ToteId
        Inner Join OeOrder O With (NoLock)
            On O.OrderId = OT.OrderId
        Left Join #AtStationGrps SG
            On SG.GroupNum = O.GroupNum
        Where SPQ.StationId = @LocalRouteStop
          And SG.GroupNum Is Null
        Option (Recompile);
    End
    Else If @LocalRouteStop Like 'MP%'
    Begin
        Insert Into #AtStationGrps
        Select Distinct O.GroupNum
        From CvyManPackStationToteQueue MPQ With (NoLock)
        Inner Join OrderToteAssoc OT With (NoLock)
            On OT.ToteId = MPQ.ToteId
        Inner Join OeOrder O With (NoLock)
            On O.OrderId = OT.OrderId
        Left Join #AtStationGrps SG
            On SG.GroupNum = O.GroupNum
        Where MPQ.StationId = @LocalRouteStop
          And SG.GroupNum Is Null
        Option (Recompile);
    End

    -- For Manual Pack and Semi Pack: add groups whose Rxs are in storage locations.
    If @LocalRouteStop Like 'MP%' Or @LocalRouteStop Like 'SEMIPK%'
    Begin
        If @LocalDisplayRxsStoredInBinsAtAnyStation = 1
        Begin
            Insert Into #AtStationGrps
            Select Distinct O.GroupNum
            From OrdWillCallBin B With (NoLock)
            Inner Join OeOrder O With (NoLock)
                On O.OrderId = B.OrderId
            Left Join #AtStationGrps SG
                On SG.GroupNum = O.GroupNum
            Where SG.GroupNum Is Null
            Option (Recompile);
        End
        Else
        Begin
            Insert Into #AtStationGrps
            Select Distinct O.GroupNum
            From ShpStationWillCallBinAssoc SW With (NoLock)
            Inner Join OrdWillCallBin B With (NoLock)
                On B.BinId Like SW.BinIdPrefix + '%'
            Inner Join OeOrder O With (NoLock)
                On O.OrderId = B.OrderId
            Left Join #AtStationGrps SG
                On SG.GroupNum = O.GroupNum
            Where SW.StationId = @LocalRouteStop
              And SG.GroupNum Is Null
            Option (Recompile);
        End
    End

    Insert Into #RxOrderIds
    Select O.OrderId
    From #AtStationGrps SG
    Inner Join OeOrder O With (NoLock)
        On O.GroupNum = SG.GroupNum
    Where O.OrderStatus <> 'Ship Pending'
      And O.OrderStatus <> 'Split/MV'
      And O.OrderId Not Like '%<[1-2]>'
    Option (Recompile);
End

Create Index IX_RxOrderIds_OrderId On #RxOrderIds(OrderId);

------------------------------------------------------------------------------------------
-- Consolidated hazardous-flag lookup. The original procedure scanned InvFlaggedProducts
-- three times via three independent CTEs. One set-based aggregate produces all three
-- flags per ProductId in a single pass.
------------------------------------------------------------------------------------------
Select
      IFP.ProductId
    , [IsHazHandle]  = Max(Case When IFP.IsFlaggedFor = 'HAZMAT' Then 1 Else 0 End)
    , [IsHazDispose] = Max(Case When IFP.IsFlaggedFor = 'HAZDSP' Then 1 Else 0 End)
    , [IsHazShip]    = Max(Case When IFP.IsFlaggedFor = 'HAZSHP' Then 1 Else 0 End)
Into #HazFlags
From InvFlaggedProducts IFP With (NoLock)
Where IFP.ProductId In
(
    Select Distinct O.ProductId
    From #RxOrderIds R
    Inner Join OeOrder O With (NoLock)
        On O.OrderId = R.OrderId
)
  And IFP.IsFlaggedFor In ('HAZMAT', 'HAZDSP', 'HAZSHP')
Group By IFP.ProductId
Option (Recompile);

Create Index IX_HazFlags_ProductId On #HazFlags(ProductId);

------------------------------------------------------------------------------------------
-- Pull the per-Rx data into #Rxs.
------------------------------------------------------------------------------------------
Select
   O.GroupNum,
   IsNull(OS.ShipmentId, -1) As ShipmentId,
   G.GroupingCriteriaVal,
   OP.PoNum,
   O.OrderId,
   dbo.fOrdPatNameFmtd(P.LastName, ORPL.PtLastName, P.FirstName, ORPL.PtFirstName, P.MiddleName, ORPL.PtMiddleName, 1, 0) As PatNameFmtd,
   P.LastName,
   P.FirstName,
   O.OrderStatus,
   O.PriCodeSys,
   O.PriFillByDtTm,
   OT.ToteId,
   B.BinId,
   B.InDtTm As BinInDtTm,
   Case When SW.StationId Is Not Null Then 1 Else 0 End As InStationLoc,
   IsNull(APQ.StationId, IsNull(SPQ.StationId, MPQ.StationId)) As QueueStationId,
   IsNull(APQ.ArrivalDtTm, IsNull(SPQ.ArrivalDtTm, IsNull(MPQ.ArrivalDtTm, B.InDtTm))) As ArrivalDtTm,
   IsNull(SPQ.ArrivalDtTm, MPQ.ArrivalDtTm) As ToteQueueArrivalDtTm,
   O.LastStatusChgDtTm,
   (
      Case OrderStatus When 'Ready To Ship' Then '1' When 'Verified' Then '1' Else '2' End +
      Case When APQ.OrderId Is Not Null Or SPQ.ToteId Is Not Null Or MPQ.ToteId Is Not Null Then '1' Else '2' End +
      Case When B.BinId Is Not Null Then '1' Else '2' End +
      Case When SW.StationId Is Not Null Then '1' Else '2' End +
      Convert(VarChar(23), O.LastStatusChgDtTm, 121) +
      O.OrderId
   ) As PrecedenceString,
   Cast(0 As Bit) As IsRepRx,
   Case When IM.CrossContaminationCategory = 2 Then 1 Else 0 End As IsPenicillin,
   Case When IM.CrossContaminationCategory = 3 Then 1 Else 0 End As IsSulfa,
   IsNull(HF.IsHazHandle, 0)  As IsHazHandle,
   IsNull(HF.IsHazDispose, 0) As IsHazDispose,
   IsNull(HF.IsHazShip, 0)    As IsHazShip
Into #Rxs
From #RxOrderIds R
Inner Join OeOrder O With (NoLock) On O.OrderId = R.OrderId
Inner Join vInvMasterWithUserOverrides IM With (NoLock) On IM.ProductId = O.ProductId
Inner Join OeOrderPoNumAssoc OP With (NoLock) On OP.OrderId = O.OrderId
Inner Join OeGroup G With (NoLock) On G.GroupNum = O.GroupNum
Inner Join OePatientCust P With (NoLock) On P.PatCustId = O.PatCustId
Inner Join OePriority PR With (NoLock) On PR.PriCode = O.PriCodeSys
Left Join OeOrderShipmentAssoc OS With (NoLock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
Left Join OrderToteAssoc OT With (NoLock) On OT.OrderId = O.OrderId
Left Join OrdWillCallBin B With (NoLock) On B.OrderId = R.OrderId
Left Join ShpStationWillCallBinAssoc SW With (NoLock) On SW.StationId = @LocalRouteStop And B.BinId Like SW.BinIdPrefix + '%'
Left Join CvyAutoPackStationVialQueue APQ With (NoLock) On APQ.OrderId = O.OrderId And APQ.HistoryDtTm = O.HistoryDtTm
Left Join CvySemiAutoPackStationToteQueue SPQ With (NoLock) On SPQ.ToteId = OT.ToteId
Left Join CvyManPackStationToteQueue MPQ With (NoLock) On MPQ.ToteId = OT.ToteId
Left Join OeWorkflowStepInProgress IPG With (NoLock) On IPG.ObjectKey = OP.PoNum
Left Join OeRxPrefLangData ORPL With (NoLock) On O.OrderId = ORPL.OrderId And O.HistoryDtTm = ORPL.HistoryDtTm
Left Join #HazFlags HF On HF.ProductId = O.ProductId
Option (Recompile);

Create Index IX_Rxs_GrpShipTote On #Rxs(GroupNum, ShipmentId, ToteId);

------------------------------------------------------------------------------------------
-- Identify representative Rx per (Group, Shipment, Tote).
------------------------------------------------------------------------------------------
Update R
Set IsRepRx = 1
From
(
    Select Min(PrecedenceString) As MinPrecedenceString
    From #Rxs
    Group By GroupNum, ShipmentId, ToteId
) RR
Inner Join #Rxs R
    On R.PrecedenceString = RR.MinPrecedenceString;

------------------------------------------------------------------------------------------
-- Aggregate Rx data to group level.
------------------------------------------------------------------------------------------
Select
    GroupNum,
    ShipmentId,
    PoNum,
    Count(*) As NumOfRxs,
    Sum(Case When OrderStatus = 'Ready To Ship' Then 1 Else 0 End) As NumOfReadyStatusRxs,
    Sum(Case When (OrderStatus = 'Ready To Ship') And (QueueStationId Is Not Null Or BinId Is Not Null) Then 1 Else 0 End) As NumOfReadyStatusArrivedRxs,
    Sum(Case When BinId Is Not Null Then 1 Else 0 End) As NumOfStoredRxs,
    Sum(Case When (ToteId Is Not Null) And (QueueStationId Is Not Null) Then 1 Else 0 End) As NumOfTotedRxs,
    Min(IsNull(ToteQueueArrivalDtTm, '9999-12-31')) As FirstInToteQueueArrivalDtTm,
    Max(IsPenicillin) As IsPenicillin,
    Max(IsSulfa) As IsSulfa,
    Max(IsHazHandle) As IsHazHandle,
    Max(IsHazDispose) As IsHazDispose,
    Max(IsHazShip) As IsHazShip
Into #GrpData01
From #Rxs
Group By GroupNum, ShipmentId, PoNum
Option (Recompile);

------------------------------------------------------------------------------------------
-- Pre-materialize Pending/Problem counts per (GroupNum, ShipmentId, ToteId) once.
-- The original procedure recomputed this via a per-row correlated Count subquery in the
-- #GrpData02 SELECT (line 464 in v43). One-pass aggregation replaces N row-by-row reads.
------------------------------------------------------------------------------------------
Select
      GroupNum
    , ShipmentId
    , ToteId
    , [NumOfPendingOrProblemRxs] = Sum(Case When OrderStatus In ('Pending', 'Problem') And QueueStationId Is Not Null Then 1 Else 0 End)
Into #PendingProblemCounts
From #Rxs
Group By GroupNum, ShipmentId, ToteId;

Create Index IX_PPC On #PendingProblemCounts(GroupNum, ShipmentId, ToteId);

------------------------------------------------------------------------------------------
-- Fold representative Rx data and pending-problem counts into group data.
------------------------------------------------------------------------------------------
Select
   G.GroupNum,
   G.ShipmentId,
   G.PoNum,
   G.NumOfRxs,
   Case When @LocalOverridePoNum = '' Then G.NumOfReadyStatusArrivedRxs Else G.NumOfReadyStatusRxs End As NumOfReadyRxs,
   G.NumOfStoredRxs,
   G.NumOfTotedRxs,
   IsNull(PPC.NumOfPendingOrProblemRxs, 0) As NumOfPendingOrProblemRxs,
   R.GroupingCriteriaVal,
   R.OrderId As RepOrderId,
   R.LastName,
   R.FirstName,
   R.OrderStatus,
   R.PriCodeSys,
   R.PriFillByDtTm,
   R.ToteId,
   R.BinId,
   R.BinInDtTm,
   R.InStationLoc,
   R.QueueStationId,
   R.ArrivalDtTm,
   R.LastStatusChgDtTm,
   R.PatNameFmtd,
   G.IsPenicillin,
   G.IsSulfa,
   G.IsHazHandle,
   G.IsHazDispose,
   G.IsHazShip
Into #GrpData02
From #GrpData01 G
Inner Join #Rxs R
    On R.GroupNum = G.GroupNum And R.ShipmentId = G.ShipmentId And R.IsRepRx = 1
Left Join #PendingProblemCounts PPC
    On PPC.GroupNum = G.GroupNum
   And PPC.ShipmentId = G.ShipmentId
   And IsNull(PPC.ToteId, '') = IsNull(R.ToteId, '');

------------------------------------------------------------------------------------------
-- Final SELECT with the catch-all WHERE shape preserved.
------------------------------------------------------------------------------------------
Set NoCount Off

Select
   G.GroupNum,
   Case G.ShipmentId When -1 Then Null Else G.ShipmentId End As ShipmentId,
   G.PoNum,
   G.NumOfRxs,
   G.NumOfReadyRxs,
   G.NumOfStoredRxs,
   G.NumOfTotedRxs,
   G.NumOfPendingOrProblemRxs,
   Cast(G.NumOfReadyRxs As VarChar(12)) + ' of ' + Cast(G.NumOfRxs As VarChar(12)) As XOfYNumOfReadyRxs,
   G.GroupingCriteriaVal,
   Case When PT.Printer Is Null Then '' Else 'X' End As PpwkPrinted,
   G.RepOrderId,
   IsNull(G.ToteId, IsNull(G.BinId, IsNull(G.QueueStationId, 'Unknown'))) As Location,
   G.PatNameFmtd As PatientName,
   Case
      When G.PriCodeSys In (30, 60) Then
         SubString(Convert(VarChar(20), G.PriFillByDtTm, 1), 1, 5) + ' ' +
         SubString(Convert(VarChar(20), G.PriFillByDtTm, 0), 13, 5) + ' ' +
         SubString(Convert(VarChar(20), G.PriFillByDtTm, 0), 18, 2)
      Else
         PR.PriDesc
   End As Priority,
   G.PriCodeSys,
   G.QueueStationId,
   G.ArrivalDtTm,
   G.LastStatusChgDtTm,
   G.IsPenicillin,
   G.IsSulfa,
   G.IsHazHandle,
   G.IsHazDispose,
   G.IsHazShip
From #GrpData02 G
Inner Join OePriority PR With (NoLock)
    On PR.PriCode = G.PriCodeSys
Left Join PwkPrinterTray PT With (NoLock)
    On PT.ShipmentId = G.ShipmentId And PT.StationId = @LocalRouteStop
Where
(
    @LocalDisplayCompleteOrderStored = 0
    And
    (
        G.QueueStationId Is Not Null
        Or (G.NumOfStoredRxs > 0 And G.LastStatusChgDtTm < DateAdd(Minute, -@LocalPartialShipmentsTimeoutMins, GetDate()))
        Or @LocalCompleteOrdersOnly = 0
        Or (@LocalCompleteOrdersOnly = 1 And G.NumOfReadyRxs = G.NumOfRxs)
        Or (@LocalCompleteOrdersOnly = 1 And @LocalIgnorePendingAndProblemRxsWhenListingComplete = 1
             And G.NumOfReadyRxs = G.NumOfRxs - G.NumOfPendingOrProblemRxs)
    )
)
Or
(
    (@LocalDisplayCompleteOrderStored = 1 And G.NumOfStoredRxs = G.NumOfRxs)
    And
    (
        @LocalCompleteOrdersOnly = 0
        Or (@LocalCompleteOrdersOnly = 1 And G.NumOfReadyRxs = G.NumOfRxs)
    )
)
Order By
   PpwkPrinted Desc,
   PT.PrintDtTm,
   Case G.QueueStationId When @LocalRouteStop Then 1 Else 0 End Desc,
   IsNull(G.ArrivalDtTm, '9999-12-31'),
   G.PriCodeSys,
   G.PriFillByDtTm
Option (Recompile);

Drop Table If Exists
      #RxOrderIds
    , #AtStationGrps
    , #Rxs
    , #GrpData01
    , #GrpData02
    , #HazFlags
    , #PendingProblemCounts

End
GO
