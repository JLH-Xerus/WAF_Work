/****** Object:  StoredProcedure [dbo].[lsp_PkgGetReadyToPackOrders]    Script Date: 4/5/2026 8:50:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create   Procedure [dbo].[lsp_PkgGetReadyToPackOrders]
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
   43

Description:
   Retrieve a list of orders to be displayed on the "Package/Ship Orders" form.
   This form is used to package orders at Manual Pack, Semi-Auto-Pack, and Auto-Pack.

   For a specified pack station, this list is (typically) made up of orders where:
      - An order has at least one Rx item in the station's queue (whether tote or vial).
      - An order has at least one Rx item in the station's storage locations or all Rx items in any station's storage locations (depending on the screen configuration).
   However, a broader list of orders can be retrieved based on system configuration and form control settings being passed as control parameters
   into this stored procedure.

Parameters:
   Input:
      @CompleteOrdersOnly            - For orders having at least one Rx item in the station's storage locations:
                                        1 = Only retrieve those having all Rx items in any station's storage locations.
                                        0 = Retrieve those having all Rx items in a ready status, even if some are not storage locations.
                                      Note:  Orders having an Rx item in the station's vial or tote queue are always returned, even if some are
                                               not in a ready status.
      @IgnorePendingAndProblemRxsWhenListingCompleteOrders
                                    - For orders having at least one Rx item in the station's storage locations:
                                        0 = Include "Pending" and "Problem" status Rx items when deriving "all Rx items for an order are at the
                                              pack station(s)".
                                        1 = Ignore "Pending" and "Problem" status Rx items when deriving "all Rx items for an order are at the
                                              pack station(s)".
      @RouteStop                     - The Station ID (station's route stop) for which to return orders "at that pack station".
      @PartialShipmentsTimeoutMins   - The window of time (in minutes) at which an order having Rx items stored in storage locations at a station longer
                                         than this time are retrieved, even when all of the order's Rx items are not in a ready status or at the
                                         pack station(s).
      @OverridePoNum                 - The order# of the order for which to retrieve the grid order data, if all of its Rxs are in a ready status.
                                         This overrides the retrieval of all other order.
                                       The "Package/Ship Orders" form sends a value for this when the user scans an Rx on the form.
                                       The form has a (silly?) constraint that the order to be packed must be populated into the grid so that
                                         it can move to the row before opening the "Package/Ship Order" form.
      @DisplayRxsStoredInBinsAtAnyStation
                                     - For orders retrieved for storage location Rx items:
                                         0 = Only include orders that have Rx items stored in "this" (@RouteStop) station's locations.
                                         1 = Include orders that have Rx items stored in any station's locations.
      @DisplayCompleteOrderStored
                                     - For orders retrieved for storage location Rx items choose whether to display only complete orders or not:
                                         0 = Include all orders that have Rx items stored station's locations
                                                defined by @DisplayRxsStoredInBinsAtAnyStation parameter
                                         1 = Include orders that are complete have Rx items stored in station's locations
                                                defined by @DisplayRxsStoredInBinsAtAnyStation parameter
   Output:
     None

Return Value:
   None

Comments:
   The term "Items" per order/group is used as logic considers each portion Rx of a multi-vial Rx as a separately "counted" and "conveyed"
         order piece.
*/

Set NoCount On

---------------------------------------------------------------------------
-- Create a temporary table to be populated with "applicable" Rx Order IDs.
---------------------------------------------------------------------------
If Object_Id('TempDb..#RxOrderIds') Is Not Null
   Drop Table #RxOrderIds

Create Table #RxOrderIds (OrderId VarChar(30))

--------------------------------------
-- If an override order# was specified
--   Retrieve its list of Rxs.
--------------------------------------
If @OverridePoNum > ''   -- @OverridePoNum was specified
   Begin

      ----------------------------------------------------------------------------------------------------------------------------
      -- Retrieve all active Rxs for the order that have not yet been packaged (i.e., that do not have a status of "Ship Pending")
      ----------------------------------------------------------------------------------------------------------------------------
      Insert Into
         #RxOrderIds
      Select
         O.OrderId
      From
         OeOrderPoNumAssoc OP With (NoLock)
            Join
         OeOrder O With (NoLock)
            On O.OrderId = OP.OrderId
      Where
         OP.PoNum = @OverridePoNum
         And
         O.OrderStatus <> 'Ship Pending'
         And
         O.OrderStatus <> 'Split/MV'     -- Ignore multi-vial Rx parent rows.
         And
         O.OrderId Not Like '%<[1-2]>'   -- Ignore dual-dispenser portion/children Rx rows.

   End   -- An override order# was specified.

----------------------------------------------------------------------------------------------
-- If no override order# was specified
--   Retrieve the typical "ready to pack" (or de-queue) orders for the specified pack station.
----------------------------------------------------------------------------------------------
Else
   Begin
      
      ------------------------------------------------------------------------
      -- Create a temporary table to be populated with "At Station" Rx groups.
      ------------------------------------------------------------------------
      If Object_Id('TempDb..#AtStationGrps') Is Not Null
         Drop Table #AtStationGrps

      Create Table #AtStationGrps (GroupNum VarChar(12))

      -----------------------------------------------------------------------------------------------------------------
      -- First, retrieve all groups having an active Rx for in the (vial or tote) queue for the specified pack station.
      -----------------------------------------------------------------------------------------------------------------
      If @RouteStop Like 'APK%' Or @RouteStop Like 'SABP%'
         Insert Into
            #AtStationGrps
         Select
            Distinct(O.GroupNum)
         From
            CvyAutoPackStationVialQueue APQ With (NoLock)
               Join
            OeOrder O With (NoLock)
               On O.OrderId = APQ.OrderId And O.HistoryDtTm = APQ.HistoryDtTm
         Where
            APQ.StationId = @RouteStop

      Else
         If @RouteStop Like 'SEMIPK%'
            Insert Into
               #AtStationGrps
            Select
               Distinct(O.GroupNum)
            From
               CvySemiAutoPackStationToteQueue SPQ With (NoLock)
                  Join
               OrderToteAssoc OT With (NoLock)
                  On OT.ToteId = SPQ.ToteId
                  Join
               OeOrder O With (NoLock)
                  On O.OrderId = OT.OrderId
                  Left Join
               #AtStationGrps SG
                  On SG.GroupNum = O.GroupNum
            Where
               SPQ.StationId = @RouteStop
               And
               SG.GroupNum Is Null   -- Only add group# to table if it does not yet exist.

         Else
            If @RouteStop Like 'MP%'
               Insert Into
                  #AtStationGrps
               Select
                  Distinct(O.GroupNum)
               From
                  CvyManPackStationToteQueue MPQ With (NoLock)
                     Join
                  OrderToteAssoc OT With (NoLock)
                     On OT.ToteId = MPQ.ToteId
                     Join
                  OeOrder O With (NoLock)
                     On O.OrderId = OT.OrderId
                     Left Join
                  #AtStationGrps SG
                     On SG.GroupNum = O.GroupNum
               Where
                  MPQ.StationId = @RouteStop
                  And
                  SG.GroupNum Is Null   -- Only add group# to table if it does not yet exist.

      ------------------------------------------------------------------------------------------------------------------------  
      -- Next, if pack station is Manual Pack or Semi-Pack
      --   Add in all groups having any of their Rxs in storage locations (at any pack station OR the specified pack station).
      ------------------------------------------------------------------------------------------------------------------------
      IF  @RouteStop LIKE 'MP%' Or @RouteStop Like 'SEMIPK%'
         BEGIN

            ------------------------------------------------------------------------------------------
            -- If parameter @DisplayRxsStoredInBinsAtAnyStation = 1
            --   Add in all groups having any of their Rxs in storage locations (at any pack station).
            ------------------------------------------------------------------------------------------
            -- Note:  A subsequent filtering will remove groups that do not have all of their Rxs in storage locations.
            --
            If @DisplayRxsStoredInBinsAtAnyStation = 1

               Insert Into
                  #AtStationGrps
               Select
                  Distinct(O.GroupNum)
               From
                  OrdWillCallBin B With (NoLock)
                     Join
                  OeOrder O With (NoLock)
                     On O.OrderId = B.OrderId
                     Left Join
                  #AtStationGrps SG
                     On SG.GroupNum = O.GroupNum
               Where
                  SG.GroupNum Is Null   -- Only add group# to table if it does not yet exist.

            -----------------------------------------------------------------------------------------------------
            -- Add in all groups having an active Rx for in the storage locations for the specified pack station.
            -----------------------------------------------------------------------------------------------------
            Else   -- @DisplayRxsStoredInBinsAtAnyStation = 0

               Insert Into
                  #AtStationGrps
               Select
                  Distinct(O.GroupNum)
               From
                  ShpStationWillCallBinAssoc SW With (NoLock)
                     Join
                  OrdWillCallBin B With (NoLock)
                     On B.BinId Like SW.BinIdPrefix + '%'
                     Join
                  OeOrder O With (NoLock)
                     On O.OrderId = B.OrderId
                     Left Join
                  #AtStationGrps SG
                     On SG.GroupNum = O.GroupNum
               Where
                  SW.StationId = @RouteStop
                  And
                  SG.GroupNum Is Null   -- Only add group# to table if it does not yet exist.

         END -- END, Pack station is Manual Pack or Semi Pack
             --   Add in all groups having any of their Rxs in storage locations (at any pack station).

      ---------------------------------------------------------
      -- Finally, generate a list of active Rxs for the groups.
      ---------------------------------------------------------
      Insert Into
         #RxOrderIds
      Select
         O.OrderId
      From
         #AtStationGrps SG
            Join
         OeOrder O With (NoLock)
            On O.GroupNum = SG.GroupNum
      Where
         O.OrderStatus <> 'Ship Pending'
         And
         O.OrderStatus <> 'Split/MV'     -- Ignore multi-vial Rx parent rows.
         And
         O.OrderId Not Like '%<[1-2]>'   -- Ignore daul-dispenser portion/children Rx rows.

   End   -- No override order# was specified.

-------------------------------------------------------------------------------------------------------------------------
-- For the Rxs, fold in all Rx data pertinent to the aggregated order data needed by the "Package/Ship Orders" form grid.
-------------------------------------------------------------------------------------------------------------------------
-- In an HV system, the system assigns a group's Rxs to a shipment when all Rxs in the group have moved to "Ready To Ship" status
--   and the group does not yet have a shipment.
--
If Object_Id('TempDb..#Rxs') Is Not Null
   Drop Table #Rxs

; With
   CandidateNdcs As (
      Select Distinct OeOrder.Ndc, OeOrder.ProductId
      From #RxOrderIds
      Join OeOrder with (nolock) On OeOrder.OrderId = #RxOrderIds.OrderId
   ),
   HazardousToHandle As (
      Select CandidateNdcs.ProductId
      From CandidateNdcs
      Join InvFlaggedProducts with (nolock) On InvFlaggedProducts.ProductId = CandidateNdcs.ProductId
      Where InvFlaggedProducts.IsFlaggedFor = 'HAZMAT'
   ),
   HazardousToDispose As (
      Select CandidateNdcs.ProductId
      From CandidateNdcs
      Join InvFlaggedProducts with (nolock) On InvFlaggedProducts.ProductId = CandidateNdcs.ProductId
      Where InvFlaggedProducts.IsFlaggedFor = 'HAZDSP'
   ),
   HazardousToShip As (
      Select CandidateNdcs.ProductId
      From CandidateNdcs
      Join InvFlaggedProducts with (nolock) On InvFlaggedProducts.ProductId = CandidateNdcs.ProductId
      Where InvFlaggedProducts.IsFlaggedFor = 'HAZSHP'
   )
Select
   O.GroupNum,
   IsNull(OS.ShipmentId, -1) As ShipmentId,   -- Use the value -1 for a Null shipment ID.  This allows the value to be compared in Where clauses, below.
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
   Case When SW.StationId Is Not Null Then 1 Else 0 End As InStationLoc,            -- 1 = Rx in a storage location at the specified pack station.
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
   ) As PrecedenceString,      -- This value is used to key on a representative Rx for each group.
   Cast(0 As Bit) As IsRepRx,   -- This will get populated below.
   Case When IM.CrossContaminationCategory = 2 Then 1 Else 0 End As IsPenicillin,
   Case When IM.CrossContaminationCategory = 3 Then 1 Else 0 End As IsSulfa,
   Case When HazardousToHandle.ProductId Is Not Null Then 1 Else 0 End As IsHazHandle,
   Case When HazardousToDispose.ProductId Is Not Null Then 1 Else 0 End As IsHazDispose,
   Case When HazardousToShip.ProductId Is Not Null Then 1 Else 0 End As IsHazShip
Into
   #Rxs
From
   #RxOrderIds R
   Join OeOrder O With (NoLock) On O.OrderId = R.OrderId
   Join vInvMasterWithUserOverrides IM with (nolock) On IM.ProductId = O.ProductId
   Join OeOrderPoNumAssoc OP With (NoLock) On OP.OrderId = O.OrderId
   Join OeGroup G With (NoLock) On G.GroupNum = O.GroupNum
   Join OePatientCust P With (NoLock) On P.PatCustId = O.PatCustId
   Join OePriority PR With (NoLock) On PR.PriCode = O.PriCodeSys
   Left Join OeOrderShipmentAssoc As OS With (NoLock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
   Left Join OrderToteAssoc OT With (NoLock) On OT.OrderId = O.OrderId
   Left Join OrdWillCallBin B With (NoLock) On B.OrderId = R.OrderId
   Left Join ShpStationWillCallBinAssoc SW With (NoLock) On SW.StationId = @RouteStop And B.BinId Like SW.BinIdPrefix + '%'
   Left Join CvyAutoPackStationVialQueue APQ With (NoLock) On APQ.OrderId = O.OrderId And APQ.HistoryDtTm = O.HistoryDtTm
   Left Join CvySemiAutoPackStationToteQueue SPQ With (NoLock) On SPQ.ToteId = OT.ToteId
   Left Join CvyManPackStationToteQueue MPQ With (NoLock) On MPQ.ToteId = OT.ToteId
   Left Join OeWorkflowStepInProgress As IPG With (NoLock)  On IPG.ObjectKey = OP.PoNum
   Left Join OeRxPrefLangData As ORPL With (NoLock) On O.OrderId = ORPL.OrderId And O.HistoryDtTm = ORPL.HistoryDtTm
   Left Join HazardousToHandle On HazardousToHandle.ProductId = O.ProductId
   Left Join HazardousToDispose On HazardousToDispose.ProductId = O.ProductId
   Left Join HazardousToShip On HazardousToShip.ProductId = O.ProductId

----------------------------------------------------------------------------
-- For the list of Rxs, identify and flag a representative Rx for each tote.
----------------------------------------------------------------------------
-- "Group", here is the combination of Group# + Shipment ID + Tote ID.
-- This is because a group's Rxs can be split up into more than shipment and
--	more than one tote.
--
-- When Rx-specific data is returned for a tote, it will be pulled from its representative Rx.
--
-- The highest to lowest precedence considerations for choosing a representative Rx:
--   - It has a pack ready status (i.e., "Ready To Ship" or "Verified").
--   - It resides in a pack station queue.
--   - It resides in a storage location.
--   - It resides in a storage location at the specified pack station.
--   - The oldest date/time of its last status change.
--
Update
   R
Set
   IsRepRx = 1
From
   (
      Select
         Min(PrecedenceString) As MinPrecedenceString
      From
         #Rxs
      Group By
         GroupNum,
         ShipmentId,
         ToteId 
   ) RR
      Join
   #Rxs R
      On R.PrecedenceString = RR.MinPrecedenceString
      
-----------------------------------------------------------------
-- From the list of Rxs, aggregate their data to the group level.
-----------------------------------------------------------------
-- "Group", here is the combination of Group# + Shipment ID.
-- This is because a group's Rxs can be split up into more than shipment and more than one tote.
-- NumOfTotedRxs represents the number of Rxs in totes routing to a workstation.
-- Note that some Rxs could be assigned to a tote that has not yet been assigned to a workstation.
--
If Object_Id('TempDb..#GrpData01') Is Not Null
   Drop Table #GrpData01

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
Into
   #GrpData01
From
   #Rxs
Group By
   GroupNum,
   ShipmentId,
   PoNum

-----------------------------------------------------------------------------------------------------------------------
-- Fold representative Rx data into the group data
--   and
-- Set Number of Ready Rxs based on call type.
------------------------------------------------------------------------------------------------------------------------
-- Setting NumOfReadyRxs:
--   The "Package/Ship Orders" form considers an Rx as being "Ready" as follows:
--     Happy-Path (i.e., no Override PO# is specified):
--       When the Rx has a "Ready To Ship" status and resides in a Pack Station Queue or Storage Location.
--     When scanned Rx is not in a Pack Station Queue or Storage Location (i.e., when Override PO# is specified)
--       When the Rx has a "Ready To Ship" status.
------------------------------------------------------------------------------------------------------------------------
-- Setting NumOfPendingOrProblemRxs:
--   The "Package/Ship Orders" form considers a NumOfPendingOrProblemRxs as follows:
--     It is NOT the number of Rxs in the group that are in a "Pending" or "Problem" state!!!
--     It IS the number of Rxs in each tote that are in a "Pending" or "Problem" state.
--			This allows a tote without problem Rxs to be stored while the tote with one
--			or more problem Rxs can be identified and taken to the EXCEPTION station.
------------------------------------------------------------------------------------------------------------------------
If Object_Id('TempDb..#GrpData02') Is Not Null
   Drop Table #GrpData02

Select
   G.GroupNum,
   G.ShipmentId,
   G.PoNum,
   G.NumOfRxs,
   Case When @OverridePoNum = '' Then NumOfReadyStatusArrivedRxs Else NumOfReadyStatusRxs End As NumOfReadyRxs,
   G.NumOfStoredRxs,
   G.NumOfTotedRxs,
   (Select Count(*) From #Rxs Where OrderStatus in ('Pending', 'Problem') And GroupNum = R.GroupNum And ShipmentId = R.ShipmentId And ToteId = R.ToteId And R.QueueStationId Is Not Null ) As NumOfPendingOrProblemRxs,
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
Into
   #GrpData02
From
   #GrpData01 G
      Join
   #Rxs R
      On R.GroupNum = G.GroupNum And R.ShipmentId = G.ShipmentId And IsRepRx = 1
         
--========================================================================================
-- At this point, all of the potential groups are in #GrpData02 with all required Rx data.
--========================================================================================

-----------------------------
-- Retrieve the final result.
-----------------------------
Set NoCount Off

---------------------------------------------------------------
-- Filter the list of groups down based on the input parameters
--   and format data for display in the form's grid.
---------------------------------------------------------------
Select
   G.GroupNum,
   Case G.ShipmentId When -1 Then Null Else G.ShipmentId End As ShipmentId,   -- Turn temporary -1 shipment ID value back into Null.
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
From
   #GrpData02 G
      Join
   OePriority PR With (NoLock)
      On PR.PriCode = G.PriCodeSys
      Left Join
   PwkPrinterTray As PT With (NoLock)
      On PT.ShipmentId = G.ShipmentId And PT.StationId = @RouteStop
Where
   (
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
      -- Always include groups having an Rx from one of the criterias below
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
      @DisplayCompleteOrderStored = 0
      And
      (
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Always include groups having an Rx item in the specified pack station's (vial or tote) queue.
         -- This allows the form to have the user act on retrieving the vial or tote and either
         --   packing its group or storing it into a storage location.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         G.QueueStationId Is Not Null

         Or

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Always include groups having an Rx that has timed out in the specified pack station's storage locations.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         (G.NumOfStoredRxs > 0 And G.LastStatusChgDtTm < DateAdd(Minute, -@PartialShipmentsTimeoutMins, GetDate()))

         Or

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- When @CompleteOrdersOnly = 0, include all of the potential groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         @CompleteOrdersOnly = 0

         Or

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- When @CompleteOrdersOnly = 1, include only all ready Rx groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         (@CompleteOrdersOnly = 1 And G.NumOfReadyRxs = G.NumOfRxs)

         Or

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- When @CompleteOrdersOnly = 1 and @IgnorePendingAndProblemRxsWhenListingCompleteOrders = 1, include only all ready Rx groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         (@CompleteOrdersOnly = 1 And @IgnorePendingAndProblemRxsWhenListingCompleteOrders = 1 And G.NumOfReadyRxs = G.NumOfRxs - G.NumOfPendingOrProblemRxs)
      )
   )

   Or

   (
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
      -- Only include groups having all Rxs stored away in locations.
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
      (@DisplayCompleteOrderStored = 1 And G.NumOfStoredRxs = G.NumOfRxs)
      And
      (
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- When @CompleteOrdersOnly = 0, include all of the potential groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         @CompleteOrdersOnly = 0
         Or
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- When @CompleteOrdersOnly = 1, include only all ready Rx groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         (@CompleteOrdersOnly = 1 And G.NumOfReadyRxs = G.NumOfRxs)
      )
   )

Order By                                                          	-- Order By:
   PpwkPrinted Desc,												-- Has paperwork printed to tray.
   PT.PrintDtTm,													-- Ordered by oldest printed date time to newest.	
   Case QueueStationId When @RouteStop Then 1 Else 0 End Desc,		-- In queue at the specified pack station.
   IsNull(ArrivalDtTm, '9999-12-31'),								-- (Queue or storage loc) arrival time.
   G.PriCodeSys,													-- Priority Code	
   G.PriFillByDtTm												    -- Priority Fill Date	
End

GO
