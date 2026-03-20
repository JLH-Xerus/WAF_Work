--/
CREATE PROCEDURE lsp_ShpGetOrdersForTopReadyToShipGroup 
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_ShpGetOrdersForTopReadyToShipGroup

Version:
   24

Description:
   Retrieve a list of qualifying Groups with Rxs that qualify as 'Ready' for a shipment id
   
   Typically, the retrieved group is the top priority group:
     - Whose Group has a Status of "Ready" that does not yet have a Shipment.
   
     Note:  A Group�s Status moves to "Ready" when all of its Rxs are in one of the following statuses:
            - "Ready For Pick Up", "Ready To Ship", "Ship Pending"
   
   However, the retrieved group can also be a group with an Rx that has timed out in a storage location or tote.
   
   This stored procedure will return all of these (good + timed out groups) ordered by PriInternal to make sure that the highest\
   priority items will be taken care of first by the Auto Shipper

Parameters:
   Input:
      None
   Output:
      None

Return Value:
   List of Groups with OrderId ready for a shipment id

Comments:
   None
*/
Set NoCount On

Declare @CfgShippingPartialShipmentsTimeoutInMinutes Int

-- Save off the value of "System-wide [Shipping] PartialShipmentsTimeoutInMinutes"
Select @CfgShippingPartialShipmentsTimeoutInMinutes = Convert(int, value)
From vCfgSystemParamVal with (nolock)
Where Section = 'Shipping' and Parameter = 'PartialShipmentsTimeoutInMinutes'

--====================================================
-- Part 1:  Look for (happy-path) all-ready-Rxs Group.
--====================================================

-------------------------------------------------------------------------------------------------------------------------------
-- Retrieve a list of Rx Groups:
--    - Having at least one Rx in "Ready To Ship" status.
--       And
--    - Not yet having a Shipment.
--       And
--    - Whose Group's Status has been set to "Ready" (indicating that all of the Rx's Group's Rxs are in "Ready To Ship" status)
--      for more than 5 seconds.
--       And
--   - A history Rx instance (moved to history within the last minute) does not exist for the group which has no matching
--     active Rx instance. (This accounts for a reschedule action taking a significant amount of time due to network latency)
-------------------------------------------------------------------------------------------------------------------------------
-- While at it, grab each Group's (Internal) Priority.
--
-- Dirty Read / Database Deadlock Note:
-- ------------------------------------
-- This query used to be two queries where:
--   Query 1 retrieved a list of groups having at least one Rx in "Ready To Ship" status (not yet having a shipment).
--   Query 2 retrieved a list of groups having at least one Rx in a status other than "Ready To Ship" (not yet having a shipment).
-- These two queries were "combined" to get groups having all of their Rxs in "Ready To Ship" status (not yet having a shipment).
-- This, however, had one of two issues:
--   1. With "NoLock" specified on the OeOrder table, a dirty read frequently cause query 1 to be "blind" to the existence of an Rx that was in the process of having its status changed.
--      This caused a shipment to be created for a subset of the Rxs in the group.  This caused split shipments / multiple packages.
--   2. With "NoLock" removed from the OeOrder table, query 1 encountered frequent database deadlocks (with the update of Rx status).
-- So, this query was reverted to acting on the Group Status (of "Ready").  This may push the dirty read issue to an Rx changing status (and its group status being updated).
-- However that will minimize the chances of this far below the frequent occurrences that occurred within this Auto-Shipper query.
-- As yet another dirty read fix, added in that the Group Status has not changed within the last 5 seconds.  This was necessary to cover a case where all Rxs but one in a group were ready,
-- and the non-ready Rx was rescheduled.  This presented a millisecond window where the query was "blind" to the rescheduled Rx when its row was moved from the OeOrder to OeOrderHistory
-- and a new row has not quite been added to table OeOrder.  Since the momentary deletion of the row from OeOrder updates the Group's Status (to "Ready", momentarily), waiting for a
-- Group's Status to not have changed for 5 seconds allows this query to avoid picking up such a Group during an Rx's rescheduling process.
--
If Object_Id('TempDb..#GroupsHavingAllReadyToShipRx') Is Not Null
   Drop Table #GroupsHavingAllReadyToShipRx

Select
   'All Rxs Ready' As GroupType, O.GroupNum, Min(O.PriInternal) As PriInternal, Min(O.DateFilled) As DateFilled
Into
   #GroupsHavingAllReadyToShipRx
From
   OeOrder O With (NoLock)
   Join OeGroup G With (NoLock) On G.GroupNum = O.GroupNum
   Left Join OeOrderShipmentAssoc OS With (NoLock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
Where
   O.OrderStatus = 'Ready To Ship'
   And OS.ShipmentId Is Null                                   -- Rx does not have a Shipment.
   And G.Status = 'Ready'                                      -- All Rxs in Group (not having Shipment) are at or beyond "Ready".  (See header comment.)
   And G.LastStatusChgDtTm < DateAdd(Second, -5, GetDate())    -- Group's Status has been "Ready" for more than 5 seconds.
   And Not Exists (                                            -- A history Rx instance (moved to history within the last minute) does not exist for the group which has no matching active Rx instance
      Select 1
      From OeOrderCurrHistoryDtTm As OC with (nolock)
      Join OeOrderHistory As OH with (nolock) On OH.OrderId = OC.OrderId And OH.HistoryDtTm = OC.HistoryDtTm
      Where OH.GroupNum = O.GroupNum
      And DateAdd(Minute, 1, OH.HistoryDtTm) > GetDate()
   )
Group By
   O.GroupNum Option (Maxdop 1)

--==========================================================
-- Part 2:  Look for Rx timed-out in storage location Group.
--==========================================================

-----------------------------------------------------------------------------------------------------------
-- Retrieve a list of Rx Groups:
--   - Having at least one Rx in "Ready To Ship" status (or "Verified" status that is a multi-vial portion)
--       that has been in a storage location longer than the time-out window.
--       And
--   - Not yet having a Shipment.
-----------------------------------------------------------------------------------------------------------
-- While at it, grab each Group's (Internal) Priority.
--
If Object_Id('TempDb..#GroupsHavingTimedOutInLocRx') Is Not Null
   Drop Table #GroupsHavingTimedOutInLocRx

Select
   'Timed Out In Loc' As GroupType, O.GroupNum, Min(O.PriInternal) As PriInternal, Min(O.DateFilled) As DateFilled
Into
   #GroupsHavingTimedOutInLocRx
From
   OeOrder O With (NoLock)
   Join OrdWillCallBin B With (NoLock) On B.OrderId = O.OrderId
   Left Join OeOrderShipmentAssoc OS With (NoLock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
Where
   (
      (O.OrderStatus = 'Ready To Ship' And Not O.OrderId Like '%-[0-9][0-9]') Or              
      (O.OrderId Like '%-[0-9][0-9]' And (Select 1 From OeOrder With (NOLOCK) Where OrderId = Left(O.OrderId, Len(O.OrderId) -3) And OrderStatus = 'Split/MV' And StatusReason = 'Ready To Ship') = 1)
   )
   And B.InDtTm <= DateAdd(Minute, -@CfgShippingPartialShipmentsTimeoutInMinutes, GetDate())  -- In storage location longer than timeout window.
   And OS.ShipmentId Is Null                                       -- Rx does not have a Shipment.
Group By
   O.GroupNum

--==============================================
-- Part 3:  Look for Rx timed-out in tote Group.
--==============================================

-----------------------------------------------------------------------------------------------------------
-- Retrieve a list of Rx Groups:
--   - Having at least one Rx in "Ready To Ship" status (or "Verified" status that is a multi-vial portion)
--       that has been in a tote longer than the time-out window.
--       And
--   - Not yet having a Shipment.
-----------------------------------------------------------------------------------------------------------
-- While at it, grab each Group's (Internal) Priority.
--
If Object_Id('TempDb..#GroupsHavingTimedOutInToteRx') Is Not Null
   Drop Table #GroupsHavingTimedOutInToteRx

Select
   'Timed Out In Tote' As GroupType, O.GroupNum, Min(O.PriInternal) As PriInternal, Min(O.DateFilled) As DateFilled
Into
   #GroupsHavingTimedOutInToteRx
From
   OeOrder O With (NoLock)
   Join OrderToteAssoc OT With (NoLock) On OT.OrderId = O.OrderId And OT.OrderToteStateCode = 'I'
   Left Join OeOrderShipmentAssoc OS With (NoLock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
Where
   (
      (O.OrderStatus = 'Ready To Ship' And Not O.OrderId Like '%-[0-9][0-9]') Or              
      (O.OrderId Like '%-[0-9][0-9]' And (Select 1 From OeOrder With (NOLOCK) Where OrderId = Left(O.OrderId, Len(O.OrderId) -3) And OrderStatus = 'Split/MV' And StatusReason = 'Ready To Ship') = 1)
   )
   And O.LastStatusChgDtTm <= DateAdd(Minute, -@CfgShippingPartialShipmentsTimeoutInMinutes, GetDate())  -- In tote longer than timeout window (softly).  Really based on Rx status change.
   And OS.ShipmentId Is Null                                                  -- Rx does not have a Shipment.
Group By
   O.GroupNum
      

--============================================================
-- Part 4:  Return all Groups and Rxs Ready for a shipment id
--============================================================
Select GRx.GroupType, GRx.GroupNum, GRx.PriInternal, GRx.DateFilled, 
       STUFF((Select N',' + O.OrderId 
              From OeOrder O With (NoLock)
               Join OeOrderPoNumAssoc OP With (NoLock) On OP.OrderId = O.OrderId
                   Left Join OeOrderShipmentAssoc OS with (nolock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
              Where O.GroupNum = GRx.GroupNum
               And OS.ShipmentId Is NULL
               And (
                        (O.OrderStatus = 'Ready To Ship' And Not O.OrderId Like '%-[0-9][0-9]') 
                        Or (O.OrderId Like '%-[0-9][0-9]' And (Select 1 From OeOrder With (NOLOCK) Where OrderId = Left(O.OrderId, Len(O.OrderId) -3) And OrderStatus = 'Split/MV' And StatusReason = 'Ready To Ship') = 1)
                        )
               Order By O.OrderId
      FOR XML PATH(''), TYPE).value(N'.[1]', N'nVarChar(max)'), 1, 1, N'') As Orders
From #GroupsHavingAllReadyToShipRx GRx
Union
Select GRx.GroupType, GRx.GroupNum, GRx.PriInternal, GRx.DateFilled,
       STUFF((Select N',' + O.OrderId 
              From OeOrder O With (NoLock)
               Join OeOrderPoNumAssoc OP With (NoLock) On OP.OrderId = O.OrderId
                   Left Join OeOrderShipmentAssoc OS with (nolock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
              Where O.GroupNum = GRx.GroupNum
               And OS.ShipmentId Is NULL
               And (
                        (O.OrderStatus = 'Ready To Ship' And Not O.OrderId Like '%-[0-9][0-9]') 
                        Or (O.OrderId Like '%-[0-9][0-9]' And (Select 1 From OeOrder With (NOLOCK) Where OrderId = Left(O.OrderId, Len(O.OrderId) -3) And OrderStatus = 'Split/MV' And StatusReason = 'Ready To Ship') = 1)
                        )
               Order By O.OrderId
      FOR XML PATH(''), TYPE).value(N'.[1]', N'nVarChar(max)'), 1, 1, N'') As Orders
From #GroupsHavingTimedOutInLocRx GRx
Union
Select GRx.GroupType, GRx.GroupNum, GRx.PriInternal, GRx.DateFilled,
       STUFF((Select N',' + O.OrderId 
              From OeOrder O With (NoLock)
               Join OeOrderPoNumAssoc OP With (NoLock) On OP.OrderId = O.OrderId
                   Left Join OeOrderShipmentAssoc OS with (nolock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
              Where O.GroupNum = GRx.GroupNum
               And OS.ShipmentId Is NULL
               And (
                        (O.OrderStatus = 'Ready To Ship' And Not O.OrderId Like '%-[0-9][0-9]') 
                        Or (O.OrderId Like '%-[0-9][0-9]' And (Select 1 From OeOrder With (NOLOCK) Where OrderId = Left(O.OrderId, Len(O.OrderId) -3) And OrderStatus = 'Split/MV' And StatusReason = 'Ready To Ship') = 1)
                        )
               Order By O.OrderId
      FOR XML PATH(''), TYPE).value(N'.[1]', N'nVarChar(max)'), 1, 1, N'') As Orders
From #GroupsHavingTimedOutInToteRx GRx
Order By GRx.DateFilled Asc

End

/
