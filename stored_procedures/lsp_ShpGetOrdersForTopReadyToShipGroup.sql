--/
ALTER PROCEDURE [dbo].[lsp_ShpGetOrdersForTopReadyToShipGroup]
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_ShpGetOrdersForTopReadyToShipGroup

Version:
   25

Description:
   Retrieve a list of qualifying Groups with Rxs that qualify as 'Ready' for a shipment id

   Typically, the retrieved group is the top priority group:
     - Whose Group has a Status of "Ready" that does not yet have a Shipment.

     Note:  A Group's Status moves to "Ready" when all of its Rxs are in one of the following statuses:
            - "Ready For Pick Up", "Ready To Ship", "Ship Pending"

   However, the retrieved group can also be a group with an Rx that has timed out in a storage location or tote.

   This stored procedure will return all of these (good + timed out groups) ordered by PriInternal to make sure that the highest
   priority items will be taken care of first by the Auto Shipper

Parameters:
   Input:
      None
   Output:
      None

Return Value:
   List of Groups with OrderId ready for a shipment id

Comments:
   v25 - Performance refactoring:
      1. Consolidated 3 separate temp tables (#GroupsHavingAllReadyToShipRx,
         #GroupsHavingTimedOutInLocRx, #GroupsHavingTimedOutInToteRx) into a
         single #AllQualifyingGroups temp table via UNION ALL, eliminating 3
         separate scans of OeOrder and association tables.
      2. Extracted the multi-vial split parent check (correlated scalar subquery
         repeated in Parts 2, 3, and the FOR XML PATH) into a CTE-based temp
         table #MultiVialReadyParents computed once and reused via EXISTS.
      3. Collapsed the triple-repeated FOR XML PATH correlated subquery into a
         single execution against #AllQualifyingGroups. The original ran the
         identical 3-table correlated join (OeOrder + OeOrderPoNumAssoc +
         OeOrderShipmentAssoc) with the same WHERE clause 3 times — once per
         UNION branch. Now it runs once.
      4. Replaced legacy Object_Id temp table checks with DROP TABLE IF EXISTS.
      5. Added WITH (NoLock) consistently.
*/
Set NoCount On

Declare @CfgShippingPartialShipmentsTimeoutInMinutes Int

-- Save off the value of "System-wide [Shipping] PartialShipmentsTimeoutInMinutes"
Select @CfgShippingPartialShipmentsTimeoutInMinutes = Convert(Int, Value)
From vCfgSystemParamVal With (NoLock)
Where Section = 'Shipping' And Parameter = 'PartialShipmentsTimeoutInMinutes'

------------------------------------------------------------------------------------------
-- Pre-compute multi-vial split parents that are in 'Ready To Ship' derived status.
-- This replaces the correlated scalar subquery that was repeated in Parts 2, 3, and 4:
--   (Select 1 From OeOrder WHERE OrderId = Left(O.OrderId, Len(O.OrderId) -3)
--      And OrderStatus = 'Split/MV' And StatusReason = 'Ready To Ship')
-- Now we materialize these parent OrderIds once and look them up via EXISTS.
------------------------------------------------------------------------------------------
Drop Table If Exists #MultiVialReadyParents

Select OrderId
Into #MultiVialReadyParents
From OeOrder With (NoLock)
Where OrderStatus = 'Split/MV'
  And StatusReason = 'Ready To Ship'

-- Index for fast lookups in the WHERE EXISTS below
Create Clustered Index IX_MVRP On #MultiVialReadyParents (OrderId)

------------------------------------------------------------------------------------------
-- Collect all qualifying groups into a single temp table.
-- Three branches, each producing the same shape: GroupType, GroupNum, PriInternal, DateFilled
------------------------------------------------------------------------------------------
Drop Table If Exists #AllQualifyingGroups

;With QualifyingGroups As
(
    --====================================================
    -- Part 1: Happy-path — all-ready-Rxs Group.
    --====================================================
    -- Groups where Status = 'Ready' for >5 seconds, all Rxs are 'Ready To Ship',
    -- no existing shipment, and no recent history-only Rx in the group.
    Select
       'All Rxs Ready' As GroupType, O.GroupNum, Min(O.PriInternal) As PriInternal, Min(O.DateFilled) As DateFilled
    From
       OeOrder O With (NoLock)
       Inner Join OeGroup G With (NoLock) On G.GroupNum = O.GroupNum
       Left Join OeOrderShipmentAssoc OS With (NoLock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
    Where
       O.OrderStatus = 'Ready To Ship'
       And OS.ShipmentId Is Null
       And G.Status = 'Ready'
       And G.LastStatusChgDtTm < DateAdd(Second, -5, GetDate())
       And Not Exists (
          Select 1
          From OeOrderCurrHistoryDtTm As OC With (NoLock)
          Inner Join OeOrderHistory As OH With (NoLock) On OH.OrderId = OC.OrderId And OH.HistoryDtTm = OC.HistoryDtTm
          Where OH.GroupNum = O.GroupNum
          And DateAdd(Minute, 1, OH.HistoryDtTm) > GetDate()
       )
    Group By
       O.GroupNum
    Option (Maxdop 1)

    Union All

    --==========================================================
    -- Part 2: Rx timed-out in storage location Group.
    --==========================================================
    -- Groups with at least one Rx (or multi-vial portion) that has been in a
    -- will-call bin longer than the configured timeout.
    Select
       'Timed Out In Loc' As GroupType, O.GroupNum, Min(O.PriInternal) As PriInternal, Min(O.DateFilled) As DateFilled
    From
       OeOrder O With (NoLock)
       Inner Join OrdWillCallBin B With (NoLock) On B.OrderId = O.OrderId
       Left Join OeOrderShipmentAssoc OS With (NoLock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
    Where
       (
          (O.OrderStatus = 'Ready To Ship' And Not O.OrderId Like '%-[0-9][0-9]')
          Or
          (O.OrderId Like '%-[0-9][0-9]' And Exists (Select 1 From #MultiVialReadyParents P Where P.OrderId = Left(O.OrderId, Len(O.OrderId) - 3)))
       )
       And B.InDtTm <= DateAdd(Minute, -@CfgShippingPartialShipmentsTimeoutInMinutes, GetDate())
       And OS.ShipmentId Is Null
    Group By
       O.GroupNum

    Union All

    --==============================================
    -- Part 3: Rx timed-out in tote Group.
    --==============================================
    -- Groups with at least one Rx (or multi-vial portion) that has been in a
    -- tote longer than the configured timeout.
    Select
       'Timed Out In Tote' As GroupType, O.GroupNum, Min(O.PriInternal) As PriInternal, Min(O.DateFilled) As DateFilled
    From
       OeOrder O With (NoLock)
       Inner Join OrderToteAssoc OT With (NoLock) On OT.OrderId = O.OrderId And OT.OrderToteStateCode = 'I'
       Left Join OeOrderShipmentAssoc OS With (NoLock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
    Where
       (
          (O.OrderStatus = 'Ready To Ship' And Not O.OrderId Like '%-[0-9][0-9]')
          Or
          (O.OrderId Like '%-[0-9][0-9]' And Exists (Select 1 From #MultiVialReadyParents P Where P.OrderId = Left(O.OrderId, Len(O.OrderId) - 3)))
       )
       And O.LastStatusChgDtTm <= DateAdd(Minute, -@CfgShippingPartialShipmentsTimeoutInMinutes, GetDate())
       And OS.ShipmentId Is Null
    Group By
       O.GroupNum
)
Select GroupType, GroupNum, PriInternal, DateFilled
Into #AllQualifyingGroups
From QualifyingGroups

--============================================================
-- Part 4: Return all Groups and Rxs Ready for a shipment id.
--
-- Single FOR XML PATH execution replaces the 3 identical
-- correlated subqueries from the original v24.
--============================================================
Select
     GRx.GroupType
   , GRx.GroupNum
   , GRx.PriInternal
   , GRx.DateFilled
   , Stuff((
        Select N',' + O.OrderId
        From OeOrder O With (NoLock)
        Inner Join OeOrderPoNumAssoc OP With (NoLock) On OP.OrderId = O.OrderId
        Left Join OeOrderShipmentAssoc OS With (NoLock) On OS.OrderId = O.OrderId And OS.HistoryDtTm = O.HistoryDtTm
        Where O.GroupNum = GRx.GroupNum
          And OS.ShipmentId Is Null
          And (
                  (O.OrderStatus = 'Ready To Ship' And Not O.OrderId Like '%-[0-9][0-9]')
                  Or
                  (O.OrderId Like '%-[0-9][0-9]' And Exists (Select 1 From #MultiVialReadyParents P Where P.OrderId = Left(O.OrderId, Len(O.OrderId) - 3)))
              )
        Order By O.OrderId
     For Xml Path(''), Type).value(N'.[1]', N'nVarChar(max)'), 1, 1, N'') As Orders
From #AllQualifyingGroups GRx
Order By GRx.DateFilled Asc

-- Cleanup
Drop Table If Exists #AllQualifyingGroups
Drop Table If Exists #MultiVialReadyParents

End
GO
/
