--/
ALTER PROCEDURE [dbo].[lsp_OrdGetRxNumsMedsFromPo]
   @PoNum VarChar(30)
As
Begin
/*
Object Type:
   Stored Procedure

Object Name:
   lsp_OrdGetRxNumsMedsFromPo

Version:
   23

Description:
   Retrieve Rx numbers and medications from a PO number.

Comments:
   v23 - Performance refactoring:
      1. Local-variable copy of @PoNum to break parameter sniffing. The cross-list capture
         shows a 1,720x worst-case-to-average ratio on this procedure, which is the
         parameter-sniffing signature for the PoNum filter.
      2. Consolidated the three `If Object_Id ... Drop Table` blocks into a single
         `Drop Table If Exists` at entry, matching the existing `Drop Table If Exists`
         block at exit.
      3. Replaced the three `OrderId In (Select OrderId From #tmpPoOrdrId)` subqueries
         with direct joins to #tmpPoOrdrId, which is now indexed by OrderId.
      4. Combined the OeOrder + OeOrderHistory INSERT pair into a single SELECT INTO
         followed by an INSERT, with the constructed parent-OrderId derivation computed
         once into a ParentOrderId column on #tmpOrders. The original procedure derived
         the parent OrderId via an Or-clause in the final SELECT's O2 join.
      5. Added indexes on the three temp tables to support downstream joins.
      6. Added Option (Recompile) on the three INSERT statements and the final SELECT.
         The 1,720x worst-case ratio indicates plan instability driven by the PoNum
         selectivity variance; Recompile gives the optimizer accurate estimates per call.
*/

Set NoCount On

------------------------------------------------------------------------------------------
-- Local variable.
------------------------------------------------------------------------------------------
Declare @LocalPoNum VarChar(30) = @PoNum

Drop Table If Exists #tmpPoOrdrId, #tmpOrdSecondaryData, #tmpOrders

------------------------------------------------------------------------------------------
-- OrderIds associated with the PO number.
------------------------------------------------------------------------------------------
Select OrderId
Into #tmpPoOrdrId
From OeOrderPoNumAssoc With (NoLock)
Where PoNum = @LocalPoNum
Option (Recompile);

Create Index IX_PoOrdrId_OrderId On #tmpPoOrdrId(OrderId)

------------------------------------------------------------------------------------------
-- Secondary data for the candidate OrderIds.
------------------------------------------------------------------------------------------
Select
      OS.OrderId
    , OS.HistoryDtTm
    , OS.CustomerPrice
Into #tmpOrdSecondaryData
From OeOrderSecondaryData OS With (NoLock)
Inner Join #tmpPoOrdrId P
    On P.OrderId = OS.OrderId
Option (Recompile);

Create Index IX_OrdSec_Key On #tmpOrdSecondaryData(OrderId, HistoryDtTm)

------------------------------------------------------------------------------------------
-- Active + History orders. Derived ParentOrderId is computed once per row so the
-- downstream join to #tmpOrdSecondaryData uses a clean equality predicate instead of
-- the Or-clause "is this a multi-vial portion or a single Rx" check.
------------------------------------------------------------------------------------------
Select
      O.OrderId
    , O.RxNum
    , O.Medication
    , O.HistoryDtTm
    , O.PatCustId
    , O.GroupNum
    , [ParentOrderId] = Case
                          When O.OrderId Like '%-[0-9][0-9]' Then Left(O.OrderId, Len(O.OrderId) - 3)
                          Else O.OrderId
                       End
Into #tmpOrders
From OeOrder O With (NoLock)
Inner Join #tmpPoOrdrId P
    On P.OrderId = O.OrderId
Where O.OrderStatus Not Like 'Split/%'
Option (Recompile);

Insert Into #tmpOrders (OrderId, RxNum, Medication, HistoryDtTm, PatCustId, GroupNum, ParentOrderId)
Select
      O.OrderId
    , O.RxNum
    , O.Medication
    , O.HistoryDtTm
    , O.PatCustId
    , O.GroupNum
    , [ParentOrderId] = Case
                          When O.OrderId Like '%-[0-9][0-9]' Then Left(O.OrderId, Len(O.OrderId) - 3)
                          Else O.OrderId
                       End
From OeOrderHistory O With (NoLock)
Inner Join #tmpPoOrdrId P
    On P.OrderId = O.OrderId
Where O.OrderStatus <> 'Canceled'
  And O.OrderStatus Not Like 'Split/%'
Option (Recompile);

Create Index IX_TmpOrders_Lookup On #tmpOrders(OrderId, HistoryDtTm)
   Include (RxNum, Medication, PatCustId, GroupNum, ParentOrderId)

------------------------------------------------------------------------------------------
-- Final SELECT with the parent-OrderId join now a clean equality.
------------------------------------------------------------------------------------------
Select Distinct
      O.RxNum
    , O.Medication
    , O2.CustomerPrice
    , E.UserDef24
    , E.UserDef31
    , E.UserDef32
    , P2.LanguageCode
    , [IsDiffDrugFromLastFill] = Case When F.OrderId Is Null Then 0 Else 1 End
    , G2.UserDef01
    , G2.UserDef02
    , G2.UserDef03
    , G2.UserDef04
    , G2.UserDef05
    , G2.UserDef06
From #tmpOrders O
Inner Join OeOrderCurrHistoryDtTm CHDT With (NoLock)
    On CHDT.OrderId = O.OrderId
   And CHDT.HistoryDtTm = O.HistoryDtTm
Inner Join #tmpOrdSecondaryData O2
    On O2.OrderId = O.ParentOrderId
   And O2.HistoryDtTm = O.HistoryDtTm
Inner Join OePatientCustSecondaryData P2 With (NoLock)
    On P2.PatCustId = O.PatCustId
Left Join OeOrderExtUserDef E With (NoLock)
    On E.OrderId = O.OrderId
   And E.HistoryDtTm = O.HistoryDtTm
Left Join OeFlaggedRxs F With (NoLock)
    On F.OrderId = O.OrderId
   And F.HistoryDtTm = O.HistoryDtTm
   And F.IsFlaggedFor = 'Diff Drug From Last Fill'
Left Join OeGroupUserDef G2 With (NoLock)
    On G2.GroupNum = O.GroupNum
Option (Recompile);

Drop Table If Exists #tmpPoOrdrId, #tmpOrdSecondaryData, #tmpOrders

End
GO
