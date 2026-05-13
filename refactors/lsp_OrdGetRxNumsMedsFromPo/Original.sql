/****** Object:  StoredProcedure [dbo].[lsp_OrdGetRxNumsMedsFromPo]    Script Date: 4/5/2026 8:50:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE Procedure [dbo].[lsp_OrdGetRxNumsMedsFromPo]
   @PoNum VarChar(30)
As
Begin
/*
Object Type:
   Stored Procedure

Object Name:
   lsp_OrdGetRxNumsMedsFromPo

Version:
   22

Description:
   Retrieve Rx numbers and medications from a PO number

Parameters:
   @PoNum - The PO number passed in fromOrdrGetLabelData
*/

If Object_Id(N'tempdb..#tmpPoOrdrId') Is Not Null
    Drop Table #tmpPoOrdrId
If Object_Id(N'tempdb..#tmpOrdSecondaryData') Is Not Null
    Drop Table #tmpOrdSecondaryData
If Object_Id(N'tempdb..#tmpOrders') Is Not Null
    Drop Table #tmpOrders


--Pull only the orderIds associated to the PO number
Select 
   OrderId 
Into #tmpPoOrdrId 
From OeOrderPoNumAssoc With (NoLock) 
Where PoNum = @PoNum

Select 
   OrderId,
   HistoryDtTm,
   CustomerPrice 
Into #tmpOrdSecondaryData
From 
OeOrderSecondaryData With (NoLock) 
Where OrderId in (Select OrderId From #tmpPoOrdrId)

Select 
     OrderId
   , RxNum
   , Medication
   , HistoryDtTm
   , PatCustId 
   , GroupNum
Into #tmpOrders
From 
    OeOrder  With (NoLock)
Where
    OrderId In (Select OrderId From #tmpPoOrdrId)
   And
     OrderStatus Not Like 'Split/%'  -- Ignore multi-vial parent Rx.  This query needs to only find the portion Rx rows that are in the possibly split, sub-order package.

Insert Into #tmpOrders
Select 
     OrderId
   , RxNum
   , Medication
   , HistoryDtTm
   , PatCustId 
   , GroupNum
From 
    OeOrderHistory  With (NoLock)
Where
   OrderId In (Select OrderId From #tmpPoOrdrId)
      And
   OrderStatus <> 'Canceled'
      And
   OrderStatus Not Like 'Split/%'  -- Ignore multi-vial parent Rx.  This query needs to only find the portion Rx rows that are in the possibly split, sub-order package.


Select Distinct
     O.RxNum
   , O.Medication
   , O2.CustomerPrice
   , E.UserDef24
   , E.UserDef31
   , E.UserDef32
   , P2.LanguageCode
   , [IsDiffDrugFromLastFill] = Case IsNull(F.OrderId, '') When '' Then 0 Else 1 End
   , G2.UserDef01
   , G2.UserDef02
   , G2.UserDef03
   , G2.UserDef04
   , G2.UserDef05
   , G2.UserDef06
From
   #tmpPoOrdrId As OP 
   Inner Join #tmpOrders As O With (NoLock)
      On OP.OrderId = O.OrderId 
   Inner Join OeOrderCurrHistoryDtTm As CHDT With (NoLock)
      On CHDT.OrderId = O.OrderId 
            And
         CHDT.HistoryDtTm = O.HistoryDtTm
   Inner Join #tmpOrdSecondaryData O2
      On ((O.OrderId Like '%-[0-9][0-9]' 
             And
          O2.OrderId = Left(O.OrderId, Len(O.OrderId) - 3))  -- For multi-vial portion Rx, join to parent O2 row to retrieve full Rx Customer Price.
             Or
          (O.OrderId Not Like '%-[0-9][0-9]'
             And
           O2.OrderId = O.OrderId))
         And
         O2.HistoryDtTm = O.HistoryDtTm
   Inner Join OePatientCustSecondaryData As P2 With (NoLock)
      On P2.PatCustId = O.PatCustId
   Left Join OeOrderExtUserDef As E With (NoLock)
      On E.OrderId = O.OrderId 
            And
         E.HistoryDtTm = O.HistoryDtTm
   Left Join OeFlaggedRxs As F With (NoLock)
      On F.OrderId = O.OrderId 
            And
         F.HistoryDtTm = O.HistoryDtTm 
            And
         F.IsFlaggedFor = 'Diff Drug From Last Fill'
   Left Join OeGroupUserDef As G2 With (NoLock)
      On G2.GroupNum = O.GroupNum


Drop Table If Exists #tmpPoOrdrId
Drop Table If Exists #tmpOrdSecondaryData
Drop Table If Exists #tmpOrders

End
GO
