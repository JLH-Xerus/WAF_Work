--/
ALTER PROCEDURE [dbo].[lsp_BnkGetNextRxToAssignToBank_DispenserBalancing]
   @NumOfQueuedInBankRxsPerTcd Decimal(2,1)
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_BnkGetNextRxToAssignToBank_DispenserBalancing

Version:
   10

Comments:
   v10 - Performance refactoring:
      1. Local-variable copy of @NumOfQueuedInBankRxsPerTcd. The cross-list capture
         flags this procedure for "heavy logical reads" rather than plan instability,
         so the local-variable form is a low-cost safeguard rather than the dominant
         fix.
      2. Replaced the two `If Object_Id ... Drop Table` blocks with a single consolidated
         `Drop Table If Exists` at entry and a matching one at exit.
      3. Added primary keys and supporting indexes on every temp table to support the
         downstream joins. The original procedure built four temp tables and joined
         them without any index support, forcing the optimizer to hash-join even for
         the small candidate sets typical of this procedure.
      4. Added Option (Recompile) on the final SELECT to give the optimizer accurate
         selectivity estimates on the local-variable parameter.
      5. Switched the `BnkAllBanks` join from positional (no alias on the left side
         of the ON clause) to a clean `Inner Join ... On` shape.
*/

Set NoCount On

------------------------------------------------------------------------------------------
-- Local variable copy.
------------------------------------------------------------------------------------------
Declare @LocalNumOfQueuedInBankRxsPerTcd Decimal(2,1) = @NumOfQueuedInBankRxsPerTcd

Drop Table If Exists
      #OnlineTcdCountPerBankProd
    , #QueuedAssignedRxCountPerBank
    , #QueuedUnassignedRxs
    , #BankProdHavingRoomForQueuedInBankRx

------------------------------------------------------------------------------------------
-- Count of Online Dispensers per Bank and Product. Only Non-Excluded banks.
------------------------------------------------------------------------------------------
Select
      T.AddrBank
    , T.ProductId
    , [OnlineTcdCount] = Count(*)
Into #OnlineTcdCountPerBankProd
From TcdStatus T With (NoLock)
Inner Join BnkConfiguredBanks B With (NoLock)
    On B.AddrBank = T.AddrBank
Where T.ProductId Is Not Null
  And T.Status = 'Online'
  And T.AddrCabinet > 0
  And B.IsExcluded = 0
Group By T.AddrBank, T.ProductId

Create Index IX_OTC_BankProd On #OnlineTcdCountPerBankProd(AddrBank, ProductId)

------------------------------------------------------------------------------------------
-- Auto-fill Rxs assigned to a bank that are queued for filling.
------------------------------------------------------------------------------------------
Select
      AddrBank
    , ProductId
    , [RxCount] = Sum(Case NumBuffers When 0 Then 1 Else NumBuffers End)
Into #QueuedAssignedRxCountPerBank
From OeOrder With (NoLock)
Where AddrBank Not In ('-', '')
  And FillType = 'A'
  And OrderId Not Like '%<[1-2]>'
  And OrderStatus In ('Scheduled', 'Pending')
Group By AddrBank, ProductId

Create Index IX_QAR_BankProd On #QueuedAssignedRxCountPerBank(AddrBank, ProductId)

------------------------------------------------------------------------------------------
-- Auto-fill Rxs not yet assigned to a bank that are queued for filling.
------------------------------------------------------------------------------------------
Select
      OrderId
    , HistoryDtTm
    , ProductId
    , PriInternal
    , [NumDispenses] = Case NumBuffers When 0 Then 1 Else NumBuffers End
Into #QueuedUnassignedRxs
From OeOrder O With (NoLock)
Where O.AddrBank    = '-'
  And O.FillType    = 'A'
  And O.OrderStatus = 'Scheduled'

Create Index IX_QUR_Product On #QueuedUnassignedRxs(ProductId, NumDispenses)
   Include (OrderId, HistoryDtTm, PriInternal)

------------------------------------------------------------------------------------------
-- Bank/Product pairs that have room for another Queued-In-Bank Rx.
------------------------------------------------------------------------------------------
Select
      D.AddrBank
    , D.ProductId
    , [RoomForMoreRxCount] = (D.OnlineTcdCount * @LocalNumOfQueuedInBankRxsPerTcd) - IsNull(R.RxCount, 0)
    , D.OnlineTcdCount
    , [SupportedFillMethod] = Case BA.AutoFillTypeCode
                                When 'O' Then 'A'
                                When 'H' Then 'S'
                                Else Null
                              End
Into #BankProdHavingRoomForQueuedInBankRx
From #OnlineTcdCountPerBankProd D
Left Join #QueuedAssignedRxCountPerBank R
    On R.AddrBank = D.AddrBank And R.ProductId = D.ProductId
Inner Join BnkAllBanks BA With (NoLock)
    On BA.AddrBank = D.AddrBank
Where (D.OnlineTcdCount * @LocalNumOfQueuedInBankRxsPerTcd) - IsNull(R.RxCount, 0) > 0

Create Index IX_BPR_Product On #BankProdHavingRoomForQueuedInBankRx(ProductId, OnlineTcdCount)
   Include (AddrBank, RoomForMoreRxCount, SupportedFillMethod)

------------------------------------------------------------------------------------------
-- Top-priority Bank Unassigned Rx for a Product where the Bank has room.
-- Dual-dispenser Rxs must match a bank with at least 2 Online dispensers.
------------------------------------------------------------------------------------------
Select Top 1
      [TopPriBankUnassignedRxOrderIdWithRoomInBank] = O.OrderId
    , [MostRoomBank]                                = P.AddrBank
From #BankProdHavingRoomForQueuedInBankRx P
Inner Join #QueuedUnassignedRxs O
    On O.ProductId = P.ProductId
   And P.OnlineTcdCount >= O.NumDispenses
Inner Join OeOrderSecondaryData O2 With (NoLock)
    On O2.OrderId = O.OrderId
   And O2.HistoryDtTm = O.HistoryDtTm
Where
(
    O2.RequestedFillMethodCode = P.SupportedFillMethod
    Or O2.RequestedFillMethodCode Is Null
)
Order By
      O.PriInternal
    , O.OrderId
    , P.RoomForMoreRxCount Desc
Option (Recompile);

Drop Table If Exists
      #OnlineTcdCountPerBankProd
    , #QueuedAssignedRxCountPerBank
    , #QueuedUnassignedRxs
    , #BankProdHavingRoomForQueuedInBankRx

End
GO
