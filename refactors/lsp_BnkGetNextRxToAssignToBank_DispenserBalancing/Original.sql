/****** Object:  StoredProcedure [dbo].[lsp_BnkGetNextRxToAssignToBank_DispenserBalancing]    Script Date: 4/5/2026 8:50:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[lsp_BnkGetNextRxToAssignToBank_DispenserBalancing]
   @NumOfQueuedInBankRxsPerTcd Decimal(2,1)
As
/*
Object Type:
   Stored Procedure

Name:
   lsp_BnkGetNextRxToAssignToBank_DispenserBalancing

Version:
   9

Description:
   Retrieve the next Rx to assign and its best cabinet bank (when in "Dispenser Balancing" bank assignment mode).
   This will be the Top Priority Bank Unassigned Rx for a Product where a Non-Excluded Bank that has room for another Queued-In-Bank Rx based on Rxs queued for per online dispenser product.

Parameters:
   Input:
      @NumOfQueuedInBankRxsPerTcd   - The number of Rxs, for each product, to assigned to each bank, at any given time, per online dispenser for the product.
   Output:
      None

Return Value:
   A single-row result set containing the Order ID of the Rx to assign and the Bank Letter of the best cabinet bank to assign it to.
   If there no Bank has room for another Rx from the Unassigned Bank pool, an empty result set is returned.
   
   Example:
   
      TopPriBankUnassignedRxOrderIdWithRoomInBank  BestBank
      -------------------------------------------  --------
      600123456                                    D

Comments:
   None
*/
Set NoCount On


-----------------------------------------------------------------------------
-- Retrieve a Count of Online Dispensers per Bank and Product.
-----------------------------------------------------------------------------
-- Only include Non-Excluded banks.
-- (An Excluded bank is one where the user has unchecked a checkbox that controls the assignment of Rxs to a bank).
--
If Object_Id('TempDb..#OnlineTcdCountPerBankProd') Is Not Null
   Drop Table #OnlineTcdCountPerBankProd

Select
   T.AddrBank,
   T.ProductId,
   Count(*) As OnlineTcdCount
Into
   #OnlineTcdCountPerBankProd
From
   TcdStatus T With (NoLock)
      Join
   BnkConfiguredBanks As B With (NoLock)
      On T.AddrBank = B.AddrBank           -- Include only banks configured in the system.
Where
   ProductId Is Not Null         -- Include only NDC Assigned dispensers.
   And
   Status = 'Online'             -- Include only Online dispensers.
   And
   T.AddrCabinet > 0             -- Include only in-cabinet dispensers.
   And
   B.IsExcluded = 0              -- Include only "non-excluded" banks.
Group By
   T.AddrBank,
   T.ProductId


-- Save off a list of auto-fill Rxs that have been assigned a bank that are queued for filling
Drop Table If Exists #QueuedAssignedRxCountPerBank
Select
   AddrBank,
   ProductId,
   Sum(Case NumBuffers When 0 Then 1 Else NumBuffers End) As RxCount
Into
   #QueuedAssignedRxCountPerBank
From
   OeOrder With (NoLock)
Where
   AddrBank Not In ('-', '')
   And
   FillType = 'A'
   And
   OrderId Not Like '%<[1-2]>'       -- Exclude Dual-Dispenser Portion Rx records.
   And
   OrderStatus In ('Scheduled', 'Pending')
Group By
   AddrBank,
   ProductId


-- Save off a list of auto-fill Rxs that have NOT been assigned a bank that are queued for filling
Drop Table If Exists #QueuedUnassignedRxs
Select
   OrderId,
   HistoryDtTm,
   ProductId,
   PriInternal,
   Case NumBuffers When 0 Then 1 Else NumBuffers End As NumDispenses
Into
   #QueuedUnassignedRxs
From
   OeOrder O With (NoLock)
Where
   O.AddrBank = '-'
   And
   O.FillType = 'A'
   And
   O.OrderStatus ='Scheduled'


---------------------------------------------------------------------------------------------------------------
-- Retrieve a list of Bank/Product that have room for another Queued-In-Bank Rx
-- (i.e., Bank/Product that have less than the configured Number Of Queued-in-Bank Rxs per Dispenser).
---------------------------------------------------------------------------------------------------------------
If Object_Id('TempDb..#BankProdHavingRoomForQueuedInBankRx') Is Not Null
   Drop Table #BankProdHavingRoomForQueuedInBankRx

Select
   D.AddrBank,
   D.ProductId,
   (D.OnlineTcdCount * @NumOfQueuedInBankRxsPerTcd) - IsNull(R.RxCount, 0) As RoomForMoreRxCount,
   D.OnlineTcdCount,
   Case BA.AutoFillTypeCode
      When 'O' Then 'A'     -- Omnidirectional Robot is fill method Auto-Fill
      When 'H' Then 'S'     -- Human Fill Type Code is Fill Method Semi-Auto-Fill
      Else Null
   End SupportedFillMethod
Into
   #BankProdHavingRoomForQueuedInBankRx
From
   #OnlineTcdCountPerBankProd D
      Left Join
   #QueuedAssignedRxCountPerBank R
      On R.AddrBank = D.AddrBank And R.ProductId = D.ProductId
   join
   BnkAllBanks BA on D.AddrBank = BA.AddrBank
Where
   (D.OnlineTcdCount * @NumOfQueuedInBankRxsPerTcd) - IsNull(R.RxCount, 0) > 0  -- Include only Bank/Products having room for one or more Rxs.


----------------------------------------------------------------------------------------------------------------------------------------
-- Retrieve the Top-Priority Bank Unassigned Rx for a Product where the Bank that has room for another Queued-In-Bank Rx.
----------------------------------------------------------------------------------------------------------------------------------------
-- For a dual-dispenser Rx only include it with a bank having 2 or more Online dispensers.
-- For it, return the Bank having the most room for the product.
--
Select
   Top 1
      O.OrderId As TopPriBankUnassignedRxOrderIdWithRoomInBank,
      P.AddrBank As MostRoomBank
From
   #BankProdHavingRoomForQueuedInBankRx P
   Join #QueuedUnassignedRxs O
      On O.ProductId = P.ProductId And P.OnlineTcdCount >= O.NumDispenses  -- Rxs for Products in a Bank that has room for another Queued-In-Bank Rx and enough Online dispensers for Dual-Dispenser Rx.
   Join OeOrderSecondaryData O2 with (nolock)
      On O2.OrderId = O.OrderId And O2.HistoryDtTm = O.HistoryDtTm
Where
   (  -- check to see if requested fill method matches the bank's supported fill method
      O2.RequestedFillMethodCode = P.SupportedFillMethod
      OR
      O2.RequestedFillMethodCode is Null   -- if no fill method requested, we always match
   )
Order By
   O.PriInternal, O.OrderId, P.RoomForMoreRxCount Desc
GO
