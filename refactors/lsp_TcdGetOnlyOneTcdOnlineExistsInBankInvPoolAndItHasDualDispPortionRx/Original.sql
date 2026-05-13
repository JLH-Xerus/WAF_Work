/****** Object:  StoredProcedure [dbo].[lsp_TcdGetOnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx]    Script Date: 4/5/2026 8:50:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE Procedure [dbo].[lsp_TcdGetOnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx]
  @ProductId VarChar(11),
  @InvPool TinyInt,
  @AddrBank VarChar(2),
  @OnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx TinyInt Output
As
----------------------------------------------------------------------------------------------------
-- Procedure:
--        lsp_TcdGetOnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx
--
-- Description:
--        Retrieve whether or not only 1 dispenser exists for a product within a specified
--          bank and inventory pool and the dispenser actively contains a dual-dispenser portion Rx.
--
-- Parameters:
--        @ProductId - VarChar(11)
--        @InvPool  - TinyInt
--        @AddrBank - Char(1)
--
-- Return Value:
--          @OnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx
--                   - 1 = Only 1 dispenser exists for the product within the specified
--                         bank and inventory pool and the dispenser actively contains a
--                         dual-dispenser portion Rx.
--
-- Comments:
--          None
--
----------------------------------------------------------------------------------------------------
 Set NoCount On 
If Object_Id('TempDb..#OrderIdsInOnlineTcdsInBankInvPool') Is Not Null
  Drop Table #OrderIdsInOnlineTcdsInBankInvPool

Select
  OrderId
Into
  #OrderIdsInOnlineTcdsInBankInvPool
From
  TcdStatus With (NoLock)
Where
  AddrBank = @AddrBank
  And
  (InvPool = @InvPool Or @InvPool = 0)
  And
  ProductId = @ProductId
  And
  AddrCabinet > 0                      -- Include only in-cabinet dispensers.
  And
  TcdSn > 0                            -- Include only dispenser rows.
  And
  Status = 'Online'
  
If @@RowCount = 1
  If (Select OrderId From #OrderIdsInOnlineTcdsInBankInvPool) Like '%<[1-2]>'
    Set @OnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx = 1
  Else
    Set @OnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx = 0
Else
  Set @OnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx = 0
 Set NoCount Off 
GO
