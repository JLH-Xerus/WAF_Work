/****** Object:  StoredProcedure [dbo].[lsp_PwkGetVerifiedRxsWithManualPriority]    Script Date: 4/5/2026 8:50:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE Procedure [dbo].[lsp_PwkGetVerifiedRxsWithManualPriority]
	@PatCustIdMask VarChar(20), @MultiToteRequired bit = 1
------------------------------------------------------------------------------------------------------
-- Procedure:
--      PwkGetVerifiedRxsWithManualPriority
-- Description:
--      Gets Top 10 verified Rxs and Splits (parent records ONLY) ordered by Verified DateTime Asc.
--      Information: OrderId, GroupNum, Ndc, RxNum, OrderStatus, FillType, PoNum, NumOfVials. 
--      Autofill singles are picked up when there are verified. 
--      Splits are grabbed as when they are verified and all items are assigned to a tote. 
--
--      Note:  This will prioritize a single manaul fill when called.     
--
-- Parameters:
--      PatCustIdMask - filter added to the Patient Id (PatCustId % 10 = 4, Mask Contains 4?)
--      MultiToteRequired - require the multivials to be in totes?
-- Return Value:
--      None
-- Comments:
--      This will allow for Manual and Autofill Rxs
--      Getting the top 10 only will prevent massive lists for the poller to try each time it polls.
------------------------------------------------------------------------------------------------------

As

	-- Drop Temp table if it exists
	If Object_Id('TempDb..#ReadyForPaperworkManPriority') Is Not Null
		Drop Table #ReadyForPaperworkManPriority

	-- Get ALL items ready for paperwork
	Select O.OrderId, O.GroupNum, O.Ndc, O.RxNum, O.OrderStatus, O.DateFilled, O.FillType, Po.PoNum, O.NumOfVials, O.PatCustId,
		IsNull((Select Top 1 ToteId From OrderToteAssoc with (NoLock) Where ((OrderId like O.OrderId + '-[0-9][0-9]') Or (OrderId = O.OrderId)) And OrderToteStateCode = 'I'),'') as ToteId
	Into #ReadyForPaperworkManPriority
	From OeOrder As O with (NoLock)
		Join
	OeOrderPoNumAssoc As Po with (NoLock)
		On O.OrderId = Po.OrderId
	Where
		((O.OrderStatus='Verified' And O.OrderId Not Like '%-[0-9][0-9]') Or
		 (O.OrderStatus = 'Split/MV' And O.StatusReason = 'Verified' And
		 (@MultiToteRequired = 0 Or (O.NumOfVials = (Select Count(OrderId) from OrderToteAssoc with (NoLock) Where OrderId like O.OrderId + '-[0-9][0-9]' And OrderToteStateCode = 'I')))))
	Order By
		O.DateFilled Asc

	-- Pulls the top 1 Manual to keep prioirty since manual travel time is far less than autofill
	Select Top(1) *
	From 
		#ReadyForPaperworkManPriority As RFP With (NoLock)
	Where
		@PatCustIdMask Like '%' + CAST((RFP.PatCustId % 10) as varchar) + '%' And
		FillType = 'M'

	UNION

	-- Pulls the next 10 items (Manual or Autofill)
	-- Since there is a 'UNION' and not a 'UNION ALL' duplicates are ignored
	Select Top(10) *
	From 
		#ReadyForPaperworkManPriority As RFP With (NoLock)
	Where
		@PatCustIdMask Like '%' + CAST((RFP.PatCustId % 10) as varchar) + '%'

	Order By
	   RFP.DateFilled Asc

GO
