--/
CREATE PROCEDURE lsp_OrdGetRxStatusCounts 

  @CountOnlyMostRecentRxForEachOrderId TinyInt = 1,
  @RangeIntoHistoryHrs Int,
  @AddrBankFilter VarChar(760) = Null,
  @ActiveRxOnly smallint = 0,
  @RxCntAwaitingWebTrx Int Output,
  @RxCntInitiated Int Output,
  @RxCntAwaitingScript Int Output,
  @RxCntAwaitingTransfer Int Output,
  @RxCntPosted Int Output,
  @RxCntNoRefillsRemain Int Output,
  @RxCntCallPrescriber Int Output,
  @RxCntFaxingPrescriber Int Output,
  @RxCntAwaitingReply Int Output,
  @RxCntFaxFailure Int Output,
  @RxCntApproved Int Output,
  @RxCntDenied Int Output,

  @RxCntGrpPending Int Output,
  @RxCntGrpScheduled Int Output,
  @RxCntProblem Int Output,
  @RxCntPending Int Output,
  @RxCntAwaitingMatch Int Output,
  @RxCntMatched Int Output,
  @RxCntOutForCentralFill Int Output,
  @RxCntScheduled Int Output,
  @RxCntSplit Int Output,
  @RxCntCounting  Int Output,
  @RxCntSuspended  Int Output,
  @RxCntPartialCounted Int Output,
  @RxCntCounted Int Output,
  @RxCntVialWait Int Output,
  @RxCntPartialFilled Int Output,
  @RxCntFilled Int Output,
  @RxCntChecked Int Output,
  @RxCntVerifying Int Output,
  @RxCntVerified Int Output,
  @RxCntInBin Int Output,
  @RxCntReadyForPickUp Int Output,
  @RxCntReadyToShip Int Output,
  @RxCntPickedUp Int Output,
  @RxCntShipPending Int Output,
  @RxCntShipped Int Output,
  @RxCntCanceled Int Output
As
----------------------------------------------------------------------------------------------------
-- Procedure:
--        lsp_OrdGetRxStatusCounts
--
-- Description:
--        Retrieve the number of Rxs (including Rx Requests) within each status (pool).
--
-- Parameters:
--        @CountOnlyMostRecentRxForEachOrderId
--                           - Whether or not to count only the most recent Rx for each Order ID:
--                               1 = Count only the most recent Rx record for each Order ID.
--                               0 = Count all Rx records for each Order ID.
--        @RangeIntoHistoryHrs
--                           - The Range Into History Hours within which to count history Rxs.
--        @AddrBankFilter
--                           - The Bank(s) for which to count Rxs.
--		  @ActiveRxOnly
--							 - Whether or not to count inactive Rxs.
--								1 = Count only active Rxs
--								0 = Count All Rxs
--
-- Return Value:
--        @RxCntXXXXXXXXXX   - The number of Rxs (& Rx Requests) within the status (pool)
--                               where XXXXXXXXXX represents the status.
--
-- Comments:
--        A parent "Split" Rx has a "derived status" stored within its StatusReason field.
--        A parent "Split" Rx is considered as being in its "derived status" for these counts.
--        A permanent-split parent "Split" Rx is ignored in these counts (as well as in the results
--          on the Track Rx form).
--
----------------------------------------------------------------------------------------------------
Set NoCount On

Declare @RestrictAddrBank Table (AddrBank varchar(2))

------------------------------------
-- Build the Addres Bank Filter list
------------------------------------
If @AddrBankFilter Is Not Null And @AddrBankFilter > '' -- If Bank values were passed in using the comma separated list parameter
Begin

	-- First check if there is a ',' at the end of the parameter, if there is not then append one (helps with the While loop)
	If (Select Substring(@AddrBankFilter, Len(@AddrBankFilter), 1)) != ','
	Begin
		Select
			@AddrBankFilter = @AddrBankFilter + ','
	End

	-- While there is a ',' found in the passed in Bank parameter, parse out the bank value and insert it into the @RestrictAddrBank table
	While CHARINDEX(',' , @AddrBankFilter) > 0
	Begin
		Insert Into @RestrictAddrBank
		Values (Left(@AddrBankFilter, CHARINDEX(',' , @AddrBankFilter) - 1))

		-- Remove the bank value from the string that was just inserted into the table
		Select
			@AddrBankFilter = SubString(@AddrBankFilter, CHARINDEX(',' , @AddrBankFilter) + 1, Len(@AddrBankFilter))
	End
End
Else  -- @AddrBankFilter Is Null
Begin

	-- Insert all existing banks into the @RestrictAddrBank table
	Insert Into
		@RestrictAddrBank
	Select
		AddrBank
	From
		BnkAllBanks With (NoLock)

End

---------------------------------------------------------------------------------------------------
-- If @CountOnlyMostRecentRxForEachOrderId specified
--   Only show History Rxs that are the "current" (most recent) for each Order ID.
--   (This is accomplished by joining the OeOrderHistory table to the OeOrderCurrHistoryDtTm table,
--    which holds the current (most recent) History Date/Time for every Order ID in the system.)
---------------------------------------------------------------------------------------------------
If @CountOnlyMostRecentRxForEachOrderId = 1
  Select
    @RxCntAwaitingWebTrx = Sum(Case When Status = 'Awaiting Web Trx' Then 1 Else 0 End),
    @RxCntInitiated = Sum(Case When Status = 'Initiated' Then 1 Else 0 End),
    @RxCntAwaitingScript = Sum(Case When Status = 'Awaiting Script' Then 1 Else 0 End),
    @RxCntAwaitingTransfer = Sum(Case When Status = 'Awaiting Transfer' Then 1 Else 0 End),
    @RxCntPosted = Sum(Case When Status = 'Posted' Then 1 Else 0 End),
    @RxCntNoRefillsRemain = Sum(Case When Status = 'No Refills Remain' Then 1 Else 0 End),
    @RxCntCallPrescriber = Sum(Case When Status = 'Call Prescriber' Then 1 Else 0 End),
    @RxCntFaxingPrescriber = Sum(Case When Status = 'Faxing Prescriber' Then 1 Else 0 End),
    @RxCntAwaitingReply = Sum(Case When Status = 'Awaiting Reply' Then 1 Else 0 End),
    @RxCntFaxFailure = Sum(Case When Status = 'Fax Failure' Then 1 Else 0 End),
    @RxCntApproved = Sum(Case When Status = 'Approved' Then 1 Else 0 End),
    @RxCntDenied = Sum(Case When Status = 'Denied' Then 1 Else 0 End),
    @RxCntGrpPending = Sum(Case When Status = 'Grp-Pending' Then 1 Else 0 End),
    @RxCntGrpScheduled = Sum(Case When Status = 'Grp-Scheduled' Then 1 Else 0 End),
    @RxCntProblem = Sum(Case When Status = 'Problem' Then 1 Else 0 End),
    @RxCntPending = Sum(Case When Status = 'Pending' Or (Status Like 'Split-%' And StatusReason = 'Pending') Then 1 Else 0 End),
    @RxCntAwaitingMatch = Sum(Case When Status = 'Awaiting Match' Then 1 Else 0 End),
    @RxCntMatched = Sum(Case When Status = 'Matched' Then 1 Else 0 End),
    @RxCntOutForCentralFill = Sum(Case When Status = 'Out For Central Fill' Then 1 Else 0 End),
    @RxCntScheduled = Sum(Case When Status = 'Scheduled' Or (Status Like 'Split-%' And StatusReason = 'Scheduled') Then 1 Else 0 End),
    @RxCntSplit = Sum(Case When Status Like 'Split-%' Then 1 Else 0 End),
    @RxCntCounting = Sum(Case When Status = 'Counting' Or (Status Like 'Split-%' And StatusReason = 'Counting') Then 1 Else 0 End),
    @RxCntSuspended = Sum(Case When Status = 'Suspended' Or (Status Like 'Split-%' And StatusReason = 'Suspended') Then 1 Else 0 End),
    @RxCntPartialCounted = Sum(Case When Status = 'Partial Counted' Or (Status Like 'Split-%' And StatusReason = 'Partial Counted') Then 1 Else 0 End),
    @RxCntCounted = Sum(Case When Status = 'Counted' Or (Status Like 'Split-%' And StatusReason = 'Counted') Then 1 Else 0 End),
    @RxCntVialWait = Sum(Case When Status = 'Vial Wait' Or (Status Like 'Split-%' And StatusReason = 'Vial Wait') Then 1 Else 0 End),
    @RxCntPartialFilled = Sum(Case When Status = 'Partial Filled' Or (Status Like 'Split-%' And StatusReason = 'Partial Filled') Then 1 Else 0 End),
    @RxCntFilled = Sum(Case When Status = 'Filled' Or (Status Like 'Split-%' And StatusReason = 'Filled') Then 1 Else 0 End),
    @RxCntChecked = Sum(Case When Status = 'Checked' Then 1 Else 0 End),
    @RxCntVerifying = Sum(Case When Status = 'Verifying' Then 1 Else 0 End),
    @RxCntVerified = Sum(Case When Status = 'Verified' Then 1 Else 0 End),
    @RxCntInBin = Sum(Case When Status = 'In Bin' Then 1 Else 0 End),
    @RxCntReadyForPickUp = Sum(Case When Status = 'Ready For Pick Up' Then 1 Else 0 End),
    @RxCntReadyToShip = Sum(Case When Status = 'Ready To Ship' Then 1 Else 0 End),
    @RxCntPickedUp = Sum(Case When Status = 'Picked Up' Then 1 Else 0 End),
    @RxCntShipPending = Sum(Case When Status = 'Ship Pending' Then 1 Else 0 End),
    @RxCntShipped = Sum(Case When Status = 'Shipped' Then 1 Else 0 End),
    @RxCntCanceled = Sum(Case When Status = 'Canceled' Or (Status Like 'Split-%' And StatusReason = 'Canceled') Then 1 Else 0 End)
  From
    (Select OrderStatus As Status, StatusReason
     From OeOrder With (NoLock)
     Where OrderId Not Like '%[(<][1-8][)>]'
     And (Exists(Select AddrBank From @RestrictAddrBank Where AddrBank = AddrBank))         -- Include only Orders in specified Bank(s) (or all banks if '' passed in).
     Union All
     Select OrderStatus As Status, StatusReason
     From OeOrderHistory As O With (NoLock, Index(ByHistoryDtTm))
            Join
          OeOrderCurrHistoryDtTm As C With (NoLock)
            On O.OrderId = C.OrderId And O.HistoryDtTm = C.HistoryDtTm
     Where O.HistoryDtTm > DateAdd(Hour, -@RangeIntoHistoryHrs, GetDate())
     And (Exists(Select AddrBank From @RestrictAddrBank Where AddrBank = AddrBank))         -- Include only Orders in specified Bank(s) (or all banks if '' passed in).
     And 1 = Case When @ActiveRxOnly = 1 Then 0 Else 1 End
	 Union All
     Select Status, '' As StatusReason
     From OeRxRequest With (NoLock)
     Where Status Not In ('Denied', 'Canceled')
     And (Exists(Select AddrBank From @RestrictAddrBank Where AddrBank = AddrBank))         -- Include only Orders in specified Bank(s) (or all banks if '' passed in).
     Union All
     Select Status, '' As StatusReason
     From OeRxRequest With (NoLock)
     Where Status In ('Denied', 'Canceled')
     And LastStsChgDtTm > DateAdd(Hour, -@RangeIntoHistoryHrs, GetDate())
     And (Exists(Select AddrBank From @RestrictAddrBank Where AddrBank = AddrBank))         -- Include only Orders in specified Bank(s) (or all banks if '' passed in).
    ) As StatusCounts
Else -- @CountOnlyMostRecentRxForEachOrderId = 0
  Select
    @RxCntAwaitingWebTrx = Sum(Case When Status = 'Awaiting Web Trx' Then 1 Else 0 End),
    @RxCntInitiated = Sum(Case When Status = 'Initiated' Then 1 Else 0 End),
    @RxCntAwaitingScript = Sum(Case When Status = 'Awaiting Script' Then 1 Else 0 End),
    @RxCntAwaitingTransfer = Sum(Case When Status = 'Awaiting Transfer' Then 1 Else 0 End),
    @RxCntPosted = Sum(Case When Status = 'Posted' Then 1 Else 0 End),
    @RxCntNoRefillsRemain = Sum(Case When Status = 'No Refills Remain' Then 1 Else 0 End),
    @RxCntCallPrescriber = Sum(Case When Status = 'Call Prescriber' Then 1 Else 0 End),
    @RxCntFaxingPrescriber = Sum(Case When Status = 'Faxing Prescriber' Then 1 Else 0 End),
    @RxCntAwaitingReply = Sum(Case When Status = 'Awaiting Reply' Then 1 Else 0 End),
    @RxCntFaxFailure = Sum(Case When Status = 'Fax Failure' Then 1 Else 0 End),
    @RxCntApproved = Sum(Case When Status = 'Approved' Then 1 Else 0 End),
    @RxCntDenied = Sum(Case When Status = 'Denied' Then 1 Else 0 End),
    @RxCntGrpPending = Sum(Case When Status = 'Grp-Pending' Then 1 Else 0 End),
    @RxCntGrpScheduled = Sum(Case When Status = 'Grp-Scheduled' Then 1 Else 0 End),
    @RxCntProblem = Sum(Case When Status = 'Problem' Then 1 Else 0 End),
    @RxCntPending = Sum(Case When Status = 'Pending' Or (Status Like 'Split-%' And StatusReason = 'Pending') Then 1 Else 0 End),
    @RxCntAwaitingMatch = Sum(Case When Status = 'Awaiting Match' Then 1 Else 0 End),
    @RxCntMatched = Sum(Case When Status = 'Matched' Then 1 Else 0 End),
    @RxCntOutForCentralFill = Sum(Case When Status = 'Out For Central Fill' Then 1 Else 0 End),
    @RxCntScheduled = Sum(Case When Status = 'Scheduled' Or (Status Like 'Split-%' And StatusReason = 'Scheduled') Then 1 Else 0 End),
    @RxCntSplit = Sum(Case When Status Like 'Split-%' Then 1 Else 0 End),
    @RxCntCounting = Sum(Case When Status = 'Counting' Or (Status Like 'Split-%' And StatusReason = 'Counting') Then 1 Else 0 End),
    @RxCntSuspended = Sum(Case When Status = 'Suspended' Or (Status Like 'Split-%' And StatusReason = 'Suspended') Then 1 Else 0 End),
    @RxCntPartialCounted = Sum(Case When Status = 'Partial Counted' Or (Status Like 'Split-%' And StatusReason = 'Partial Counted') Then 1 Else 0 End),
    @RxCntCounted = Sum(Case When Status = 'Counted' Or (Status Like 'Split-%' And StatusReason = 'Counted') Then 1 Else 0 End),
    @RxCntVialWait = Sum(Case When Status = 'Vial Wait' Or (Status Like 'Split-%' And StatusReason = 'Vial Wait') Then 1 Else 0 End),
    @RxCntPartialFilled = Sum(Case When Status = 'Partial Filled' Or (Status Like 'Split-%' And StatusReason = 'Partial Filled') Then 1 Else 0 End),
    @RxCntFilled = Sum(Case When Status = 'Filled' Or (Status Like 'Split-%' And StatusReason = 'Filled') Then 1 Else 0 End),
    @RxCntChecked = Sum(Case When Status = 'Checked' Then 1 Else 0 End),
    @RxCntVerifying = Sum(Case When Status = 'Verifying' Then 1 Else 0 End),
    @RxCntVerified = Sum(Case When Status = 'Verified' Then 1 Else 0 End),
    @RxCntInBin = Sum(Case When Status = 'In Bin' Then 1 Else 0 End),
    @RxCntReadyForPickUp = Sum(Case When Status = 'Ready For Pick Up' Then 1 Else 0 End),
    @RxCntReadyToShip = Sum(Case When Status = 'Ready To Ship' Then 1 Else 0 End),
    @RxCntPickedUp = Sum(Case When Status = 'Picked Up' Then 1 Else 0 End),
    @RxCntShipPending = Sum(Case When Status = 'Ship Pending' Then 1 Else 0 End),
    @RxCntShipped = Sum(Case When Status = 'Shipped' Then 1 Else 0 End),
    @RxCntCanceled = Sum(Case When Status = 'Canceled' Or (Status Like 'Split-%' And StatusReason = 'Canceled') Then 1 Else 0 End)
  From
    (Select OrderStatus As Status, StatusReason
     From OeOrder With (NoLock)
     Where OrderId Not Like '%[(<][1-8][)>]'
     And (Exists(Select AddrBank From @RestrictAddrBank Where AddrBank = AddrBank))         -- Include only Orders in specified Bank(s) (or all banks if '' passed in).
     Union All
     Select OrderStatus As Status, StatusReason
     From OeOrderHistory With (NoLock, Index(ByHistoryDtTm))
     Where HistoryDtTm > DateAdd(Hour, -@RangeIntoHistoryHrs, GetDate())
     And (Exists(Select AddrBank From @RestrictAddrBank Where AddrBank = AddrBank))         -- Include only Orders in specified Bank(s) (or all banks if '' passed in).
     And 1 = Case When @ActiveRxOnly = 1 Then 0 Else 1 End
	 Union All
     Select Status, '' As StatusReason
     From OeRxRequest With (NoLock)
     Where Status Not In ('Denied', 'Canceled')
     And (Exists(Select AddrBank From @RestrictAddrBank Where AddrBank = AddrBank))         -- Include only Orders in specified Bank(s) (or all banks if '' passed in).
     Union All
     Select Status, '' As StatusReason
     From OeRxRequest With (NoLock)
     Where Status In ('Denied', 'Canceled')
     And LastStsChgDtTm > DateAdd(Hour, -@RangeIntoHistoryHrs, GetDate())
     And (Exists(Select AddrBank From @RestrictAddrBank Where AddrBank = AddrBank))         -- Include only Orders in specified Bank(s) (or all banks if '' passed in).
    ) As StatusCounts

/
