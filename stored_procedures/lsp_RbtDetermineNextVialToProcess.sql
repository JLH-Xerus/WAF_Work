--/
CREATE PROCEDURE lsp_RbtDetermineNextVialToProcess 
  @AddrBank VarChar(2),
  @InProcessTcdResets TinyInt,
  @InProcessCountDryRxsBeingFilled TinyInt,
  @TcdSn_ResetToIgnore_1 Int = 0,
  @TcdSn_ResetToIgnore_2 Int = 0
  As
/*
Object Type:
	Stored Procedure

Name:
	lsp_RbtDetermineNextVialToProcess

Version:
	10

Description:
	Determines the next Rx vial type to be processed by the robot.

Parameters:
	Input:
		@AddrBank - Robot bank name
		@InProcessTcdResets - The total number of Tcd resets currently being processed by the robot.
		@InProcessCountDryRxsBeingFilled - The total number of Count Dry Rxs being filled by the robot.
		@TcdSn_ResetToIgnore_1 - Dispenser SN of the dispenser for which the robot is currently processing a dispenser reset vial. - Robot Vial being produced
		@TcdSn_ResetToIgnore_2 - Dispenser SN of the dispenser for which the robot is currently reseting.  - Robot Vial being filled/processed
	Output:
		Recordset - Returns two Record sets 

Return Value:
	None

Comments:
   This SP returns two datasets. 
   First one contains the info for Vial type to process - Dispenser reset/ Count Dry NON RX or Standard Rxs. 
   Second is the main output the contains the details of the next vials to process depending upon the priority.

Debug SQL:

	DECLARE	@return_value int
	EXEC	@return_value = [dbo].[lsp_RbtDetermineNextVialToProcess]
			@AddrBank = N'B', -- Change bank name
			@InProcessTcdResets = 0,
			@InProcessCountDryRxsBeingFilled = 0,
			@TcdSn_ResetToIgnore_1 = 0,
			@TcdSn_ResetToIgnore_2 = 0
	SELECT	'Return Value' = @return_value

*/

Set NoCount On

Declare @TotalCountedRxs Int = 0, @TotalCountedNONRxs Int = 0, @TotalDispenserResetsRequired Int = 0, @LargeVialSizeAvailable bit = 0, @VialTypeToProcess Int = 0

-- Get total counted Rxs
Select @TotalCountedRxs = Count(*) From
	OeOrder As O With (NOLOCK)
Where
	AddrBank = @AddrBank                                        -- Specified Bank
	And
	FillType = 'A'                                              -- Auto-Fill
	And
	OrderId Not Like '%<[1-2]>'                                 -- Ignore Dual-Dispenser Portion/Children Rxs
	And
	Not OrderStatus Like 'Split_MV'                             -- Ignore Multi-Vial Parent Rxs
	And                                                         -- (Multi-Vial Portion/Children Rxs are returned.  The portions for these are dispensed as independent Rxs.)
	(
		OrderStatus In ('Counted', 'Partial Counted')            -- Counted Auto-Fill
		Or
		(OrderStatus = 'Split-DD' And StatusReason = 'Counted')  -- Dual-Dispenser Parent Rxs with both portions Counted
	)

-- Get total counted Non Rxs
Select 
	@TotalCountedNONRxs = Count(*) 
From 
	TcdStatus As T With (NOLOCK)
	INNER JOIN
	RvoNonRxProductVial As RNPV With (NOLOCK) 
	ON T.OrderId = RNPV.VialId
Where 
	AddrBank = @AddrBank And 
	OrderId Like 'NONRX%' And 
	((Status = 'Online' And DispMainState In (19,2)) Or -- NON RX in Counted state
	(Status = 'Offline' And StatusReasonCode = 'Run Dry' And DispMainState = 2))  -- NON RX Partial Counted
	And RNPV.StatusCode IN ('N','P')  -- Non RX in counted and problem status. TcdController can place non RX in problem status if unable to determine count qty.

--Get total dispenser resets required
Select 
	@TotalDispenserResetsRequired = Count(*) 
From 
	RbtQueuedForResetTcds As RT WITH (NOLOCK) 
	INNER JOIN 
	TcdStatus As T WITH (NOLOCK) 
	ON RT.TcdSn = T.TcdSn
Where 
	 T.ProductId IS NOT NULL And 
	 RT.AddrBank = @AddrBank And
     RT.RemainingTries > 0 And 
	 RT.TcdSn <> @TcdSn_ResetToIgnore_1 And 
	 RT.TcdSn <> @TcdSn_ResetToIgnore_2 

/*--No Rxs to fill?*/
-- If there nothing to process just return @VialTypeToProcess = 0
If @TotalCountedRxs = 0 And @TotalCountedNONRxs = 0 And @TotalDispenserResetsRequired = 0 
BEGIN
	Set @VialTypeToProcess = 0
	Select @VialTypeToProcess As VialTypeToProcess
	RETURN 
END

-- Check if the largest vial size is available. Process dispenser resets only if large vial size is available.
IF EXISTS (Select SupplyNum From RbtVialSupply WITH (NOLOCK) Where StatusId IN (0,1) And AddrBank = @AddrBank and SupplyNum = (Select Top 1 SupplyNum from RbtVialSupply With (NOLOCK) Where AddrBank  = @AddrBank Order BY StoredVialSizeDrams DESC))
BEGIN
    Set @LargeVialSizeAvailable = 1
END

/*-PRIORITY 1*/
/*--Any dispenser requires a reset?*/
/*--Keep the number of In-process Tcd resets < 2 if there are other Rxs to be filled */
/*--Process only when large vial supply is available */
If ((@InProcessTcdResets < 2) Or (@TotalCountedRxs = 0 And @TotalCountedNONRxs = 0)) And @TotalDispenserResetsRequired <> 0 And @LargeVialSizeAvailable = 1
BEGIN
	Set @VialTypeToProcess = 1
	--Return the final result (two datasets)
	Select @VialTypeToProcess As VialTypeToProcess, @TotalCountedRxs As TotalCountedRxs, @TotalCountedNONRxs As TotalCountedNONRxs, @TotalDispenserResetsRequired As TotalDispenserResetsRequired, @LargeVialSizeAvailable As LargeVialSizeAvailable
	Select RQT.TcdSn, RQT.RemainingTries, T.Status, T.AddrBank, T.AddrCabinet, T.AddrRow, T.AddrSlot, T.ProductId, T.QtyInBuffer, T.OrderId, T.StatusReasonCode, T.InvPool, ISNULL(T.LastNdcReplenished, INV.FirstNdc) As Ndc, TS.CostPerPill
	From 
		RbtQueuedForResetTcds As RQT WITH (NOLOCK) 
		INNER JOIN 
		TcdStatus As T WITH (NOLOCK) 
		ON RQT.TcdSn = T.TcdSn
		INNER JOIN
		TcdSecondaryData As TS WITH (NOLOCK)
		ON TS.Tcdsn = T.Tcdsn
		INNER JOIN
		vInvMasterWithUserOverrides As INV With (NoLock)
		ON INV.ProductId = T.ProductId
	Where 
		T.ProductId IS NOT NULL And 
		RQT.AddrBank = @AddrBank And
		RQT.RemainingTries > 0 And 
		RQT.TcdSn <> @TcdSn_ResetToIgnore_1 And 
		RQT.TcdSn <> @TcdSn_ResetToIgnore_2
	Order By 
		RemainingTries Desc, QueuedDtTm
	RETURN
END

/*-PRIORITY 2*/
/*--Any Count Dry NON RX Ready to be filled?*/
/*--Keep the number of In-process Count Dry Rxs being filled < 2 if there are standard Rxs that are waiting be filled */
/*--Process only when large vial supply is available */
Else if ((@InProcessCountDryRxsBeingFilled < 2) Or (@TotalCountedRxs = 0)) And @TotalCountedNONRxs <> 0 And @LargeVialSizeAvailable = 1
BEGIN
	Set @VialTypeToProcess = 2
	--Return the final result (two datasets) 
	Select @VialTypeToProcess As VialTypeToProcess, @TotalCountedRxs As TotalCountedRxs, @TotalCountedNONRxs As TotalCountedNONRxs, @TotalDispenserResetsRequired As TotalDispenserResetsRequired, @LargeVialSizeAvailable As LargeVialSizeAvailable
	Select 
		T.OrderId, T.AddrBank, T.AddrCabinet, T.AddrRow, T.AddrSlot, T.TcdSn, T.ProductId, CASE WHEN RNPV.Qty > 0 THEN CAST(((RNPV.Qty * 100.0) / INV.QtyPer100Cc) * 0.2705 As Decimal(9,3)) ELSE 0 END As ProductVolumeInDrams, INV.QtyPer100Cc, RNPV.Qty, RNPV.CreationDtTm
	From 
		TcdStatus As T With (NOLOCK)
		INNER JOIN
		RvoNonRxProductVial As RNPV With (NOLOCK) 
		ON T.OrderId = RNPV.VialId
		INNER JOIN
		vInvMasterWithUserOverrides As INV With (NOLOCK)
		ON INV.ProductId = T.ProductId
	Where 
		AddrBank = @AddrBank And 
		OrderId Like 'NONRX%' And 
		((Status = 'Online' And DispMainState In (19,2)) Or -- NON RX in Counted state
		(Status = 'Offline' And StatusReasonCode = 'Run Dry' And DispMainState = 2))  -- NON RX Partial Counted
		And RNPV.StatusCode IN ('N','P')  -- Non RX in counted and problem status. TcdController can place non RX in problem status if unable to determine count qty.
	RETURN
END 

/*-PRIORITY 3*/
/*--Any Regular Rx Ready to be filled?*/
/*For Regular Rxs the vial size is checked within RFM at this time.*/
If @TotalCountedRxs <> 0
BEGIN
	Set @VialTypeToProcess = 3

	Drop Table If Exists #temp_table2 
	CREATE TABLE #temp_table2   
   (   
     OrderId VarChar(30),
	 ProductId VarChar(20),
     FillToMakeWayForUpstreamRxOrderId varchar(50),
	 QtyOrdered Decimal(9,3),
	 QtyPer100cc SmallInt,
	 ProductVolumeInDrams Decimal(9,3),
	 OverFlowLabelRequired Bit,
	 ActOnAsPriInternal VarChar(40),
	 PriInternal VarChar(40)
	) 
  INSERT INTO #temp_table2 
  Exec lsp_OrdGetReadyForDispenseRxsInBank @AddrBank, 0

  --Return the final result (two datasets)
  --For Regular Rxs the vial size is checked within RFM at this time.
  Select @VialTypeToProcess As VialTypeToProcess, @TotalCountedRxs As TotalCountedRxs, @TotalCountedNONRxs As TotalCountedNONRxs, @TotalDispenserResetsRequired As TotalDispenserResetsRequired, @LargeVialSizeAvailable As LargeVialSizeAvailable
  Select * From #temp_table2 With (NOLOCK) Order By ActOnAsPriInternal, PriInternal
  --Drop temp table
  Drop Table If Exists #temp_table2 	
  RETURN
END

/*--No Rxs to fill due to unavailable vial supply?* - this will catch cases where there are dispenser resets and count dry rxs need to processed but the robot does not have a large vial size*/
Else if @TotalCountedRxs = 0 And ((@TotalDispenserResetsRequired <> 0 and @LargeVialSizeAvailable = 0) Or (@TotalCountedNONRxs <> 0 and @LargeVialSizeAvailable = 0))
BEGIN
	Set @VialTypeToProcess = -1 
	Select @VialTypeToProcess As VialTypeToProcess, @TotalCountedRxs As TotalCountedRxs, @TotalCountedNONRxs As TotalCountedNONRxs, @TotalDispenserResetsRequired As TotalDispenserResetsRequired, @LargeVialSizeAvailable As LargeVialSizeAvailable
	RETURN
END

/*--The execution should never come here but adding as a safety case to catch issues with this SP*/
Set @VialTypeToProcess = -99
Select @VialTypeToProcess As VialTypeToProcess, @TotalCountedRxs As TotalCountedRxs, @TotalCountedNONRxs As TotalCountedNONRxs, @TotalDispenserResetsRequired As TotalDispenserResetsRequired, @LargeVialSizeAvailable As LargeVialSizeAvailable


/
