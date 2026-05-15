
--GO
/*
Object Type:
   Stored Procedure

Name:
   lsp_DbPurgeV6

Version:
   01

Description:
   Delete data for Rxs and other records completed (and moved to history) prior to X days ago.
   This deletes rows from many, many tables, including Rx-related data tables as well as other object tables propagated to for the deleted Rxs
     (e.g., Rx group, shipment tables, image tables) in a record by record basis. 

Parameters:
   Input:
      @OlderThanXDays      - Data for Rxs that were completed prior to this many days ago will be deleted.
      @HistoryDtTmFrom		- Starting Custom Date Range.. If not supplied and OlderThanXDays is supplied the value is calculated.cking.
      @HistoryDtTmTo        - The ending date range, if not supplied and OlderthanXDays is supplied the value is calculated then adding 90 days to it
	  
   Output:
      None

Return Value:
   @Rc - 0 = Successful; Non-0 = Failed

Comments:
   This procedure was taken from the nightly maintenance procedure and altered to do single row deletes.
   The oldest Rxs are deleted first.

   This procedure deletes other object data based on the relationship of the Rxs being deleted to the "other objects" (e.g. shipments, image associations)
   rather than issuing delete statements such as "Delete From Shipment Where Id Not In (Select Shipment From OeOrderShipmentAssoc)" (or Left Join 
   checking Null)   
*/


--Setup Environment. 
SET NOCOUNT ON
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED 
DECLARE @Rc Int                                 -- SQL Command Return Code.  (Saved Value of @@Error.)


DECLARE @OlderThanXDays Int = null --600 --36500   -- If From / To Dates are populated then override the datediff
   , @HistoryDtTmFrom DateTime = '2024-02-01'   -- inclusive Optional: purge only within this window (inclusive/exclusive). If both Null, uses @OlderThanXDays cutoff.
   , @HistoryDtTmTo DateTime = '2024-02-02'    -- exclusive Optional: purge only within this window (inclusive/exclusive). If both Null, uses @OlderThanXDays cutoff.
   , @DelayBetweenRowsMs INT = 100 --milliseconds. 
   , @MaxToDelete INT = 30000
DECLARE @EvtNotes VarChar(255)         -- Logged event notes.
	, @Qty INT = NULL
	, @Results VARCHAR(20) = ''
	, @NumOfRowsDeleted Int = 0      -- Number of rows that have been deleted. 
	, @CutoffDtTm DateTime                    -- "X Days Ago" Cutoff Date/Time.
	, @HistoryDtTm_EffectiveFrom DateTime     -- Effective inclusive lower bound.
	, @HistoryDtTm_EffectiveTo DateTime       -- Effective exclusive upper bound.
	, @ErrMsg NVarChar(4000)         -- Error message (if error occurs).


	DECLARE @PurgedTables TABLE (TableName VARCHAR(50), RecordsDeleted BIGINT, PurgeDate DATETIME DEFAULT(GETDATE()))
	If (@HistoryDtTmFrom Is Null And @HistoryDtTmTo Is Null) And @OlderThanXDays <= 0
	Begin
		Set @ErrMsg = FormatMessage('Invalid parameter value. (@OlderThanXDays = %i)', @OlderThanXDays)
		RaisError(@ErrMsg, 16, 1)
	End

  IF @OlderThanXDays IS NOT NULL AND @HistoryDtTmFrom IS NULL
	SET @HistoryDtTmFrom = CAST(CONVERT(VARCHAR(20), DATEADD(DAY, -(@OlderThanXDays), CAST(GETDATE() AS DATE))) + ' 23:59:59.998' AS datetime)


 IF @OlderThanXDays IS NOT NULL AND @HistoryDtTmTo IS NULL
	SET @HistoryDtTmTo = DATEADD(d, -(@OlderThanXDays - 90), GETDATE())
	select @HistoryDtTmTo as HistoryDtTmTo
	
  IF ISNULL(@OlderThanXDays, 0) = 0 AND @HistoryDtTmFrom IS NOT NULL AND @HistoryDtTmTo IS NOT NULL
	--If no OlderThanXDays and dates are populated then override
	SET @OlderThanXDays = DATEDIFF(D, @HistoryDtTmFrom, @HistoryDtTmTo)
ELSE
-- If From / To Dates are populated then override the datediff
	IF ISNULL(@OlderThanXDays, 0) <> 0 AND @HistoryDtTmFrom IS NOT NULL AND @HistoryDtTmTo IS NOT NULL
		SET @OlderThanXDays = DATEDIFF(D, @HistoryDtTmFrom, @HistoryDtTmTo)

Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())

	SELECT @OlderThanXDays,@HistoryDtTmFrom,  @HistoryDtTmTo

 ---------------------------------------------------------------------------
   -- Determine effective purge window.
   --  - If @HistoryDtTmFrom/@HistoryDtTmTo are provided: purge within [From, To)
   --  - Else: purge all rows before @CutoffDtTm (legacy @OlderThanXDays behavior)
   ---------------------------------------------------------------------------
   If ( (@HistoryDtTmFrom Is Null And @HistoryDtTmTo Is Not Null)
     Or (@HistoryDtTmFrom Is Not Null And @HistoryDtTmTo Is Null) )
   Begin
      Set @ErrMsg = 'Invalid parameter values. Provide BOTH @HistoryDtTmFrom and @HistoryDtTmTo, or neither.'
      RaisError(@ErrMsg, 16, 1)
   End

   If @HistoryDtTmFrom Is Not Null And @HistoryDtTmTo <= @HistoryDtTmFrom
      Begin
         Set @ErrMsg = 'Invalid parameter values. @HistoryDtTmTo must be greater than @HistoryDtTmFrom.'
         RaisError(@ErrMsg, 16, 1)
      End

   If @HistoryDtTmFrom Is Null
      Begin
         -- Legacy: delete everything before cutoff.
         Set @HistoryDtTm_EffectiveFrom = '19000101'
         Set @HistoryDtTm_EffectiveTo = @CutoffDtTm
      End
   Else
      Begin
         Set @HistoryDtTm_EffectiveFrom = @HistoryDtTmFrom
         Set @HistoryDtTm_EffectiveTo = @HistoryDtTmTo
      End

IF @DelayBetweenRowsMs IS NULL
	SET @DelayBetweenRowsMs = 0 

IF @DelayBetweenRowsMs NOT BETWEEN 1 and 100
BEGIN
        Set @ErrMsg = 'Invalid parameter values. @WaitTime_BetweenExecs must be between 1 and 100.'
         RaisError(@ErrMsg, 16, 1)
END
DECLARE @delay varchar(12) = '00:00:00.' + RIGHT(CONCAT('0', @DelayBetweenRowsMs), 3);

IF @Qty IS NULL
	SET @Qty = 0 

--Start Main Loop, Log Record / Event to EvtEvent
SET @Results = ''
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@HistoryDtTmFrom=' + Cast(@HistoryDtTmFrom As VarChar(20)) 
Exec lsp_DbLogSqlEvent 'A', 'DbPurgeV6 Beg', '', @EvtNotes, 'lsp_DbPurgeV6'




--select top 100 * from EvtEvent order by 1 desc
	  select @HistoryDtTm_EffectiveFrom, @HistoryDtTm_EffectiveTo

DECLARE @RetryCount INT,  @Success    BIT, @RetryLimit INT, @ImageCount INT = 0, @Testing INT = 1
SELECT @RetryCount = 1, @Success = 0, @RetryLimit = 3
DECLARE @LoopCount INT, @LoopNo INT, @ErrorNumber INT, @ErrorMessage VARCHAR(max);
DECLARE @OeOrderSecondaryDataCount BIGINT = 0, @OeOrderCurrHistoryDtTmCount BIGINT = 0, @CaAuditCount BIGINT = 0, @CvyRxRouteCount BIGINT = 0 
DECLARE @OeDurDataCount BIGINT = 0, @OeDurFreeFormTextCount BIGINT = 0, @OeFlaggedRxsCount BIGINT = 0, @OeLotCodeCount BIGINT = 0 
DECLARE @OeOrderAuxLabelTextCount BIGINT = 0, @OeOrderExtSysDocumentCount BIGINT = 0, @OeOrderExtUserDefCount BIGINT = 0, @OeOrderTcdAssocCount BIGINT = 0 
DECLARE @OeOrderTextDocumentCount BIGINT = 0, @OeOrderThirdPartyPlanCount BIGINT = 0, @OeRxBagAssocCount BIGINT = 0, @OeRxItemHistoryCount BIGINT = 0
DECLARE @OeRxPouchDispenserAssocCount BIGINT = 0, @OeRxPrefLangDataCount BIGINT = 0 , @ImgRxImgAssocCount BIGINT = 0 , @ImgImageCount BIGINT = 0 
DECLARE @RxImgIdsCount BIGINT = 0, @OeRxDoseSchedCount BIGINT = 0, @DsRxDoseSchedDoseCount BIGINT = 0, @DsRxDoseSchedDoseByDayOfWeekCount BIGINT = 0 
DECLARE @PwkPaperworkSetOrderAssocCount BIGINT = 0, @PwkPaperworkSetCount BIGINT = 0 
DECLARE @DsRxDoseSchedCount BIGINT = 0, @OeOrderCanReplenAssocCount BIGINT = 0, @CanLotCodeHistoryCount BIGINT = 0, @CanCanisterHistoryCount BIGINT = 0 
DECLARE @ImgCanImgAssocCount BIGINT = 0, @ImgImage_IntIdCount BIGINT = 0, @OeOrderShipmentAssocCount BIGINT = 0, @CvyPackageRouteCount BIGINT = 0
DECLARE @PwkPrinterTrayCount BIGINT = 0, @ShpShipmentPkgLabelDataCount BIGINT = 0, @ShpShipmentSpecialServiceAssocCount BIGINT = 0
DECLARE @SrtShipToteShipmentAssocCount BIGINT = 0, @ShpManifestShipmentAssocCount BIGINT = 0, @ShpShipmentPackageLabelInstanceCount BIGINT = 0
DECLARE @ShpShipmentCount BIGINT = 0, @ShpPalletManifestAssocCount BIGINT = 0, @ShpLoadPalletAssocCount BIGINT = 0, @ShpLoadCount BiginT = 0 
DECLARE @ShpPalletCount BIGINT = 0, @ShpManifestCount BIGINT = 0, @OeOrderHistoryCount BIGINT = 0, @OeCommentsCount BIGINT = 0, @OeOrderPoNumAssocCount BIGINT = 0 
DECLARE @OeProblemRxsCount BIGINT = 0, @OeRequireCorrespondenceRxsCount BIGINT = 0, @OeStatusTrailCount BIgINT = 0, @GovOnHoldGroupsCount BIGINT = 0 , @OeExceptionGroupsCount BIGINT = 0 
DECLARE @OeGroupCvyFillDataCount BIGINT = 0, @OeGroupOrderDataCount BIGINT = 0, @OeGroupPaymentCount BIGINT = 0 
DECLARE @OeGroupUserDefCount BIGINT = 0, @OeGroupCount BIGINT = 0, @OePatientCustSecondarydataCount BIGINT = 0, @PrivacySignatureCount BIGINT = 0
DECLARE @OePatientCustCount BIGINT = 0, @OeOrderAcceptRejectCount BIGINT = 0, @OeOrderAuxLabelFileCount BIGINT = 0, @OePrescriberCount BIGINT = 0 

WHILE @RetryCount < =  @RetryLimit AND @Success = 0
BEGIN
   BEGIN TRY
   	---------------------------------------------------------
		-- Generate a list (block) of the oldest history Rx rows.
		---------------------------------------------------------
		Print 'Populating #TempTables'  + '   (' + Convert(VarChar(30), GetDate(), 120) + ')' + '-Try Count:' + CAST(@RetryCount AS VARCHAR(10))

		If Object_Id('TempDb..#BlkOfRxsToDel') Is Not Null
		Drop Table #BlkOfRxsToDel
		SELECT @LoopCount = 0, @LoopNo = 0
		Select
			distinct top (@MaxToDelete) CONVERT(varchar(30),OrderId) as OrderId, HistoryDtTm
		Into
			#BlkOfRxsToDel
		From
			OeOrderHistory WITH (nolock)
		Where
			HistoryDtTm >= @HistoryDtTm_EffectiveFrom And HistoryDtTm < @HistoryDtTm_EffectiveTo
			and 1=1 
		Order By
			HistoryDtTm
	SET @LoopCount  = @@ROWCOUNT
	---Now grab images..
		If Object_Id('TempDb..#RxImgIntIds') Is Not Null
		Drop Table #RxImgIntIds

			Select
			Distinct RIA.ImgId, R.OrderId
			Into 
			#RxImgIntIds
			From
			#BlkOfRxsToDel R
				Join
			ImgRxImgAssoc RIA WITH(NOLOCK)
				On RIA.OrderId = R.OrderId And RIA.HistoryDtTm = R.HistoryDtTm
				Join
			ImgImage_IntId II
				On II.Id = RIA.ImgId And II.IsMovedToFile = 1
		SET @ImageCount = @@ROWCOUNT
		CREATE CLUSTERED INDEX cdx_tmpRxImgIntIds ON #RxImgIntIds(OrderId)
		--Images.
			If Object_Id('TempDb..#RxImgIds') Is Not Null
        Drop Table #RxImgIds

        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        --Generate the list of Image Ids to delete for the block of Rxs (pick only the images that are already saved in file system)
        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Select
        Distinct RIA.ImgId,  R.OrderId
        Into
        #RxImgIds
        From
        #BlkOfRxsToDel R
            Join
        ImgRxImgAssoc RIA  WITH(NOLOCK)
            On RIA.OrderId = R.OrderId And RIA.HistoryDtTm = R.HistoryDtTm
            Join ImgImage II On II.Id = RIA.ImgId And II.IsMovedToFile = 1
		CREATE CLUSTERED INDEX cdx_tmpRxImgIds ON #RxImgIds(OrderId)

	If Object_Id('TempDb..#RxDoseSchedIds') Is Not Null
		Drop Table #RxDoseSchedIds

		Select Distinct DS.RxDoseSchedId, R.OrderId
		Into
		#RxDoseSchedIds
		From #BlkOfRxsToDel R
		INNER JOIN OeRxDoseSched DS  WITH(NOLOCK)
			On DS.OrderId = R.OrderId And DS.HistoryDtTm = R.HistoryDtTm
		CREATE CLUSTERED INDEX cdx_tmpRxDoseSchedIds ON #RxDoseSchedIds(OrderId)
	
		Create NONCLUSTERED Index ByOrderIdHistoryDtTm On #BlkOfRxsToDel(OrderId, HistoryDtTm)
		ALTER TABLE #BlkOfRxsToDel ADD TableID BIGINT IDENTITY(1,1)
		Create CLUSTERED Index ByTableID On #BlkOfRxsToDel(TableID)
select @LoopCount as LoopCount, @ImageCount As ImageCount
		-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		-- Generate a list of the PaperworkSetIds for the block of Rxs.
		-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		If Object_Id('TempDb..#PwkSetIds') Is Not Null
			Drop Table #PwkSetIds

		Select
		Distinct PO.PaperworkSetId, R.OrderId
		Into #PwkSetIds
		From #BlkOfRxsToDel R
		INNER JOIN PwkPaperworkSetOrderAssoc PO  WITH(NOLOCK)
			On PO.OrderId = R.OrderId And PO.HistoryDtTm = R.HistoryDtTm
		CREATE CLUSTERED INDEX cdx_tmpPwkSetIds ON #PwkSetIds(OrderId)

/*
		sp_help 'OeOrderSecondaryData'
		sp_help 'ShpShipType'


*/	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		-- Generate a list of the History Canister Replenishments (CanisterSn/LastReplenDtTm combos) for the block of Rxs.
		-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		If Object_Id('TempDb..#CanReplens') Is Not Null
			Drop Table #CanReplens

		Select	Distinct OCR.CanisterSn, OCR.ReplenDtTm--, R.OrderId
		Into #CanReplens
		From #BlkOfRxsToDel R
		INNER JOIN OeOrderCanReplenAssoc OCR WITH(NOLOCK)
			On OCR.OrderId = R.OrderId And OCR.HistoryDtTm = R.HistoryDtTm
		CREATE CLUSTERED INDEX cdx_tmpCanReplens ON #CanReplens(CanisterSn, ReplenDtTm)

		If Object_Id('TempDb..#ShpIds') Is Not Null
			Drop Table #ShpIds

		Select
			Distinct OS.ShipmentId
		Into #ShpIds
		From #BlkOfRxsToDel R
			INNER JOIN OeOrderShipmentAssoc OS
			On OS.OrderId = R.OrderId And OS.HistoryDtTm = R.HistoryDtTm

      BEGIN TRANSACTION
		-- This line is to show you on which execution we successfully commit.
		SELECT CAST (@RetryCount AS VARCHAR(5)) +  'st. Attempt'
	

			DECLARE @OrderId	varchar(30), @HistoryDtTm datetime
			SET @LoopNo  = 1
			Print 'Deleting Order Data rows and looping...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
			SET @CaAuditCount = 0
			SET @CvyRxRouteCount = 0
			SET @OeDurDataCount = 0
			SET @OeDurFreeFormTextCount = 0
			SET @OeLotCodeCount = 0
			SET @OeFlaggedRxsCount = 0
			SET @OeOrderAuxLabelTextCount = 0
			SET @OeOrderExtSysDocumentCount = 0
			SET @OeOrderExtUserDefCount = 0
			SET @OeOrderTcdAssocCount = 0
			SET @OeOrderThirdPartyPlanCount = 0
			SET @OeRxBagAssocCount = 0
			SET @OeRxItemHistoryCount = 0
			SET @OeRxPrefLangDataCount = 0
			SET @ImgRxImgAssocCount = 0
			SET @ImgImageCount = 0
			SET @RxImgIdsCount = 0
			SET @OeRxDoseSchedCount = 0
			SET @DsRxDoseSchedDoseCount = 0
			SET @DsRxDoseSchedDoseByDayOfWeekCount = 0
			SET @DsRxDoseSchedCount = 0
			SET @PwkPaperworkSetOrderAssocCount = 0
			SET @PwkPaperworkSetCount = 0 
			SET @OeOrderCanReplenAssocCount = 0 
			WHILE @LoopNo <= @LoopCount
			BEGIN 
				SET @OrderId = NULL
				SET @HistoryDtTm = NULL
				SELECT @OrderId = OrderID, 
					@HistoryDtTm = HistoryDtTm
				FROM #BlkOfRxsToDel WHERE TableID = @LoopNo
				--print @OrderId
				DELETE o2
				FROM  OeOrderSecondaryData O2 WITH(ROWLOCK)
				WHERE o2.OrderID = @OrderID AND O2.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeOrderSecondaryDataCount = @OeOrderSecondaryDataCount + @@ROWCOUNT
				--Next Table
				DELETE oc
				FROM  OeOrderCurrHistoryDtTm oc WITH(ROWLOCK)
				WHERE oc.OrderID = @OrderID AND oc.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeOrderCurrHistoryDtTmCount = @OeOrderCurrHistoryDtTmCount + @@ROWCOUNT

				--Ca Audit
				DELETE ca
				FROM  CaAudit ca WITH(ROWLOCK)
				WHERE ca.OrderID = @OrderID AND ca.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @CaAuditCount = @CaAuditCount + @@ROWCOUNT

				--Next Table - CvyRxRoute
				DELETE cx
				FROM  CvyRxRoute  cx WITH(ROWLOCK)
				WHERE cx.OrderID = @OrderID AND cx.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @CvyRxRouteCount = @CvyRxRouteCount + @@ROWCOUNT

				--Next Table
				DELETE oud
				FROM  OeDurData   oud WITH(ROWLOCK)
				WHERE oud.OrderID = @OrderID AND oud.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
	
				SET @OeDurDataCount = @OeDurDataCount + @@ROWCOUNT

				--Next Table -- OeDurFreeFormText
				DELETE DT
				FROM  OeDurFreeFormText DT WITH(ROWLOCK)
				WHERE DT.OrderID = @OrderID AND DT.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				--IF @@ROWCOUNT > 0 
					SET @OeDurFreeFormTextCount = @OeDurFreeFormTextCount + @@ROWCOUNT

				--Next Table  - OeFlaggedRxs
				DELETE FR
				FROM  OeFlaggedRxs FR WITH(ROWLOCK)
				WHERE FR.OrderID = @OrderID AND FR.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeFlaggedRxsCount = @OeFlaggedRxsCount + @@ROWCOUNT
				-- next table
				DELETE LC
				FROM  OeLotCode LC WITH(ROWLOCK)
				WHERE LC.OrderID = @OrderID AND LC.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeLotCodeCount = @OeLotCodeCount + @@ROWCOUNT

				--------------------------------------------------------------
				-- Delete OeOrderAuxLabelFile table rows for the block of Rxs.
				--------------------------------------------------------------
--				Print 'Deleting OeOrderAuxLabelFile rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

				Delete ALF
				From OeOrderAuxLabelFile ALF WITH(ROWLOCK)
				WHERE  ALF.OrderId = @OrderId And ALF.HistoryDtTm = @HistoryDtTm
				AND 1=@Testing
				SET @OeOrderAuxLabelFileCount = @OeOrderAuxLabelFileCount + @@ROWCOUNT


				--Next Table
				DELETE ALT
				FROM OeOrderAuxLabelText ALT WITH(ROWLOCK)
				WHERE ALT.OrderID = @OrderID AND ALT.HistoryDtTm = @HistoryDtTm
				and 1=@Testing

				SET @OeOrderAuxLabelTextCount = @OeOrderAuxLabelTextCount + @@ROWCOUNT

				--Next Table
				DELETE OD
				FROM OeOrderExtSysDocument OD WITH(ROWLOCK)
				WHERE OD.OrderID = @OrderID AND OD.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeOrderExtSysDocumentCount = @OeOrderExtSysDocumentCount + @@ROWCOUNT
				--Next Table OeOrderExtUserDef
				DELETE UD
				FROM   OeOrderExtUserDef UD WITH(ROWLOCK)
				WHERE UD.OrderID = @OrderID AND UD.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeOrderExtUserDefCount = @OeOrderExtUserDefCount + @@ROWCOUNT
				--
				--Next Table - OeOrderTcdAssoc
				DELETE OT
				FROM     OeOrderTcdAssoc OT WITH(ROWLOCK)
				WHERE OT.OrderID = @OrderID AND OT.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
		
				SET @OeOrderTcdAssocCount = @OeOrderTcdAssocCount + @@ROWCOUNT

				--Next Table  -OeOrderTextDocument
				DELETE TD
				FROM     OeOrderTextDocument TD WITH(ROWLOCK)
				WHERE TD.OrderID = @OrderID AND TD.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeOrderTextDocumentCount = @OeOrderTextDocumentCount + @@ROWCOUNT

				--Next Table  OeOrderThirdPartyPlan
				DELETE TP
				FROM     OeOrderThirdPartyPlan TP WITH(ROWLOCK)
				WHERE TP.OrderID = @OrderID AND TP.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeOrderThirdPartyPlanCount = @OeOrderThirdPartyPlanCount + @@ROWCOUNT

				--Next Table OeRxBagAssoc
				DELETE RB
				FROM      OeRxBagAssoc RB WITH(ROWLOCK)
				WHERE RB.OrderID = @OrderID AND RB.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeRxBagAssocCount = @OeRxBagAssocCount  + @@ROWCOUNT

				-- Next Table OeRxItemHistory
				DELETE RIH
				FROM      OeRxItemHistory RIH WITH(ROWLOCK)
				WHERE RIH.OrderID = @OrderID AND RIH.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeRxItemHistoryCount = @OeRxItemHistoryCount + @@ROWCOUNT

				--Next Table - OeRxPouchDispenserAssoc
				DELETE RPD
				FROM      OeRxPouchDispenserAssoc RPD WITH(ROWLOCK)
				WHERE RPD.OrderID = @OrderID AND RPD.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeRxPouchDispenserAssocCount = @OeRxPouchDispenserAssocCount + @@ROWCOUNT

			-- Next Table  - OeRxPrefLangData
				DELETE PLD
				FROM      OeRxPrefLangData PLD WITH(ROWLOCK)
				WHERE PLD.OrderID = @OrderID AND PLD.HistoryDtTm = @HistoryDtTm
				and 1=@Testing
				SET @OeRxPrefLangDataCount = @OeRxPrefLangDataCount + @@ROWCOUNT

				-- Next Talbe Start Images
				Delete
				RIA
				From (SELECT ImgID FROM #RxImgIntIds WHERE OrderID = @OrderId) RII
				Join ImgRxImgAssoc RIA WITH(ROWLOCK) On RII.ImgId = RIA.ImgId
				AND 1=@Testing 
				SET @ImgRxImgAssocCount = @ImgRxImgAssocCount + @@ROWCOUNT

				--Next Table
				Delete
				IMG
				From (SELECT ImgID FROM #RxImgIntIds WHERE OrderID = @OrderId) RII
				Join ImgImage_IntId IMG WITH(ROWLOCK)
				On IMG.Id = RII.ImgId
				AND 1=@Testing 
				SET @ImgImageCount = @ImgImageCount + @@ROWCOUNT

				-- Next Table
				Delete	RIA
				From #RxImgIds RII
				INNER Join ImgRxImgAssoc RIA WITH(ROWLOCK) On RII.ImgId = RIA.ImgId
				WHERE RII.OrderId = @OrderId
				and 1=@Testing 
				SET @ImgRxImgAssocCount = @ImgRxImgAssocCount + @@ROWCOUNT

				--Next Table
				Delete IMG
				From #RxImgIds RII
				Join ImgImage IMG WITH(ROWLOCK) On IMG.Id = RII.ImgId
				WHERE RII.OrderId = @OrderId
				AND 1=@Testing 
				SET @RxImgIdsCount = @RxImgIdsCount + @@ROWCOUNT

				Delete DS
				From OeRxDoseSched DS WITH(ROWLOCK)
				WHERE DS.OrderId = @OrderId And DS.HistoryDtTm = @HistoryDtTm
				AND 1=@Testing 
				SET @OeRxDoseSchedCount = @OeRxDoseSchedCount + @@ROWCOUNT

				Delete DSD
				From (SELECT RxDoseSchedId FROM #RxDoseSchedIds WHERE OrderID = @OrderID) DSIDS
				INNER Join DsRxDoseSchedDose DSD WITH(ROWLOCK)
					On DSD.RxDoseSchedId = DSIDS.RxDoseSchedId
				WHERE 1=@Testing 
				SET @DsRxDoseSchedDoseCount = @DsRxDoseSchedDoseCount + @@ROWCOUNT

				--Next Table DsDoseSched
				Delete DSD
				From (SELECT RxDoseSchedId FROM #RxDoseSchedIds WHERE OrderID = @OrderID) DSIDS
				INNER Join DsRxDoseSchedDoseByDayOfWeek DSD WITH(ROWLOCK)
					On DSD.RxDoseSchedId = DSIDS.RxDoseSchedId
				WHERE 1=@Testing 
				SET @DsRxDoseSchedDoseByDayOfWeekCount = @DsRxDoseSchedDoseByDayOfWeekCount + @@ROWCOUNT

				--Next Table  DsRxDoseSched
				Delete
				DS
				From (SELECT RxDoseSchedId FROM #RxDoseSchedIds WHERE OrderID = @OrderID) DSIDS 
					INNER Join DsRxDoseSched DS WITH(RowLock)
					On DS.Id = DSIDS.RxDoseSchedId
				WHERE 1=@Testing
				SET @DsRxDoseSchedCount = @DsRxDoseSchedCount + @@ROWCOUNT

				-- Next Table PwkPaperworkSetOrderAssoc
				Delete
				PO
				From
				(SELECT DISTINCT PaperworkSetId FROM #PwkSetIds WHERE OrderID = @OrderId) R
					INNER JOIN PwkPaperworkSetOrderAssoc PO WITH(ROWLOCK)
					On PO.PaperworkSetId = R.PaperworkSetId
					AND PO.OrderID = @OrderID
				WHERE 1=@Testing 
				SET @PwkPaperworkSetOrderAssocCount = @PwkPaperworkSetOrderAssocCount + @@ROWCOUNT

				-- Next Table
				-- Delete rows only if they no longer have (other Rx) PwkPaperworkSetOrderAssoc rows.
				--
				Delete	PWS
				From (SELECT DISTINCT PaperworkSetId FROM #PwkSetIds WHERE OrderID = @OrderId) PWSIDS
				INNER JOIN PwkPaperworkSet PWS WITH(ROWLOCK) On PWS.Id = PWSIDS.PaperworkSetId
				LEFT JOIN PwkPaperworkSetOrderAssoc PO WITH(NOLOCK) On PO.PaperworkSetId = PWS.Id
				Where
				PO.PaperworkSetId Is Null  -- No remaining PwkPaperworkSetOrderAssoc rows exists for PaperworkSetId.
				SET @PwkPaperworkSetCount = @PwkPaperworkSetCount + @@ROWCOUNT

				-- Next Table   OeOrderCanReplenAssoc
				Delete OCR
				From OeOrderCanReplenAssoc OCR WITH(ROWLOCK)
				WHERE OCR.OrderId = @OrderId And OCR.HistoryDtTm = @HistoryDtTm
				AND 1=@Testing
				SET @OeOrderCanReplenAssocCount = @OeOrderCanReplenAssocCount  + @@ROWCOUNT
				
				-- NExt Table  OeOrderCanReplenAssoc
				DELETE oac
				FROM OeOrderCanReplenAssoc OAC WITH(ROWLOCK)
				LEFT OUTER JOIN OeOrderHistory oh ON oh.orderID = OAC.OrderId AND OH.OrderId = @OrderId
				WHERE oh.OrderId is null
				and oac.OrderId = @OrderID
				and 1=@Testing
				SET @OeOrderCanReplenAssocCount = @OeOrderCanReplenAssocCount  + @@ROWCOUNT

				Delete OS
				FROM OeOrderShipmentAssoc OS WITH(ROWLOCK)
				WHERE OS.OrderId = @OrderID And OS.HistoryDtTm = @HistoryDtTm
					AND 1=@Testing
				SET @OeOrderShipmentAssocCount = @OeOrderShipmentAssocCount + @@ROWCOUNT
	
				SET @LoopNo = @LoopNo + 1
			END  -- Main While Loop End.. 
			Print 'Deleting Bulk Data / TempTable Joins...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
-- Outside of While Loop Put other code here. 
--Delete Can Replens
			 Delete
				CRS
			 From
				#CanReplens CRS
				   Join
				OeOrderCanReplenAssoc OCR
				   On OCR.CanisterSn = CRS.CanisterSn And OCR.ReplenDtTm = CRS.ReplenDtTm
				Delete
					CRL
					From
					#CanReplens CRS
						Join
					CanLotCodeHistory CRL WITH(ROWLOCK)
						On CRL.CanisterSn = CRS.CanisterSn And CRL.LastReplenDtTm = CRS.ReplenDtTm
							and 1=@Testing
					SET @CanLotCodeHistoryCount = @CanLotCodeHistoryCount  + @@ROWCOUNT 

					Delete
					CR
					From
					#CanReplens CRS
						Join
					CanCanisterHistory CR WITH(ROWLOCK)
						On CR.CanisterSn = CRS.CanisterSn And CR.LastReplenDtTm = CRS.ReplenDtTm
							and 1=@Testing

					SET @CanCanisterHistoryCount = @CanCanisterHistoryCount  + @@ROWCOUNT 
			  If Object_Id('#CanImgIntIds') Is Not Null
					Drop Table #CanImgIntIds
         
				 If Object_Id('TempDb..#CanImgIds') Is Not Null
					Drop Table #CanImgIds

			  Select
					Distinct CIA.ImgId
				 Into
					#CanImgIntIds
				 From
					#CanReplens CR
					   Join
					ImgCanImgAssoc CIA
					   On CR.CanisterSn = CIA.CanisterSn And CIA.LastReplenDtTm = CR.ReplenDtTm
					   Join
					ImgImage_IntId II
					   On II.Id = CIA.ImgId And II.IsMovedToFile = 1
					   --CanImgIds
				   Select DISTINCT CIA.ImgId
					 Into
						#CanImgIds
					 From #CanReplens CR
					INNER JOIN ImgCanImgAssoc CIA
						   On CR.CanisterSn = CIA.CanisterSn And CIA.LastReplenDtTm = CR.ReplenDtTm
						   INNER JOIN ImgImage II
						   On II.Id = CIA.ImgId And II.IsMovedToFile = 1
				  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
					 --Delete ImgCanImgAssoc table rows for the list of History Canister Replenishments.
					 -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
					 Delete 
						CIA
					 From 
						#CanImgIntIds CII
						   Join
						ImgCanImgAssoc CIA WITH(RowLock)
						   On CIA.ImgId = CII.ImgId 
					SET @ImgCanImgAssocCount = @ImgCanImgAssocCount + @@ROWCOUNT
					 Delete 
						CIA
					 From 
						#CanImgIds CII
						   Join
						ImgCanImgAssoc CIA WITH(RowLock)
						   On CIA.ImgId = CII.ImgId 
					WHERE 1=@Testing
				SET @ImgCanImgAssocCount = @ImgCanImgAssocCount + @@ROWCOUNT

					------------------------------------------------------------------------------------------------
					--Delete ImgImage and ImgImage_IntId table rows for the list of History Canister Replenishments.
					------------------------------------------------------------------------------------------------
					Delete Img
					From #CanImgIds CII
					INNER JOIN ImgImage_IntId Img WITH(ROWLOCK)
					On Img.Id = CII.ImgId
					WHERE 1=@Testing
					SET @ImgImage_IntIdCount = @ImgImage_IntIdCount + @@ROWCOUNT
         
					Delete Img
					From #CanImgIds CII
					INNER JOIN ImgImage Img  WITH(ROWLOCK)
					On Img.Id = CII.ImgId
					WHERE 1=@Testing
					SET @ImgImageCount = @ImgImageCount + @@ROWCOUNT

					--Delete Matching SpIDs
					Delete SI
					From #ShpIds SI
					INNER JOIN OeOrderShipmentAssoc OS WITH(NOLOCK)
						On OS.ShipmentId = SI.ShipmentId

					--Delete CvyPackageRoute Records
					Delete CPR
					From #ShpIds SI
					INNER Join CvyPackageRoute CPR WITH(ROWLOCK)
					On CPR.ShipmentId = SI.ShipmentId
					WHERE 1=@Testing
					SET @CvyPackageRouteCount = @CvyPackageRouteCount + @@ROWCOUNT

					--Delete PwkPrinterTray Records
					Delete PT
					From #ShpIds SI
					INNER Join PwkPrinterTray PT WITH(NOLOCK)
						On PT.ShipmentId = SI.ShipmentId
					WHERE 1=@Testing
					SET @PwkPrinterTrayCount = @PwkPrinterTrayCount + @@ROWCOUNT

					--Delete ShpShipmentPkgLabelData 
					Delete SL
					From #ShpIds SI
					INNER Join ShpShipmentPkgLabelData SL WITH(ROWLOCK)
						On SL.ShipmentId = SI.ShipmentId
					WHERE 1=@Testing
					SET @ShpShipmentPkgLabelDataCount = @ShpShipmentPkgLabelDataCount + @@ROWCOUNT

					--Delete ShpShipmentSpecialServiceAssoc
					Delete SSS
					From #ShpIds SI
					INNER Join ShpShipmentSpecialServiceAssoc SSS
						On SSS.ShipmentId = SI.ShipmentId
					WHERE 1=@Testing
					SET @ShpShipmentSpecialServiceAssocCount = @ShpShipmentSpecialServiceAssocCount + @@ROWCOUNT

					--Delete SrtShipToteShipmentAssoc
					Delete ST
					From #ShpIds SI
					INNER Join SrtShipToteShipmentAssoc ST
						On ST.ShipmentId = SI.ShipmentId
					WHERE 1=@Testing
					SET @SrtShipToteShipmentAssocCount = @SrtShipToteShipmentAssocCount  + @@ROWCOUNT

					--Now
					If Object_Id('TempDb..#ManIds') Is Not Null
						Drop Table #ManIds

					Select Distinct MS.ManifestId
					Into #ManIds
					From #ShpIds SI
					INNER Join ShpManifestShipmentAssoc MS WITH(ROWLOCK)
						On MS.ShipmentId = SI.ShipmentId

						        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
					-- Delete ShpManifestShipmentAssoc table rows for the list of Shipments.
					-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
					Delete MS
					From #ShpIds SI
					INNER JOIN ShpManifestShipmentAssoc MS WITH(ROWLOCK)
						On MS.ShipmentId = SI.ShipmentId
					WHERE 1=@Testing
					SET @ShpManifestShipmentAssocCount = @ShpManifestShipmentAssocCount  + @@ROWCOUNT

					-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- - - - - - - - - -
					-- Delete ShpShipmentPackageLabelInstance table rows for the list of Shipments.
					-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- - - - - - - - - -

					Delete SSPLI
					From #ShpIds SI
					INNER JOIN  ShpShipmentPackageLabelInstance SSPLI WITH(ROWLOCK)
					On SSPLI.ShipmentId = SI.ShipmentId
					WHERE 1=@Testing
					SET @ShpShipmentPackageLabelInstanceCount = @ShpShipmentPackageLabelInstanceCount + @@ROWCOUNT

					        -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
					-- Delete ShpShipment table rows for the list of Shipments.
					-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
					Delete	S
					From #ShpIds SI
					INNER JOIN ShpShipment S WITH(ROWLOCK)
						On S.Id = SI.ShipmentId
					WHERE 1=@Testing
					SET @ShpShipmentCount = @ShpShipmentCount + @@ROWCOUNT

					-- - - - - - - - - - - - - -
					-- Drop the temporary table.
					-- - - - - - - - - - - - - -
					Drop Table #ShpIds

					-- Reduce the list of the Manifests to those no longer having (other shipment) ShpManifestShipmentAssoc rows.
					-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
					-- To do this, remove from the list any row still having rows in table ShpManifestShipmentAssoc.
					--
					Delete MI
					From #ManIds MI
					INNER JOIN ShpManifestShipmentAssoc MS WITH(NOLOCK)
					On MS.ManifestId = MI.ManifestId

					      -- - - - - - - - - - - - - - - - - - - - - - - - - -
					-- Generate a list of the Pallets for the Manifests.
					-- - - - - - - - - - - - - - - - - - - - - - - - - -
					If Object_Id('TempDb..#PalIds') Is Not Null
						Drop Table #PalIds

					Select Distinct PM.PalletId
					Into #PalIds
					From #ManIds MI
					INNER JOIN ShpPalletManifestAssoc PM
						On PM.ManifestId = MI.ManifestId

					-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
					-- Delete ShpPalletManifestAssoc table rows for the list of Manifests.
					-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
					Delete PM
					From #ManIds MI
					INNER JOIN ShpPalletManifestAssoc PM  
						On PM.ManifestId = MI.ManifestId
					WHERE 1=@Testing
					SET @ShpPalletManifestAssocCount = @ShpPalletManifestAssocCount + @@ROWCOUNT

					-- - - - - - - - - - - - - - - - - - - - - - - -
					-- Generate a list of the Loads for the Pallets.
					-- - - - - - - - - - - - - - - - - - - - - - - -
					If Object_Id('TempDb..#LoadIds') Is Not Null
						Drop Table #LoadIds

					Select Distinct LP.LoadId
					Into #LoadIds
					From #PalIds [PI]
					INNER JOIN ShpLoadPalletAssoc LP
						On LP.PalletId = [PI].PalletId

						       -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
					-- Delete ShpLoadPalletAssoc table rows for the list of Pallets.
					-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
					--Print 'Deleting #PalIds/ShpLoadPalletAssoc' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
					Delete LP
					From #PalIds [PI]
					INNER JOIN ShpLoadPalletAssoc LP WITH(ROWLOCK)
						On LP.PalletId = [PI].PalletId
					WHERE 1=@Testing
					SET @ShpLoadPalletAssocCount = @ShpLoadPalletAssocCount  + @@ROWCOUNT

										-- - - - - - - - - - - - - - - - - - - - - - - - - -
					-- Delete ShpLoad table rows for the list of Loads.
					-- - - - - - - - - - - - - - - - - - - - - - - - - -
					Print 'Deleting #LoadIds/ShpLoad' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
					Delete L
					From #LoadIds LI
					INNER JOIN ShpLoad L WITH(ROWLOCK)
						On L.Id = LI.LoadId
					where not exists(select 1 from ShpLoadPalletAssoc lp where lp.LoadId = L.Id)
					AND 1=@Testing
					SET @ShpLoadCount = @ShpLoadCount  + @@ROWCOUNT


					Drop Table #CanReplens, #CanImgIds, #CanImgIntIds

					-- - - - - - - - - - - - - -
					-- Drop the temporary table.
					-- - - - - - - - - - - - - -
					Drop Table #LoadIds

					-- - - - - - - - - - - - - - - - - - - - - - - - - - - -
					-- Delete ShpPallet table rows for the list of Pallets.
					-- - - - - - - - - - - - - - - - - - - - - - - - - - - -
					--Print 'Deleting #PalIds/ShpPallet' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
					Delete P
					From #PalIds [PI]
					INNER JOIN ShpPallet P WITH(ROWLOCK)
						On P.Id = [PI].PalletId
					where not exists(select 1 from ShpPalletManifestAssoc pm where pm.PalletId = P.Id) 
					AND 1=@Testing

					SET @ShpPalletCount = @ShpPalletCount + @@ROWCOUNT
					-- - - - - - - - - - - - - -
					-- Drop the temporary table.
					Drop Table #PalIds

					       -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
					-- Delete ShpManifest table rows for the list of Manifests.
					-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--					Print 'Deleting #ManIds/ShpManifest' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
					Delete M
					From #ManIds MI
					INNER JOIN ShpManifest M WITH(ROWLOCK)
						On M.Id = MI.ManifestId
					WHERE 1=@Testing
					SET @ShpManifestCount = @ShpManifestCount + @@ROWCOUNT

					-- - - - - - - - - - - - - -
					-- Drop the temporary table.
					Drop Table #ManIds


				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Generate a list of the Groups for the block of Rxs (before deleting the OeOrderHistory table rows).
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				If Object_Id('TempDb..#Grps') Is Not Null
					Drop Table #Grps

				Select	Distinct OH.GroupNum
				INTO #Grps
				FROM #BlkOfRxsToDel R
				INNER JOIN OeOrderHistory OH
					On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Generate a list of the Rx Patients for the block of Rxs (before deleting the OeOrderHistory table rows).
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				If Object_Id('TempDb..#RxPats') Is Not Null
				Drop Table #RxPats

				Select Distinct OH.PatCustId
				Into #RxPats
				FROM #BlkOfRxsToDel R
				INNER JOIN OeOrderHistory OH
					On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm


				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Generate a list of the Group Patients (Order Customers) for the block of Rxs (before deleting the OeOrderHistory table rows).
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				If Object_Id('TempDb..#GrpPats') Is Not Null
					Drop Table #GrpPats

				Select Distinct G.PatCustId
				Into #GrpPats
				From #BlkOfRxsToDel R
				INNER JOIN OeOrderHistory OH
					On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm
					INNER JOIN OeGroup G
					On G.GroupNum = OH.GroupNum
            
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Generate a list of the distinct Rx & Group Patients for the block of Rxs (from the two lists).
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				If Object_Id('TempDb..#Pats') Is Not Null
					Drop Table #Pats

				Select	PatCustId
				Into	#Pats
				From (Select PatCustId From #RxPats Union Select PatCustId From #GrpPats) As RxAndGrpPats  -- Union results in distinct values.

				-- - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Drop the temporary tables folded into table #Pats.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - -
				Drop Table #RxPats
				Drop Table #GrpPats

				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Generate a list of the Prescribers for the block of Rxs (before deleting the OeOrderHistory table rows).
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				If Object_Id('TempDb..#Prescrs') Is Not Null
					Drop Table #Prescrs

				Select Distinct OH.PrescrId
				Into #Prescrs
				FROM #BlkOfRxsToDel R
				INNER JOIN OeOrderHistory OH
				On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm
				DECLARE @Loop2No INT = 1
				SET @OeOrderHistoryCount = 0 
				--Resume looping based on orderID now.
				SET @Loop2No  = 1
				Print 'Running Secondary Loop for OrderHistory Data...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

				WHILE @Loop2No <= @LoopCount
				BEGIN 
					SET @OrderId = NULL
					SET @HistoryDtTm = NULL
					SELECT @OrderId = OrderID, 
						@HistoryDtTm = HistoryDtTm
					FROM #BlkOfRxsToDel WHERE TableID = @Loop2No

					Delete	OH
					From OeOrderHistory OH WITH(ROWLOCK)
						WHERE OH.OrderId = @OrderID And OH.HistoryDtTm = @HistoryDtTm
						AND 1=@Testing
					SET @OeOrderHistoryCount = @OeOrderHistoryCount + @@ROWCOUNT


					Delete OC
					From OeComments OC WITH(ROWLOCK)
					LEFT OUTER JOIN OeOrder O On O.OrderId = OC.OrderId AND O.ORderID = @OrderId
					LEFT OUTER JOIN OeOrderHistory OH On OH.OrderId = OC.OrderId AND OH.ORderID = @OrderId
					Where
						OC.OrderId = @OrderId
					AND 
						O.OrderId Is Null   -- No OeOrder row exists for OrderId.
					And
						OH.OrderId Is Null  -- No OeOrderHistory row exists for OrderId.
					AND 1=@Testing
					SET @OeCommentsCount = @OeCommentsCount + @@ROWCOUNT
					IF @Loop2No = 1
						Print 'Deleting OeOrderPoNumAssoc rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

					Delete OP
					From OeOrderPoNumAssoc OP WITH(ROWLOCK)
					LEFT OUTER JOIN OeOrder O
						On O.OrderId = OP.OrderId AND O.OrderId = @OrderId
					LEFT OUTER JOIN OeOrderHistory OH
						On OH.OrderId = OP.OrderId AND OH.OrderId = @OrderId
					Where
					OP.OrderId = @OrderID 
					AND
					O.OrderId Is Null   -- No OeOrder row exists for OrderId.
					And
					OH.OrderId Is Null  -- No OeOrderHistory row exists for OrderId.
					AND 1=@Testing
					SET @OeOrderPoNumAssocCount = @OeOrderPoNumAssocCount + @@ROWCOUNT

--					        Print 'Deleting OeProblemRxs rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
						Delete PR
						From OeProblemRxs PR WITH(ROWLOCK)
						LEFT OUTER JOIN OeOrder O
							On O.OrderId = PR.OrderId AND O.OrderId = @OrderId
						LEFT OUTER JOIN OeOrderHistory OH
							On OH.OrderId = PR.OrderId AND OH.OrderId = @OrderId
						Where
						PR.OrderId = @OrderID 
						AND
						O.OrderId Is Null   -- No OeOrder row exists for OrderId.
						And
						OH.OrderId Is Null  -- No OeOrderHistory row exists for OrderId.
						AND 1=@Testing
						SET @OeProblemRxsCount = @OeProblemRxsCount + @@ROWCOUNT

												---------------------------------------------------------------------
					-- Delete OeRequireCorrespondenceRxs table rows for the block of Rxs.
					---------------------------------------------------------------------
					-- This table is keyed by OrderId alone (i.e., no HistoryDtTm).
					-- Delete rows only if they no longer have an OrderId row in OeOrder or OeOrderHistory.  (So, this deletion must follow the deletion from OeOrderHistory.)
					-- Note:  The "From" can result in more than one row for each OrderId, but a "Delete <table> From" statement is forgiving and acts on unique OrderId values.
					--
					--Print 'Deleting OeRequireCorrespondenceRxs rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

						Delete RC
						From OeRequireCorrespondenceRxs RC WITH(ROWLOCK)
							LEFT OUTER JOIN OeOrder O
							On O.OrderId = RC.RxNumOrOrderId AND O.OrderId = @OrderId
							LEFT OUTER JOIN OeOrderHistory OH 
							On OH.OrderId = RC.RxNumOrOrderId 
							AND OH.OrderId = @OrderId
						Where
						RC.RxNumOrOrderId = @OrderId
						AND 
						O.OrderId Is Null   -- No OeOrder row exists for OrderId.
						And
						OH.OrderId Is Null  -- No OeOrderHistory row exists for OrderId.
						AND 1=@Testing
						SET @OeRequireCorrespondenceRxsCount = @OeRequireCorrespondenceRxsCount + @@ROWCOUNT

						--OeStatusTrail
						Delete ST
						From OeStatusTrail ST WITH(ROWLOCK)
						LEFT OUTER JOIN OeOrder O On O.OrderId = ST.OrderId
						Left OUTER Join OeOrderHistory OH On OH.OrderId = ST.OrderId
						Where
							O.OrderId Is Null   -- No OeOrder row exists for OrderId.
						And
							OH.OrderId Is Null  -- No OeOrderHistory row exists for OrderId.
						AND ST.OrderId = @OrderId
						AND 1=@Testing
						SET @OeStatusTrailCount = @OeStatusTrailCount + @@ROWCOUNT

						--OeOrderAcceptReject 
						DELETE OOA
						FROM OeOrderAcceptReject OOA WITH(ROWLOCK)
						WHERE OOA.OrderId= @OrderId
						AND 1=@Testing
						SET @OeOrderAcceptRejectCount = @OeOrderAcceptRejectCount + @@ROWCOUNT

					SET @Loop2No = @Loop2No + 1
				END
			--Next Temp Table Actions
				Delete GN
				From #Grps GN
				INNER Join OeOrderHistory OH On OH.GroupNum = GN.GroupNum

				Delete GN
				From #Grps GN
				INNER JOIN OeOrder O On O.GroupNum = GN.GroupNum

				Delete GN
				From #Grps GN
				INNER JOIN  OeRxRequest RQ On RQ.GroupNum = GN.GroupNum

				      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Delete GovOnHoldGroups table rows for the list of Groups.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- This table should only contain rows for active groups, but the deletion is done for safety to ensure the groups will delete.
				Delete HG
				From #Grps GN
				INNER JOIN GovOnHoldGroups HG
					On HG.GroupNum = GN.GroupNum
				WHERE 1=@Testing
				SET @GovOnHoldGroupsCount = @GovOnHoldGroupsCount + @@ROWCOUNT

				-- Delete OeExceptionGroups table rows for the list of Groups.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- This table should only contain rows for active groups, but the deletion is done for safety to ensure the groups will delete.
				Delete XG
				From #Grps GN
				INNER JOIN OeExceptionGroups XG
					On XG.GroupNum = GN.GroupNum
				WHERE 1=@Testing
				SET @OeExceptionGroupsCount = @OeExceptionGroupsCount + @@ROWCOUNT


							-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Delete OeGroupCvyFillData table rows for the list of Groups.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- This table should only contain rows for active groups, but the deletion is done for safety to ensure the groups will delete.
				Delete GCF
				From #Grps GN
				INNER JOIN OeGroupCvyFillData GCF
					On GCF.GroupNum = GN.GroupNum
				WHERE 1=@Testing
				SET @OeGroupCvyFillDataCount = @OeGroupCvyFillDataCount + @@ROWCOUNT
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Delete OeGroupOrderData table rows for the list of Groups.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				Delete [GO]
				From #Grps GN
				INNER JOIN OeGroupOrderData [GO]
					On [GO].GroupNum = GN.GroupNum
				WHERE 1=@Testing

				SET @OeGroupOrderDataCount = @OeGroupOrderDataCount + @@ROWCOUNT
	  

	  ---
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Delete OeGroupPayment table rows for the list of Groups.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				Delete GP
				From #Grps GN
				INNER JOIN OeGroupPayment GP
					On GP.GroupNum = GN.GroupNum
				WHERE 1=@Testing
				SET @OeGroupPaymentCount = @OeGroupPaymentCount + @@ROWCOUNT

				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Delete OeGroupUserDef table rows for the list of Groups.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				Delete GUD
				From #Grps GN
				INNER JOIN OeGroupUserDef GUD
					On GUD.GroupNum = GN.GroupNum
				WHERE 1=@Testing
				SET @OeGroupUserDefCount = @OeGroupUserDefCount + @@ROWCOUNT

				         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
				-- Delete the references of ParentGroupNum Column from OeGroup table rows for the list of Groups.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
				Delete PG
				From #Grps GN
				INNER JOIN OeGroup PG
					On PG.ParentGroupNum = GN.GroupNum
				WHERE 1=@Testing
				SET @OeGroupCount = @OeGroupCount + @@ROWCOUNT

				-- - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Delete OeGroup table rows for the list of Groups.
				-- - - - - - - - - - - - - - - - - - - - - - - - - -
				Delete G
				From #Grps GN
				INNER JOIN OeGroup G On G.GroupNum = GN.GroupNum
				WHERE 1=@Testing
				SET @OeGroupCount = @OeGroupCount + @@ROWCOUNT
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Reduce the list of the Patients to those no longer having (other Rx) OeOrderHistory rows (or other Rx/Request referencing tables).
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- To do this, remove from the list any row still having rows in referencing tables.
				--
				Delete PT
				From #Pats PT
				INNER JOIN 
				OeOrderHistory OH
					On OH.PatCustId = PT.PatCustId

				Delete PT
				From #Pats PT
				INNER JOIN OeOrder O
					On O.PatCustId = PT.PatCustId

				Delete PT
				From #Pats PT
				INNER JOIN OeRxRequest RQ
					On RQ.PatCustId = PT.PatCustId

				Delete PT
				From #Pats PT
				INNER JOIN OeGroup G
					On G.PatCustId = PT.PatCustId
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Delete OePatientCustSecondarydata table rows for the list of Patients.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				Delete P2
				From #Pats PT
				INNER JOIN OePatientCustSecondarydata P2
					On P2.PatCustId = PT.PatCustId
				WHERE 1=@Testing
				SET @OePatientCustSecondarydataCount = @OePatientCustSecondarydataCount + @@ROWCOUNT

				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Delete PrivacySignature table rows for the list of Patients.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- This table is not used in HV, but the deletion is done for safety to ensure the patients will delete.
				--
				Delete PS
				From #Pats PT
				INNER JOIN PrivacySignature PS
					On PS.PatCustId = PT.PatCustId
				WHERE 1=@Testing
				SET @PrivacySignatureCount = @PrivacySignatureCount + @@ROWCOUNT

				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				-- Delete OePatientCust table rows for the list of Patients.
				-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				Delete P
				From #Pats PT
				INNER JOIN OePatientCust P
					On P.PatCustId = PT.PatCustId
				WHERE 1=@Testing
				SET @OePatientCustCount = @OePatientCustCount + @@ROWCOUNT

	
	If Object_Id('TempDb..#Pats') Is Not Null
			Drop Table #Pats

		If Object_Id('TempDb..#Grps') Is Not Null
			Drop Table #Grps
		If Object_Id('TempDb..#ShpIds') Is Not Null
			Drop Table #ShpIds
	
	-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Reduce the list of the Prescribers to those no longer having (other Rx) OeOrderHistory rows (or other Rx/Request referencing tables).
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- To do this, remove from the list any row still having rows in referencing tables.

				Delete PRI
				From #Prescrs PRI
				INNER JOIN OeOrderHistory OH
					On OH.PrescrId = PRI.PrescrId

				Delete PRI
				From #Prescrs PRI
				INNER JOIN OeRxRequest RQ
					On RQ.PrescrId = PRI.PrescrId

	

-- Commenting out OePrescriber delete to avoid deadlocking in OeOrder. Orphaned OePrescriber rows to cleaned up separately.
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--Delete OePrescriber table rows for the list of Prescribers.
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
				DELETE PR
				From #Prescrs PRI
				INNER JOIN OePrescriber PR WITH(ROWLOCK)  On PR.PrescrId = PRI.PrescrId
				WHERE 1=@Testing
				SET @OePrescriberCount = @OePrescriberCount + @@ROWCOUNT


	--Log Recourd Counts
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderCurrHistoryDtTm', @OeOrderCurrHistoryDtTmCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderSecondaryData', @OeOrderSecondaryDataCount, GETDATE()	
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'CvyRxRoute', @CvyRxRouteCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeDurData', @OeDurDataCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeDurFreeFormText', @OeDurFreeFormTextCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeFlaggedRxs', @OeFlaggedRxsCount, GETDATE()	
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeLotCode', @OeLotCodeCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderAuxLabelFile', @OeOrderAuxLabelFileCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'CaAudit', @CaAuditCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderAuxLabelText', @OeOrderAuxLabelTextCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderExtSysDocument', @OeOrderExtSysDocumentCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderExtUserDef', @OeOrderExtUserDefCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderTcdAssoc', @OeOrderTcdAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderTextDocument', @OeOrderTextDocumentCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderThirdPartyPlan', @OeOrderThirdPartyPlanCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeRxBagAssoc', @OeRxBagAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeRxItemHistory', @OeRxItemHistoryCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeRxPouchDispenserAssoc', @OeRxPouchDispenserAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeRxPrefLangData', @OeRxPrefLangDataCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ImgImage_IntId', @ImgImage_IntIdCount, GETDATE()
			
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ImgRxImgAssoc', @ImgRxImgAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ImgImage', @RxImgIdsCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeRxDoseSched', @OeRxDoseSchedCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'DsRxDoseSchedDose', @DsRxDoseSchedDoseCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'DsRxDoseSchedDoseByDayOfWeek', @DsRxDoseSchedDoseByDayOfWeekCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'DsRxDoseSched', @DsRxDoseSchedCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'PwkPaperworkSetOrderAssoc', @PwkPaperworkSetOrderAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'PwkPaperworkSet', @PwkPaperworkSetCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderCanReplenAssoc', @OeOrderCanReplenAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'CanLotCodeHistory', @CanLotCodeHistoryCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'CanCanisterHistory', @CanCanisterHistoryCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ImgCanImgAssoc', @ImgCanImgAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderShipmentAssoc', @OeOrderShipmentAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'CvyPackageRoute', @CvyPackageRouteCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'PwkPrinterTray', @PwkPrinterTrayCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ShpShipmentPkgLabelData', @ShpShipmentPkgLabelDataCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ShpShipmentSpecialServiceAssoc', @ShpShipmentSpecialServiceAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'SrtShipToteShipmentAssoc', @SrtShipToteShipmentAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ShpManifestShipmentAssoc', @ShpManifestShipmentAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ShpShipmentPackageLabelInstance', @ShpShipmentPackageLabelInstanceCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ShpShipment', @ShpShipmentCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ShpPalletManifestAssoc', @ShpPalletManifestAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ShpLoadPalletAssoc', @ShpLoadPalletAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ShpLoad', @ShpLoadCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ShpPallet', @ShpPalletCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'ShpManifest', @ShpManifestCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderHistory', @OeOrderHistoryCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeComments', @OeCommentsCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderPoNumAssoc', @OeOrderPoNumAssocCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeProblemRxs', @OeProblemRxsCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeRequireCorrespondenceRxs', @OeRequireCorrespondenceRxsCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeStatusTrail', @OeStatusTrailCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'GovOnHoldGroups', @GovOnHoldGroupsCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeExceptionGroups', @OeExceptionGroupsCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeGroupCvyFillData', @OeGroupCvyFillDataCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeGroupOrderData', @OeExceptionGroupsCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeGroupPayment', @OeGroupPaymentCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeGroupUserDef', @OeGroupUserDefCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeGroup', @OeGroupCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OePatientCustSecondarydata', @OePatientCustSecondarydataCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'PrivacySignature', @PrivacySignatureCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'PrivacySignature', @OePatientCustCount, GETDATE()
			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OePrescriber', @OePrescriberCount, GETDATE()

				

			INSERT INTO @PurgedTables (TableName, RecordsDeleted, PurgeDate)
				SELECT 'OeOrderAcceptReject', @OeOrderAcceptRejectCount, GETDATE()
	


				
	
	   print @loopNo
	  -- SET @LoopNo = @LoopNo + 1

				-- This Delay is set in order to simulate failure, DO NOT USE IN REAL CODE!
			WAITFOR DELAY @delay;

			--SELECT * FROM #BlkOfRxsToDel
			COMMIT TRANSACTION
			SELECT 'Success!'

			SELECT @Success = 1 -- To exit the loop
			SET @Results = 'Qty: ' + CAST(1000 AS VARCHAR(20))
			Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@HistoryDtTmFrom=' + Cast(@HistoryDtTmFrom As VarChar(20)) 
			Exec lsp_DbLogSqlEvent 'A', 'DbPurgeV6 End', @Results, @EvtNotes, 'lsp_DbPurgeV6'
	   END TRY
	   BEGIN CATCH
		  SELECT  @ErrorNumber = ERROR_NUMBER(),
			  @ErrorMessage = ERROR_MESSAGE();     
	   	IF @@TRANCOUNT > 0
		  ROLLBACK TRANSACTION
		  -- Now we check the error number to, only use retry logic on the errors we are able to handle.

		  IF @ErrorNumber IN (  1204, -- SqlOutOfLocks
								  1205, -- SqlDeadlockVictim
								  1222 -- SqlLockRequestTimeout
								  )
		  BEGIN
				SET @RetryCount = @RetryCount + 1
				-- This delay is to give the blocking transaction time to finish. So you need to tune according to your environment
				WAITFOR DELAY '00:00:02'
		  END
		  ELSE
		  BEGIN
				-- If we don’t have a handler for current error then we throw an exception and abort the loop
				IF @@TRANCOUNT > 0
					rollback;
				THROW;
		  END
	   END CATCH
print 'next'
END
If Object_Id('TempDb..#RxImgIntIds') Is Not Null
    Drop Table #RxImgIntIds
If Object_Id('TempDb..#BlkOfRxsToDel') Is Not Null
    Drop Table #BlkOfRxsToDel
If Object_Id('TempDb..#RxImgIds') Is Not Null
    Drop Table #RxImgIds

If Object_Id('TempDb..#RxDoseSchedIds') Is Not Null
    Drop Table #RxDoseSchedIds

select * from @PurgedTables order by tableNAme