ALTER PROCEDURE "dbo"."lsp_DbPurgeHistoryData" 
     @OlderThanXDays Int = 36500
   , @NumOfRowsBlockSize Int = 100000
   , @MaxToDelete Int = 25000000

   -- Optional: purge only within this window (inclusive/exclusive). If both Null, uses @OlderThanXDays cutoff.
   , @HistoryDtTmFrom DateTime = Null   -- inclusive
   , @HistoryDtTmTo DateTime = Null   -- exclusive

   -- Optional outputs
   , @HistoryRowsDeleted Int = Null Output
   , @AcceptRejectRowsDeleted Int = Null Output
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_DbPurgeHistoryData

Version:
   01

Description:
   Delete data for Rxs that were completed (and moved to history) prior to X days ago.
   This deletes rows from many, many tables, including Rx-related data tables as well as other object tables propagated to for the deleted Rxs
     (e.g., Rx group, shipment tables, image tables).

Parameters:
   Input:
      @OlderThanXDays      - Data for Rxs that were completed prior to this many days ago will be deleted.
      @NumOfRowsBlockSize  - The history Rxs (rows) will be deleted in blocks of this many rows at a time
                               along with rows from the tables propagated to for these Rxs.
                             Deletions are performed in blocks of rows to prevent performing one huge transaction that would inflate the tempdb
                               and the t-log (even more than it holding the deletes themselves) and perform slowly due to broad locking.
      @MaxToDelete         - The maximum number of history Rxs to delete.
                             If this value is not a multiple of @NumOfRowsBlockSize, the effect will be approximate.
                             Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition where a huge
                               number of "older than X days" rows exist when this action is performed.
                             The goal is to delete a "finite" amount of data between full database backups where the t-log is truncated.
   Output:
      None

Return Value:
   @Rc - 0 = Successful; Non-0 = Failed

Comments:
   This procedure is called from the nightly maintenance procedure.
   The oldest Rxs are deleted first.

   This procedure deletes other object data based on the relationship of the Rxs being deleted to the "other objects" (e.g. shipments, image associations)
   rather than issuing delete statements such as "Delete From Shipment Where Id Not In (Select Shipment From OeOrderShipmentAssoc)" (or Left Join 
   checking Null)   because the later can be hugely inefficient or less efficient because it requires dealing with the entire tables.
   As such, this makes the steps involved in the deletion of data much more complex.
*/
Declare @Rc Int                                 -- SQL Command Return Code.  (Saved Value of @@Error.)
      , @CutoffDtTm DateTime                    -- "X Days Ago" Cutoff Date/Time.
      , @HistoryDtTm_EffectiveFrom DateTime     -- Effective inclusive lower bound.
      , @HistoryDtTm_EffectiveTo DateTime       -- Effective exclusive upper bound.
      , @DeletedThisPass Int = 0                -- Rows deleted in the most recent delete statement.
      , @HistoryRowsDeletedLocal Int = 0
      , @AcceptRejectRowsDeletedLocal Int = 0
      , @NumOfRowsDeleted Int = 0      -- Number of rows that have been deleted.  For this SP, this is OeOrderHistory rows, specifically.
      , @EvtNotes VarChar(255)         -- Logged event notes.
      , @ErrNum Int                    -- Error number (if error occurs).
      , @ErrMsg NVarChar(4000)         -- Error message (if error occurs).

Declare @BlkIds Table(Id Int)          -- Block of IDs to be deleted.
Declare @ContentCodes Table(Code Int)  -- List of @ImgsOfContentCodes as a table.
-------------------------------------------------------------
-- Set the Cutoff Date/Time from the Older Than X Days value.
-------------------------------------------------------------
Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())

-- - - - - - - - - - -
-- Log a begin event.
-- - - - - - - - - - -
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@NumOfRowsBlockSize=' + Cast(@NumOfRowsBlockSize As VarChar(12)) + ';@MaxToDelete=' + Cast(@MaxToDelete As VarChar(12))
If @HistoryDtTmFrom Is Not Null
   Set @EvtNotes = @EvtNotes + ';@HistoryDtTmFrom=' + Convert(VarChar(30), @HistoryDtTmFrom, 120) + ';@HistoryDtTmTo=' + Convert(VarChar(30), @HistoryDtTmTo, 120)
Exec lsp_DbLogSqlEvent 'A', 'PurgeOldData Beg', '', @EvtNotes, 'lsp_DbPurgeHistoryData'


   If (@HistoryDtTmFrom Is Null And @HistoryDtTmTo Is Null) And @OlderThanXDays <= 0
      Begin
         Set @ErrMsg = FormatMessage('Invalid parameter value. (@OlderThanXDays = %i)', @OlderThanXDays)
         RaisError(@ErrMsg, 16, 1)
      End

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

   --====================
   -- OeOrderHistory Loop
   --====================
   Set @NumOfRowsDeleted = 0

   ------------------------------------------------------------------------------------------
   -- While there are old rows and the maximum number of deletions has not occurred:
   --   Progress with deleting a block of history Rx rows and all propagated-to tables' rows.
   ------------------------------------------------------------------------------------------
   While Exists (Select * From OeOrderHistory Where HistoryDtTm >= @HistoryDtTm_EffectiveFrom And HistoryDtTm < @HistoryDtTm_EffectiveTo) And @NumOfRowsDeleted < @MaxToDelete
      Begin
         
         ---------------------------------------------------------
         -- Generate a list (block) of the oldest history Rx rows.
         ---------------------------------------------------------
         Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         If Object_Id('TempDb..#BlkOfRxsToDel') Is Not Null
            Drop Table #BlkOfRxsToDel

         Select
            Top (@NumOfRowsBlockSize) OrderId, HistoryDtTm
         Into
            #BlkOfRxsToDel
         From
            OeOrderHistory
         Where
            HistoryDtTm >= @HistoryDtTm_EffectiveFrom And HistoryDtTm < @HistoryDtTm_EffectiveTo
         Order By
            HistoryDtTm
      
         Create Index ByOrderIdHistoryDtTm On #BlkOfRxsToDel(OrderId, HistoryDtTm)

         ---------------------------------------------------------------
         -- Delete OeOrderSecondaryData table rows for the block of Rxs.
         ---------------------------------------------------------------
         Print 'Deleting OeOrderSecondaryData rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            O2
         From
            #BlkOfRxsToDel R
               Join
            OeOrderSecondaryData O2
               On O2.OrderId = R.OrderId And O2.HistoryDtTm = R.HistoryDtTm

         -----------------------------------------------------------------
         -- Delete OeOrderCurrHistoryDtTm table rows for the block of Rxs.
         -----------------------------------------------------------------
         Print 'Deleting OeOrderCurrHistoryDtTm rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            OC
         From
            #BlkOfRxsToDel R
               Join
            OeOrderCurrHistoryDtTm OC
               On OC.OrderId = R.OrderId And OC.HistoryDtTm = R.HistoryDtTm

         --------------------------------------------------
         -- Delete CaAudit table rows for the block of Rxs.
         --------------------------------------------------
         Print 'Deleting CaAudit rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            CA
         From
            #BlkOfRxsToDel R
               Join
            CaAudit CA
               On CA.OrderId = R.OrderId And CA.HistoryDtTm = R.HistoryDtTm

         -----------------------------------------------------
         -- Delete CvyRxRoute table rows for the block of Rxs.
         -----------------------------------------------------
         Print 'Deleting CvyRxRoute rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            CR
         From
            #BlkOfRxsToDel R
               Join
            CvyRxRoute CR
               On CR.OrderId = R.OrderId And CR.HistoryDtTm = R.HistoryDtTm

         ----------------------------------------------------
         -- Delete OeDurData table rows for the block of Rxs.
         ----------------------------------------------------
         Print 'Deleting OeDurData rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            DD
         From
            #BlkOfRxsToDel R
               Join
            OeDurData DD
               On DD.OrderId = R.OrderId And DD.HistoryDtTm = R.HistoryDtTm

         ------------------------------------------------------------
         -- Delete OeDurFreeFormText table rows for the block of Rxs.
         ------------------------------------------------------------
         Print 'Deleting OeDurFreeFormText rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            DT
         From
            #BlkOfRxsToDel R
               Join
            OeDurFreeFormText DT
               On DT.OrderId = R.OrderId And DT.HistoryDtTm = R.HistoryDtTm

         -------------------------------------------------------
         -- Delete OeFlaggedRxs table rows for the block of Rxs.
         -------------------------------------------------------
         Print 'Deleting OeFlaggedRxs rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            FR
         From
            #BlkOfRxsToDel R
               Join
            OeFlaggedRxs FR
               On FR.OrderId = R.OrderId And FR.HistoryDtTm = R.HistoryDtTm

         ----------------------------------------------------
         -- Delete OeLotCode table rows for the block of Rxs.
         ----------------------------------------------------
         Print 'Deleting OeLotCode rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            LC
         From
            #BlkOfRxsToDel R
               Join
            OeLotCode LC
               On LC.OrderId = R.OrderId And LC.HistoryDtTm = R.HistoryDtTm

         --------------------------------------------------------------
         -- Delete OeOrderAuxLabelFile table rows for the block of Rxs.
         --------------------------------------------------------------
         Print 'Deleting OeOrderAuxLabelFile rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            ALF
         From
            #BlkOfRxsToDel R
               Join
            OeOrderAuxLabelFile ALF
               On ALF.OrderId = R.OrderId And ALF.HistoryDtTm = R.HistoryDtTm

         --------------------------------------------------------------
         -- Delete OeOrderAuxLabelText table rows for the block of Rxs.
         --------------------------------------------------------------
         Print 'Deleting OeOrderAuxLabelText rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            ALT
         From
            #BlkOfRxsToDel R
               Join
            OeOrderAuxLabelText ALT
               On ALT.OrderId = R.OrderId And ALT.HistoryDtTm = R.HistoryDtTm

         ----------------------------------------------------------------
         -- Delete OeOrderExtSysDocument table rows for the block of Rxs.
         ----------------------------------------------------------------
         Print 'Deleting OeOrderExtSysDocument rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            OD
         From
            #BlkOfRxsToDel R
               Join
            OeOrderExtSysDocument OD
               On OD.OrderId = R.OrderId And OD.HistoryDtTm = R.HistoryDtTm

         ------------------------------------------------------------
         -- Delete OeOrderExtUserDef table rows for the block of Rxs.
         ------------------------------------------------------------
         Print 'Deleting OeOrderExtUserDef rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            UD
         From
            #BlkOfRxsToDel R
               Join
            OeOrderExtUserDef UD
               On UD.OrderId = R.OrderId And UD.HistoryDtTm = R.HistoryDtTm

         ----------------------------------------------------------
         -- Delete OeOrderTcdAssoc table rows for the block of Rxs.
         ----------------------------------------------------------
         Print 'Deleting OeOrderTcdAssoc rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            OT
         From
            #BlkOfRxsToDel R
               Join
            OeOrderTcdAssoc OT
               On OT.OrderId = R.OrderId And OT.HistoryDtTm = R.HistoryDtTm

         --------------------------------------------------------------
         -- Delete OeOrderTextDocument table rows for the block of Rxs.
         --------------------------------------------------------------
         Print 'Deleting OeOrderTextDocument rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            TD
         From
            #BlkOfRxsToDel R
               Join
            OeOrderTextDocument TD
               On TD.OrderId = R.OrderId And TD.HistoryDtTm = R.HistoryDtTm

         ----------------------------------------------------------------
         -- Delete OeOrderThirdPartyPlan table rows for the block of Rxs.
         ----------------------------------------------------------------
         Print 'Deleting OeOrderThirdPartyPlan rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            TP
         From
            #BlkOfRxsToDel R
               Join
            OeOrderThirdPartyPlan TP
               On TP.OrderId = R.OrderId And TP.HistoryDtTm = R.HistoryDtTm

         -------------------------------------------------------
         -- Delete OeRxBagAssoc table rows for the block of Rxs.
         -------------------------------------------------------
         Print 'Deleting OeRxBagAssoc rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            RB
         From
            #BlkOfRxsToDel R
               Join
            OeRxBagAssoc RB
               On RB.OrderId = R.OrderId And RB.HistoryDtTm = R.HistoryDtTm

         ----------------------------------------------------------
         -- Delete OeRxItemHistory table rows for the block of Rxs.
         ----------------------------------------------------------
         Print 'Deleting OeRxItemHistory rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            RIH
         From
            #BlkOfRxsToDel R
               Join
            OeRxItemHistory RIH
               On RIH.OrderId = R.OrderId And RIH.HistoryDtTm = R.HistoryDtTm

         ------------------------------------------------------------------
         -- Delete OeRxPouchDispenserAssoc table rows for the block of Rxs.
         ------------------------------------------------------------------
         Print 'Deleting OeRxPouchDispenserAssoc rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            RPD
         From
            #BlkOfRxsToDel R
               Join
            OeRxPouchDispenserAssoc RPD
               On RPD.OrderId = R.OrderId And RPD.HistoryDtTm = R.HistoryDtTm

         ------------------------------------------------------------------
         -- Delete OeRxPouchDispenserAssoc table rows for the block of Rxs.
         ------------------------------------------------------------------
         Print 'Deleting OeRxPrefLangData rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            PLD
         From 
            #BlkOfRxsToDel R
               Join
            OeRxPrefLangData PLD
               On PLD.OrderId = R.OrderId And PLD.HistoryDtTm = R.HistoryDtTm

         -------------------------------------------------------------------------------------
         --Delete ImgRxImageAssoc, ImgImage and ImgImage_IntId table rows for the block of Rxs
         -------------------------------------------------------------------------------------
         
         Print 'Deleting ImgRxImgAssoc, ImgImage, ImgImage_IntId rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         -- - - - - - - - - - - - - - - 
         -- Process ImgImage_IntId table
         -- - - - - - - - - - - - - - - 
         If Object_Id('TempDb..#RxImgIntIds') Is Not Null
            Drop Table #RxImgIntIds

         Select
            Distinct RIA.ImgId
         Into 
            #RxImgIntIds
         From
            #BlkOfRxsToDel R
               Join
            ImgRxImgAssoc RIA
               On RIA.OrderId = R.OrderId And RIA.HistoryDtTm = R.HistoryDtTm
               Join
            ImgImage_IntId II
               On II.Id = RIA.ImgId And II.IsMovedToFile = 1

         Delete
            RIA
         From 
            #RxImgIntIds RII
            Join ImgRxImgAssoc RIA
               On RII.ImgId = RIA.ImgId

         Delete
            IMG
         From #RxImgIntIds RII
            Join ImgImage_IntId IMG
               On IMG.Id = RII.ImgId

         Drop Table #RxImgIntIds

         -- - - - - - - - - - - - 
         -- Process ImgImage table
         -- - - - - - - - - - - - 

         If Object_Id('TempDb..#RxImgIds') Is Not Null
            Drop Table #RxImgIds

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         --Generate the list of Image Ids to delete for the block of Rxs (pick only the images that are already saved in file system)
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Select
            Distinct RIA.ImgId
         Into
            #RxImgIds
         From
            #BlkOfRxsToDel R
               Join
            ImgRxImgAssoc RIA
               On RIA.OrderId = R.OrderId And RIA.HistoryDtTm = R.HistoryDtTm
               Join
            ImgImage II
               On II.Id = RIA.ImgId And II.IsMovedToFile = 1

         -- - - - - - - - - - - - - - - - - - 
         --Delete the ImgRxImgAssoc table rows
         -- - - - - - - - - - - - - - - - - - 
         Delete
            RIA
         From #RxImgIds RII
            Join
         ImgRxImgAssoc RIA
            On RII.ImgId = RIA.ImgId

         -- - - - - - - - - - - - - - - -
         --Delete the ImgImage table rows
         -- - - - - - - - - - - - - - - -
         Delete
            IMG
         From #RxImgIds RII
            Join
         ImgImage IMG
            On IMG.Id = RII.ImgId

         -- - - - - - - - - - - - -
         --Drop the temporary table
         -- - - - - - - - - - - - -
         Drop Table #RxImgIds

         ----------------------------------------------------------------------------------------------------------------------------
         -- Delete OeRxDoseSched, DsRxDoseSched, DsRxDoseSchedDose, and DsRxDoseSchedDoseByDayOfWeek table rows for the block of Rxs.
         ----------------------------------------------------------------------------------------------------------------------------
         Print 'Deleting OeRxDoseSched, DsRxDoseSched, DsRxDoseSchedDose, & DsRxDoseSchedDoseByDayOfWeek rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the RxDoseSchedIds for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#RxDoseSchedIds') Is Not Null
            Drop Table #RxDoseSchedIds

         Select
            Distinct DS.RxDoseSchedId
         Into
            #RxDoseSchedIds
         From
            #BlkOfRxsToDel R
               Join
            OeRxDoseSched DS
               On DS.OrderId = R.OrderId And DS.HistoryDtTm = R.HistoryDtTm
            
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OeRxDoseSched table rows for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            DS
         From
            #BlkOfRxsToDel R
               Join
            OeRxDoseSched DS
               On DS.OrderId = R.OrderId And DS.HistoryDtTm = R.HistoryDtTm

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete DsRxDoseSchedDose table rows for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            DSD
         From
            #RxDoseSchedIds DSIDS
               Join
            DsRxDoseSchedDose DSD
               On DSD.RxDoseSchedId = DSIDS.RxDoseSchedId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete DsRxDoseSchedDoseByDayOfWeek table rows for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            DSD
         From
            #RxDoseSchedIds DSIDS
               Join
            DsRxDoseSchedDoseByDayOfWeek DSD
               On DSD.RxDoseSchedId = DSIDS.RxDoseSchedId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete DsRxDoseSched table rows for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            DS
         From
            #RxDoseSchedIds DSIDS
               Join
            DsRxDoseSched DS
               On DS.Id = DSIDS.RxDoseSchedId

         -- - - - - - - - - - - - - -
         -- Drop the temporary table.
         -- - - - - - - - - - - - - -
         Drop Table #RxDoseSchedIds

         ----------------------------------------------------------------------------------------
         -- Delete PwkPaperworkSetOrderAssoc and PwkPaperworkSet table rows for the block of Rxs.
         ----------------------------------------------------------------------------------------
         Print 'Deleting PwkPaperworkSetOrderAssoc & PwkPaperworkSet rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the PaperworkSetIds for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#PwkSetIds') Is Not Null
            Drop Table #PwkSetIds

         Select
            Distinct PO.PaperworkSetId
         Into
            #PwkSetIds
         From
            #BlkOfRxsToDel R
               Join
            PwkPaperworkSetOrderAssoc PO
               On PO.OrderId = R.OrderId And PO.HistoryDtTm = R.HistoryDtTm
            
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete PwkPaperworkSetOrderAssoc table rows for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            PO
         From
            #BlkOfRxsToDel R
               Join
            PwkPaperworkSetOrderAssoc PO
               On PO.OrderId = R.OrderId And PO.HistoryDtTm = R.HistoryDtTm

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete PwkPaperworkSet table rows for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete rows only if they no longer have (other Rx) PwkPaperworkSetOrderAssoc rows.
         --
         Delete
            PWS
         From
            #PwkSetIds PWSIDS
               Join
            PwkPaperworkSet PWS
               On PWS.Id = PWSIDS.PaperworkSetId
               Left Join
            PwkPaperworkSetOrderAssoc PO
               On PO.PaperworkSetId = PWS.Id
         Where
            PO.PaperworkSetId Is Null  -- No remaining PwkPaperworkSetOrderAssoc rows exists for PaperworkSetId.

         -- - - - - - - - - - - - - -
         -- Drop the temporary table.
         -- - - - - - - - - - - - - -
         Drop Table #PwkSetIds

         ----------------------------------------------------------------------------------------------------------------------------------------------------
         -- Delete OeOrderCanReplenAssoc, CanCanisterHistory, CanLotCodeHistory, ImgCanImgAssoc, ImgImage and ImgImage_IntId table rows for the block of Rxs.
         ----------------------------------------------------------------------------------------------------------------------------------------------------
         Print 'Deleting OeOrderCanReplenAssoc, CanCanisterHistory, CanLotCodeHistory, ImgCanImgAssoc, ImgImage (along with ImgImage_IntId) rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the History Canister Replenishments (CanisterSn/LastReplenDtTm combos) for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#CanReplens') Is Not Null
            Drop Table #CanReplens

         Select
            Distinct OCR.CanisterSn, OCR.ReplenDtTm
         Into
            #CanReplens
         From
            #BlkOfRxsToDel R
               Join
            OeOrderCanReplenAssoc OCR
               On OCR.OrderId = R.OrderId And OCR.HistoryDtTm = R.HistoryDtTm
            
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OeOrderCanReplenAssoc table rows for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            OCR
         From
            #BlkOfRxsToDel R
               Join
            OeOrderCanReplenAssoc OCR
               On OCR.OrderId = R.OrderId And OCR.HistoryDtTm = R.HistoryDtTm

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Reduce the list of the History Canister Replenishments to those no longer having (other Rx) OeOrderCanReplenAssoc rows.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- To do this, remove from the list any row still having rows in table OeOrderCanReplenAssoc.
         --
         Delete
            CRS
         From
            #CanReplens CRS
               Join
            OeOrderCanReplenAssoc OCR
               On OCR.CanisterSn = CRS.CanisterSn And OCR.ReplenDtTm = CRS.ReplenDtTm

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete CanLotCodeHistory table rows for the list of History Canister Replenishments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            CRL
         From
            #CanReplens CRS
               Join
            CanLotCodeHistory CRL
               On CRL.CanisterSn = CRS.CanisterSn And CRL.LastReplenDtTm = CRS.ReplenDtTm

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete CanCanisterHistory table rows for the list of History Canister Replenishments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            CR
         From
            #CanReplens CRS
               Join
            CanCanisterHistory CR
               On CR.CanisterSn = CRS.CanisterSn And CR.LastReplenDtTm = CRS.ReplenDtTm

         ----------------------------------------------------------------------------------------------------------------
         --Delete ImgCanImgAssoc, ImgImage and ImgImage_IntId table rows for the list of History Canister Replenishments.
         ----------------------------------------------------------------------------------------------------------------
         
         
         If Object_Id('#CanImgIntIds') Is Not Null
            Drop Table #CanImgIntIds
         
         If Object_Id('TempDb..#CanImgIds') Is Not Null
            Drop Table #CanImgIds
         
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
         --Generate the list of Image Ids to delete (pick only the images that are already exported or saved to a physical location)
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
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
         
         Select
            Distinct CIA.ImgId
         Into
            #CanImgIds
         From
            #CanReplens CR
               Join
            ImgCanImgAssoc CIA
               On CR.CanisterSn = CIA.CanisterSn And CIA.LastReplenDtTm = CR.ReplenDtTm
               Join
            ImgImage II
               On II.Id = CIA.ImgId And II.IsMovedToFile = 1
         
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
         --Delete ImgCanImgAssoc table rows for the list of History Canister Replenishments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
         Delete 
            CIA
         From 
            #CanImgIntIds CII
               Join
            ImgCanImgAssoc CIA
               On CIA.ImgId = CII.ImgId 

         Delete 
            CIA
         From 
            #CanImgIds CII
               Join
            ImgCanImgAssoc CIA
               On CIA.ImgId = CII.ImgId 

         ------------------------------------------------------------------------------------------------
         --Delete ImgImage and ImgImage_IntId table rows for the list of History Canister Replenishments.
         ------------------------------------------------------------------------------------------------
         Delete
            Img
         From #CanImgIds CII
            Join
         ImgImage_IntId Img
            On Img.Id = CII.ImgId
         
         Delete
            Img
         From #CanImgIds CII
            Join
         ImgImage Img
            On Img.Id = CII.ImgId
         
         -- - - - - - - - - - - - - -- - - - - - - - - - - - -
         -- Drop the temporary tables. #CanReplens, #CanImgIds
         -- - - - - - - - - - - - - -- - - - - - - - - - - - -
         Drop Table #CanReplens, #CanImgIds, #CanImgIntIds

         -----------------------------------------------------------------------------------------------------------------------------------------------
         -- Delete OeOrderShipmentAssoc, ShpShipment, CvyPackageRoute, PwkPrinterTray, ShpManifestShipmentAssoc, ShpManifest,
         --        ShpShipmentPkgLabelData, ShpShipmentSpecialServiceAssoc, ShpSpecialService, SrtShipToteShipmentAssoc table rows for the block of Rxs.
         -----------------------------------------------------------------------------------------------------------------------------------------------
         Print 'Deleting OeOrderShipmentAssoc, ShpShipment, CvyPackageRoute, PwkPrinterTray, ' +
               'ShpManifestShipmentAssoc, ShpPalletManifestAssoc, ShpManifest, ShpLoadPalletAssoc, ShpLoad, ShpPallet, ' +
               'ShpShipmentPkgLabelData, ShpShipmentSpecialServiceAssoc, ShpSpecialService, SrtShipToteShipmentAssoc rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the Shipments for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#ShpIds') Is Not Null
            Drop Table #ShpIds

         Select
            Distinct OS.ShipmentId
         Into
            #ShpIds
         From
            #BlkOfRxsToDel R
               Join
            OeOrderShipmentAssoc OS
               On OS.OrderId = R.OrderId And OS.HistoryDtTm = R.HistoryDtTm
            Print 'Deleting #BlkOfRxsToDel/OeOrderShipmentAssoc' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OeOrderShipmentAssoc table rows for the block of Rxs.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            OS
         From
            #BlkOfRxsToDel R
               Join
            OeOrderShipmentAssoc OS
               On OS.OrderId = R.OrderId And OS.HistoryDtTm = R.HistoryDtTm

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Reduce the list of the Shipments to those no longer having (other Rx) OeOrderShipmentAssoc rows.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- To do this, remove from the list any row still having rows in table OeOrderShipmentAssoc.
         --
			Print 'Deleting #ShpIds/OeOrderShipmentAssoc' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            SI
         From
            #ShpIds SI
               Join
            OeOrderShipmentAssoc OS
               On OS.ShipmentId = SI.ShipmentId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete CvyPackageRoute table rows for the list of Shipments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- This table should only contain rows for active shipments, but the deletion is done for safety to ensure the shipments will delete.
         --
		 Print 'Deleting #ShpIds/CvyPackageRoute' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            CPR
         From
            #ShpIds SI
               Join
            CvyPackageRoute CPR
               On CPR.ShipmentId = SI.ShipmentId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete PwkPrinterTray table rows for the list of Shipments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- This table should only contain rows for active shipments, but the deletion is done for safety to ensure the shipments will delete.
         --
		 Print 'Deleting #ShpIds/PwkPrinterTray' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            PT
         From
            #ShpIds SI
               Join
            PwkPrinterTray PT
               On PT.ShipmentId = SI.ShipmentId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete ShpShipmentPkgLabelData table rows for the list of Shipments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		 Print 'Deleting #ShpIds/ShpShipmentPkgLabelData' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            SL
         From
            #ShpIds SI
               Join
            ShpShipmentPkgLabelData SL
               On SL.ShipmentId = SI.ShipmentId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete ShpShipmentSpecialServiceAssoc table rows for the list of Shipments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		 Print 'Deleting #ShpIds/ShpShipmentSpecialServiceAssoc' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            SSS
         From
            #ShpIds SI
               Join
            ShpShipmentSpecialServiceAssoc SSS
               On SSS.ShipmentId = SI.ShipmentId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete SrtShipToteShipmentAssoc table rows for the list of Shipments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- This table should only contain rows for active shipments, but the deletion is done for safety to ensure the shipments will delete.
         --
		 Print 'Deleting #ShpIds/SrtShipToteShipmentAssoc' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            ST
         From
            #ShpIds SI
               Join
            SrtShipToteShipmentAssoc ST
               On ST.ShipmentId = SI.ShipmentId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the Manifests for the Shipments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#ManIds') Is Not Null
            Drop Table #ManIds

         Select
            Distinct MS.ManifestId
         Into
            #ManIds
         From
            #ShpIds SI
               Join
            ShpManifestShipmentAssoc MS
               On MS.ShipmentId = SI.ShipmentId
            
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete ShpManifestShipmentAssoc table rows for the list of Shipments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		 Print 'Deleting #ShpIds/ShpManifestShipmentAssoc' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            MS
         From
            #ShpIds SI
               Join
            ShpManifestShipmentAssoc MS
               On MS.ShipmentId = SI.ShipmentId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- - - - - - - - - -
         -- Delete ShpShipmentPackageLabelInstance table rows for the list of Shipments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- - - - - - - - - -
		 Print 'Deleting #ShpIds/ShpShipmentPackageLabelInstance' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete 
            SSPLI
         From
            #ShpIds SI
               Join
            ShpShipmentPackageLabelInstance SSPLI
               On SSPLI.ShipmentId = SI.ShipmentId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete ShpShipment table rows for the list of Shipments.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		 Print 'Deleting #ShpIds/ShpShipment' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            S
         From
            #ShpIds SI
               Join
            ShpShipment S
               On S.Id = SI.ShipmentId

         -- - - - - - - - - - - - - -
         -- Drop the temporary table.
         -- - - - - - - - - - - - - -
         Drop Table #ShpIds

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Reduce the list of the Manifests to those no longer having (other shipment) ShpManifestShipmentAssoc rows.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- To do this, remove from the list any row still having rows in table ShpManifestShipmentAssoc.
         --
		 Print 'Deleting #ManIds/ShpManifestShipmentAssoc' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            MI
         From
            #ManIds MI
               Join
            ShpManifestShipmentAssoc MS
               On MS.ManifestId = MI.ManifestId

         -- - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the Pallets for the Manifests.
         -- - - - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#PalIds') Is Not Null
            Drop Table #PalIds

         Select
            Distinct PM.PalletId
         Into
            #PalIds
         From
            #ManIds MI
               Join
            ShpPalletManifestAssoc PM
               On PM.ManifestId = MI.ManifestId
            
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete ShpPalletManifestAssoc table rows for the list of Manifests.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		 Print 'Deleting #ManIds/ShpPalletManifestAssoc' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            PM
         From
            #ManIds MI
               Join
            ShpPalletManifestAssoc PM
               On PM.ManifestId = MI.ManifestId

         -- - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the Loads for the Pallets.
         -- - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#LoadIds') Is Not Null
            Drop Table #LoadIds

         Select
            Distinct LP.LoadId
         Into
            #LoadIds
         From
            #PalIds [PI]
               Join
            ShpLoadPalletAssoc LP
               On LP.PalletId = [PI].PalletId
            
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete ShpLoadPalletAssoc table rows for the list of Pallets.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		 Print 'Deleting #PalIds/ShpLoadPalletAssoc' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            LP
         From
            #PalIds [PI]
               Join
            ShpLoadPalletAssoc LP
               On LP.PalletId = [PI].PalletId

         -- - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete ShpLoad table rows for the list of Loads.
         -- - - - - - - - - - - - - - - - - - - - - - - - - -
		 Print 'Deleting #LoadIds/ShpLoad' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            L
         From
            #LoadIds LI
               Join
            ShpLoad L
               On L.Id = LI.LoadId
		 where not exists(select 1 from ShpLoadPalletAssoc lp where lp.LoadId = L.Id) 

         -- - - - - - - - - - - - - -
         -- Drop the temporary table.
         -- - - - - - - - - - - - - -
         Drop Table #LoadIds

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete ShpPallet table rows for the list of Pallets.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - -
		 Print 'Deleting #PalIds/ShpPallet' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            P
         From
            #PalIds [PI]
               Join
            ShpPallet P
               On P.Id = [PI].PalletId
		 where not exists(select 1 from ShpPalletManifestAssoc pm where pm.PalletId = P.Id) 

         -- - - - - - - - - - - - - -
         -- Drop the temporary table.
         -- - - - - - - - - - - - - -
         Drop Table #PalIds

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete ShpManifest table rows for the list of Manifests.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		 Print 'Deleting #ManIds/ShpManifest' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
         Delete
            M
         From
            #ManIds MI
               Join
            ShpManifest M
               On M.Id = MI.ManifestId

         -- - - - - - - - - - - - - -
         -- Drop the temporary table.
         -- - - - - - - - - - - - - -
         Drop Table #ManIds

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the Groups for the block of Rxs (before deleting the OeOrderHistory table rows).
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#Grps') Is Not Null
            Drop Table #Grps

         Select
            Distinct OH.GroupNum
         Into
            #Grps
         From
            #BlkOfRxsToDel R
               Join
            OeOrderHistory OH
               On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm
            
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the Rx Patients for the block of Rxs (before deleting the OeOrderHistory table rows).
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#RxPats') Is Not Null
            Drop Table #RxPats

         Select
            Distinct OH.PatCustId
         Into
            #RxPats
         From
            #BlkOfRxsToDel R
               Join
            OeOrderHistory OH
               On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm
            
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the Group Patients (Order Customers) for the block of Rxs (before deleting the OeOrderHistory table rows).
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#GrpPats') Is Not Null
            Drop Table #GrpPats

         Select
            Distinct G.PatCustId
         Into
            #GrpPats
         From
            #BlkOfRxsToDel R
               Join
            OeOrderHistory OH
               On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm
               Join
            OeGroup G
               On G.GroupNum = OH.GroupNum
            
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Generate a list of the distinct Rx & Group Patients for the block of Rxs (from the two lists).
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         If Object_Id('TempDb..#Pats') Is Not Null
            Drop Table #Pats

         Select
            PatCustId
         Into
            #Pats
         From
            (Select PatCustId From #RxPats Union Select PatCustId From #GrpPats) As RxAndGrpPats  -- Union results in distinct values.
            
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

         Select
            Distinct OH.PrescrId
         Into
            #Prescrs
         From
            #BlkOfRxsToDel R
               Join
            OeOrderHistory OH
               On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm

         ---------------------------------------------------------
         -- Delete OeOrderHistory table rows for the block of Rxs.
         ---------------------------------------------------------
         Print 'Deleting OeOrderHistory rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            OH
         From
            #BlkOfRxsToDel R
               Join
            OeOrderHistory OH
               On OH.OrderId = R.OrderId And OH.HistoryDtTm = R.HistoryDtTm

         ------------------------------------
         -- Track the number of rows deleted.
         ------------------------------------
         Set @DeletedThisPass = @@RowCount
         Set @NumOfRowsDeleted = @NumOfRowsDeleted + @DeletedThisPass
         Set @HistoryRowsDeletedLocal = @HistoryRowsDeletedLocal + @DeletedThisPass

         -----------------------------------------------------
         -- Delete OeComments table rows for the block of Rxs.
         -----------------------------------------------------
         -- This table is keyed by OrderId alone (i.e., no HistoryDtTm).
         -- Delete rows only if they no longer have an OrderId row in OeOrder or OeOrderHistory.  (So, this deletion must follow the deletion from OeOrderHistory.)
         -- Note:  The "From" can result in more than one row for each OrderId, but a "Delete <table> From" statement is forgiving of this and deletes the one or less row(s) in the target table.
         --
         Print 'Deleting OeComments rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            OC
         From
            #BlkOfRxsToDel R
               Join
            OeComments OC
               On OC.OrderId = R.OrderId
               Left Join
            OeOrder O
               On O.OrderId = OC.OrderId
               Left Join
            OeOrderHistory OH
               On OH.OrderId = OC.OrderId
         Where
            O.OrderId Is Null   -- No OeOrder row exists for OrderId.
            And
            OH.OrderId Is Null  -- No OeOrderHistory row exists for OrderId.

         ------------------------------------------------------------
         -- Delete OeOrderPoNumAssoc table rows for the block of Rxs.
         ------------------------------------------------------------
         -- This table is keyed by OrderId alone (i.e., no HistoryDtTm).
         -- Delete rows only if they no longer have an OrderId row in OeOrder or OeOrderHistory.  (So, this deletion must follow the deletion from OeOrderHistory.)
         -- Note:  The "From" can result in more than one row for each OrderId, but a "Delete <table> From" statement is forgiving and acts on unique OrderId values.
         --
         Print 'Deleting OeOrderPoNumAssoc rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            OP
         From
            #BlkOfRxsToDel R
               Join
            OeOrderPoNumAssoc OP
               On OP.OrderId = R.OrderId
               Left Join
            OeOrder O
               On O.OrderId = OP.OrderId
               Left Join
            OeOrderHistory OH
               On OH.OrderId = OP.OrderId
         Where
            O.OrderId Is Null   -- No OeOrder row exists for OrderId.
            And
            OH.OrderId Is Null  -- No OeOrderHistory row exists for OrderId.

         -------------------------------------------------------
         -- Delete OeProblemRxs table rows for the block of Rxs.
         -------------------------------------------------------
         -- This table is keyed by OrderId alone (i.e., no HistoryDtTm).
         -- Delete rows only if they no longer have an OrderId row in OeOrder or OeOrderHistory.  (So, this deletion must follow the deletion from OeOrderHistory.)
         -- Note:  The "From" can result in more than one row for each OrderId, but a "Delete <table> From" statement is forgiving and acts on unique OrderId values.
         --
         Print 'Deleting OeProblemRxs rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            PR
         From
            #BlkOfRxsToDel R
               Join
            OeProblemRxs PR
               On PR.OrderId = R.OrderId
               Left Join
            OeOrder O
               On O.OrderId = PR.OrderId
               Left Join
            OeOrderHistory OH
               On OH.OrderId = PR.OrderId
         Where
            O.OrderId Is Null   -- No OeOrder row exists for OrderId.
            And
            OH.OrderId Is Null  -- No OeOrderHistory row exists for OrderId.

         ---------------------------------------------------------------------
         -- Delete OeRequireCorrespondenceRxs table rows for the block of Rxs.
         ---------------------------------------------------------------------
         -- This table is keyed by OrderId alone (i.e., no HistoryDtTm).
         -- Delete rows only if they no longer have an OrderId row in OeOrder or OeOrderHistory.  (So, this deletion must follow the deletion from OeOrderHistory.)
         -- Note:  The "From" can result in more than one row for each OrderId, but a "Delete <table> From" statement is forgiving and acts on unique OrderId values.
         --
         Print 'Deleting OeRequireCorrespondenceRxs rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         Delete
            RC
         From
            #BlkOfRxsToDel R
               Join
            OeRequireCorrespondenceRxs RC
               On RC.RxNumOrOrderId = R.OrderId
               Left Join
            OeOrder O
               On O.OrderId = RC.RxNumOrOrderId
               Left Join
            OeOrderHistory OH
               On OH.OrderId = RC.RxNumOrOrderId
         Where
            O.OrderId Is Null   -- No OeOrder row exists for OrderId.
            And
            OH.OrderId Is Null  -- No OeOrderHistory row exists for OrderId.

         --------------------------------------------------------
         -- Delete OeStatusTrail table rows for the block of Rxs.
         --------------------------------------------------------
         -- This table is keyed by OrderId alone (i.e., no HistoryDtTm).
         -- Delete rows only if they no longer have an OrderId row in OeOrder or OeOrderHistory.  (So, this deletion must follow the deletion from OeOrderHistory.)
         -- Note:  The "From" can result in more than one row for each OrderId, but a "Delete <table> From" statement is forgiving of this and deletes the one or less row(s) in the target table.
         --
         Print 'Deleting OeStatusTrail rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         If Object_Id('tempdb..#RxsToDeleteFromStatusTrail') Is Not Null
            Drop Table #RxsToDeleteFromStatusTrail

         Select
            R.OrderId
         Into
            #RxsToDeleteFromStatusTrail
         From
            #BlkOfRxsToDel R
               Left Join
            OeOrder O
               On O.OrderId = R.OrderId
               Left Join
            OeOrderHistory OH
               On OH.OrderId = R.OrderId
         Where
            O.OrderId Is Null   -- No OeOrder row exists for OrderId.
            And
            OH.OrderId Is Null  -- No OeOrderHistory row exists for OrderId.

         Delete
            ST
         From
            #RxsToDeleteFromStatusTrail R
               Join
            OeStatusTrail ST
               On ST.OrderId = R.OrderId


         ------------------------------------------------------------------------------------------------------------------------------------------------------------
         -- Delete OeGroup, GovOnHoldGroups, OeExceptionGroups, OeGroupCvyFillData, OeGroupOrderData, OeGroupPayment, OeGroupUserDef table rows for the block of Rxs.
         ------------------------------------------------------------------------------------------------------------------------------------------------------------
         Print 'Deleting OeGroup, GovOnHoldGroups, OeExceptionGroups, OeGroupCvyFillData, OeGroupOrderData, OeGroupPayment, OeGroupUserDef rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Reduce the list of the Groups to those no longer having (other Rx) OeOrderHistory rows (or other Rx/Request referencing tables).
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- To do this, remove from the list any row still having rows in referencing tables.
         --
         Delete
            GN
         From
            #Grps GN
               Join
            OeOrderHistory OH
               On OH.GroupNum = GN.GroupNum

         Delete
            GN
         From
            #Grps GN
               Join
            OeOrder O
               On O.GroupNum = GN.GroupNum

         Delete
            GN
         From
            #Grps GN
               Join
            OeRxRequest RQ
               On RQ.GroupNum = GN.GroupNum

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete GovOnHoldGroups table rows for the list of Groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- This table should only contain rows for active groups, but the deletion is done for safety to ensure the groups will delete.
         Delete
            HG
         From
            #Grps GN
               Join
            GovOnHoldGroups HG
               On HG.GroupNum = GN.GroupNum

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OeExceptionGroups table rows for the list of Groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- This table should only contain rows for active groups, but the deletion is done for safety to ensure the groups will delete.
         Delete
            XG
         From
            #Grps GN
               Join
            OeExceptionGroups XG
               On XG.GroupNum = GN.GroupNum

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OeGroupCvyFillData table rows for the list of Groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- This table should only contain rows for active groups, but the deletion is done for safety to ensure the groups will delete.
         Delete
            GCF
         From
            #Grps GN
               Join
            OeGroupCvyFillData GCF
               On GCF.GroupNum = GN.GroupNum

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OeGroupOrderData table rows for the list of Groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            [GO]
         From
            #Grps GN
               Join
            OeGroupOrderData [GO]
               On [GO].GroupNum = GN.GroupNum

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OeGroupPayment table rows for the list of Groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            GP
         From
            #Grps GN
               Join
            OeGroupPayment GP
               On GP.GroupNum = GN.GroupNum

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OeGroupUserDef table rows for the list of Groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            GUD
         From
            #Grps GN
               Join
            OeGroupUserDef GUD
               On GUD.GroupNum = GN.GroupNum

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
         -- Delete the references of ParentGroupNum Column from OeGroup table rows for the list of Groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
         Delete
            PG
         From
            #Grps GN
               Join
            OeGroup PG
               On PG.ParentGroupNum = GN.GroupNum
         
         -- - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OeGroup table rows for the list of Groups.
         -- - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            G
         From
            #Grps GN
               Join
            OeGroup G
               On G.GroupNum = GN.GroupNum

         -- - - - - - - - - - - - - -
         -- Drop the temporary table.
         -- - - - - - - - - - - - - -
         Drop Table #Grps

         --------------------------------------------------------------------------
         -- Delete OePatientCust, PrivacySignature table rows for the block of Rxs.
         --------------------------------------------------------------------------
         Print 'Deleting OePatientCust, PrivacySignature rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Reduce the list of the Patients to those no longer having (other Rx) OeOrderHistory rows (or other Rx/Request referencing tables).
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- To do this, remove from the list any row still having rows in referencing tables.
         --
         Delete
            PT
         From
            #Pats PT
               Join
            OeOrderHistory OH
               On OH.PatCustId = PT.PatCustId

         Delete
            PT
         From
            #Pats PT
               Join
            OeOrder O
               On O.PatCustId = PT.PatCustId

         Delete
            PT
         From
            #Pats PT
               Join
            OeRxRequest RQ
               On RQ.PatCustId = PT.PatCustId

         Delete
            PT
         From
            #Pats PT
               Join
            OeGroup G
               On G.PatCustId = PT.PatCustId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OePatientCustSecondarydata table rows for the list of Patients.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            P2
         From
            #Pats PT
               Join
            OePatientCustSecondarydata P2
               On P2.PatCustId = PT.PatCustId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete PrivacySignature table rows for the list of Patients.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- This table is not used in HV, but the deletion is done for safety to ensure the patients will delete.
         --
         Delete
            PS
         From
            #Pats PT
               Join
            PrivacySignature PS
               On PS.PatCustId = PT.PatCustId

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OePatientCust table rows for the list of Patients.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Delete
            P
         From
            #Pats PT
               Join
            OePatientCust P
               On P.PatCustId = PT.PatCustId

         -- - - - - - - - - - - - - -
         -- Drop the temporary table.
         -- - - - - - - - - - - - - -
         Drop Table #Pats

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Reduce the list of the Prescribers to those no longer having (other Rx) OeOrderHistory rows (or other Rx/Request referencing tables).
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- To do this, remove from the list any row still having rows in referencing tables.
         --
         Delete
            PRI
         From
            #Prescrs PRI
               Join
            OeOrderHistory OH
               On OH.PrescrId = PRI.PrescrId

		 --Commenting out OeOrder delete to avoid deadlocking.
         --Delete
         --   PRI
         --From
         --   #Prescrs PRI
         --      Join
         --   OeOrder O
         --      On O.PrescrId = PRI.PrescrId

         Delete
            PRI
         From
            #Prescrs PRI
               Join
            OeRxRequest RQ
               On RQ.PrescrId = PRI.PrescrId
			   
		 -- Commenting out OePrescriber delete to avoid deadlocking in OeOrder. Orphaned OePrescriber rows to cleaned up separately.

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Delete OePrescriber table rows for the list of Prescribers.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         --Delete
         --   PR
         --From
         --   #Prescrs PRI
         --      Join
         --   OePrescriber PR
         --      On PR.PrescrId = PRI.PrescrId

      End  -- While Exists (Select * From OeOrderHistory Where HistoryDtTm >= @HistoryDtTm_EffectiveFrom And HistoryDtTm < @HistoryDtTm_EffectiveTo) And @NumOfRowsDeleted < @MaxToDelete

   --=========================
   -- OeOrderAcceptReject Loop
   --=========================
   Set @NumOfRowsDeleted = 0

   -------------------------------------------------------------------------
   -- Delete OeOrderAcceptReject rows logged prior to the cut-off date/time.
   -------------------------------------------------------------------------
   Print 'Deleting OeOrderAcceptReject rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

   ---------------------------------------------------------------------------------
   -- While there are old rows and the maximum number of deletions has not occurred:
   --   Progress with deleting a block of O/A Rx Rejection rows.
   ---------------------------------------------------------------------------------
   While Exists (Select * From OeOrderAcceptReject Where RejectDtTm >= @HistoryDtTm_EffectiveFrom And RejectDtTm < @HistoryDtTm_EffectiveTo) And @NumOfRowsDeleted < @MaxToDelete
      Begin
         
         -----------------------------------
         -- Delete the oldest block of rows.
         -----------------------------------
         Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         ;With Cte As
            (
               Select Top (@NumOfRowsBlockSize) * From OeOrderAcceptReject Where RejectDtTm >= @HistoryDtTm_EffectiveFrom And RejectDtTm < @HistoryDtTm_EffectiveTo Order By Id
            )
         Delete From Cte

         ------------------------------------
         -- Track the number of rows deleted.
         ------------------------------------
         Set @DeletedThisPass = @@RowCount
         Set @NumOfRowsDeleted = @NumOfRowsDeleted + @DeletedThisPass
         Set @AcceptRejectRowsDeletedLocal = @AcceptRejectRowsDeletedLocal + @DeletedThisPass

      End  -- While Exists (Select * From OeOrderHistory Where HistoryDtTm >= @HistoryDtTm_EffectiveFrom And HistoryDtTm < @HistoryDtTm_EffectiveTo) And @NumOfRowsDeleted < @MaxToDelete

   -- - - - - - - - - - - - - - -
   -- Log a successful end event.
   -- - - - - - - - - - - - - - -
   -------------------------------------------------------------------------
   -- Output row counts (useful for a looping driver).
   -------------------------------------------------------------------------
   Set @HistoryRowsDeleted = @HistoryRowsDeletedLocal
   Set @AcceptRejectRowsDeleted = @AcceptRejectRowsDeletedLocal

   Print 'Complete.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Set @EvtNotes = ''
   Exec lsp_DbLogSqlEvent 'A', 'PurgeOldData End', 'Successful', @EvtNotes, 'lsp_DbPurgeHistoryData'

   Return 0  -- Return Success
   
End
