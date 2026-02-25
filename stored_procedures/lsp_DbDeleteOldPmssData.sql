ALTER PROCEDURE "dbo"."lsp_DbDeleteOldPmssData" 
     @NumOfRowsBlockSize Int = 100000
   , @MaxToDelete Int = 25000000
As
Begin
/*
Object Type:
   Stored Procedure

Object Name:
   lsp_DbDeleteOldPmssData

Version:
   1

Description:
   Delete data from following PMSS tables based on the System-Wide > PMSS > DaysToKeepEncryptedLogs parameter.
      - PmssCapturedOrderTrxs
      - PmssCapturedXmlTrxs
      - CfCollectedCentralFillPmssTrxs
      - CfCapturedPmssTrxs

Return:
   @Rc - 0     = Successful
         Non-0 = Failed

Comments:
   This stored procedure is called from nightly maintenance procedure.
   The oldest records are deleted first.

*/

Declare @Rc Int                          -- SQL Command Return Code.  (Saved Value of @@Error.)
      , @CutoffDtTm DateTime             -- "X Days Ago" Cutoff Date/Time.
      , @NumOfRowsDeleted Int = 0        -- Number of rows that have been deleted.
      , @EventType Char(1)               -- Type of event.
      , @EventPrefix VarChar(15)         -- Prefix of the logged event.
      , @Event VarChar(20)               -- Logged event.
      , @EvtNotes VarChar(255)           -- Logged event notes.
      , @Routine VarChar(50)             -- Source of the event.
      , @ErrNum Int                      -- Error number (if error occurs).
      , @ErrMsg NVarChar(4000)           -- Error message (if error occurs).
      , @configuredRetentionPeriod Int   -- Configured value of PMSS -> DaysToKeepEncryptedLogs parameter

Set @EventType = 'A'
Set @EventPrefix = 'DelOldPmssData'
Set @Routine = 'lsp_DbDeleteOldPmssData'

Begin Try
   -- Look for the configured value of PMSS -> DaysToKeepEncryptedLogs parameter to determine how many days of data to retain
   If Exists (Select * from CfgSystemParamDef Where Section = 'PMSS' And Parameter = 'DaysToKeepEncryptedLogs')
   Begin
      Select @configuredRetentionPeriod = IsNull([Value], 0) From vCfgSystemParamVal Where Section = 'PMSS' And Parameter = 'DaysToKeepEncryptedLogs'
   End

   If @configuredRetentionPeriod > 0
   Begin
      Set @CutoffDtTm = DateAdd(Day, @configuredRetentionPeriod * -1, GetDate())
      
      Set @EvtNotes = Concat('CutoffDate=',Format(@cutoffDtTm, 'MM/dd/yyyy'),'(', Cast(@configuredRetentionPeriod As VarChar(5)), 'd);@NumOfRowsBlockSize=', Cast(@NumOfRowsBlockSize As VarChar(12)), ';@MaxToDelete=', Cast(@MaxToDelete As VarChar(12)))
      Set @Event = Concat(@EventPrefix, ' Beg')
      Exec lsp_DbLogSqlEvent @EvtType = @eventType, @Event = @Event, @Results = '', @Notes = @EvtNotes, @Routine = @Routine


      --=====================================
      -- PMSS & Central Fill Transaction Logs
      --=====================================

      --================================
      -- Start > CfCapturedPmssTrxs Loop
      --================================
      Declare @NumOfRowsDeleted_CfCapturedPmssTrxs Int = 0

      ------------------------------------------------------------------------
      -- Delete CfCapturedPmssTrxs rows logged prior to the cut-off date/time.
      ------------------------------------------------------------------------
      Print 'Deleting CfCapturedPmssTrxs rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

      ---------------------------------------------------------------------------------
      -- While there are old rows and the maximum number of deletions has not occurred:
      --   Progress with deleting a block of Trx rows.
      ---------------------------------------------------------------------------------
      While Exists (Select * From CfCapturedPmssTrxs Where TrxDtTm < @CutoffDtTm) And @NumOfRowsDeleted_CfCapturedPmssTrxs < @MaxToDelete
      Begin
      
         -----------------------------------
         -- Delete the oldest block of rows.
         -----------------------------------
         Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         ;With Cte As
            (
               Select Top (@NumOfRowsBlockSize) * From CfCapturedPmssTrxs Where TrxDtTm < @CutoffDtTm Order By Id
            )
         Delete From Cte

         ------------------------------------
         -- Track the number of rows deleted.
         ------------------------------------
         Set @NumOfRowsDeleted_CfCapturedPmssTrxs = @NumOfRowsDeleted_CfCapturedPmssTrxs + @@ROWCOUNT

      End  -- While Exists (Select * From CfCapturedPmssTrxs Where TrxDtTm < @CutoffDtTm) And @NumOfRowsDeleted_CfCapturedPmssTrxs < @MaxToDelete

      --==============================
      -- End > CfCapturedPmssTrxs Loop
      --==============================


      --============================================
      -- Start > CfCollectedCentralFillPmssTrxs Loop
      --============================================
      Declare @NumOfRowsDeleted_CfCollectedCentralFillPmssTrxs Int = 0

      ------------------------------------------------------------------------------------
      -- Delete CfCollectedCentralFillPmssTrxs rows logged prior to the cut-off date/time.
      ------------------------------------------------------------------------------------
      Print 'Deleting CfCollectedCentralFillPmssTrxs rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

      ---------------------------------------------------------------------------------
      -- While there are old rows and the maximum number of deletions has not occurred:
      --   Progress with deleting a block of Trx rows.
      ---------------------------------------------------------------------------------
      While Exists (Select * From CfCollectedCentralFillPmssTrxs Where TrxDtTm < @CutoffDtTm) And @NumOfRowsDeleted_CfCollectedCentralFillPmssTrxs < @MaxToDelete
      Begin
      
         -----------------------------------
         -- Delete the oldest block of rows.
         -----------------------------------
         Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         ;With Cte As
            (
               Select Top (@NumOfRowsBlockSize) * From CfCollectedCentralFillPmssTrxs Where TrxDtTm < @CutoffDtTm Order By Id
            )
         Delete From Cte

         ------------------------------------
         -- Track the number of rows deleted.
         ------------------------------------
         Set @NumOfRowsDeleted_CfCollectedCentralFillPmssTrxs = @NumOfRowsDeleted_CfCollectedCentralFillPmssTrxs + @@ROWCOUNT

      End  -- While Exists (Select * From CfCollectedCentralFillPmssTrxs Where TrxDtTm < @CutoffDtTm) And @NumOfRowsDeleted_CfCollectedCentralFillPmssTrxs < @MaxToDelete

      --==========================================
      -- End > CfCollectedCentralFillPmssTrxs Loop
      --==========================================

      --===================================
      -- Start > PmssCapturedOrderTrxs Loop
      --===================================
      Declare @NumOfRowsDeleted_PmssCapturedOrderTrxsint Int = 0
      ---------------------------------------------------------------------------
      -- Delete PmssCapturedOrderTrxs rows logged prior to the cut-off date/time.
      ---------------------------------------------------------------------------
      Print 'Deleting PmssCapturedOrderTrxs rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

      ---------------------------------------------------------------------------------
      -- While there are old rows and the maximum number of deletions has not occurred:
      --   Progress with deleting a block of Trx rows.
      ---------------------------------------------------------------------------------
      While Exists (Select * From PmssCapturedOrderTrxs Where TrxDtTm < @CutoffDtTm) And @NumOfRowsDeleted_PmssCapturedOrderTrxsint < @MaxToDelete
      Begin
      
         -----------------------------------
         -- Delete the oldest block of rows.
         -----------------------------------
         Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         ;With Cte As
            (
               Select Top (@NumOfRowsBlockSize) * From PmssCapturedOrderTrxs Where TrxDtTm < @CutoffDtTm Order By Id
            )
         Delete From Cte

         ------------------------------------
         -- Track the number of rows deleted.
         ------------------------------------
         Set @NumOfRowsDeleted_PmssCapturedOrderTrxsint = @NumOfRowsDeleted_PmssCapturedOrderTrxsint + @@ROWCOUNT

      End  -- While Exists (Select * From PmssCapturedOrderTrxs Where TrxDtTm < @CutoffDtTm) And @NumOfRowsDeleted_PmssCapturedOrderTrxsint < @MaxToDelete

      --=================================
      -- End > PmssCapturedOrderTrxs Loop
      --=================================


      --=================================
      -- Start > PmssCapturedXmlTrxs Loop
      --=================================
      Declare @NumOfRowsDeleted_PmssCapturedXmlTrxs Int = 0

      -------------------------------------------------------------------------
      -- Delete PmssCapturedXmlTrxs rows logged prior to the cut-off date/time.
      -------------------------------------------------------------------------
      Print 'Deleting PmssCapturedXmlTrxs rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

      ---------------------------------------------------------------------------------
      -- While there are old rows and the maximum number of deletions has not occurred:
      --   Progress with deleting a block of Trx rows.
      ---------------------------------------------------------------------------------
      While Exists (Select * From PmssCapturedXmlTrxs Where TrxDtTm < @CutoffDtTm) And @NumOfRowsDeleted_PmssCapturedXmlTrxs < @MaxToDelete
      Begin
      
         -----------------------------------
         -- Delete the oldest block of rows.
         -----------------------------------
         Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

         ;With Cte As
            (
               Select Top (@NumOfRowsBlockSize) * From PmssCapturedXmlTrxs Where TrxDtTm < @CutoffDtTm Order By Id
            )
         Delete From Cte

         ------------------------------------
         -- Track the number of rows deleted.
         ------------------------------------
         Set @NumOfRowsDeleted_PmssCapturedXmlTrxs = @NumOfRowsDeleted_PmssCapturedXmlTrxs + @@ROWCOUNT

      End  -- While Exists (Select * From PmssCapturedXmlTrxs Where TrxDtTm < @CutoffDtTm) And @NumOfRowsDeleted_PmssCapturedXmlTrxs < @MaxToDelete

      Set @NumOfRowsDeleted =   @NumOfRowsDeleted_CfCapturedPmssTrxs
                              + @NumOfRowsDeleted_CfCollectedCentralFillPmssTrxs
                              + @NumOfRowsDeleted_PmssCapturedOrderTrxsint
                              + @NumOfRowsDeleted_PmssCapturedXmlTrxs
      --=================================
      -- End > PmssCapturedXmlTrxs Loop
      --=================================

    -- - - - - - - - - - - - ----
    -- Log a successful end event.
    -- - - - - - - - - - - - ----
    Set @EvtNotes = Concat('Total Number of rows deleted - ', Cast(@NumOfRowsDeleted As VarChar(20)))
    Set @Event = Concat(@EventPrefix, ' End')
    Exec lsp_DbLogSqlEvent @EvtType = @eventType, @Event = @Event, @Results = 'Success', @Notes = @EvtNotes, @Routine = @Routine

   End
   Else
   Begin
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
      -- Log a successful end event, but keep a note that the parameter is not configured.
      -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
      Set @EvtNotes = 'System-Wide -> PMSS -> DaysToKeepEncryptedLogs parameter is not configured'
      Set @Event = Concat(@EventPrefix, ' !Prm')
      Exec lsp_DbLogSqlEvent @EvtType = @eventType, @Event = @Event, @Results = 'Success', @Notes = @EvtNotes, @Routine = @Routine
   End

   Set @RC = 0
   Return @RC
End Try
Begin Catch

   Set @ErrNum = ERROR_NUMBER()
   Set @ErrMsg = ERROR_MESSAGE()
   
   Set @Event = Concat(@EventPrefix, ' Err')
   Exec lsp_DbLogSqlError 
        @Event = @Event
      , @ErrCode = @ErrNum
      , @ErrDesc = @ErrMsg
      , @Routine = @Routine

   -- - - - - - - - - - - - -
   -- Log a failed end event.
   -- - - - - - - - - - - - -
   Print 'Failed.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Set @EvtNotes = Concat('Total Number of rows deleted - ', Cast (@NumOfRowsDeleted As VarChar(20)))
   Exec lsp_DbLogSqlEvent
        @EvtType = @eventType
      , @Event = @Event
      , @Results = 'Failed'
      , @Notes = @EvtNotes
      , @Routine = @Routine

   Return @ErrNum
End Catch
End
