ALTER PROCEDURE "dbo"."lsp_DbDeleteOldAutoVerificationData" 
     @OlderThanXDays Int = 36500
   , @MaxToDelete Int = 25000000
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_DbDeleteOldAutoVerificationData

Version:
   1

Description:
   Delete data from following three tables (as of 02/12/2025)
      - InvAliasCodeAutoVerifyRxDirections
      - InvNdcAutoVerifyRxDirections
      - InvProductAutoVerifyRxDirections

Parameters:
   Input:
      @OlderThanXDays      - Data for verifications that were completed prior to this many days ago will be deleted.
      @MaxToDelete         - The maximum number of verification data to delete.
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
   The oldest verification data will be deleted first.
*/

Declare @Rc Int                        -- SQL Command Return Code.  (Saved Value of @@Error.)
      , @CutoffDtTm DateTime           -- "X Days Ago" Cutoff Date/Time.
      , @NumOfRowsDeleted Int = 0      -- Number of rows that have been deleted.  For this SP, this is OeOrderHistory rows, specifically.
      , @EvtNotes VarChar(255)         -- Logged event notes.
      , @ErrNum Int                    -- Error number (if error occurs).
      , @ErrMsg NVarChar(4000)         -- Error message (if error occurs).

Declare @BlkIds Table(Id Int)          -- Block of IDs to be deleted.

-------------------------------------------------------------
-- Set the Cutoff Date/Time from the Older Than X Days value.
-------------------------------------------------------------
Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())

-- - - - - - - - - - -
-- Log a begin event.
-- - - - - - - - - - -
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@MaxToDelete=' + Cast(@MaxToDelete As VarChar(12))
Exec lsp_DbLogSqlEvent 'A', 'DelOldVefcData Beg', '', @EvtNotes, 'lsp_DbDeleteOldAutoVerificationData'

Begin Try

   If @OlderThanXDays <= 0
      Begin
         Set @ErrMsg = FormatMessage('Invalid parameter value. (@OlderThanXDays = %i)', @OlderThanXDays)
         RaisError(@ErrMsg, 16, 1)
      End

   Print 'Deleting InvAliasCodeAutoVerifyRxDirections rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Delete InvAliasCodeAutoVerifyRxDirections
      Where LastRxVerificationDtTm < @CutoffDtTm

   Set @NumOfRowsDeleted = IsNull(@@ROWCOUNT, 0)
   Set @EvtNotes = 'DeleteCount: InvAliasCodeAutoVerifyRxDirections('+ Cast(@NumOfRowsDeleted As VarChar(10)) +');'


   Print 'Deleting InvNdcAutoVerifyRxDirections rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Delete InvNdcAutoVerifyRxDirections
      Where LastRxVerificationDtTm < @CutoffDtTm
   Set @NumOfRowsDeleted = IsNull(@@ROWCOUNT, 0)
   Set @EvtNotes = @EvtNotes + 'InvNdcAutoVerifyRxDirections('+ Cast(@NumOfRowsDeleted As VarChar(10)) +');'


   Print 'Deleting InvProductAutoVerifyRxDirections rows...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Delete InvProductAutoVerifyRxDirections
      Where LastRxVerificationDtTm < @CutoffDtTm
   Set @NumOfRowsDeleted = IsNull(@@ROWCOUNT, 0)
   Set @EvtNotes = @EvtNotes + 'InvProductAutoVerifyRxDirections('+ Cast(@NumOfRowsDeleted As VarChar(10)) +')'

   -- - - - - - - - - - - - - - -
   -- Log a successful end event.
   -- - - - - - - - - - - - - - -
   Print 'Complete.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Exec lsp_DbLogSqlEvent 'A', 'DelOldVefcData End', 'Successful', @EvtNotes, 'lsp_DbDeleteOldAutoVerificationData'

   Return 0  -- Return Success
   
End Try

Begin Catch

   Set @ErrNum = ERROR_NUMBER()
   Set @ErrMsg = ERROR_MESSAGE()
   Exec dbo.lsp_DbLogSqlError 'DelOldVefcData Err', @ErrNum, @ErrMsg, 'lsp_DbDeleteOldAutoVerificationData'

   -- - - - - - - - - - - - -
   -- Log a failed end event.
   -- - - - - - - - - - - - -
   Print 'Failed.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Set @EvtNotes = Left(@ErrMsg, 255)
   Exec lsp_DbLogSqlEvent 'A', 'DelOldVefcData End', 'Failed', @EvtNotes, 'lsp_DbDeleteOldAutoVerificationData'

   Return @ErrNum

End Catch
End
