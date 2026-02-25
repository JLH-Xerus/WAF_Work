ALTER PROCEDURE "dbo"."lsp_DbDeleteOldInvAuditEvents" 
   @OlderThanXDays Int = 36500,
   @NumOfRowsBlockSize Int = 100000,
   @MaxToDelete Int = 25000000
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_DbDeleteOldInvAuditEvents

Version:
   1

Description:
   Delete Inventory Audit events that were logged prior to X days ago.
   This deletes rows from tables:  EvtInvEventLog

Parameters:
   Input:
      @OlderThanXDays      - Events logged prior to this many days ago will be deleted.
      @NumOfRowsBlockSize  - The event (rows) will be deleted in blocks of this many rows at a time.
                             Deletions are performed in blocks of rows to prevent performing one huge transaction that would inflate the tempdb
                               and the t-log (even more than it holding the deletes themselves) and perform slowly due to broad locking.
      @MaxToDelete         - The maximum number of events to delete.
                             If this value is not a multiple of @NumOfRowsBlockSize, the effect will be approximate.
                             Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition where a huge
                               number of "older than X days" rows exist when this action is performed.
                             The goal is to delete a "finite" amount of data between full database backups where the t-log is truncated.
   Output:
      None

Return Value:
   Return Code:
          0 = Successful
      Non-0 = Failed

Comments:
   This procedure is called from the nightly maintenance procedure.
   The oldest inventory events are deleted first.
*/

Declare @CutoffDtTm DateTime           -- "X Days Ago" Cutoff Date/Time.
      , @NumOfRowsDeleted Int = 0      -- Number of rows that have been deleted.
      , @EvtNotes VarChar(255)         -- Logged event notes.
      , @ErrNum Int                    -- Error number (if error occurs).
      , @ErrMsg NVarChar(4000)         -- Error message (if error occurs).
      , @Routine VarChar(50)           -- SP Name (lsp_DbDeleteOldInvAuditEvents)

-------------------------------------
--Set the Routine name for common use
-------------------------------------
Set @Routine = 'lsp_DbDeleteOldInvAuditEvents'

-------------------------------------------------------------
-- Set the Cutoff Date/Time from the Older Than X Days value.
-------------------------------------------------------------
Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())

Print 'Deleting inventory events older than ' + Convert(VarChar(30), @CutoffDtTm, 121) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

-- - - - - - - - - - -
-- Log a begin event.
-- - - - - - - - - - -
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@NumOfRowsBlockSize=' + Cast(@NumOfRowsBlockSize As VarChar(12)) + ';@MaxToDelete=' + Cast(@MaxToDelete As VarChar(12))
Exec lsp_DbLogSqlEvent 'A', 'DelOldInvEvts Beg', '', @EvtNotes, @Routine

Begin Try
   If @OlderThanXDays <= 0
   Begin
      Set @ErrMsg = FormatMessage('Invalid parameter value. (@OlderThanXDays = %i)', @OlderThanXDays)
      RaisError(@ErrMsg, 16, 1)
   End

   ---------------------------------------------------------------------------------
   -- While there are old rows and the maximum number of deletions has not occurred:
   --   Delete a block of rows.
   ---------------------------------------------------------------------------------
   -----------------------------------------
   -- Delete from table EvtInvEventLog next.
   -----------------------------------------
   Print 'Deleting (EvtInvEventLog) events older than ' + Convert(VarChar(30), @CutoffDtTm, 121) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

   ---------------------------------------------------------------------------------
   -- While there are old rows and the maximum number of deletions has not occurred:
   --   Delete a block of rows.
   ---------------------------------------------------------------------------------
   While Exists (Select * From EvtInvEventLog Where EventDtTm < @CutoffDtTm) And @NumOfRowsDeleted < @MaxToDelete
   Begin

      -----------------------------------
      -- Delete the oldest block of rows.
      -----------------------------------
      Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

      ;With Cte As
         (
            Select Top (@NumOfRowsBlockSize) * From EvtInvEventLog Where EventDtTm < @CutoffDtTm Order By Id
         )
      Delete From Cte

      ------------------------------------
      -- Track the number of rows deleted.
      ------------------------------------
      Set @NumOfRowsDeleted = @NumOfRowsDeleted + @NumOfRowsBlockSize

   End

   -- - - - - - - - - - - - - - -
   -- Log a successful end event.
   -- - - - - - - - - - - - - - -
   Print 'Complete.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Set @EvtNotes = ''
   Exec lsp_DbLogSqlEvent 'A', 'DelOldInvEvts End', 'Successful', @EvtNotes, @Routine

   Return 0  -- Return Success
   
End Try

Begin Catch

   Set @ErrNum = ERROR_NUMBER()
   Set @ErrMsg = ERROR_MESSAGE()
   Exec dbo.lsp_DbLogSqlError 'DelOldInvEvts Err', @ErrNum, @ErrMsg, @Routine

   -- - - - - - - - - - - - -
   -- Log a failed end event.
   -- - - - - - - - - - - - -
   Print 'Failed.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Set @EvtNotes = Left(@ErrMsg, 255)
   Exec lsp_DbLogSqlEvent 'A', 'DelOldInvEvts End', 'Failed', @EvtNotes, @Routine

   Return @ErrNum

End Catch
End

