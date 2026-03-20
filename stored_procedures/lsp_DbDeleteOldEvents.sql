ALTER PROCEDURE "dbo"."lsp_DbDeleteOldEvents" 
   @OlderThanXDays Int = 36500,
   @NumOfRowsBlockSize Int = 100000,
   @MaxToDelete Int = 25000000
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_DbDeleteOldEvents

Version:
   3

Description:
   Delete (Audit) events that were logged prior to X days ago.
   This deletes rows from tables:  EvtEvent

   NOTE: This procedure does not purge the legacy EvtEvent_IntId table (pre-schema 17.03.07 events).
         If EvtEvent_IntId still contains data, a separate purge strategy may be needed.

Parameters:
   Input:
      @OlderThanXDays      - Events logged prior to this many days ago will be deleted.
      @NumOfRowsBlockSize  - The event (rows) will be deleted in blocks of this many rows at a time.
                             Deletions are performed in blocks of rows to prevent performing one huge transaction that would inflate the tempdb
                               and the t-log (even more than it holding the deletes themselves) and perform slowly due to broad locking.
      @MaxToDelete         - The maximum number of events to delete.
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
   The oldest events are deleted first.

Updates (v3):
   - Narrowed CTE select from "Select *" to "Select TrxId" to reduce I/O per block iteration.
   - Replaced approximate row count tracking (@NumOfRowsBlockSize) with actual @@ROWCOUNT after each delete.
   - Added total rows deleted to both the success and failure end event log entries for observability.
   - Removed unused variable declarations (@Rc, @BlkIds, @ContentCodes).
   - Removed duplicate print statement that repeated the deletion message before the main loop.
   - Centralized procedure name into @Routine variable for consistency with related procedures.
*/
Declare @CutoffDtTm DateTime           -- "X Days Ago" Cutoff Date/Time.
      , @NumOfRowsDeleted Int = 0      -- Number of rows that have been deleted.
      , @EvtNotes VarChar(255)         -- Logged event notes.
      , @ErrNum Int                    -- Error number (if error occurs).
      , @ErrMsg NVarChar(4000)         -- Error message (if error occurs).
      , @Routine VarChar(50)           -- SP Name (lsp_DbDeleteOldEvents)

-------------------------------------
--Set the Routine name for common use
-------------------------------------
Set @Routine = 'lsp_DbDeleteOldEvents'

-------------------------------------------------------------
-- Set the Cutoff Date/Time from the Older Than X Days value.
-------------------------------------------------------------
Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())

-- Table EvtEvent_IntId holds the oldest events and those logged prior to an upgrade through HV schema version 17.03.07, where the existing
--   EvtEvent table was renamed to EvtEvent_IntId and the system began logging to a new EvtEvent table (whose TrxId is of type BigInt).
--
Print 'Deleting (EvtEvent) events older than ' + Convert(VarChar(30), @CutoffDtTm, 121) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

-- - - - - - - - - - -
-- Log a begin event.
-- - - - - - - - - - -
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@NumOfRowsBlockSize=' + Cast(@NumOfRowsBlockSize As VarChar(12)) + ';@MaxToDelete=' + Cast(@MaxToDelete As VarChar(12))
Exec lsp_DbLogSqlEvent 'A', 'DelOldEvts Beg', '', @EvtNotes, @Routine

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
   -----------------------------------
   -- Delete from table EvtEvent next.
   -----------------------------------

   ---------------------------------------------------------------------------------
   -- While there are old rows and the maximum number of deletions has not occurred:
   --   Delete a block of rows.
   ---------------------------------------------------------------------------------
   While Exists (Select * From EvtEvent Where EventDtTm < @CutoffDtTm) And @NumOfRowsDeleted < @MaxToDelete
   Begin

      -----------------------------------
      -- Delete the oldest block of rows.
      -----------------------------------
      Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

      ;With Cte As
         (
            Select Top (@NumOfRowsBlockSize) TrxId From EvtEvent Where EventDtTm < @CutoffDtTm Order By TrxId
         )
      Delete From Cte

      ------------------------------------
      -- Track the number of rows deleted.
      ------------------------------------
      Set @NumOfRowsDeleted = @NumOfRowsDeleted + @@ROWCOUNT

   End

   -- - - - - - - - - - - - - - -
   -- Log a successful end event.
   -- - - - - - - - - - - - - - -
   Print 'Complete.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Set @EvtNotes = 'Total Number of rows deleted - ' + Cast(@NumOfRowsDeleted As VarChar(20))
   Exec lsp_DbLogSqlEvent 'A', 'DelOldEvts End', 'Successful', @EvtNotes, @Routine

   Return 0  -- Return Success
   
End Try

Begin Catch

   Set @ErrNum = ERROR_NUMBER()
   Set @ErrMsg = ERROR_MESSAGE()
   Exec dbo.lsp_DbLogSqlError 'DelOldEvts Err', @ErrNum, @ErrMsg, @Routine

   -- - - - - - - - - - - - -
   -- Log a failed end event.
   -- - - - - - - - - - - - -
   Print 'Failed.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Set @EvtNotes = Left('Rows deleted before failure - ' + Cast(@NumOfRowsDeleted As VarChar(20)) + '; ' + @ErrMsg, 255)
   Exec lsp_DbLogSqlEvent 'A', 'DelOldEvts End', 'Failed', @EvtNotes, @Routine

   Return @ErrNum

End Catch
End

