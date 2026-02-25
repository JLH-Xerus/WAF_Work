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
   2

Description:
   Delete (Audit) events that were logged prior to X days ago.
   This deletes rows from tables:  EvtEvent

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
   @Rc - 0 = Successful; Non-0 = Failed

Comments:
   This procedure is called from the nightly maintenance procedure.
   The oldest events are deleted first.
*/
Declare @Rc Int                        -- SQL Command Return Code.  (Saved Value of @@Error.)
      , @CutoffDtTm DateTime           -- "X Days Ago" Cutoff Date/Time.
      , @NumOfRowsDeleted Int = 0      -- Number of rows that have been deleted.
      , @EvtNotes VarChar(255)         -- Logged event notes.
      , @ErrNum Int                    -- Error number (if error occurs).
      , @ErrMsg NVarChar(4000)         -- Error message (if error occurs).

Declare @BlkIds Table(Id Int)          -- Block of IDs to be deleted.
Declare @ContentCodes Table(Code Int)  -- List of @ImgsOfContentCodes as a table.

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
Exec lsp_DbLogSqlEvent 'A', 'DelOldEvts Beg', '', @EvtNotes, 'lsp_DbDeleteOldEvents'

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
   Print 'Deleting (EvtEvent) events older than ' + Convert(VarChar(30), @CutoffDtTm, 121) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'

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
            Select Top (@NumOfRowsBlockSize) * From EvtEvent Where EventDtTm < @CutoffDtTm Order By TrxId
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
   Exec lsp_DbLogSqlEvent 'A', 'DelOldEvts End', 'Successful', @EvtNotes, 'lsp_DbDeleteOldEvents'

   Return 0  -- Return Success
   
End Try

Begin Catch

   Set @ErrNum = ERROR_NUMBER()
   Set @ErrMsg = ERROR_MESSAGE()
   Exec dbo.lsp_DbLogSqlError 'DelOldEvts Err', @ErrNum, @ErrMsg, 'lsp_DbDeleteOldEvents'

   -- - - - - - - - - - - - -
   -- Log a failed end event.
   -- - - - - - - - - - - - -
   Print 'Failed.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Set @EvtNotes = Left(@ErrMsg, 255)
   Exec lsp_DbLogSqlEvent 'A', 'DelOldEvts End', 'Failed', @EvtNotes, 'lsp_DbDeleteOldEvents'

   Return @ErrNum

End Catch
End

