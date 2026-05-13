ALTER PROCEDURE "SmartShelf"."lsp_DbDeleteOldSmartShelfEvents" 
     @OlderThanXDays Int = 36500
   , @NumOfRowsBlockSize Int = 100000
   , @MaxToDelete Int = 25000000
As
Begin
/*
Object Type:
   Stored Procedure

Object Name:
   lsp_DbDeleteOldSmartShelfEvents

Version:
   1

Description:
   Delete all Events (i.e., SmartShelf.EvtEventLog Rows) older than a specified date.

Parameters:
   @CutOffDate          - Events older than this date will be deleted.
   @NumOfRowsBlockSize  - The event (rows) will be deleted in blocks of this many rows at a time.
                          Deletions are performed in blocks of rows to prevent performing one huge transaction that would inflate the tempdb
                          and the t-log (even more than it holding the deletes themselves) and perform slowly due to broad locking.
   @MaxToDelete         - The maximum number of events to delete.
                          If this value is not a multiple of @NumOfRowsBlockSize, the effect will be approximate.
                          Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition where a huge
                            number of "older than X days" rows exist when this action is performed.
                          The goal is to delete a "finite" amount of data between full database backups where the t-log is truncated.

Return Value:
   Return Code:
      0 = Success
     -1 = Failed to delete old events.

Comments:
   None

*/
Declare @Rc Int                        -- SQL Command Return Code.  (Saved Value of @@Error.)
      , @CutoffDtTm DateTime           -- "X Days Ago" Cutoff Date/Time.
      , @NumOfRowsDeleted Int = 0      -- Number of rows that have been deleted.
      , @PrintMsg VarChar(512)         -- Message printed for SQL Server Events
      , @EvtNotes VarChar(255)         -- Logged event notes.
      , @ErrNum Int                    -- Error number (if error occurs).
      , @ErrMsg NVarChar(4000)         -- Error message (if error occurs).


-------------------------------------------------------------
-- Set the Cutoff Date/Time from the Older Than X Days value.
-------------------------------------------------------------
Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())

Set @PrintMsg = Concat('Deleting SmartShelf.EvtEventLog events older than ', Convert(VarChar(30), @CutoffDtTm, 121), '...(', Convert(VarChar(30), GetDate(), 120), ')')
Print @PrintMsg

-- - - - - - - - - - -
-- Log a begin event.
-- - - - - - - - - - -
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@NumOfRowsBlockSize=' + Cast(@NumOfRowsBlockSize As VarChar(12)) + ';@MaxToDelete=' + Cast(@MaxToDelete As VarChar(12))
Exec lsp_DbLogSqlEvent 'A', 'DelOldSSEvts Beg', '', @EvtNotes, 'lsp_DbDeleteOldSmartShelfEvents'

--------------------------------------------------------------------
-- Delete in blocks of based on @NumOfRowsBlockSize parameter value.
--------------------------------------------------------------------
-- Deleting a finite number of rows at a time avoids over-inflating the transaction log.
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
   While Exists(Select * From SmartShelf.EvtEventLog Where DtTm < @CutoffDtTm) And @NumOfRowsDeleted > @MaxToDelete
   Begin
      -----------------------------------
      -- Delete the oldest block of rows.
      -----------------------------------
      Set @PrintMsg = Concat('Processing block of ', Cast(@NumOfRowsBlockSize As VarChar(12)), '...   (', Convert(VarChar(30), GetDate(), 120), ')')
      Print @PrintMsg
      
      ;With Cte As
         (Select Top(@NumOfRowsBlockSize) * From SmartShelf.EvtEventLog Where DtTm < @CutoffDtTm Order By Id)
       Delete From Cte
       ------------------------------------
       -- Track the number of rows deleted.
       ------------------------------------
       Set @NumOfRowsDeleted = @NumOfRowsDeleted + @NumOfRowsBlockSize
   End
End Try
Begin Catch
   Set @ErrNum = ERROR_NUMBER()
   Set @ErrMsg = ERROR_MESSAGE()
   Exec dbo.lsp_DbLogSqlError 'DelOldSSEvts Err', @ErrNum, @ErrMsg, 'lsp_DbDeleteOldSmartShelfEvents'

   -- - - - - - - - - - - - -
   -- Log a failed end event.
   -- - - - - - - - - - - - -
   Set @PrintMsg = Concat('Failed. (', Convert(VarChar(30), GetDate(), 120), ')')
   Print @PrintMsg

   Set @EvtNotes = Left(@ErrMsg, 255)
   Exec lsp_DbLogSqlEvent 'A', 'DelOldSSEvts End', 'Failed', @EvtNotes, 'lsp_DbDeleteOldSmartShelfEvents'

   Return @ErrNum
End Catch
End

