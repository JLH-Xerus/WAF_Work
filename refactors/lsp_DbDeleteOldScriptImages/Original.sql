ALTER PROCEDURE "dbo"."lsp_DbDeleteOldScriptImages" 
	@OlderThanXDays Int = 36500,
	@NumOfRowsBlockSize Int = 1000,
	@MaxToDelete Int = 50000
As
------------------------------------------------------------------------------------------------------------------------------------------------------
-- Procedure:
--        lsp_DbDeleteOldScriptImages
--
-- Description:
--        Delete Rx Script Images that were scanned prior to X days ago.
--        This deletes rows from table OeScriptImage.
--
--        This process does NOT delete rows for archived images (whose ArchivedDtTm value is set (non-null) and whose image data is, therefore, "0x").
--
-- Version:
--        1
--
-- Parameters:
--        @OlderThanXDays      - Script Images scanned prior to this many days ago will be deleted.
--        @NumOfRowsBlockSize  - The images (rows) will be deleted in blocks of this many rows at a time.
--                               Deletions are performed in blocks of rows to prevent performing one huge transaction that would inflate the t-log. 
--        @MaxToDelete         - The maximum number of script images to delete.
--                               If this value is not a multiple of @NumOfRowsBlockSize, the effect will be approximate.
--                               Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition where a huge
--                                 number of "older than X days" rows exist when this action is performed.
--                               The goal is to delete a "finite" amount of data between full databaes backups where the t-log is truncated.
--
-- Return Value:
--        Return Code:
--           0     = Success
--           Non-0 = Failure
--
-- Comments:
--        This procedure is called from the nightly maintenance procedure.
--
--        The oldest scanned script images are deleted first.
--
------------------------------------------------------------------------------------------------------------------------------------------------------
Declare @Rc Int                        -- SQL Command Return Code.  (Saved Value of @@Error.)
Declare @CutoffDtTm DateTime           -- "X Days Ago" Cutoff Date/Time.
Declare @NumOfRowsDeleted Int = 0      -- Number of rows that have been deleted.
Declare @EvtNotes VarChar(255)         -- Logged event notes.
Declare @ErrNum Int                    -- Error number (if error occurs).
Declare @ErrMsg NVarChar(4000)         -- Error message (if error occurs).

-------------------------------------------------------------
-- Set the Cutoff Date/Time from the Older Than X Days value.
-------------------------------------------------------------
Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())

Print 'Deleting script images older than ' + Convert(VarChar(30), @CutoffDtTm, 121) + '...'

-- - - - - - - - - - -
-- Log a begin event.
-- - - - - - - - - - -
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@NumOfRowsBlockSize=' + Cast(@NumOfRowsBlockSize As VarChar(12)) + ';@MaxToDelete=' + Cast(@MaxToDelete As VarChar(12))
Exec lsp_DbLogSqlEvent 'A', 'DelOldScrImgs Beg', '', @EvtNotes, 'lsp_DbDeleteOldScriptImages'

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
	While Exists (Select * From OeScriptImage Where ScanDateTime < @CutoffDtTm And ArchivedDtTm Is Null) And @NumOfRowsDeleted < @MaxToDelete
		Begin

			-----------------------------------
			-- Delete the oldest block of rows.
			-----------------------------------
			Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...'

			;With BlockOfRows As
				(
					Select
						Top (@NumOfRowsBlockSize) *
					From
						OeScriptImage
					Where
						ScanDateTime < @CutoffDtTm  -- Include old rows.
						And
						ArchivedDtTm Is Null        -- Include non-archived rows.
					Order By
						ScanDateTime
				)
			Delete From BlockOfRows

			Set @Rc = @@Error
    
			If @Rc <> 0
				Return @Rc  -- Return Failing R/C.

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
	Exec lsp_DbLogSqlEvent 'A', 'DelOldScrImgs End', 'Successful', @EvtNotes, 'lsp_DbDeleteOldScriptImages'

	Return 0  -- Return Success

End Try

Begin Catch

	Set @ErrNum = ERROR_NUMBER()
	Set @ErrMsg = ERROR_MESSAGE()
	Exec dbo.lsp_DbLogSqlError 'DelOldScrImgs Err', @ErrNum, @ErrMsg, 'lsp_DbDeleteOldScriptImages'

	-- - - - - - - - - - - - -
	-- Log a failed end event.
	-- - - - - - - - - - - - -
	Print 'Failed.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
	Set @EvtNotes = Left(@ErrMsg, 255)
	Exec lsp_DbLogSqlEvent 'A', 'DelOldScrImgs End', 'Failed', @EvtNotes, 'lsp_DbDeleteOldScriptImages'

	Return @ErrNum

End Catch
