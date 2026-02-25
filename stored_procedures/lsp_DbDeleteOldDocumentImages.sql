ALTER PROCEDURE "dbo"."lsp_DbDeleteOldDocumentImages" 
	@OlderThanXDays Int = 36500,
	@NumOfRowsBlockSize Int = 1000,
	@MaxToDelete Int = 50000
As
------------------------------------------------------------------------------------------------------------------------------------------------------
-- Procedure:
--        lsp_DbDeleteOldDocumentImages
--
-- Description:
--        Delete Scanned Document Images that were scanned prior to X days ago.
--        This deletes rows from tables:  DscDocument, DscPatientDocAssoc, DscRxDocAssoc.
--
--        This process does NOT delete rows for archived images (whose ArchivedDtTm value is set (non-null) and whose image data is, therefore, "0x").
--
-- Version:
--        1
--
-- Parameters:
--        @OlderThanXDays      - Document Images scanned prior to this many days ago will be deleted.
--        @NumOfRowsBlockSize  - The images (rows) will be deleted in blocks of this many rows at a time.
--                               Deletions are performed in blocks of rows to prevent performing one huge transaction that would inflate the t-log. 
--        @MaxToDelete         - The maximum number of documents to delete.
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
--        The oldest scanned images are deleted first.
--
------------------------------------------------------------------------------------------------------------------------------------------------------
Declare @Rc Int                        -- SQL Command Return Code.  (Saved Value of @@Error.)
Declare @CutoffDtTm DateTime           -- "X Days Ago" Cutoff Date/Time.
Declare @NumOfRowsDeleted Int = 0      -- Number of rows that have been deleted.
Declare @BlkIds Table(Id Int)          -- Block of IDs to be deleted.
Declare @EvtNotes VarChar(255)         -- Logged event notes.
Declare @ErrNum Int                    -- Error number (if error occurs).
Declare @ErrMsg NVarChar(4000)         -- Error message (if error occurs).

-------------------------------------------------------------
-- Set the Cutoff Date/Time from the Older Than X Days value.
-------------------------------------------------------------
Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())

Print 'Deleting document images older than ' + Convert(VarChar(30), @CutoffDtTm, 121) + '...'

-- - - - - - - - - - -
-- Log a begin event.
-- - - - - - - - - - -
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@NumOfRowsBlockSize=' + Cast(@NumOfRowsBlockSize As VarChar(12)) + ';@MaxToDelete=' + Cast(@MaxToDelete As VarChar(12))
Exec lsp_DbLogSqlEvent 'A', 'DelOldDocImgs Beg', '', @EvtNotes, 'lsp_DbDeleteOldDocumentImages'

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
	While Exists (Select * From DscDocument Where ScanDtTm < @CutoffDtTm And ArchivedDtTm Is Null) And @NumOfRowsDeleted < @MaxToDelete
		Begin

			-----------------------------------
			-- Delete the oldest block of rows.
			-----------------------------------
			Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...'

			-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			-- Build a table of IDs for the next block of rows to delete.
			-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			Set NoCount On

			Insert Into
				@BlkIds
					Select
						Top (@NumOfRowsBlockSize) Id
					From
						DscDocument
					Where
						ScanDtTm < @CutoffDtTm  -- Include old rows.
						And
						ArchivedDtTm Is Null    -- Include non-archived rows.
					Order By
						ScanDtTm

			Set @Rc = @@Error
			If @Rc <> 0
				Return @Rc  -- Return Failing R/C.

			Set NoCount Off

			-- - - - - - - - - - - - - - - - -
			-- Delete DscPatientDocAssoc rows.
			-- - - - - - - - - - - - - - - - -
			Delete
				PD
			From
				@BlkIds B
					Join
				DscPatientDocAssoc PD
					On PD.DocId = B.Id

			Set @Rc = @@Error
			If @Rc <> 0
				Return @Rc  -- Return Failing R/C.

			-- - - - - - - - - - - - - - -
			-- Delete DscRxDocAssoc rows.
			-- - - - - - - - - - - - - - -
			Delete
				RD
			From
				@BlkIds B
					Join
				DscRxDocAssoc RD
					On RD.DocId = B.Id

			Set @Rc = @@Error
			If @Rc <> 0
				Return @Rc  -- Return Failing R/C.

			Set @Rc = @@Error
			If @Rc <> 0
				Return @Rc  -- Return Failing R/C.

			-- - - - - - - - - - - - - -
			-- Delete DscDocument rows.
			-- - - - - - - - - - - - - -
			Delete
				D
			From
				@BlkIds B
					Join
				DscDocument D
					On D.Id = B.Id

			Set @Rc = @@Error
			If @Rc <> 0
				Return @Rc  -- Return Failing R/C.

			-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			-- Clear the table of IDs for the next block of rows to delete.
			-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			Set NoCount On

			Delete From @BlkIds
		
			Set @Rc = @@Error
			If @Rc <> 0
				Return @Rc  -- Return Failing R/C.

			Set NoCount Off

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
		Exec lsp_DbLogSqlEvent 'A', 'DelOldDocImgs End', 'Successful', @EvtNotes, 'lsp_DbDeleteOldDocumentImages'

	Return 0  -- Return Success

End Try

Begin Catch

	Set @ErrNum = ERROR_NUMBER()
	Set @ErrMsg = ERROR_MESSAGE()
	Exec dbo.lsp_DbLogSqlError 'DelOldDocImgs Err', @ErrNum, @ErrMsg, 'lsp_DbDeleteOldDocumentImages'

	-- - - - - - - - - - - - -
	-- Log a failed end event.
	-- - - - - - - - - - - - -
	Print 'Failed.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
	Set @EvtNotes = Left(@ErrMsg, 255)
	Exec lsp_DbLogSqlEvent 'A', 'DelOldDocImgs End', 'Failed', @EvtNotes, 'lsp_DbDeleteOldDocumentImages'

	Return @ErrNum

End Catch
