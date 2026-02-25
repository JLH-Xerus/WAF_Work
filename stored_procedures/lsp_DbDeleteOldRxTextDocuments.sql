ALTER PROCEDURE "dbo"."lsp_DbDeleteOldRxTextDocuments" 
	@OlderThanXDays Int = 36500,
	@DocsOfTypeCodes VarChar(30) = '*',
	@NumOfRowsBlockSize Int = 1000,
	@MaxToDelete Int = 50000
As
------------------------------------------------------------------------------------------------------------------------------------------------------
-- Procedure:
--        lsp_DbDeleteOldRxTextDocuments
--
-- Description:
--        Delete Rx Text Documents whose Rx was completed or canceled prior to X days ago.
--        This deletes rows from table OeOrderTextDocument.
--
-- Version:
--        1
--
-- Parameters:
--        @OlderThanXDays      - Rx Text Documents whose Rx was completed or canceled prior to this many days ago will be deleted.
--        @DocsOfTypeCodes     - A list of Rx text document type codes defining which Rx Text Documents to delete.
--                               '*' = All types.
--                               Specify the list as a comma delimited string list of codes.  See reference table OeTextDocumentType for the complete
--                                 list of type codes.
--                               Example:  'R,P,K,X'
--        @NumOfRowsBlockSize  - The documents (rows) will be deleted in blocks of this many rows at a time.
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
--        The oldest scanned documents are deleted first.
--
------------------------------------------------------------------------------------------------------------------------------------------------------
Declare @Rc Int                            -- SQL Command Return Code.  (Saved Value of @@Error.)
Declare @CutoffDtTm DateTime               -- "X Days Ago" Cutoff Date/Time.
Declare @NumOfRowsDeleted Int = 0          -- Number of rows that have been deleted.
Declare @TypeCodes Table(Code VarChar(2))  -- List of @DocsOfTypeCodes as a table.
Declare @EvtNotes VarChar(255)         -- Logged event notes.
Declare @ErrNum Int                    -- Error number (if error occurs).
Declare @ErrMsg NVarChar(4000)         -- Error message (if error occurs).

Set NoCount On

---------------------------------------------------------------------------
-- Convert the @DocsOfTypeCodes string list into a table of the Type Codes.
---------------------------------------------------------------------------
If @DocsOfTypeCodes = '*'
	Insert Into @TypeCodes Select Code From OeTextDocumentType
Else
	Insert Into @TypeCodes Select Value As Code From dbo.fStringListToVarCharTable(@DocsOfTypeCodes)

Set NoCount Off

-------------------------------------------------------------
-- Set the Cutoff Date/Time from the Older Than X Days value.
-------------------------------------------------------------
Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())

Print 'Deleting Rx text documents older than ' + Convert(VarChar(30), @CutoffDtTm, 121) + '...'

-- - - - - - - - - - -
-- Log a begin event.
-- - - - - - - - - - -
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@NumOfRowsBlockSize=' + Cast(@NumOfRowsBlockSize As VarChar(12)) + ';@MaxToDelete=' + Cast(@MaxToDelete As VarChar(12))
Exec lsp_DbLogSqlEvent 'A', 'DelOldRxTxtDocs Beg', '', @EvtNotes, 'lsp_DbDeleteOldRxTextDocuments'

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
	While Exists (Select * From OeOrderTextDocument D Join @TypeCodes C On C.Code = D.TextDocTypeCode Where HistoryDtTm < @CutoffDtTm) And @NumOfRowsDeleted < @MaxToDelete
		Begin

			-----------------------------------
			-- Delete the oldest block of rows.
			-----------------------------------
			Print 'Processing block of ' + Cast(@NumOfRowsBlockSize As VarChar(12)) + '...'

			Delete
				D
			From
				(
					Select
						Top (@NumOfRowsBlockSize) D.Id
					From
						OeOrderTextDocument D
							Join
						@TypeCodes C
							On C.Code = D.TextDocTypeCode  -- Include rows of the specified document type codes.
					Where
						D.HistoryDtTm < @CutoffDtTm       -- Include old rows.
					Order By
						D.HistoryDtTm
				) BlkIds
					Join
				OeOrderTextDocument D
					On D.Id = BlkIds.Id

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
		Exec lsp_DbLogSqlEvent 'A', 'DelOldRxTxtDocs End', 'Successful', @EvtNotes, 'lsp_DbDeleteOldRxTextDocuments'

	Return 0  -- Return Success

End Try

Begin Catch

	Set @ErrNum = ERROR_NUMBER()
	Set @ErrMsg = ERROR_MESSAGE()
	Exec dbo.lsp_DbLogSqlError 'DelOldRxTxtDocs Err', @ErrNum, @ErrMsg, 'lsp_DbDeleteOldRxTextDocuments'

	-- - - - - - - - - - - - -
	-- Log a failed end event.
	-- - - - - - - - - - - - -
	Print 'Failed.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
	Set @EvtNotes = Left(@ErrMsg, 255)
	Exec lsp_DbLogSqlEvent 'A', 'DelOldRxTxtDocs End', 'Failed', @EvtNotes, 'lsp_DbDeleteOldRxTextDocuments'

	Return @ErrNum

End Catch
