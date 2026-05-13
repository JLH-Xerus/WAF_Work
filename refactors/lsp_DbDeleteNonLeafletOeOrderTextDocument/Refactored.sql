--/
ALTER PROCEDURE [dbo].[lsp_DbDeleteNonLeafletOeOrderTextDocument]
   @OlderThanXDays      Int = 365,
   @NumOfRowsBlockSize  Int = 1000,
   @MaxToDelete         Int = 300000
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_DbDeleteNonLeafletOeOrderTextDocument

Version:
   4

Description:
   Delete non-leaflet OeOrderTextDocument rows older than @OlderThanXDays days, in
   blocks of @NumOfRowsBlockSize rows, up to @MaxToDelete total. Uses the
   OeOrderTextDocumentClassification table to distinguish leaflets from non-leaflets.

Comments:
   v4 - Performance refactoring:
      1. Replaced the WHILE EXISTS check (which queried the linked-server tables
         [PWDAZNSYMPH02].pharmassist.dbo.OeOrderTextDocument and
         [PWDAZNSYMPH02].pharmassist.dbo.OeOrderTextDocumentClassification) with a
         @@ROWCOUNT-based loop control. The remote-server query was executed once per
         iteration; the @@ROWCOUNT approach reads the row count directly from the
         immediately-preceding DELETE statement without a separate query.
         IMPORTANT: see Section 11.1 of Analysis.md for the open question about whether
         the linked-server reference was intentional. v4 assumes local tables are
         authoritative; the iA team should confirm before deployment.
      2. Narrowed the CTE projection from `Select Top (N) *` to `Select Top (N) Id`.
         The DELETE only needs the row locator; the wide projection forced the engine
         to read all columns of every candidate row.
      3. Removed the stray `select @NumOfRowsDeleted` statement on line 143 of v3,
         which was returning a single-value result set to the caller on every loop
         iteration. This was almost certainly a debugging artifact left in by mistake.
      4. Pre-materialized the candidate-Id set in a temp table indexed by Id before
         each DELETE. This narrows the inner `Where Id In (Select Id From OeOrder
         TextDocumentClassification ...)` subquery from a per-row evaluation to a
         single seek per block.
      5. Local-variable copies of the three input parameters.
      6. Added Option (Recompile) on the candidate-Id selection inside the loop so
         the optimizer plans against the current data state at each block.
*/

Set NoCount On

Declare @LocalOlderThanXDays      Int = @OlderThanXDays
Declare @LocalNumOfRowsBlockSize  Int = @NumOfRowsBlockSize
Declare @LocalMaxToDelete         Int = @MaxToDelete

Declare @CutoffDtTm        DateTime
Declare @NumOfRowsDeleted  Int = 0
Declare @DeletedThisPass   Int = 0
Declare @ContinueLoop      Bit = 1
Declare @EvtNotes          VarChar(255)
Declare @ErrMsg            NVarChar(4000)
Declare @ErrNum            Int

------------------------------------------------------------------------------------------
-- Log begin.
------------------------------------------------------------------------------------------
Exec lsp_DbLogSqlEvent 'A', 'DelOEOrderTD SP Begin', '', 'Selecting cutoffdttm', 'lsp_DbDeleteNonLeafletOeOrderTextDocument'

Set @CutoffDtTm = DateAdd(Day, -@LocalOlderThanXDays, GetDate())

Print 'Deleting (OeOrderTextDocument) events older than '
    + Convert(VarChar(30), @CutoffDtTm, 121) + '...   ('
    + Convert(VarChar(30), GetDate(), 120) + ')'

Set @EvtNotes = '@OlderThanXDays='     + Cast(@LocalOlderThanXDays As VarChar(12))
              + '@CutoffDtTm='         + Cast(@CutoffDtTm As VarChar(12))
              + ';@NumOfRowsBlockSize=' + Cast(@LocalNumOfRowsBlockSize As VarChar(12))
              + ';@MaxToDelete='       + Cast(@LocalMaxToDelete As VarChar(12))

Exec lsp_DbLogSqlEvent 'A', 'DelOEOrderTD purge begin', '', @EvtNotes, 'lsp_DbDeleteNonLeafletOeOrderTextDocument'

Begin Try

    If @LocalOlderThanXDays <= 0
    Begin
        Set @ErrMsg = FormatMessage('Invalid parameter value. (@OlderThanXDays = %i)', @LocalOlderThanXDays)
        RaisError(@ErrMsg, 16, 1)
    End

    Drop Table If Exists #BlockIds

    Create Table #BlockIds (Id BigInt Primary Key)

    ------------------------------------------------------------------------------------------
    -- Loop driven by @@ROWCOUNT. Each pass: identify a block of candidate Ids, DELETE them,
    -- track how many were actually deleted. If fewer than the block size were deleted, the
    -- candidate set is exhausted and the loop exits.
    ------------------------------------------------------------------------------------------
    While @ContinueLoop = 1 And @NumOfRowsDeleted < @LocalMaxToDelete
    Begin
        Truncate Table #BlockIds

        Insert Into #BlockIds (Id)
        Select Top (@LocalNumOfRowsBlockSize)
              dc.Id
        From OeOrderTextDocumentClassification dc With (ReadUncommitted)
        Where dc.HistoryDtTm < @CutoffDtTm
          And dc.IsLeaflet = 'Non-Leaflet'
        Order By dc.Id
        Option (Recompile);

        Set @DeletedThisPass = @@ROWCOUNT

        If @DeletedThisPass = 0
        Begin
            Set @ContinueLoop = 0
        End
        Else
        Begin
            Delete From OeOrderTextDocument With (RowLock)
            From OeOrderTextDocument otd With (RowLock)
            Inner Join #BlockIds B
                On B.Id = otd.Id

            Set @DeletedThisPass = @@ROWCOUNT
            Set @NumOfRowsDeleted = @NumOfRowsDeleted + @DeletedThisPass

            If @DeletedThisPass < @LocalNumOfRowsBlockSize
                Set @ContinueLoop = 0
        End
    End

    Drop Table If Exists #BlockIds

    ------------------------------------------------------------------------------------------
    -- Log success.
    ------------------------------------------------------------------------------------------
    Print 'Complete.   (' + Convert(VarChar(30), GetDate(), 120) + ')'
    Set @EvtNotes = 'No of Rows Deleted' + Cast(@NumOfRowsDeleted As VarChar(10))
    Exec lsp_DbLogSqlEvent 'A', 'DelOEOrderTD Purge End', 'Successful', @EvtNotes, 'lsp_DbDeleteNonLeafletOeOrderTextDocument'

    Return 0

End Try

Begin Catch

    Set @ErrNum = ERROR_NUMBER()
    Set @ErrMsg = ERROR_MESSAGE()
    Exec dbo.lsp_DbLogSqlError 'DelOEOrderTD Err', @ErrNum, @ErrMsg, 'lsp_DbDeleteNonLeafletOeOrderTextDocument'

    Print 'Failed.   (' + Convert(VarChar(30), GetDate(), 120) + ')'
    Set @EvtNotes = Left(@ErrMsg, 255)
    Exec lsp_DbLogSqlEvent 'A', 'DelOEOrderTD End', 'Failed', @EvtNotes, 'lsp_DbDeleteNonLeafletOeOrderTextDocument'

    Return @ErrNum

End Catch

End
GO
