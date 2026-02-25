ALTER PROCEDURE "dbo"."lsp_DbDeleteOldWorkflowImages" 
   @OlderThanXDays Int = 36500,
   @ImgsOfContentCodes VarChar(30) = '*',
   @NumOfRowsBlockSize Int = 1000,
   @MaxToDelete Int = 50000
As
Begin
/*
Object Type:
   Stored Procedure

Object Name:
   lsp_DbDeleteOldWorkflowImages

Description:
   Delete Workflow Captured Images that were captured prior to X days ago.
   This deletes rows from tables:  ImgImage, ImgCanImgAssoc, ImgRxImgAssoc.

   This process does NOT delete rows for archived images (whose ArchivedDtTm value is set (non-null) and whose image data is, therefore, "0x").

Version:
   3

Parameters:
   Input:
      @OlderThanXDays      - Workflow Images captured prior to this many days ago will be deleted.
      @ImgsOfContentCodes  - A list of workflow image content codes defining which Workflow Images to delete.
                             '*' = All types.
                             Specify the list as a comma delimited string list of codes.  See reference table ImgContent for the complete
                               list of content codes.
                             Example:  '1,2,3,4,5,6,7'
      @NumOfRowsBlockSize  - The images (rows) will be deleted in blocks of this many rows at a time.
                             Deletions are performed in blocks of rows to prevent performing one huge transaction that would inflate the t-log. 
      @MaxToDelete         - The maximum number of Workflow to delete.
                             If this value is not a multiple of @NumOfRowsBlockSize, the effect will be approximate.
                             Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition where a huge
                               number of "older than X days" rows exist when this action is performed.
                             The goal is to delete a "finite" amount of data between full database backups where the t-log is truncated.
   Output:
      None

Return Value:
       Return Code:
          0     = Success
          Non-0 = Failure

Comments:
       This procedure is called from the nightly maintenance procedure.
       The oldest captured images are deleted first.

*/
Declare @Rc Int                        -- SQL Command Return Code.  (Saved Value of @@Error.)
Declare @CutoffDtTm DateTime           -- "X Days Ago" Cutoff Date/Time.
Declare @NumOfRowsDeleted Int = 0      -- Number of rows that have been deleted.

Declare @ContentCodes Table(Code Int)  -- List of @ImgsOfContentCodes as a table.
Declare @EvtNotes VarChar(255)         -- Logged event notes.
Declare @ErrNum Int                    -- Error number (if error occurs).
Declare @ErrMsg NVarChar(4000)         -- Error message (if error occurs).


If Object_Id('TempDb..#BlkIds') Is Not Null
   Drop Table #BlkIds

If Object_Id('TempDb..#OlderImageIds') Is Not Null
   Drop Table #OlderImageIds


Create Table #OlderImageIds (Id BigInt)
Create Index ById On #OlderImageIds(Id)

Create Table #BlkIds (Id Int)          -- Block of IDs to be deleted.
Create Index ById on #BlkIds(Id)

Set NoCount On

---------------------------------------------------------------------------------
-- Convert the @ImgsOfContentCodes string list into a table of the Content Codes.
---------------------------------------------------------------------------------
If @ImgsOfContentCodes = '*'
   Insert Into @ContentCodes Select Code From ImgContent
Else
   Insert Into @ContentCodes Select Value As Code From dbo.fStringListToIntTable(@ImgsOfContentCodes)

Set NoCount Off

-------------------------------------------------------------
-- Set the Cutoff Date/Time from the Older Than X Days value.
-------------------------------------------------------------
Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())

Print 'Deleting Workflow images older than ' + Convert(VarChar(30), @CutoffDtTm, 121) + '...'

-- - - - - - - - - - -
-- Log a begin event.
-- - - - - - - - - - -
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12)) + ';@NumOfRowsBlockSize=' + Cast(@NumOfRowsBlockSize As VarChar(12)) + ';@MaxToDelete=' + Cast(@MaxToDelete As VarChar(12))
Exec lsp_DbLogSqlEvent 'A', 'DelOldWrkFlwImgs Beg', '', @EvtNotes, 'lsp_DbDeleteOldWorkflowImages'

Begin Try

   If @OlderThanXDays <= 0
      Begin
         Set @ErrMsg = FormatMessage('Invalid parameter value. (@OlderThanXDays = %i)', @OlderThanXDays)
         RaisError(@ErrMsg, 16, 1)
      End

   -------------------------------------------------------------------------
   --Capture the list of ImageIds older than the cutoff date to a temp table
   -------------------------------------------------------------------------
   Set NoCount On

   --Get the Image Ids from ImgImage table
   Insert Into 
      #OlderImageIds
   Select
      I.Id
   From
      ImgImage I
         Join 
      @ContentCodes C
         On C.Code = I.ContentCode  -- Include rows of the specified image content codes.
   Where
      CapturedDtTm < @CutoffDtTm    -- Include old rows.
         And
      ArchivedDtTm Is Null          -- Include non-archived rows.
   Order By
      CapturedDtTm

   --Get the Image Ids from ImgImage_IntId table
   Insert Into
      #OlderImageIds
   Select
      I.Id
   From
      ImgImage_IntId I
         Join 
      @ContentCodes C
         On C.Code = I.ContentCode  -- Include rows of the specified image content codes.
   Where
      CapturedDtTm < @CutoffDtTm    -- Include old rows.
         And
      ArchivedDtTm Is Null          -- Include non-archived rows.
   Order By
      CapturedDtTm

   Set @Rc = @@Error
   If @Rc <> 0
      Return @Rc  -- Return Failing R/C.
   
   Set NoCount Off
   ---------------------------------------------------------------------------------
   -- While there are old rows and the maximum number of deletions has not occurred:
   --   Delete a block of rows.
   ---------------------------------------------------------------------------------
   While Exists (Select * From #OlderImageIds) And @NumOfRowsDeleted < @MaxToDelete
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
            #BlkIds
               Select
                  Top (@NumOfRowsBlockSize) Id
               From
                  #OlderImageIds
               Order By
                  Id
      
         Set @Rc = @@Error
         If @Rc <> 0
            Return @Rc  -- Return Failing R/C.

         Set NoCount Off

         -- - - - - - - - - - - - - - - - -
         -- Delete ImgCanImgAssoc rows.
         -- - - - - - - - - - - - - - - - -
         Delete
            CI
         From
            #BlkIds B
               Join
            ImgCanImgAssoc CI
               On CI.ImgId = B.Id

         Set @Rc = @@Error
         If @Rc <> 0
            Return @Rc  -- Return Failing R/C.

         -- - - - - - - - - - - - - - -
         -- Delete ImgRxImgAssoc rows.
         -- - - - - - - - - - - - - - -
         Delete
            RI
         From
            #BlkIds B
               Join
            ImgRxImgAssoc RI
               On RI.ImgId = B.Id

         Set @Rc = @@Error
         If @Rc <> 0
            Return @Rc  -- Return Failing R/C.

         -- - - - - - - - - - - - - -
         -- Delete ImgImage rows.
         -- - - - - - - - - - - - - -
         Delete
            I
         From
            #BlkIds B
               Join
            ImgImage I
               On I.Id = B.Id

         -- - - - - - - - - - - - - - -
         -- Delete ImgImage_IntId rows.
         -- - - - - - - - - - - - - - -
         Delete
            I
         From
            #BlkIds B
               Join
            ImgImage_IntId I
               On I.Id = B.Id


         Set @Rc = @@Error
         If @Rc <> 0
            Return @Rc  -- Return Failing R/C.

         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         -- Clear the table of IDs for the next block of rows to delete.
         -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         Set NoCount On

         Delete From 
            #OlderImageIds
               Where Id <= (Select Max(Id) From #BlkIds)
         
         Truncate Table #BlkIds
      
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
      Exec lsp_DbLogSqlEvent 'A', 'DelOldWrkFlwImgs End', 'Successful', @EvtNotes, 'lsp_DbDeleteOldWorkflowImages'

      Drop Table #OlderImageIds
      Drop Table #BlkIds
      
      Return 0  -- Return Success

End Try

Begin Catch

   Set @ErrNum = ERROR_NUMBER()
   Set @ErrMsg = ERROR_MESSAGE()
   Exec dbo.lsp_DbLogSqlError 'DelOldWrkFlwImgs Err', @ErrNum, @ErrMsg, 'lsp_DbDeleteOldWorkflowImages'

   -- - - - - - - - - - - - -
   -- Log a failed end event.
   -- - - - - - - - - - - - -
   Print 'Failed.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
   Set @EvtNotes = Left(@ErrMsg, 255)
   Exec lsp_DbLogSqlEvent 'A', 'DelOldWrkFlwImgs End', 'Failed', @EvtNotes, 'lsp_DbDeleteOldWorkflowImages'

   Return @ErrNum

End Catch
End
