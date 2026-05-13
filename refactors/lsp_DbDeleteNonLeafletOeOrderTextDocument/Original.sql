/****** Object:  StoredProcedure [dbo].[lsp_DbDeleteNonLeafletOeOrderTextDocument]    Script Date: 4/5/2026 8:50:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE Procedure [dbo].[lsp_DbDeleteNonLeafletOeOrderTextDocument]
 @OlderThanXDays Int = 365,  
 @NumOfRowsBlockSize Int = 1000,  
 @MaxToDelete Int = 300000 

 As
 /*
Object Type: 
Stored Procedure 
 
Name:
lsp_DbDeleteNonLeafletOeOrderTextDocument
 
Version: 
3
 
Description: 
This stored procedure will check for the patient paperwork documents (medguides and adheris only) and documenttype (only pdf's)  
and deletes the documents that are older than 7 days. The logic for determining the medguides and adheris documents 
is to check for the size of the docdata field ad delete anything that's larger than 100kb (all leaflets are ~30-40kb) . 
This deletes rows from tables: OeOrderTextDocument

9/19/2025 - Modified the SP to use "READUNCOMMITTED" table hint and changed the logic for the "@OlderThanXDays" parameter to
retrieve 7 days of data from the min(historydttm)
 
9/29/2025 - Modified SP to to check and compare id column from OeOrderTextDocumentClassification (data loaded from secodary node)
and oeordertextdocument

Parameters: 
Input: 
 @OlderThanXDays      - Retrieve the events from min(HistoryDttm) to min(HistoryDtTm)+@OlderThanXDays 
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
--SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

set nocount on

Declare @CutoffDtTm DateTime           -- "X Days Ago" Cutoff Date/Time.  
Declare @NumOfRowsDeleted Int = 0
Declare @EvtNotes VarChar(255)         -- Logged event notes.
Declare @ErrMsg NVarChar(4000)         -- Error message (if error occurs). 
Declare @ErrNum Int                    -- Error number (if error occurs).

-------------------------------------------------------------
-- Set the Cutoff Date/Time from the Older Than X Days value.
-------------------------------------------------------------
-- - - - - - - - - - -
-- Log a begin event. 
-- - - - - - - - - - -
Exec lsp_DbLogSqlEvent 'A', 'DelOEOrderTD SP Begin', '', 'Selecting cutoffdttm', 'lsp_DbDeleteNonLeafletOeOrderTextDocument' 


Set @CutoffDtTm = DateAdd(Day, -@OlderThanXDays, GetDate())  

------------------------------------------
-- Delete from table OeOrderTextDocument.
------------------------------------------
/* Table OeOrderTextDocument holds<Nexia used this table to store documents that are printed from Nexia 
including patient paper work. However currently in Walgreens, we are storing only the patient paperwork that is received 
from the host system (PPI/Leaflets, Medguides and Adheris) in this table.>
*/
--
Print 'Deleting (OeOrderTextDocument) events older than ' + Convert(VarChar(30), @CutoffDtTm, 121) + '...' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')' 
 
-- - - - - - - - - - -
-- Log a begin event. 
-- - - - - - - - - - -
Set @EvtNotes = '@OlderThanXDays=' + Cast(@OlderThanXDays As VarChar(12))+ '@CutoffDtTm=' + Cast(@CutoffDtTm As VarChar(12)) + ';@NumOfRowsBlockSize=' + Cast(@NumOfRowsBlockSize As VarChar(12)) + ';@MaxToDelete=' + Cast(@MaxToDelete As VarChar(12))
Exec lsp_DbLogSqlEvent 'A', 'DelOEOrderTD purge begin', '', @EvtNotes, 'lsp_DbDeleteNonLeafletOeOrderTextDocument' 

Begin Try 
 
If @OlderThanXDays <= 0 
 Begin 
  Set @ErrMsg = FormatMessage('Invalid parameter value. (@OlderThanXDays = %i)', @OlderThanXDays) 
  RaisError(@ErrMsg, 16, 1) 
 End 

 --------------------------------------------------------------------------------- 
-- While there are old rows and the maximum number of deletions has not occurred: 
--  Delete a block of rows. 
--------------------------------------------------------------------------------- 


While Exists

(    Select 1
    From [PWDAZNSYMPH02].pharmassist.dbo.OeOrderTextDocument od with (readuncommitted)
    Join [PWDAZNSYMPH02].pharmassist.dbo.OeOrderTextDocumentClassification dc on od.id = dc.id
    Where dc.historydttm < @CutoffDtTm
      And dc.IsLeaflet = 'Non-Leaflet'
      -- Uncomment if needed:
      -- And od.IsDeleted = 0
)
And @NumOfRowsDeleted < @MaxToDelete
  Begin  
  
   -----------------------------------  
   -- Delete the oldest block of rows.  
   -----------------------------------  
     
 
   ;With Cte As  
    (  
     Select Top (@NumOfRowsBlockSize) * From OeOrderTextDocument 
	 Where id in (select id from OeOrderTextDocumentClassification where historydttm < @CutoffDtTm and [IsLeaflet]='Non-Leaflet') --and IsDeleleted =0) 
    )  


--Delete from the main table
   Delete From Cte with (rowlock)

   Set @NumOfRowsDeleted = @NumOfRowsDeleted + @@ROWCOUNT  
  select @NumOfRowsDeleted
  End  

  -- - - - - - - - - - - - - - - 
-- Log a successful end event. 
-- - - - - - - - - - - - - - - 
Print 'Complete.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')' 
Set @EvtNotes = 'No of Rows Deleted' + + CAST(@NumOfRowsDeleted AS VARCHAR(10))
Exec lsp_DbLogSqlEvent 'A', 'DelOEOrderTD Purge End', 'Successful', @EvtNotes, 'lsp_DbDeleteNonLeafletOeOrderTextDocument' 

Return 0 -- Return Success 
  
End Try 
 
Begin Catch 
 
Set @ErrNum = ERROR_NUMBER() 
Set @ErrMsg = ERROR_MESSAGE() 
Exec dbo.lsp_DbLogSqlError 'DelOEOrderTD Err', @ErrNum, @ErrMsg, 'lsp_DbDeleteNonLeafletOeOrderTextDocument' 
 
-- - - - - - - - - - - - - 
-- Log a failed end event.  **Should we do this**
-- - - - - - - - - - - - - 
Print 'Failed.' + '   (' + Convert(VarChar(30), GetDate(), 120) + ')' 
Set @EvtNotes = Left(@ErrMsg, 255) 
Exec lsp_DbLogSqlEvent 'A', 'DelOEOrderTD End', 'Failed', @EvtNotes, 'lsp_DbDeleteNonLeafletOeOrderTextDocument' 
 
Return @ErrNum 
 
End Catch 
GO
