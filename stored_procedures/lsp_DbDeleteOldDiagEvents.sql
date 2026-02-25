ALTER PROCEDURE "dbo"."lsp_DbDeleteOldDiagEvents" 
   @CutOffDate DateTime
As
Begin
/*
Object Type:
   Stored Procedure

Object Name:
   lsp_DbDeleteOldDiagEvents

Version:
   1

Description:
   Delete all NEXiA Diagnostic Events (i.e., EvtDiagEvent table rows) older than a specified date.

Parameters:
   Input:
      @CutOffDate        - Diagnostic events older than this date will be deleted.
   Output:
      None

Return Value:
   Return Code:
      0 = Success
     -1 = Failed to delete old diagnostic events.

Comments:
   This procedure is called from the nightly maintenance procedure.

*/
Declare @Rc Int                        -- SQL Command Return Code.  (Saved Value of @@Error.)

-------------------------------------
-- Delete/Commit in blocks of 50,000.
-------------------------------------
-- Committing a finite number of rows at a time avoids over-inflating the transaction log.
--
While Exists (Select * From EvtDiagEvent Where EventDtTm < @CutOffDate)
Begin
   Begin Tran

   Print 'Processing block of 50,000...'

   Delete
     Top (50000)          -- This syntax only good in SQL Server 2005 and later.
   From
     EvtDiagEvent
   Where
     EventDtTm < @CutOffDate

   Delete EvtDiagEvent
   From EvtDiagEvent As AllEvts
   Join (Select Top 50000 TrxId From EvtDiagEvent Where EventDtTm < @CutOffDate) As DelEvts
      On AllEvts.TrxId = DelEvts.TrxId

   Set @Rc = @@Error

   If @Rc = 0
      Begin
         Commit Tran
         Print '  - Complete.'
      End
   Else
      Begin
         Rollback Tran
         Print '  - Failed.'
      Return @Rc           -- Return Failing R/C.
      End
End  -- While Exists (Select * From EvtDiagEvent Where EventDtTm < @CutOffDate)

Return 0                   -- Return Success
End
