ALTER PROCEDURE "dbo"."lsp_DbDeleteOldErrors" 
	@CutOffDate DateTime
As
----------------------------------------------------------------------------------------------------
-- Procedure:
--        lsp_DbDeleteOldErrors
--
-- Description:
--        Delete all Symphony Errors (i.e., EvtError Rows) older than a specified date.
--
-- Parameters:
--        @CutOffDate        - Errors older than this date will be deleted.
--
-- Return Value:
--        Return Code:
--           0 = Success
--          -1 = Failed to delete old errors.
--
-- Comments:
--        This procedure is called from the nightly maintenance procedure.
--
----------------------------------------------------------------------------------------------------
Declare @Rc Int                        -- SQL Command Return Code.  (Saved Value of @@Error.)

-------------------------------------
-- Delete/Commit in blocks of 50,000.
-------------------------------------
-- Committing a finite number of rows at a time avoids over-inflating the transaction log.
--
While Exists (Select * From EvtError Where ErrorDtTm < @CutOffDate)
	Begin
		Begin Tran

		Print 'Processing block of 50,000...'

--    Delete
--      Top (50000)          -- This syntax only good in SQL Server 2005 and later.
--    From
--      EvtError
--    Where
--      ErrorDtTm < @CutOffDate

		Delete EvtError
		From EvtError As AllErrs
		Join (
			Select Top 50000 ErrorId 
			From EvtError 
			Where ErrorDtTm < @CutOffDate
		) As DelErrs
			On AllErrs.ErrorId = DelErrs.ErrorId

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
	End  -- While Exists (Select * From EvtError Where ErrorDtTm < @CutOffDate)
Return 0                   -- Return Success
