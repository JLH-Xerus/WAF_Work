ALTER PROCEDURE "dbo"."lsp_SsDeleteOldEvents" 
	@CutOffDate DateTime
As
BEGIN
----------------------------------------------------------------------------------------------------
-- Procedure:
--        lsp_SsDeleteOldEvents
--
-- Description:
--        Delete all Lightway Events (i.e., EvtLightwayEventLog table rows)
--          older than a specified date.
--
-- Parameters:
--        @CutOffDate        - Events older than this date will be deleted.
--
-- Return Value:
--        Return Code:
--           0 = Success
--          -1 = Failed to delete old events.
--
-- Comments:
--        None
--
----------------------------------------------------------------------------------------------------
	Declare @Rc Int                        -- SQL Command Return Code.  (Saved Value of @@Error.)

	------------------------------
	-- Delete in blocks of 50,000.
	------------------------------
	-- Deleting a finite number of rows at a time avoids over-inflating the transaction log.
	--
	While Exists (Select * From SmartShelf.EvtEventLog Where DtTm < @CutOffDate)
		Begin

		Print 'Processing block of 50,000...'

		Delete
			Top (50000)          -- This syntax only good in SQL Server 2005 and later.
		From
			SmartShelf.EvtEventLog
		Where
			DtTm < @CutOffDate

		Set @Rc = @@Error
    
		If @Rc = 0
			Begin
				Print '  - Complete.'
			End
		Else
			Begin
				Print '  - Failed.'
				Return @Rc           -- Return Failing R/C.
			End

		End  -- While Exists (Select * From EvtEvent Where EventDtTm < @CutOffDate)

	Return 0                   -- Return Success
END
