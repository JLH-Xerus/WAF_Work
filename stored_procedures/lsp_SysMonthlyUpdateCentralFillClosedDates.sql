------------------------------------------------------------------------------------------------------------------------------------------------------
-- Procedure:
--		lsp_SysMonthlyUpdateCentralFillClosedDates
-- Description:
--		Determines the current set of repeating closed dates and updates the closed dates for the Central Fill facility
--		by deleting dates older than the current date and adding dates for the next month.
-- Parameters:
--		None
-- Returns:
--    None
-- Comments:
--    Success/Failure is logged to the event table as a diagnostic event
-- NOTE:  
--		This stored procedure must be scheduled to run once a month.
--		Government Holiday names here correspond to the names used in lsp_CfGetCentralFillClosedDates for Government Holidays.  If Government Holiday 
--			names are added or changed here, the lsp_CfGetCentralFillClosedDates stored procedure should also be updated.
------------------------------------------------------------------------------------------------------------------------------------------------------
ALTER PROCEDURE "dbo"."lsp_SysMonthlyUpdateCentralFillClosedDates" 

As
BEGIN
	SET NOCOUNT ON;

	DECLARE
	@WeeklyRepetition As Int,											
	@StartRepetitionsFromThisDate As Date,				
	@ClosedOnSunday As Bit,											
	@ClosedOnMonday As Bit = 0,											
	@ClosedOnTuesday As Bit = 0,											
	@ClosedOnWednesday As Bit = 0,										
	@ClosedOnThursday As Bit = 0,											
	@ClosedOnFriday As Bit = 0,											
	@ClosedOnSaturday As Bit = 0,		
	@ClosedOnNewYearsDay As Bit = 0,
	@ClosedOnMLKJrDay	As Bit = 0,
	@ClosedOnPresidentsDay As Bit = 0,
	@ClosedOnMemorialDay As Bit = 0,
	@ClosedOnIndependenceDay As Bit = 0,
	@ClosedOnLaborDay As Bit = 0,
	@ClosedOnColumbusDay As Bit = 0,
	@ClosedOnVeteransDay As Bit = 0,
	@ClosedOnThanksgivingDay As Bit = 0,
	@ClosedOnChristmasDay As Bit = 0,									
	@ConfiguredClosedDatesList As udtTableOfVarChars,	
	@NumItemsUpdated As Int = 0,
	@Notes VarChar(50) = '',

	-- Constants
	@WeekdayClosed As VarChar(20) = 'IA-Weekday Closed',
	@NewYearsDay As VarChar(20) = 'IA-New Year''s Day',
	@MLKJrDay As VarChar(20) = 'IA-MLK Jr Day',
	@PresidentsDay As VarChar(20) = 'IA-President''s Day',
	@MemorialDay As VarChar(20) = 'IA-Memorial Day',
	@IndependenceDay As VarChar(20) = 'IA-Independence Day',
	@LaborDay As VarChar(20) = 'IA-Labor Day',
	@ColumbusDay As VarChar(20) = 'IA-Columbus Day',
	@VeteransDay As VarChar(20) = 'IA-Veteran''s Day',
	@ThanksgivingDay As VarChar(20) = 'IA-Thanksgiving Day',
	@ChristmasDay As VarChar(20) = 'IA-Christmas Day'

	-- Initiate the monthly update.
	BEGIN TRANSACTION
	---------------------------------------------------------------------------
	-- Get the currently configured Weekly and Government Holiday closed dates.
	---------------------------------------------------------------------------
	EXEC 
		lsp_SysGetCentralFillWeekdayAndHolidayClosedDates	
			@WeeklyRepetition Output,											
			@StartRepetitionsFromThisDate Output,				
			@ClosedOnSunday Output,											
			@ClosedOnMonday Output,											
			@ClosedOnTuesday Output,											
			@ClosedOnWednesday Output,										
			@ClosedOnThursday Output,											
			@ClosedOnFriday Output,											
			@ClosedOnSaturday Output,	
			@ClosedOnNewYearsDay Output,
			@ClosedOnMLKJrDay Output,
			@ClosedOnPresidentsDay Output,
			@ClosedOnMemorialDay Output,
			@ClosedOnIndependenceDay Output,
			@ClosedOnLaborDay Output,
			@ClosedOnColumbusDay Output,
			@ClosedOnVeteransDay Output,
			@ClosedOnThanksgivingDay Output,
			@ClosedOnChristmasDay Output;
	
	IF @@ERROR <> 0
		GOTO PROCERR
	-------------------------------------------------------------
	-- Get the list of currently Configured Holiday closed dates.
	-------------------------------------------------------------
	INSERT INTO
		@ConfiguredClosedDatesList
	SELECT
		CAST(Descrip As VarChar) + ',' + CAST(ClosedDate As VarChar) As Value	
	FROM SysPharmacyClosedDates 
	WHERE Descrip Not In (@WeekdayClosed, @NewYearsDay,@MLKJrDay, @PresidentsDay, 
								 @MemorialDay, @IndependenceDay, @LaborDay,@ColumbusDay,
								 @VeteransDay, @ThanksgivingDay,@ChristmasDay);			
	
	IF @@ERROR <> 0
		GOTO PROCERR																			
	-------------------------------
	-- Use today as the start date.
	------------------------------- 
	SET @StartRepetitionsFromThisDate = DATEADD(month, 1, GetDate());

	---------------------------
	-- Update the closed dates.
	---------------------------
	EXEC
		lsp_SysUpdateCentralFillClosedDates 
			@WeeklyRepetition,											
			@StartRepetitionsFromThisDate,				
			@ClosedOnSunday,											
			@ClosedOnMonday,											
			@ClosedOnTuesday,											
			@ClosedOnWednesday,										
			@ClosedOnThursday,											
			@ClosedOnFriday,											
			@ClosedOnSaturday,		
			@ClosedOnNewYearsDay,
			@ClosedOnMLKJrDay,
			@ClosedOnPresidentsDay,
			@ClosedOnMemorialDay,
			@ClosedOnIndependenceDay,
			@ClosedOnLaborDay,
			@ClosedOnColumbusDay,
			@ClosedOnVeteransDay,
			@ClosedOnThanksgivingDay,
			@ClosedOnChristmasDay,									
			@ConfiguredClosedDatesList,	
			@NumItemsUpdated Output;

	IF @@ERROR <> 0
		GOTO PROCERR

	COMMIT TRANSACTION
	-------------------
	-- Log the results.
	-------------------
	SET @NumItemsUpdated = @@ROWCOUNT;	
	SET @Notes = IIF(@NumItemsUpdated <> 0,CAST(@NumItemsUpdated As varchar(19)) + ' Central Fill closed dates added as monthly job',CAST(@NumItemsUpdated As varchar(19)) + ' Central Fill closed dates NOT added as monthly job');

	EXEC lsp_DbLogSqlEvent 'D', 'AddCFClosedDatesJob', @NumItemsUpdated,  @Notes, 'lsp_SysMonthlyUpdateCentralFillClosedDates';
		
	RETURN

PROCERR:
	ROLLBACK TRANSACTION

END
