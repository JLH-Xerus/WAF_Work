ALTER PROCEDURE "dbo"."lsp_DbLogDbFileSpaceStats" 
As
--------------------------------------------------------------------
-- Log Database File Space Stats from table DbFileSpaceStatsSnapshot
-- into table DbFileSpaceStatsHistory.
--------------------------------------------------------------------
-- Also log data as event entries within EvtEvent.
--

Set NoCount On

-----------------------------------------------------
-- Refresh the data in table DbFileSpaceStatsSnapshot
-- with current database file space statistcics.
-----------------------------------------------------
Exec lsp_DbPopulateTableDbFileSpaceStatsSnapshot

--
-- Capture the current date/time to log a same-exact value in rows of data.
--
Declare @CurrDtTm DateTime

Set @CurrDtTm = GetDate()

---------------------------------------------------------------------------------------------
-- Copy all rows of data from table DbFileSpaceStatsSnapshot to table DbFileSpaceStatsHistory
-- adding in a Date/Time column value.
---------------------------------------------------------------------------------------------
Insert Into DbFileSpaceStatsHistory
	Select @CurrDtTm As DtTm,
		DataOrLogFile,
		LogicalFileName,
		[FileName],
		SizeMb,
		UsedSpaceMb,
		Drive,
		FreeDriveSpaceMb
From DbFileSpaceStatsSnapshot

-------------------------------------------------------------------------
-- For each database file entry in DbFileSpaceStatsSnapshot
--   Log a "DB File Space Stats" event to the PharmASSIST EvtEvent table.
-------------------------------------------------------------------------
-- This allows the collection of these stats to Innovation Associates.
--
Declare csrDataAndLogFileSpaceStats Cursor For
	Select DataOrLogFile,
		LogicalFileName,
		[FileName],
		SizeMb,
		UsedSpaceMb,
		Drive,
		FreeDriveSpaceMb
	From DbFileSpaceStatsSnapshot
	Order By DataOrLogFile   -- 'D's before 'L's
For Read Only

Declare @DataOrLogFile Char(1)
Declare @LogicalFileName VarChar(256)
Declare @FileName VarChar(520)
Declare @FileSizeMb Real
Declare @UsedSpaceMb Real
Declare @Drive Char(1)
Declare @FreeDriveSpaceMb Real

Open csrDataAndLogFileSpaceStats

Fetch Next From csrDataAndLogFileSpaceStats
  Into @DataOrLogFile, 
	@LogicalFileName, 
	@FileName, 
	@FileSizeMb, 
	@UsedSpaceMb, 
	@Drive, 
	@FreeDriveSpaceMb

While @@Fetch_Status = 0
	Begin
		Declare @Notes VarChar(255)

		Set @Notes = Left(@DataOrLogFile + ',' +
			@LogicalFileName + ',' +
			Cast(Cast(@FileSizeMb As Decimal(14,0)) As VarChar(12)) + ',' +
			Cast(Cast(@UsedSpaceMb As Decimal(14,0)) As VarChar(12)) + ',' +
			@Drive + ',' +
			Cast(Cast(@FreeDriveSpaceMb As Decimal(14,0)) As VarChar(12))
			,50)

		Exec lsp_DbLogSqlEvent
			'D', 'DB File Space Stats', @FileName, @Notes, 'lsp_DbLogDbFileSpaceStats'

		Fetch Next From csrDataAndLogFileSpaceStats
			Into @DataOrLogFile, 
			@LogicalFileName, 
			@FileName, 
			@FileSizeMb, 
			@UsedSpaceMb, 
			@Drive, 
			@FreeDriveSpaceMb
	End

Close csrDataAndLogFileSpaceStats
Deallocate csrDataAndLogFileSpaceStats
