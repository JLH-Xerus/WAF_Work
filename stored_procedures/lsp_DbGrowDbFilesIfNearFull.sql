ALTER PROCEDURE "dbo"."lsp_DbGrowDbFilesIfNearFull" 
As
---------------------------------------------------------
-- Proactively Grow the PharmASSIST System Database Files
-- if they are becoming full.
---------------------------------------------------------
-- Notes:
--   This proactive growth is performed during off-shift, nightly maintenance time hours
--   in order to prevent/limit database auto-growths during prime-shift.  Growing the database
--   can be time consuming, causing long pauses in PharmASSIST response times.  On some systems,
--   the pauses exceeded 30 seconds.  This causes the PharmASSIST queries (that initiate the auto-growths)
--   to time out at 30 seconds and roll back the auto-grow operation.
--
--   This stored procedure should be called prior to the backup of the PharmASSIST system database
--   within the nightly maintenance so that the transaction log file's used space will be interrogated
--   at its "fattest" point.
--

Set NoCount On

-----------------------------------------------------
-- Refresh the data in table DbFileSpaceStatsSnapshot
-- with current database file space statistcics.
-----------------------------------------------------
Exec lsp_DbPopulateTableDbFileSpaceStatsSnapshot

-----------------------------------------------------------
-- For each database file entry in DbFileSpaceStatsSnapshot
--   If the free space within the file has become "small"
--     Grow the file.
-----------------------------------------------------------
Declare csrDataAndLogFileSpaceStats Cursor
For
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

Declare @GrowToSizeMb Int

Open csrDataAndLogFileSpaceStats

Fetch Next From csrDataAndLogFileSpaceStats
  Into @DataOrLogFile, @LogicalFileName, @FileName, @FileSizeMb, @UsedSpaceMb, @Drive, @FreeDriveSpaceMb

While @@Fetch_Status = 0
  Begin
    Set @GrowToSizeMb = 0    -- Will be set to > 0 if database file is to be grown.
    
    ----------------------------------------------------------------------------------------------------
    -- If the file's free space has dropped below 1GB and the file's drive has 2GB or more of free space
    --   Grow the file's size by 1GB.
    ----------------------------------------------------------------------------------------------------
    If @FileSizeMb - @UsedSpaceMb < 1000 And @FreeDriveSpaceMb >= 2000
      Set @GrowToSizeMb = Round(@FileSizeMb, 0) + 1000

    Else
      ----------------------------------------------------------------------------------------------------------
      -- Else:
      -- If the file's free space has dropped below 250MB and the file's drive has more than 250MB of free space
      --   Grow the file's size by 250MB.
		-- Schema 7.2.29: Changed the autogrow size of the database's size from 10%
      ----------------------------------------------------------------------------------------------------------
      If @FileSizeMb - @UsedSpaceMb < 250 And @FreeDriveSpaceMb > 250
        Set @GrowToSizeMb = Round(@FileSizeMb, 0) + 250

      Else
        --------------------------------------------------------------------------------------------------------
        -- Else:
        -- If the file's free space has dropped below 250MB and the file's drive has more than 1MB of free space
        --   Grow the file's size by the remaining drive free space (minus 1MB for safety).
		  -- Schema 7.2.29: Changed the autogrow size of the database's size from 10%
        --------------------------------------------------------------------------------------------------------
        If @FileSizeMb - @UsedSpaceMb < 250 And @FreeDriveSpaceMb > 1
          Set @GrowToSizeMb = Round(@FileSizeMb, 0) + (Round(@FreeDriveSpaceMb, 0) - 1)

    --------------------------------------------------
    -- Perform the actual grow if grow size set above.
    --------------------------------------------------
    If @GrowToSizeMb > 0
      Exec lsp_DbGrowDbFile @LogicalFileName, @GrowToSizeMb

    Fetch Next From csrDataAndLogFileSpaceStats
      Into @DataOrLogFile, @LogicalFileName, @FileName, @FileSizeMb, @UsedSpaceMb, @Drive, @FreeDriveSpaceMb
  End

Close csrDataAndLogFileSpaceStats
Deallocate csrDataAndLogFileSpaceStats
