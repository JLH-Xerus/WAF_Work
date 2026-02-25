ALTER PROCEDURE "dbo"."lsp_DbRebuildFragmentedIndexes" 
	@FragThresholdPct Int = 20,
	@Online Bit = 0,
	@MaxDop SmallInt = 0
As
----------------------------------------------------------------------------------------------------------
-- Version:  4
--
-- Rebuild All Indexes in the Current Database that are Fragmented above a Specified Percentage Threshold.
----------------------------------------------------------------------------------------------------------
Set NoCount On

Declare @TableObjId Int
Declare @IndexId Int

Declare @SchemaName VarChar(128)
Declare @TableName VarChar(128)
Declare @IndexName VarChar(128)
Declare @AvgFragPct Decimal
Declare @PageCount Int

Declare @SqlCmd NVarChar(1024)
Declare @StartTm DateTime
Declare @EndTm DateTime

Declare @OnlineHintValue VarChar(3)    -- 'On' or 'Off'.

----------------------------------------------------------------------------------------
-- Create a Cursor of Table Indexes that are Fragmented beyond the Threshold Percentage.
----------------------------------------------------------------------------------------
Declare curFragIdxs Insensitive Cursor
	For
		Select
			Object_Schema_Name(S.Object_Id) As SchemaName,
			Object_Name(S.Object_Id) As TableName,
			I.Name As IndexName,
			S.Avg_Fragmentation_In_Percent,
			S.Page_Count
		From
			Sys.Dm_Db_Index_Physical_Stats(Db_Id(), Null, Null, Null, Null) As S
				Join
			Sys.Indexes As I
				On I.Object_Id = S.Object_Id And I.Index_Id = S.Index_Id
		Where
			S.Avg_Fragmentation_In_Percent > @FragThresholdPct
			And
			S.Index_Id > 0    -- Exclude Heaps (Tables with no Cluster Index)
		Order By
			S.Avg_Fragmentation_In_Percent Desc
For Read Only

--------------------------------------
-- Walk through each Fragmented Index.
-- For each:
--   Rebuild the Index.
--------------------------------------
Open curFragIdxs

Fetch Next From curFragIdxs Into @SchemaName, @TableName, @IndexName, @AvgFragPct, @PageCount

While @@Fetch_Status = 0
	Begin
		
		Print 'Rebuilding Index ' + @SchemaName + '.' + @TableName + ', ' + @IndexName + ' (AvgFragPct=' + Cast(@AvgFragPct As VarChar(30)) + ').'

		-- - - - - - - - - - - - - - - - - - - - - - - - - -
		-- Do not rebuild the primary key of table EvtEvent.
		-- - - - - - - - - - - - - - - - - - - - - - - - - -
		-- This index does not get fragmented as the table is inserted to with a climbing Identity TrxId (PK).
		-- However, in the earliest fielded Symphony HV site, the conversion to a revised table having a BigInt TrxId included
		--   the process of moving data from former table, EvtEvent_IntId, to this table caused its primary key / clustered index to become fragmented.
		-- Defragmenting this very large table takes many hours.
		--
		If @TableName = 'EvtEvent' And @IndexName = 'PK_EvtEvent'
			Print 'Bypassing index ' + @TableName + '.' + @IndexName

		Else  -- Index is not the EvtEvent table's primary key.
			Begin

				If @Online = 1
					Set @OnlineHintValue = 'On'
				Else
					Set @OnlineHintValue = 'Off'

				Set @SqlCmd = 'Alter Index [' + @IndexName +'] On [' + @SchemaName + '].[' + @TableName + '] Rebuild With (Online = ' + @OnlineHintValue + ', MaxDop = ' + Cast(@MaxDop As VarChar(5)) + ')'

				Set @StartTm = GetDate()

				Print @SqlCmd

				Begin Try
					Execute (@SqlCmd)
				End Try

				Begin Catch
					--
					-- If the rebuild of the index fails because the Online option was specified, but the index contains a column type that disallows the Online option
					--   Switch to using the Offline option.
					--
					If @OnlineHintValue = 'On'
						If Error_Number() = 2725 Or Error_Message() Like '%Online index operation cannot be performed%'
							Begin
								Print 'Index cannot be rebuilt with Online option.  Switching to Offline option.'
								Set @OnlineHintValue = 'Off'
								Set @SqlCmd = 'Alter Index [' + @IndexName +'] On [' + @SchemaName + '].[' + @TableName + '] Rebuild With (Online = ' + @OnlineHintValue + ', MaxDop = ' + Cast(@MaxDop As VarChar(5)) + ')'
								Print @SqlCmd
								Execute (@SqlCmd)
							End
				End Catch

				Set @EndTm = GetDate()

				Print 'Run Time (Minutes):  ' + Cast(DateDiff(Minute, @StartTm, @EndTm) As VarChar(12))

			End  -- Index is not the EvtEvent table's primary key.

		Fetch Next From curFragIdxs Into @SchemaName, @TableName, @IndexName, @AvgFragPct, @PageCount

	End

Close curFragIdxs
Deallocate curFragIdxs
