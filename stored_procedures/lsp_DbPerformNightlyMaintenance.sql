ALTER PROCEDURE "dbo"."lsp_DbPerformNightlyMaintenance" 
     @CheckAndBackupMasterAndMsdbDbs TinyInt = 0
   , @GrowPaDbFilesIfNearFull TinyInt = 0
   , @RebuildPaDbFragmentedIndexes TinyInt = 1
   , @CheckAndBackupPaDb TinyInt = 1
   , @CopyPaDbBackupFileToRemoteRemoteFolder TinyInt = 0
   , @CopyMasterAndMsdbDbBackupFilesToRemoteRemoteFolder TinyInt = 0
   , @RebuildIndexesFragThresholdPct TinyInt = 20
   , @RebuildIndexesOnline Bit = 0
   , @RebuildIndexesMaxDop SmallInt = 0
   , @CompressBackupFiles Bit = 0
   , @DeleteRxTextDocsOlderThanXDays Int = 0
   , @DeleteRxTextDocsOfTypeCodes VarChar(30) = '*'
   , @MaxRxTextDocsToDelete Int = 50000
   , @DeleteScriptImagesOlderThanXDays Int = 0
   , @MaxScriptImagesToDelete Int = 50000
   , @DeleteDocImagesOlderThanXDays Int = 0
   , @MaxDocImagesToDelete Int = 50000
   , @DeleteWorkflowImagesOlderThanXDays Int = 0
   , @DeleteWorkflowImagesOfTypeCodes VarChar(30) = '*'
   , @MaxWorkflowImagesToDelete Int = 50000
   , @DeleteEventsOlderThanXDays Int = 0
   , @DeleteEventsNumOfRowsBlockSize Int = 100000
   , @MaxEventsToDelete Int = 25000000
   , @DeleteRxsAndRelatedDataOlderThanXDays Int = 0
   , @DeleteRxsAndRelatedDataNumOfRowsBlockSize Int = 100000
   , @MaxRxsAndRelatedDataToDelete Int = 25000000
   , @UseCopyOnlyOptionForBackups Bit = 0
   , @UpdateCentralFillClosedDates Bit = 1
   , @FullDBCCCheckOn nVarChar(20) = ''
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_DbPerformNightlyMaintenance

Description:
   Perform nightly maintenance on the Symphony system database.

Version:
   26

Parameters:
   Input:
      @CheckAndBackupMasterAndMsdbDbs
         - Whether or not to check the integrity of the master and msdb databases and, if clean, backup these system databases.
           1 = Perform this action.
           0 = Do not perform this action.
      
      @GrowPaDbFilesIfNearFull
         - Whether of not to grow each database/log (.mdf, .ndf, .ldf) file if it is becoming full.
           1 = Perform this action.
           0 = Do not perform this action.
      The primary rule that is followed is:
         If the file's free space has dropped below 1GB and the file's drive has more than 2GB of free space, grow the file's size by 1GB.
      This proactive growth is performed during the off-shift, nightly maintenance in order to prevent/limit database auto-growths during prime-shift.
      Growing the database can be time consuming, causing long pauses in the NEXiA application.
      
      @RebuildPaDbFragmentedIndexes 
         - Whether or not to rebuild fragmented table indexes.
           1 = Perform this action.
           0 = Do not perform this action.
      
      @CheckAndBackupPaDb
         - Whether or not check the integrity of the Symphony system database and, if clean, backup this database.
           1 = Perform this action.
           0 = Do not perform this action.

      @CopyPaDbBackupFileToRemoteRemoteFolder
         - This parameter and action are no longer used.  It requires the xp_CmdShell system stored procedure to be enabled.
           The folder, if any, to which to copy the Symphony system database backup file.
      
      @CopyMasterAndMsdbDbBackupFilesToRemoteRemoteFolder
         - This parameter and action are no longer used. It requires the xp_CmdShell system stored procedure to be enabled.
           The folder, if any, to which to copy the master and msdb system database backup files.
      
      @RebuildIndexesFragThresholdPct
         - The percentage threshold that controls which table indexes are rebuilt.
           Any table index fragmented at a percentage above this threshold is rebuilt.

      @RebuildIndexesOnline
         - Whether to rebuild fragmented indexes in Online or Offline mode.
           1 = Online.
           0 = Offline.

      @RebuildIndexesMaxDop
         - The maximum number of processors (degrees of parallelism) to use in rebuilding the table indexes.

      @CompressBackupFiles
         - Whether or not to compress the database backup files.
           1 = Compress.
           0 = Do not compress.

      @DeleteRxTextDocsOlderThanXDays
         - Whether or not to delete Rx "Text" Documents older than a specified number of days.
           >0 = The number of days old threshold beyond which older Rx Text Documents are deleted.
            0 = Do not delete Rx Text Documents.
         This deletes rows from table OeOrderTextDocument.  This table contains true "text" content Rx documents, 
         but can also contain some Base64 encoded Rx "non-text" content Rx documents (e.g.,PDF Rx Label, PCL Paperwork Document).

      @DeleteRxTextDocsOfTypeCodes
         - A list of Rx text document type codes defining which Rx Text Documents to delete.
           '*' = All types.
           Specify the list as a comma delimited string list of codes.  See reference table OeTextDocumentType for the complete list of type codes. Example:  'R,P,K,X'

      @MaxRxTextDocsToDelete
         - The maximum number of Rx Test Documents to delete (within a nightly run). 
           Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition where a huge number of "older than X days" rows exist when this action is enabled.
           The goal is to delete a "finite" amount of data between full database backups where the t-log is truncated.
      
      @DeleteScriptImagesOlderThanXDays
         - Whether or not to delete Rx Script Images older than a specified number of days.
           >0 = The number of days old threshold beyond which older Rx Script Images are deleted.
            0 = Do not delete Rx Script Images.
         This deletes rows from table OeScriptImage.
         This process does NOT delete rows for archived images (whose ArchivedDtTm value is set (non-null) and whose image data is, therefore, "0x").
      
      @MaxScriptImagesToDelete
         - The maximum number of Rx Script Images to delete (within a nightly run).
           Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition where a huge number of "older than X days" rows exist when this action is enabled.
           The goal is to delete a "finite" amount of data between full database backups where the t-log is truncated.
      
      @DeleteDocImagesOlderThanXDays
         - Whether or not to delete Document Images older than a specified number of days.
           >0 = The number of days old threshold beyond which older Document Images are deleted.
            0 = Do not delete Document Images.
           This deletes rows from table DscDocument.
      
      @MaxDocImagesToDelete
         - The maximum number of Document Images to delete (within a nightly run).
           Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition where a huge number of "older than X days" rows exist when this action is enabled.
           The goal is to delete a "finite" amount of data between full database backups where the t-log is truncated.
      
      @DeleteWorkflowImagesOlderThanXDays
         - Whether or not to delete Workflow Images meta data older than a specified number of days.
           Images files are saved in a shared folder with proper naming convention and this deletion of meta data does not affect the physical files.
           >0 = The number of days old threshold beyond which older Workflow Images are deleted.
            0 = Do not delete Workflow Images.
           This deletes rows from table ImgImage (and associated tables, ImgRxImgAssoc, ImgCanImgAssoc).
           Workflow images are images typically captured during the fill of Rxs/Orders.
           (Examples: Pill Counting Tray, Vial Contents, Rx Package, Vial Label, Rx Package Label, Rx Label Page.)
      
      @DeleteWorkflowImagesOfTypeCodes
         - A list of workflow image type codes defining which Workflow Images to delete.
           '*' = All types.
           Specify the list as a comma delimited string list of codes.  See reference table ImgContent for the complete list of type codes.
           Example:  '1,2,3,4,5,6,7'
      
      @MaxWorkflowImagesToDelete
         - The maximum number of Workflow Images to delete (within a nightly run).
           Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition 
           where a huge number of "older than X days" rows exist when this action is enabled.
           The goal is to delete a "finite" amount of data between full database backups where the t-log is truncated.
      
      @DeleteEventsOlderThanXDays
         - Whether or not to delete audit logged events (EvtEvent, EvtInvEventLog, SmartShelf.EvtEventLog) older than a specified number of days.
           >0 = The number of days old threshold beyond which older logged events are deleted.
            0 = Do not delete old logged events.
           This deletes rows from EvtEvent, EvtInvEventLog and SmartShelf.EvtEventLog tables.
      
      @DeleteEventsNumOfRowsBlockSize
         - The event (rows) will be deleted in blocks of this many rows at a time.
           Deletions are performed in blocks of rows to prevent performing one huge transaction that would inflate the tempdb 
           and the t-log (even more than it holding the deletes themselves) and perform slowly due to broad locking.
      
      @MaxEventsToDelete
         - The maximum number of old events to delete.
          If this value is not a multiple of @DeleteEventsNumOfRowsBlockSize, the effect will be approximate.
          Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition where a huge
          number of "older than X days" rows exist when this action is performed.
          The goal is to delete a "finite" amount of data between full database backups where the t-log is truncated.
      
      @DeleteRxsAndRelatedDataOlderThanXDays
         - Whether or not to delete Rxs (and their related data) older than a specified number of days.
           >0 = The number of days old threshold beyond which older Rxs (and their related data) are deleted.
            0 = Do not delete old Rxs (and their related data).
           This deletes rows from table OeOrderHistory and many, many other tables, including Rx-related data tables
           as well as other object tables propagated to for the deleted Rxs (e.g., Rx group & shipment tables).
      
      @DeleteRxsAndRelatedDataNumOfRowsBlockSize
         - The Rxs (rows) (and their related data) will be deleted in blocks of this many history Rx rows at a time.
           Deletions are performed in blocks of rows to prevent performing one huge transaction that would inflate the tempdb
           and the t-log (even more than it holding the deletes themselves) and perform slowly due to broad locking.
      
      @MaxRxsAndRelatedDataToDelete
         - The maximum number of old Rxs (and their related data) to delete.
           If this value is not a multiple of @DeleteRxsAndRelatedDataNumOfRowsBlockSize, the effect will be approximate.
           Specifying a maximum prevents growing the t-log so large it uses up the disk space under a condition where a huge
           number of "older than X days" rows exist when this action is performed.
           The goal is to delete a "finite" amount of data between full database backups where the t-log is truncated.
      
      @UseCopyOnlyOptionForBackups
         - Whether to make the full backups as standard backups or copy-only backups.
           0 = Perform standard full backups.
           1 = Use the Copy-Only option for the database backups.
     
      @UpdateCentralFillClosedDates
         - Whether or not to execute the set/update closed dates for the Fulfillment facility.
           0 = Skip executing lsp_SysMonthlyUpdateCentralFillClosedDates stored procedure
           1 = Execute lsp_SysMonthlyUpdateCentralFillClosedDates stored procedure
   
   Output:
      None

Return Value:
   @Rc - 0 = Successful; Non-0 = Failed

Comments:
   Stored procedure "sp_ExecuteSql" is used to run most of the steps within this procedure so that the failure of a particular step does not
     prevent subsequent steps from being executed.  When sp_ExecuteSql executes a string, it is executed as its own self-contained batch.
   SQL Server compiles the T-SQL statement(s) in the string into an execution plan that is separate from the execution plan of the batch that
     contains the sp_executesql statement.
   When a SQL command is executed directly within a batch:
     - If it contains any type of compile error, the entire batch is not be executed.
     - If it contains some semantic error, the remainder of the batch might not be executed.
*/
Declare @CurrStepName varchar(100)          -- Current step name (e.g., 'Backup master').
Declare @Outcome varchar(50)                -- Overall Job Outcome:  'Succeeded' or 'Failed'.
Declare @FailingStepNamesList varchar(4000) -- Failing step as list (e.g., 'Check master;Backup master;').
Declare @ThisDbName varchar(50)             -- The name of "this" database.  (The database context within which this stored procedure is running.)
Declare @BuDevicePathAndFileName varchar(260)
                                            -- The path and file name of "this" database's backup device (.bak) file.
Declare @Parm1 varchar(400)                  -- Stored procedure parameter 1 (VarChar) parameter.
Declare @Parm2 varchar(400)                  -- Stored procedure parameter 2 (VarChar) parameter.
Declare @SqlCmd nVarChar(4000)              -- An SQL Command to be executed.
Declare @Rc int                             -- SQL Command Return Code.  (Saved Value of @@Error.)

--**************************************************************************************************
-- Initialize Variables.
--**************************************************************************************************
Set @Outcome = 'Succeeded'     -- Initialize as 'Succeeded'; Set to 'Failed' if "critical" step fails below.
Set @FailingStepNamesList = ''

Set @ThisDbName = Db_Name()


--**************************************************************************************************
-- Log Nightly Job "Last Run Date/Time" to SysProperty table.
--**************************************************************************************************
Set @Parm1 = 'JobNightly_LastRunDtTm'
Set @Parm2 = Convert(varchar(19), GetDate(), 120)
Exec lsp_DbInsertOrUpdateSysProperty @Parm1, @Parm2

--********
-- Step 01
--**************************************************************************************************
-- Check Integrity of master Database.
--**************************************************************************************************
Set @CurrStepName = 'Check master'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @CheckAndBackupMasterAndMsdbDbs = 0
  Begin
    Print 'Bypassing step.  Step not configured to run.'
    Goto SkipBackupOfMasterDb
  End

Set @SqlCmd = 'Dbcc CheckDb (''master'')'

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @Outcome = 'Failed'                      -- This is a "critical" step.  Flag overall outcome as Failed.
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
    Goto SkipBackupOfMasterDb
  End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--********
-- Step 02
--**************************************************************************************************
-- Backup master Database.
--**************************************************************************************************
Set @CurrStepName = 'Backup master'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

Set @SqlCmd = FormatMessage('Backup Database master To MasterBk With Format, Stats = 10, %s%s',
                            IIf(@CompressBackupFiles = 1, 'Compression', 'No_Compression'),
                            IIf(@UseCopyOnlyOptionForBackups = 1, ', Copy_Only', '')
                           )

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @Outcome = 'Failed'                      -- This is a "critical" step.  Flag overall outcome as Failed.
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipBackupOfMasterDb:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--********
-- Step 03
--**************************************************************************************************
-- Check Integrity of msdb Database.
--**************************************************************************************************
Set @CurrStepName = 'Check msdb'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @CheckAndBackupMasterAndMsdbDbs = 0
  Begin
    Print 'Bypassing step.  Step not configured to run.'
    Goto SkipBackupOfMsdbDb
  End

Set @SqlCmd = 'Dbcc CheckDb (''msdb'')'

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @Outcome = 'Failed'                      -- This is a "critical" step.  Flag overall outcome as Failed.
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
    Goto SkipBackupOfMsdbDb
  End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--********
-- Step 04
--**************************************************************************************************
-- Backup msdb Database.
--**************************************************************************************************
Set @CurrStepName = 'Backup msdb'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

Set @SqlCmd = FormatMessage('Backup Database msdb To MsdbBk With Format, Stats = 10, %s%s',
                            IIf(@CompressBackupFiles = 1, 'Compression', 'No_Compression'),
                            IIf(@UseCopyOnlyOptionForBackups = 1, ', Copy_Only', '')
                           )

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @Outcome = 'Failed'                      -- This is a "critical" step.  Flag overall outcome as Failed.
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipBackupOfMsdbDb:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--********
-- Step 05
--**************************************************************************************************
-- Perform Daily Purge of Temporary Data.
--**************************************************************************************************

------------------------------
-- Rx Label Print Stream Data.
------------------------------
Set @CurrStepName = 'Purge Temp Data (Rx Label Print Streams)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--
-- Delete Rx Label Page Print Streams that are more than 24 hours old
-- (and that do not still have an active Rx).
--

Set @SqlCmd =
  'Delete' + Char(13) +
  'From OrdLabelPrintStream' + Char(13) +
  'Where ReceivedDtTm < DateAdd(Hour, -24, GetDate())' + Char(13) +
  '  And OrderId Not In (Select OrderId From OeOrder With (NoLock))'

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

--------------------------------------------
-- Shipment Package Label Print Stream Data.
--------------------------------------------
Set @CurrStepName = 'Purge Temp Data (Shipment Pkg Label Print Streams)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--
-- Delete Shipment Package Label Print Streams that are more than 7 days old
-- (and that do not still have a shipment with active Rxs).
--

Set @SqlCmd =
  'Delete' + Char(13) +
  'From' + Char(13) +
  '  ShpShipmentPkgLabelData' + Char(13) +
  'Where' + Char(13) +
  '  ReceivedDtTm < DateAdd(Day, -7, GetDate())           -- Label Data is older than 7 days.' + Char(13) +
  '  And' + Char(13) +
  '  ShipmentId Not In                                    -- Label Data has no "Active" Shipment.' + Char(13) +
  '    (' + Char(13) +
  '      --' + Char(13) +
  '      -- Shipment IDs of Old Shipment Package Label Data rows' + Char(13) +
  '      --   that still have a Shipment with Active Rxs.' + Char(13) +
  '      --' + Char(13) +
  '      Select' + Char(13) +
  '        L.ShipmentId' + Char(13) +
  '      From' + Char(13) +
  '        ShpShipmentPkgLabelData As L With (NoLock)' + Char(13) +
  '          Join' + Char(13) +
  '        OeOrderShipmentAssoc As A With (NoLock)' + Char(13) +
  '          On L.ShipmentId = A.ShipmentId' + Char(13) +
  '      Where' + Char(13) +
  '        L.ReceivedDtTm < DateAdd(Day, -7, GetDate())   -- Label Data is older than 7 days.' + Char(13) +
  '        And' + Char(13) +
  '        A.HistoryDtTm = ''12/31/9999''                   -- Shipment still has Active Rx.' + Char(13) +
  '    )'

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

----------------------------------------------
-- TCD Controller Communication Log Dump Data.
----------------------------------------------
Set @CurrStepName = 'Purge Temp Data (TCD Controller Communication Log Dump Data)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--
-- Delete TCD Controller Communication Log Dump Data that is more than 90 days old.
--

Set @SqlCmd =
   'Delete From TcdCommDumpLog Where DumpDtTm < DateAdd(Day, -90, GetDate())'

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

----------------------------
-- System Action Queue Data.
----------------------------
Set @CurrStepName = 'Purge System Action Queue Entries'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--
-- Delete System Action Queue entries that are more than 7 days old.
--

Set @SqlCmd =
  'Delete' + Char(13) +
  'From SysActionQueue' + Char(13) +
  'Where ActionDtTm < DateAdd(Day, -7, GetDate())'

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

----------------------------
-- System Is Active Data.
----------------------------
Set @CurrStepName = 'Purge Old "System Is Active" Entries from SysSytemIsActiveLog table '

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'


--
-- Delete System Is Active entries that are more than 90 days old.
--

Set @SqlCmd =
  'Delete' + Char(13) +
  'From SysSystemIsActiveTimeLog' + Char(13) +
  'Where EndDtTm < DateAdd(Day, -90, GetDate())'

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As VarChar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

----------------------------
-- Rx Route Data.
----------------------------
Set @CurrStepName = 'Delete Rx Route table rows that are older than 7 days and whose Rx is no longer active'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

-- Performing these deletes where no active Rx exist rather than where the HistoryDtTm is not active
--   cares for an exceptional case where a CvyRxRoute row is left "floating", associated to no Rx 
--   (active or history) at all.
--

Set @SqlCmd =
   'Delete' + Char(13) +
   '  R' + Char(13) +
   'From' + Char(13) +
   '  CvyRxRoute As R' + Char(13) +
   '    Left Join' + Char(13) +
   '  OeOrder As O' + Char(13) +
   '    On R.OrderId = O.OrderId And R.HistoryDtTm = O.HistoryDtTm' + Char(13) +
   'Where' + Char(13) +
   'O.OrderId Is Null' + Char(13) +            -- Include Rx routes having no active Rx.
   '  And' + Char(13) +
   '  R.LastLocChgDtTm < DateAdd(Day, -7, GetDate())'  -- Include Rx routes older than 7 days.

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As VarChar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

----------------------------
-- System Stat History Data.
----------------------------
Set @CurrStepName = 'Purge Old "System Stat History" Entries'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--
-- Delete System Stat History entries that are more than 90 days old.
--

Set @SqlCmd =
  'Delete' + Char(13) +
  'From SysStatHistory' + Char(13) +
  'Where DtTm < DateAdd(Day, -90, GetDate())'

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As VarChar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

------------------------------
-- (Diagnostic) Tote Log Data.
------------------------------
Set @CurrStepName = 'Purge Old "Tote Log" Entries'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--
-- Delete Tote Log entries that are more than 90 days old.
--

Set @SqlCmd = 'Delete From ToteLog Where EventDtTm < DateAdd(Day, -90, GetDate())'

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As VarChar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

------------------------------------------
-- Delete expired InvWkstnNdcLotCode Data.
------------------------------------------
Set @CurrStepName = 'Delete InvWkstnNdcLotCode table rows whose ExpireDate has passed'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(VarChar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

Set @SqlCmd =
   'Delete From dbo.InvWkstnNdcLotCode ' + Char(13) +
   'Where ExpireDate <= GetDate()'

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As VarChar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--**********
-- Step 05.1
--**************************************************************************************************
-- Perform Daily Manipulation of Data.
--**************************************************************************************************

----------------------------------
-- Maintain Pharmacy Closed Dates.
----------------------------------
Set @CurrStepName = 'Maintain Pharmacy Closed Dates'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @UpdateCentralFillClosedDates = 0
Begin
   Print 'Bypassing step.  Step not configured to run.'
   Goto SkipUpdatingFillColsedDates
End

Set @SqlCmd =
  'Exec lsp_SysMonthlyUpdateCentralFillClosedDates'

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipUpdatingFillColsedDates:
RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--**********
-- Step 05.2
--**************************************************************************************************
-- Delete Old Errors & Diagnostic Events.
--**************************************************************************************************

---------------------
-- Delete Old Errors.
---------------------
Set @CurrStepName = 'Delete Old Errors'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--
-- Delete Errors that are more than X days old
-- (where X = Value from SysProperty table Keyword row "DelDiagDataOlderThanXDays"
--  and defaults to 90 if no row exists).
--
Select @Parm1 = Value From SysProperty Where Keyword = 'DelDiagDataOlderThanXDays'

If IsNumeric(@Parm1) = 0
  Set @Parm1 = '90'

Set @Parm1 = Convert(varchar(10), DateAdd(Day, -Cast(@Parm1 As int), GetDate()), 101)

Set @SqlCmd = 'Exec lsp_DbDeleteOldErrors ''' + @Parm1 + ''''

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

--------------------------------
-- Delete Old Diagnostic Events.
--------------------------------
Set @CurrStepName = 'Delete Old Diagnostic Events'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--
-- Delete Diagnostic Events that are more than X days old
-- (where X = Value from SysProperty table Keyword row "DelDiagDataOlderThanXDays"
--  and defaults to 90 if no row exists).
--
Select @Parm1 = Value From SysProperty Where Keyword = 'DelDiagDataOlderThanXDays'

If IsNumeric(@Parm1) = 0
  Set @Parm1 = '90'

Set @Parm1 = Convert(varchar(10), DateAdd(Day, -Cast(@Parm1 As int), GetDate()), 101)

Set @SqlCmd = 'Exec lsp_DbDeleteOldDiagEvents ''' + @Parm1 + ''''

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

-------------------------------
-- Delete Old SmartShelf Events. (Use Param1 from Old Diagnostic events clear)
-------------------------------
Set @CurrStepName = 'Delete Old SmartShelf Events'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--
-- Delete Lightway Events that are more than X days old
-- (where X = Value from SysProperty table Keyword row "DelDiagDataOlderThanXDays"
--  and defaults to 90 if no row exists).
--
Set @SqlCmd =
   'Exec lsp_SsDeleteOldEvents ''' + @Parm1 + ''''

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As varchar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence.
--**********
-- Step 05.3
--**************************************************************************************************
-- Delete Old Data (based on input parameters).
--**************************************************************************************************

--------------------------------
-- Delete Old Rx Text Documents.  (Table:  OeOrderTextDocument)
--------------------------------
Set @CurrStepName = 'Delete Old Rx Text Documents (OeOrderTextDocument)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @DeleteRxTextDocsOlderThanXDays <= 0
   Begin
      Print 'Bypassing step.  Step not configured to run.'
      Goto SkipDeleteRxTextDocuments
   End

Set @SqlCmd = FormatMessage('Exec lsp_DbDeleteOldRxTextDocuments @OlderThanXDays = %i, @DocsOfTypeCodes = ''%s'', @MaxToDelete = %i',
                            @DeleteRxTextDocsOlderThanXDays, @DeleteRxTextDocsOfTypeCodes, @MaxRxTextDocsToDelete)

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipDeleteRxTextDocuments:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
----------------------------
-- Delete Old Script Images.  (Table:  OeScriptImage)
----------------------------
Set @CurrStepName = 'Delete Old Script Images (OeScriptImage)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @DeleteScriptImagesOlderThanXDays <= 0
   Begin
      Print 'Bypassing step.  Step not configured to run.'
      Goto SkipDeleteScriptImages
   End

Set @SqlCmd = FormatMessage('Exec lsp_DbDeleteOldScriptImages @OlderThanXDays = %i, @MaxToDelete = %i',
                            @DeleteScriptImagesOlderThanXDays, @MaxScriptImagesToDelete)

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipDeleteScriptImages:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
------------------------------
-- Delete Old Document Images.  (Tables:  DscDocument, DscPatientDocAssoc, DscRxDocAssoc)
------------------------------
Set @CurrStepName = 'Delete Old Document Images (DscDocument, DscPatientDocAssoc, DscRxDocAssoc)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @DeleteDocImagesOlderThanXDays <= 0
   Begin
      Print 'Bypassing step.  Step not configured to run.'
      Goto SkipDeleteDocumentImages
   End

Set @SqlCmd = FormatMessage('Exec lsp_DbDeleteOldDocumentImages @OlderThanXDays = %i, @MaxToDelete = %i',
                            @DeleteDocImagesOlderThanXDays, @MaxDocImagesToDelete)

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipDeleteDocumentImages:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

------------------------------
-- Delete Old Workflow Images.  (Tables:  ImgImage, ImgCanImgAssoc, ImgRxImgAssoc)
------------------------------
Set @CurrStepName = 'Delete Old Workflow Images (ImgImage, ImgCanImgAssoc, ImgRxImgAssoc)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'


If @DeleteWorkflowImagesOlderThanXDays <= 0
   Begin
      Print 'Bypassing step.  Step not configured to run.'
      Goto SkipDeleteWorkflowImages
   End

Set @SqlCmd = FormatMessage('Exec lsp_DbDeleteOldWorkflowImages @OlderThanXDays = %i, @ImgsOfContentCodes = ''%s'', @MaxToDelete = %i',
                            @DeleteWorkflowImagesOlderThanXDays, @DeleteWorkflowImagesOfTypeCodes, @MaxWorkflowImagesToDelete)

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipDeleteWorkflowImages:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

---------------------------
-- Delete Old Audit Events.  (Table:  EvtEvent)
---------------------------
Set @CurrStepName = 'Delete Old Audit Events (EvtEvent)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @DeleteEventsOlderThanXDays <= 0
   Begin
      Print 'Bypassing step.  Step not configured to run.'
      Goto SkipDeleteAuditEvents
   End

Set @SqlCmd = FormatMessage('Exec lsp_DbDeleteOldEvents @OlderThanXDays = %i, @NumOfRowsBlockSize = %i, @MaxToDelete = %i',
                            @DeleteEventsOlderThanXDays, @DeleteEventsNumOfRowsBlockSize, @MaxEventsToDelete)

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As varchar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

SkipDeleteAuditEvents:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

-------------------------------------
-- Delete Old Inventory Audit Events.  (Table:  EvtInvEventLog)
-------------------------------------
Set @CurrStepName = 'Delete Old Inventory Audit Events (EvtInvEventLog)'
Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

Declare @InvEventLogRetentionPeriod Int

--Default inventory event log retention period is 90 days.
Set @InvEventLogRetentionPeriod = Case When @DeleteEventsOlderThanXDays <= 0 Then 90
                                  Else @DeleteEventsOlderThanXDays End

Set @SqlCmd = FormatMessage('Exec lsp_DbDeleteOldInvAuditEvents @OlderThanXDays = %i, @NumOfRowsBlockSize = %i, @MaxToDelete = %i',
                            @InvEventLogRetentionPeriod, @DeleteEventsNumOfRowsBlockSize, @MaxEventsToDelete)

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As varchar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

SkipDeleteInventoryAuditEvents:


RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

----------------------
-- Delete Old Rx Data.  (Table:  OeOrderHistory + Many other Rx and propagated-to other-object tables)
----------------------
Set @CurrStepName = 'Delete Old Rx Data (OeOrderHistory + Many other Rx and propagated-to other-object tables)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @DeleteRxsAndRelatedDataOlderThanXDays <= 0
   Begin
      Print 'Bypassing step.  Step not configured to run.'
      Goto SkipDeleteRxData
   End

Set @SqlCmd = FormatMessage('Exec lsp_DbDeleteOldRxData @OlderThanXDays = %i, @NumOfRowsBlockSize = %i, @MaxToDelete = %i',
                            @DeleteRxsAndRelatedDataOlderThanXDays, @DeleteRxsAndRelatedDataNumOfRowsBlockSize, @MaxRxsAndRelatedDataToDelete)

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As varchar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

SkipDeleteRxData:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

-----------------------
-- Delete Old PMSS Data.  (Table:  CfCapturedPmssTrxs, CfCollectedCentralFillPmssTrxs, PmssCapturedOrderTrxs, PmssCapturedXmlTrxs tables)
-----------------------
Set @CurrStepName = 'Delete Old PMSS Data (4 PMSS Tables)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

Set @SqlCmd = FormatMessage('Exec lsp_DbDeleteOldPmssData @NumOfRowsBlockSize = %i, @MaxToDelete = %i',
                            @DeleteRxsAndRelatedDataNumOfRowsBlockSize, @MaxRxsAndRelatedDataToDelete)

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As varchar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

-----------------------------------
-- Delete Old SmartShelf Error Data  (Table: SmartShelf.EvtErrorLog)
-----------------------------------
Set @CurrStepName = 'Delete Old SmartShelf Error Logs (SmartShelf.EvtErrorLog)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @DeleteEventsOlderThanXDays <= 0
   Begin
      Print 'Bypassing step.  Step not configured to run.'
      Goto SkipDeleteSmartShelfErrorLogs
   End

Set @SqlCmd = FormatMessage('Exec [SmartShelf].[lsp_DbDeleteOldSmartShelfErrors] @OlderThanXDays = %i, @NumOfRowsBlockSize = %i, @MaxToDelete = %i',
                            @DeleteEventsOlderThanXDays, @DeleteEventsNumOfRowsBlockSize, @MaxEventsToDelete)

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As varchar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

SkipDeleteSmartShelfErrorLogs:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 


-----------------------------------
-- Delete Old SmartShelf Event Data  (Table: SmartShelf.EvtEventLog)
-----------------------------------
Set @CurrStepName = 'Delete Old SmartShelf Event Logs (SmartShelf.EvtEventLog)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @DeleteEventsOlderThanXDays <= 0
   Begin
      Print 'Bypassing step.  Step not configured to run.'
      Goto SkipDeleteSmartShelfEventLogs
   End

Set @SqlCmd = FormatMessage('Exec [SmartShelf].[lsp_DbDeleteOldSmartShelfEvents] @OlderThanXDays = %i, @NumOfRowsBlockSize = %i, @MaxToDelete = %i',
                            @DeleteEventsOlderThanXDays, @DeleteEventsNumOfRowsBlockSize, @MaxEventsToDelete)

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As varchar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

SkipDeleteSmartShelfEventLogs:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 



------------------------------------
-- Delete Old Auto-Verification Data
------------------------------------
Set @CurrStepName = 'Delete Old Auto-Verification data (InvAliasCodeAutoVerifyRxDirections;InvNdcAutoVerifyRxDirections;InvProductAutoVerifyRxDirections)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @DeleteEventsOlderThanXDays <= 0
   Begin
      Print 'Bypassing step.  Step not configured to run.'
      Goto SkipDeleteOldAutoVerificationData
   End

Set @SqlCmd = FormatMessage('Exec [dbo].[lsp_DbDeleteOldAutoVerificationData] @OlderThanXDays = %i, @MaxToDelete = %i', @DeleteEventsOlderThanXDays, @MaxEventsToDelete)

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As varchar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End

SkipDeleteOldAutoVerificationData:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 


-------------------------
-- Delete Old API History (Table: EvtAPIHistoryEvent)
-------------------------
Set @CurrStepName = 'Delete Old API History (EvtAPIHistoryEvent)'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'


Delete
   EvtAPIHistoryEvent
Where
   ExpireDtTm < DateAdd(DAY, -1, GetDate())


Set @Rc = @@Error

If @Rc <> 0
   Begin
      Print '@@Error = ' + Cast(@Rc As varchar(12))
      Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
   End


RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 


--**********
-- Step 05.5
--**************************************************************************************************
-- Log Database File Space Statistics.
--**************************************************************************************************
Set @CurrStepName = 'Log File Space Stats'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

Set @SqlCmd = 'Exec lsp_DbLogDbFileSpaceStats'

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--**********
-- Step 05.6
--**************************************************************************************************
-- Grow Database Files If Near Full.
--**************************************************************************************************
Set @CurrStepName = 'Grow Database Files'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @GrowPaDbFilesIfNearFull = 0
  Begin
    Print 'Bypassing step.  Step not configured to run.'
    Goto SkipGrowPaDbFilesIfNearFull
  End

Set @SqlCmd =
  'Exec lsp_DbGrowDbFilesIfNearFull'

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipGrowPaDbFilesIfNearFull:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--********
-- Step 06
--**************************************************************************************************
-- Rebuild PharmAssist System Table Fragmented Indexes.
--**************************************************************************************************
Set @CurrStepName = 'Rebuild Fragmented Indexes'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @RebuildPaDbFragmentedIndexes = 0
  Begin
    Print 'Bypassing step.  Step not configured to run.'
    Goto SkipRebuildPaDbFragmentedIndexes
  End

Set @SqlCmd = 'Exec lsp_DbRebuildFragmentedIndexes' + Char(13) + Char(10) +
              Char(9) + '@FragThresholdPct = ' + Cast(@RebuildIndexesFragThresholdPct As VarChar(3)) + ', @Online = ' + Cast(@RebuildIndexesOnline As Char(1)) + ', @MaxDop = ' + Cast(@RebuildIndexesMaxDop As VarChar(3))

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipRebuildPaDbFragmentedIndexes:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--********
-- Step 07
--**************************************************************************************************
-- Check Integrity of this (NEXiA System) Database.
--**************************************************************************************************
Set @CurrStepName = 'Check ' + @ThisDbName

If @CheckAndBackupPaDb = 0
  Begin
    Print 'Bypassing step.  Step not configured to run.'
    Goto SkipBackupOfPaDbAndLogAndCopyOfBackupFilesToRemoteFolder
  End

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--Nightly job will perform full DBCC only on the configured day (default: Null/Empty String)
If IsNull(@FullDBCCCheckOn, '') = ''
      Or
   DateName(DW,GetDate()) = @FullDBCCCheckOn
  Begin
    Dbcc CheckDb   -- Check "this" database.  (No database parameter specified.)
                   -- This stored procedure will be run within the context of the NEXiA system database (whatever it is named).
  End
Else
  Begin
    Dbcc CheckDb With Physical_Only   -- Check "this" database with PHYSICAL_ONLY option on non-configured day to keep the run-time shorter
  End

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @Outcome = 'Failed'                      -- This is a "critical" step.  Flag overall outcome as Failed.
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
    Goto SkipBackupOfPaDbAndLogAndCopyOfBackupFilesToRemoteFolder
  End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--********
-- Step 08
--**************************************************************************************************
-- Backup this (NEXiA System) Database.
--**************************************************************************************************
Set @CurrStepName = 'Backup ' + @ThisDbName + ' DB'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

Set @SqlCmd = FormatMessage('Backup Database %s To %s With Format, Stats = 10, %s%s',
                            @ThisDbName, @ThisDbName + 'DataBk',
                            IIf(@CompressBackupFiles = 1, 'Compression', 'No_Compression'),
                            IIf(@UseCopyOnlyOptionForBackups = 1, ', Copy_Only', '')
                           )

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @Outcome = 'Failed'                      -- This is a "critical" step.  Flag overall outcome as Failed.
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
    Goto SkipBackupOfPaLog
  End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--********
-- Step 09
--**************************************************************************************************
-- Backup this (NEXiA System) Transaction Log.
--**************************************************************************************************
Set @CurrStepName = 'Backup ' + @ThisDbName + ' Log'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

If @UseCopyOnlyOptionForBackups = 1
  Begin
    Print 'Bypassing step.  Backup is set to be created as Copy-Only backup.'
    Goto SkipBackupOfPaLog
  End

If (Select recovery_model_desc From sys.databases Where name = @ThisDbName) = 'SIMPLE'
  Begin
    Print 'Bypassing step.  Database recovery model is set to Simple.'
    Goto SkipBackupOfPaLog
  End

Set @SqlCmd = 'Backup Log ' + @ThisDbName + ' To ' + @ThisDbName + 'LogBk With Format, Stats = 10'

Print @SqlCmd

Exec sp_ExecuteSql @SqlCmd

Set @Rc = @@Error

If @Rc <> 0
  Begin
    Print '@@Error = ' + Cast(@Rc As varchar(12))
    Set @Outcome = 'Failed'                      -- This is a "critical" step.  Flag overall outcome as Failed.
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipBackupOfPaLog:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 
--********
-- Step 10
--**************************************************************************************************
-- Copy this (NEXiA System) Database's backup file to a "remote" folder,
--   if a "copy to remote folder" is specified.
--**************************************************************************************************
Set @CurrStepName = 'Copy Backup Files to Remote Folder'

Print '--------------------------------------------------------------------------------'
Print @CurrStepName + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

Print '- - - - - - - - - - - - - - - - - - -'
Print 'NEXiA Database Backup Files...'
Print '- - - - - - - - - - - - - - - - - - -'

If @CopyPaDbBackupFileToRemoteRemoteFolder = 0
  Begin
    Print 'Bypassing step.  Step not configured to run.'
    Goto SkipCopyPaDbBackupFileToRemoteRemoteFolder
  End

--
-- Retrieve the configured "Backup File Copies Remote Folder", if any.
--
-- A 'BackupFileCopiesRemoteFolder' SysProperty table entry indicates that the copying of the
--   backup files to a remote folder is enabled.
--
Select @Parm1 = Value From SysProperty Where Keyword = 'JobNightly_CopyBackupFilesToRemoteFolder'

If @@RowCount = 0
  Begin
    Print '  - Not enabled.'
    Goto SkipCopyAllBackupFilesToRemoteFolder
  End

--
-- Retrieve a list of backup devices into a temporary table.
--
If Object_Id('TempDb..#Devices') Is Not Null
  Drop Table #Devices

Create Table #Devices                  -- Table Schema to match results of sp_HelpDevice.
   (  DeviceName SysName
    , PhysicalName nVarChar(260)
    , Description nVarChar(255)
    , Status int
    , CntrlType SmallInt
    , Size int
   )
Insert Into #Devices
  Execute('sp_HelpDevice')

---------------------------------------------
-- Copy this Symphony database's backup file.
---------------------------------------------
--
-- Retrieve this database's backup device path & file name.
--
Select
  @BuDevicePathAndFileName = PhysicalName
From
  #Devices
Where
  DeviceName = @ThisDbName + 'DataBk'

If @@RowCount > 0

  Begin

    Set @Parm2 = 'XCopy /Y "' + @BuDevicePathAndFileName + '" "' + @Parm1 + '"'

    Print @Parm2

    RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                     -- This aids in producing results/output in chronological sequence. 

    Exec @Rc = master..xp_CmdShell @Parm2

  End

If @Rc <> 0
  Begin
    Set @Outcome = 'Failed'                      -- This is a "critical" step.  Flag overall outcome as Failed.
    Set @FailingStepNamesList = @FailingStepNamesList + @CurrStepName + ';'
  End

SkipCopyPaDbBackupFileToRemoteRemoteFolder:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

------------------------------------------
-- Copy the master database's backup file.
------------------------------------------
Print '- - - - - - - - - - - - - - - - - - - - -'
Print 'master and msdb Database Backup Files...'
Print '- - - - - - - - - - - - - - - - - - - - -'

If @CopyMasterAndMsdbDbBackupFilesToRemoteRemoteFolder = 0
  Begin
    Print 'Bypassing step.  Step not configured to run.'
    Goto SkipCopyMasterAndMsdbDbBackupFilesToRemoteRemoteFolder
  End

--
-- Retrieve the master database's backup device path & file name.
--
Select
  @BuDevicePathAndFileName = PhysicalName
From
  #Devices
Where
  DeviceName = 'MasterBk'

If @@RowCount > 0

  Begin

    Set @Parm2 = 'XCopy /Y "' + @BuDevicePathAndFileName + '" "' + @Parm1 + '"'

    Print @Parm2

    RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                     -- This aids in producing results/output in chronological sequence. 

    Exec @Rc = master..xp_CmdShell @Parm2

  End

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

-- Do not check R/C.  Consider this a noncritical step.

----------------------------------------
-- Copy the msdb database's backup file.
----------------------------------------
--
-- Retrieve the msdb database's backup device path & file name.
--
Select
  @BuDevicePathAndFileName = PhysicalName
From
  #Devices
Where
  DeviceName = 'MsdbBk'

If @@RowCount > 0

  Begin

    Set @Parm2 = 'XCopy /Y "' + @BuDevicePathAndFileName + '" "' + @Parm1 + '"'

    Print @Parm2

    RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                     -- This aids in producing results/output in chronological sequence. 

    Exec @Rc = master..xp_CmdShell @Parm2

  End

SkipCopyMasterAndMsdbDbBackupFilesToRemoteRemoteFolder:

RaisError('', 0, 1) With NoWait  -- Flush "Print" Buffer.  (Note:  Generates a blank line too.)
                                 -- This aids in producing results/output in chronological sequence. 

-- Do not check R/C.  Consider this a noncritical step.

--
-- Drop the temporary table.
--
If Object_Id('TempDb..#Devices') Is Not Null
  Drop Table #Devices

SkipBackupOfPaDbAndLogAndCopyOfBackupFilesToRemoteFolder:
SkipCopyAllBackupFilesToRemoteFolder:

Print ''
Print '--------------------------------------------------------------------------------'
Print 'Complete.' + '   (' + Convert(varchar(30), GetDate(), 120) + ')'
Print '--------------------------------------------------------------------------------'

--**************************************************************************************************
-- Log Nightly Job "Last Run Outcome" to SysProperty table.
-- Log Nightly Job "Last Run Failing Steps" to SysProperty table.
--**************************************************************************************************
Set @Parm1 = 'JobNightly_LastRunOutcome'
Exec lsp_DbInsertOrUpdateSysProperty @Parm1, @Outcome

Set @Parm1 = 'JobNightly_LastRunFailSteps'
Exec lsp_DbInsertOrUpdateSysProperty @Parm1, @FailingStepNamesList  -- Stored Procedure automatically truncates VarChar parameters to max. declared size.

--**************************************************************************************************
-- Return Success (0) or Failure (1)
--**************************************************************************************************
If @Outcome = 'Succeeded'
  Return 0
Else -- @Outcome = 'Failed'
  Return 1

End

