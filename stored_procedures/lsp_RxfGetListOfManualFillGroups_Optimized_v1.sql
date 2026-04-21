Version=48

GO

Create Or Alter Procedure dbo.lsp_RxfGetListOfManualFillGroups
    @StationId VarChar(8)
  , @UseStockLocations Bit
  , @FilteredLocationsOnly Bit
  , @FillType varchar(1)
  , @RetrieveTopXGroups Int
  , @ToteId VarChar(24) = NULL
  , @GroupByStartingTotePopWorkflowStepCode Bit
  , @UseHubGroupFilter Bit
  , @HubNames VarChar(1000)
  , @BatchFillId Int = Null
  , @SortByColumn VarChar(30)
  , @AscendingSort bit = 1
As
Begin
   /*
   Object Type:
      Stored Procedure

   Name:
      lsp_RxfGetListOfManualFillGroups

   Version:
      48

   Description:
      Get the groups queued for filling

   Parameters:
      Input:
         @StationId              - The workstation's configured Station ID.
         @UseStockLocations      - Whether or not to only include Rxs whose NDC stock locations match one of the workstation's configured Location Prefixes.
                                       1 = Include only Rxs whose NDC stock locations match one of the workstation's configured Location Prefixes.
                                       0 = Include all Rxs regardless of their NDC stock locations.
         @FilteredLocationsOnly  - Only applicable when @UseStockLocations = 1; Whether or not to only return Rxs whose NDC is associated with stock locations matching this stations location filter
         @FillType               - Whether or not to include Rxs for a specific fill type.
         @RetrieveTopXGroups     - Number of Group results to return (rows)
         @ToteId                 - ToteId if any was scanned on the form instead of reading from RfToteNest
         @GroupByStartingTotePopWorkflowStepCode
                                 - Whether or not to prioritize sorting of the results according to those that start at manual fill.
         @UseHubGroupFilter      - Whether or not to only include Rxs whose Delivery Hubs match one of the workstation's configured Hub Groups.
                                       1 = Include only Rxs whose Delivery Hubs match one of the workstation's configured Hub Groups.
                                       0 = Include all Rxs regardless of their Delivery Hub.
         @HubNames               - List of Hub Names associated to the filtered Hub Groups as they are configured for the system.
         @BatchFillId            - To retrieve all the groups associated with a specific batch fill
         @SortByColumn           - The results will be returned with a sort sequence where the top-precedence column is this specified one.
                                       Specify one of the following string values:
                                          'Priority', 'Group', 'GroupSize', 'DeliveryHub', 'DeliveryRoute'
         @AscendingSort          - Whether to sort the resulting table in ascending order
                                    1 = Sort in ascending order
                                    0 = Sort in descending order
      Output:
         None

   Return Values:
      None

   Comments:
      None
   */

   Set NoCount On;

   Drop Table If Exists
         #ResultTable
       , #Hubs
       , #NdcInvPoolExclusionList
       , #NdcInvPoolInclusionList
       , #OeData
       , #OeDataUserLocs
       , #ToteGroups;

   -- Replaced table variable with temp table to improve cardinality estimates and join performance
   Create Table #ToteGroups
   (
       ToteId varchar(24),
       GroupNum varchar(10),
       ToteGroupNum tinyint
   );

   -- Added index to support joins on GroupNum
   Create Index IX_ToteGroups_GroupNum On #ToteGroups(GroupNum);

   Create Table #OeDataUserLocs
   (
       OrderId varchar(30),
       GroupNum varchar(10),
       PriFillByDtTm datetime,
       PriCodeSys smallint,
       PriInternal varchar(40),
       StoreNum varchar(19),
       PharmacyId int,
       Ndc varchar(20),
       GroupingCriteriaVal varchar(120),
       NumOfRxs int,
       PriDesc varchar(18),
       NdcInvPool varchar(51)
   );

   -- Normalize input parameters
   If @StationId = ''
       Set @StationId = Null;

   If @UseStockLocations Is Null
       Set @UseStockLocations = 0;

   If @FilteredLocationsOnly Is Null
       Set @FilteredLocationsOnly = 0;

   If @FillType = ''
       Set @FillType = Null;

   If @RetrieveTopXGroups <= 0
       Set @RetrieveTopXGroups = 50;

   If @ToteId = ''
       Set @ToteId = Null;

   If @GroupByStartingTotePopWorkflowStepCode Is Null
       Set @GroupByStartingTotePopWorkflowStepCode = 0;

   If @HubNames = ''
       Set @HubNames = Null;

   If @BatchFillId <= 0
       Set @BatchFillId = Null;

   -- If no fill type was provided, default the fill type to 'M' (Manual Fill)
   If @FillType Is Null
       Set @FillType = 'M';

   -- Adding Hubs data to a temp table
   Select
       stringValue As Hub
   Into #Hubs
   From dbo.fStringToTableByDelimiter(@HubNames, ',');

   -- Added index to support hub filtering lookups
   Create Index IX_Hubs_Hub On #Hubs(Hub);

   -- If the results are to be filtered to only Rxs whose NDC + InvPool is for the station's location prefixes
   If @UseStockLocations = 1
   Begin
       If @FilteredLocationsOnly = 0
       Begin
           -- Retrieve a list of NDC + InvPool pairs to exclude from the results
           Select
               [NdcInvPool] = A.Ndc + '-' + Convert(varchar(30), IsNull(A.InvPool, 0))
           Into #NdcInvPoolExclusionList
           From
           (
               -- Build exclusion list: NDC + InvPool not valid for this station
               Select
                   ID.Ndc,
                   SNA.InvPool
               From vInvItemDistWithUserOverrides As ID With (NoLock)
               Left Join InvStationNdcAssoc As SNA With (NoLock)
                   On ID.Ndc = SNA.Ndc
               Where
                   SNA.StationId Is Not Null
                   And SNA.StationId != @StationId

               Except

               Select
                   ID.Ndc,
                   SNA.InvPool
               From vInvItemDistWithUserOverrides As ID With (NoLock)
               Left Join InvStationNdcAssoc As SNA With (NoLock)
                   On ID.Ndc = SNA.Ndc
               Where
                   SNA.StationId = @StationId
                   Or SNA.StationId Is Null
           ) A
           Option (Use Hint('ENABLE_PARALLEL_PLAN_PREFERENCE'));
       End
       Else
       Begin
           -- Build inclusion list for station specific NDC + InvPool filtering
           Select
               [NdcInvPool] = Ndc + '-' + Convert(varchar(30), IsNull(InvPool, 0))
           Into #NdcInvPoolInclusionList
           From InvStationNdcAssoc
           Where StationId = @StationId;
       End
   End

   -- Default ToteId to current station tote if not provided
   If @ToteId Is Null
   Begin
       Select
           @ToteId = ToteId
       From CvyToteAtManFillStation With (NoLock)
       Where StationId = @StationId;
   End

   -- Get the list of Rxs already assigned/in the tote
   If @ToteId Is Not Null
   Begin
       Insert Into #ToteGroups
       (
           ToteId,
           GroupNum,
           ToteGroupNum
       )
       Select Distinct
           OTA.ToteId,
           O.GroupNum,
           GR.ToteGroupNum
       From OrderToteAssoc OTA With (NoLock)
       Inner Join OeOrder O With (NoLock)
           On O.OrderId = OTA.OrderId
       Left Join GovRx GR With (NoLock)
           On O.OrderId = GR.OrderId
       Where OTA.ToteId = @ToteId;
   End

   -- Get OeData to a temp table
   Select
         O.OrderId
       , O.GroupNum
       , O.PriFillByDtTm
       , O.PriCodeSys
       , O.PriInternal
       , O.StoreNum
       , O.PharmacyId
       , O.Ndc
       , G.GroupingCriteriaVal
       , G.NumOfRxs
       , PR.PriDesc
       , [NdcInvPool] = O.Ndc + '-' + Convert(varchar(30), IsNull(P.InvPool, 0))
   Into #OeData
   From OeOrder As O With (NoLock)
   Inner Join OeGroup As G With (NoLock)
       On G.GroupNum = O.GroupNum
   Inner Join OePriority PR With (NoLock)
       On PR.PriCode = O.PriCodeSys
   Left Join Pharmacy As P With (NoLock)
       On P.PharmacyId = O.PharmacyId
   Where
       O.OrderStatus = 'Scheduled'
       And O.FillType = @FillType;

   If @UseStockLocations = 1
   Begin
       If @FilteredLocationsOnly = 0
       Begin
           Insert Into #OeDataUserLocs
           (
               OrderId,
               GroupNum,
               PriFillByDtTm,
               PriCodeSys,
               PriInternal,
               StoreNum,
               PharmacyId,
               Ndc,
               GroupingCriteriaVal,
               NumOfRxs,
               PriDesc,
               NdcInvPool
           )
           Select
               O.OrderId,
               O.GroupNum,
               O.PriFillByDtTm,
               O.PriCodeSys,
               O.PriInternal,
               O.StoreNum,
               O.PharmacyId,
               O.Ndc,
               O.GroupingCriteriaVal,
               O.NumOfRxs,
               O.PriDesc,
               O.NdcInvPool
           From #OeData O
           Where Not Exists
           (
               Select 1
               From #NdcInvPoolExclusionList X
               Where X.NdcInvPool = O.NdcInvPool
           );
       End
       Else
       Begin
           Insert Into #OeDataUserLocs
           (
               OrderId,
               GroupNum,
               PriFillByDtTm,
               PriCodeSys,
               PriInternal,
               StoreNum,
               PharmacyId,
               Ndc,
               GroupingCriteriaVal,
               NumOfRxs,
               PriDesc,
               NdcInvPool
           )
           Select
               O.OrderId,
               O.GroupNum,
               O.PriFillByDtTm,
               O.PriCodeSys,
               O.PriInternal,
               O.StoreNum,
               O.PharmacyId,
               O.Ndc,
               O.GroupingCriteriaVal,
               O.NumOfRxs,
               O.PriDesc,
               O.NdcInvPool
           From #OeData O
           Inner Join #NdcInvPoolInclusionList I
               On I.NdcInvPool = O.NdcInvPool;
       End
   End
   Else
   Begin
       Insert Into #OeDataUserLocs
       (
           OrderId,
           GroupNum,
           PriFillByDtTm,
           PriCodeSys,
           PriInternal,
           StoreNum,
           PharmacyId,
           Ndc,
           GroupingCriteriaVal,
           NumOfRxs,
           PriDesc,
           NdcInvPool
       )
       Select
           O.OrderId,
           O.GroupNum,
           O.PriFillByDtTm,
           O.PriCodeSys,
           O.PriInternal,
           O.StoreNum,
           O.PharmacyId,
           O.Ndc,
           O.GroupingCriteriaVal,
           O.NumOfRxs,
           O.PriDesc,
           O.NdcInvPool
       From #OeData O;
   End

   -- Create indexes after temp table population
   Create Index ByOrderId On #OeDataUserLocs(OrderId);
   Create Index IX_OeDataUserLocs_GroupNum On #OeDataUserLocs(GroupNum);

   -- Removed DISTINCT: GROUP BY already ensures uniqueness
   Select Top (@RetrieveTopXGroups)
        [Priority] =
           Case
               When O.PriCodeSys = 60 Or O.PriCodeSys = 30
                   Then SubString(Convert(VarChar(20), O.PriFillByDtTm, 1), 1, 5)
                        + Char(13) + '   '
                        + SubString(Convert(VarChar(20), O.PriFillByDtTm, 0), 13, 5)
                        + ' '
                        + SubString(Convert(VarChar(20), O.PriFillByDtTm, 0), 18, 2)
               Else O.PriDesc
           End
      , O.PriFillByDtTm
      , O.GroupingCriteriaVal
      , O.NumOfRxs
      , CF.DeliveryHub
      , CF.DeliveryRoute
      , O.GroupNum
      , GR.ToteGroupNum
      , O.PriCodeSys
      , [StartingTotePopWorkflowStepCode] = IsNull(OG.StartingTotePopWorkflowStepCode, '')
      , [MinPriInternal] = Min(O.PriInternal)
   Into #ResultTable
   From #OeDataUserLocs O
   Left Join GovRx As GR With (NoLock)
       On GR.OrderId = O.OrderId
   Left Join BfBatchFillRxAssoc As B With (NoLock)
       On B.OrderId = O.OrderId
      And B.RemoveDtTm Is Null
   Left Join OrderToteAssoc As OTA With (NoLock)
       On OTA.OrderId = O.OrderId
   Left Join #ToteGroups As TG
       On TG.GroupNum = O.GroupNum
      And
      (
          (TG.ToteGroupNum Is Null And GR.ToteGroupNum Is Null)
          Or
          (TG.ToteGroupNum Is Not Null And GR.ToteGroupNum = TG.ToteGroupNum)
      )
   Left Join OeWorkflowStepInProgress As IPG With (NoLock)
       On IPG.ObjectKey = (O.GroupNum + '-' + Cast(GR.ToteGroupNum As varchar))
   Left Join OeGroupCvyFillData OG With (NoLock)
       On OG.GroupNum = O.GroupNum
      And OG.ToteGroupNum = GR.ToteGroupNum
   Left Join SysEndPharmacy As EP With (NoLock)
       On O.StoreNum = EP.StoreNum
      And IsNull(O.PharmacyId, 0) = IsNull(EP.PharmacyId, 0)
   Left Join CfStoreDeliveryCourierCutOff CF With (NoLock)
       On EP.Id = CF.SysEndPharmacyId
   Outer Apply
   (
       Select
           CutoffCount = Count(*),
           NextWeekDayCode =
               Case
                   When Count(Case When WeekDayCode >= DatePart(WEEKDAY, GetDate()) Then 1 End) >= 1
                       Then Min(Case When WeekDayCode >= DatePart(WEEKDAY, GetDate()) Then WeekDayCode End)
                   Else Min(WeekDayCode)
               End
       From CfStoreDeliveryCourierCutOff c With (NoLock)
       Where c.SysEndPharmacyId = EP.Id
   ) CutoffData
   Where
       -- Is not an in-progress fill at some station
       IPG.ObjectKey Is Null
       And ((@BatchFillId Is Not Null And B.BatchFillId = @BatchFillId) Or @BatchFillId Is Null)
       And
       (
           -- If tote has assigned Rxs
           (Exists (Select 1 From #ToteGroups)
               And TG.ToteId Is Not Null
               And OTA.OrderToteStateCode = 'A')

           Or

           -- If tote has NO assigned Rxs
           (Not Exists (Select 1 From #ToteGroups)
               And OTA.OrderId Is Null)
       )
       And
       (
           (CutoffData.CutoffCount > 0 And CF.WeekDayCode = CutoffData.NextWeekDayCode)
           Or
           (CutoffData.CutoffCount <= 0)
       )
       And
       (
           @UseHubGroupFilter = 0
           Or
           (@UseHubGroupFilter = 1 And CF.DeliveryHub In (Select Hub From #Hubs))
       )
   Group By
       O.PriCodeSys,
       O.PriFillByDtTm,
       O.PriDesc,
       O.GroupingCriteriaVal,
       O.NumOfRxs,
       CF.DeliveryHub,
       CF.DeliveryRoute,
       O.GroupNum,
       GR.ToteGroupNum,
       OG.StartingTotePopWorkflowStepCode
   Order By
       MinPriInternal;

   -- Order the resulting groups by the applicable column
   With
        DistinctGroups As
        (
           -- Distinct groups present in the result set
           Select Distinct
               r.GroupNum
           From #ResultTable As r
        )
      , DistintGroupProducts As
        (
           -- Distinct products per group to avoid duplicate hits
           Select Distinct
               o.GroupNum,
               o.ProductId
           From OeOrder As o With (NoLock)
           Inner Join DistinctGroups g
               On g.GroupNum = o.GroupNum
        )
      , InventoryFlags As
        (
           -- Inventory-based flags (one row per group)
           Select
               gp.GroupNum,
               IsColdStorage         = Max(Case When m.StorageCondCode IN ('F', 'R') Then 1 Else 0 End),
               IsControlledSubstance = Max(Case When m.DeaClassCode > 0 Then 1 Else 0 End),
               IsPen                 = Max(Case When m.CrossContaminationCategory = '2' Then 1 Else 0 End),
               IsSulfa               = Max(Case When m.CrossContaminationCategory = '3' Then 1 Else 0 End)
           From DistintGroupProducts gp
           Join vInvMasterWithUserOverrides As m With (NoLock)
               On m.ProductId = gp.ProductId
           Group By gp.GroupNum
        )
      , ProductFlags As
        (
           -- Flag table (one row per group)
           Select
               dgp.GroupNum,
               IsHazMat = Max(Case When FP.IsFlaggedFor = 'HAZMAT' Then 1 Else 0 End),
               IsHazDsp = Max(Case When FP.IsFlaggedFor = 'HAZDSP' Then 1 Else 0 End),
               IsHazShp = Max(Case When FP.IsFlaggedFor = 'HAZSHP' Then 1 Else 0 End)
           From DistintGroupProducts dgp
           Join InvFlaggedProducts As FP With (NoLock)
               On FP.ProductId = dgp.ProductId
           Group By dgp.GroupNum
        )
      , GroupingCriteria As
        (
           -- Compute once per GroupNum via APPLY
           Select
               G.GroupNum,
               X.FmtdGroupingCriteriaVal
           From DistinctGroups G
           Outer Apply
           (
               Select dbo.fOrdGroupingCritValForDisplay(G.GroupNum) As FmtdGroupingCriteriaVal
           ) As X
        )
   Select
        rt.*
      , gc.FmtdGroupingCriteriaVal
      , [IsColdStorage]         = Coalesce(inf.IsColdStorage, 0)
      , [IsControlledSubstance] = Coalesce(inf.IsControlledSubstance, 0)
      , [IsHazMat]              = Coalesce(pf.IsHazMat, 0)
      , [IsHazDsp]              = Coalesce(pf.IsHazDsp, 0)
      , [IsHazShp]              = Coalesce(pf.IsHazShp, 0)
      , [IsPen]                 = Coalesce(inf.IsPen, 0)
      , [IsSulfa]               = Coalesce(inf.IsSulfa, 0)
   From #ResultTable As rt
   Left Join GroupingCriteria gc
       On gc.GroupNum = rt.GroupNum
   Left Join InventoryFlags inf
       On inf.GroupNum = rt.GroupNum
   Left Join ProductFlags pf
       On pf.GroupNum = rt.GroupNum
   Order By
       Case When @GroupByStartingTotePopWorkflowStepCode = 1 And StartingTotePopWorkflowStepCode = 'M' Then 0 Else 1 End,
       Case When @SortByColumn Not In ('Priority', 'Group', 'GroupSize', 'DeliveryHub', 'DeliveryRoute') Then MinPriInternal End,
       -- Sort by Priority ASC/Desc
       Case When @SortByColumn = 'Priority' And @AscendingSort = 1 Then MinPriInternal End ASC,
       Case When @SortByColumn = 'Priority' And @AscendingSort = 0 Then MinPriInternal End Desc,
       -- Sort by GroupSize ASC/Desc
       Case When @SortByColumn = 'GroupSize' And @AscendingSort = 1 Then NumOfRxs End ASC,
       Case When @SortByColumn = 'GroupSize' And @AscendingSort = 0 Then NumOfRxs End Desc,
       -- Sort by DeliveryHub ASC/Desc
       Case When @SortByColumn = 'DeliveryHub' And @AscendingSort = 1 Then DeliveryHub End ASC,
       Case When @SortByColumn = 'DeliveryHub' And @AscendingSort = 0 Then DeliveryHub End Desc,
       -- Sort by DeliveryRoute ASC/Desc
       Case When @SortByColumn = 'DeliveryRoute' And @AscendingSort = 1 Then DeliveryRoute End ASC,
       Case When @SortByColumn = 'DeliveryRoute' And @AscendingSort = 0 Then DeliveryRoute End Desc,
       -- Sort by Group ASC/Desc
       Case When @SortByColumn = 'Group' And @AscendingSort = 1 Then GroupingCriteriaVal End ASC,
       Case When @SortByColumn = 'Group' And @AscendingSort = 0 Then GroupingCriteriaVal End Desc,
       -- Finally always sort by PriInternal in ascending order
       MinPriInternal Asc;

   Drop Table If Exists
         #ResultTable
       , #Hubs
       , #NdcInvPoolExclusionList
       , #NdcInvPoolInclusionList
       , #OeData
       , #OeDataUserLocs
       , #ToteGroups;
End

GO

Grant Execute On dbo.lsp_RxfGetListOfManualFillGroups To PaApp

GO
