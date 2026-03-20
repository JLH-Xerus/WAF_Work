--/
set statistics io on
set statistics time on
--CREATE PROCEDURE lsp_RxfGetListOfManualFillGroups 
declare
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
   
select
        @StationId='MANUAL03',@UseStockLocations=1,@FilteredLocationsOnly=1,@FillType='M',@RetrieveTopXGroups=40,@ToteId=NULL,
        @GroupByStartingTotePopWorkflowStepCode=0,@UseHubGroupFilter=0,
        @HubNames='1073,1117,1202,1070,1069,1066,1199,1094,1065,1095,1068,1071,1107,1035,1036,1067,1108,1116,1198,1058,1072,1191',
        @BatchFillId=NULL,@SortByColumn='Priority',@AscendingSort=1
--As
--Begin
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
      @StationId              - The workstation's configured Station ID.
      @UseStockLocations      - Whether or not to only include Rxs whose NDC stock locations match one of the workstation's configured Location Prefixes.
                                    1 = Include only Rxs whose NDC stock locations match one of the workstation's configured Location Prefixes.
                                    0 = Include all Rxs regardless of their NDC stock locations.
      @FilteredLocationsOnly  - Only applicable when @UseStockLocations = 1; Whether or not to only return Rxs whose NDC is associated with stock locations matching this stations location filter
      @FillType               - Whether or not to include Rxs for a specific fill type.
      @RetrieveTopXGroups     - Number of Group results to return (rows)
      @ToteId                 - ToteId if any was scanned on the form instead of reading from RfToteNest
      @GroupByStartingTotePopWorkflowStepCode
                              - Whether or not to prioritize sorting of the results according to those that start at manual fill.
      @UseHubGroupFilter      - Whether or not to only include Rxs whose Delivery Hubs match one of the workstation's configured Hub Groups.
                                    1 = Include only Rxs whose Delivery Hubs match one of the workstation's configured Hub Groups.
                                    0 = Include all Rxs regardless of their Delivery Hub.
      @HubNames               - List of Hub Names associated to the filtered Hub Groups as they are configured for the system.
      @BatchFillId            - To retrieve all the groups associated with a specific batch fill
      @SortByColumn           - The results will be returned with a sort sequence where the top-precedence column is this specified one.
                                    Specify one of the following string values:
                                       "Priority", "Group", "GroupSize", "DeliveryHub", "DeliveryRoute"
      @AscendingSort          - Whether to sort the resulting table in ascending order
                                 1 = Sort in ascending order
                                 0 = Sort in descending order
   Output:
      None

Return Values:
   None

Comments:
   None
*/
Set NoCount On

--JLH: creating temp table instead of using a tablevar. Optimizer helper.
--Declare @ToteGroups Table (ToteId varchar(24), GroupNum varchar(10), ToteGroupNum tinyint)

--JLH: possible parameter sniffing here.  assigning local vars to prevent.
declare @localstationId varchar(8)
declare @localfilltype varchar(1)
declare @localtoteid varchar(24)
declare @localbatchfillid int

Drop Table If Exists #ResultTable
                   , #Hubs
                   , #NdcInvPoolExclusionList
                   , #NdcInvPoolInclusionList
                   , #OeData
                   , #OeDataUserLocs
                   , #ToteGroups

Create Table #OeDataUserLocs(OrderId varchar(30), GroupNum varchar(10), PriFillByDtTm   datetime, PriCodeSys smallint,
                             PriInternal varchar(40), StoreNum  varchar(19), PharmacyId int, Ndc varchar(20), GroupingCriteriaVal   varchar (120),
                             NumOfRxs   int, PriDesc varchar(18), NdcInvPool varchar(51))

-- Normalize input parameters
If @StationId = ''
   Set @StationId = Null
If @UseStockLocations Is Null
   Set @UseStockLocations = 0
If @FilteredLocationsOnly Is Null
   Set @FilteredLocationsOnly = 0
If @FillType = ''
   Set @FillType = Null
If @RetrieveTopXGroups <= 0
   Set @RetrieveTopXGroups = 50
If @ToteId = ''
   Set @ToteId = Null
If @GroupByStartingTotePopWorkflowStepCode Is Null
   Set @GroupByStartingTotePopWorkflowStepCode = 0
If @HubNames = ''
   Set @HubNames = Null
If @BatchFillId <= 0
   Set @BatchFillId = Null


-- If no fill type was provided, default the fill type to "M" (Manual Fill)
If @FillType Is Null
   Set @FillType = 'M'

--set local vars to break param sniff
select 
        @localstationid = @StationId,
        @localfilltype = @FillType,
        @localtoteid = @ToteId,
        @localbatchfillid = @BatchFillId
        
        
--Adding Hubs data to a temp table
Select stringValue As Hub 
Into #Hubs
From dbo.fStringToTableByDelimiter(@HubNames, ',')

-- If the results are to be filtered to only Rxs whose NDC + InvPool is for the station's location prefixes
If @UseStockLocations = 1
Begin

   If @FilteredLocationsOnly = 0
      Begin

         -- Retrieve a list of NDC + InvPool pairs to exclude from the results
         Select
            [NdcInvPool] = A.Ndc + '-' + Convert(varchar(30),IsNull(InvPool, 0))
         Into 
            #NdcInvPoolExclusionList
            -- This query will return all NDC + InvPool pairs which are only associated with stations other than the specified station ID
            -- (Does not include NDC + InvPool pairs associated with the specified station ID or NDC + InvPool pairs associated with no station)
         From (Select 
                    ID.Ndc
                  , SNA.InvPool
               From
                  vInvItemDistWithUserOverrides As ID With (NoLock)
                  Left Join InvStationNdcAssoc As SNA With (NoLock) On Id.Ndc = SNA.Ndc
               Where
                  SNA.StationId Is Not Null
                  And SNA.StationId != @localstationid
               Except
               Select 
                  ID.Ndc, SNA.InvPool
               From
                  vInvItemDistWithUserOverrides As ID With (NoLock)
                  Left Join InvStationNdcAssoc As SNA With (NoLock) On Id.Ndc = SNA.Ndc
               Where
                  SNA.StationId = @localStationId
                  Or SNA.StationId Is Null) A
         Option (Use Hint('ENABLE_PARALLEL_PLAN_PREFERENCE'))

      End

   Else
      Begin

         -- It has been indicated to only return Rxs whose NDC is associated with locations matching this stations filter
         -- So create a temporary table of NDC + InvPool pairs to include in the results (as opposed to excluding)
         Select
            [NdcInvPool] = Ndc + '-' + Convert(varchar(30),IsNull(InvPool, 0))
         Into 
            #NdcInvPoolInclusionList
         From 
            InvStationNdcAssoc with (nolock)
            --JLH: added missing NOLOCK
         Where
            StationId = @localStationId
      End
End



-- If a Tote ID was not passed into this stored procedure
If @localToteId Is Null
   -- Use the ID of the tote currently at the station passed into this stored procedure
   Select @localToteId = ToteId From CvyToteAtManFillStation With (NoLock) Where StationId = @localStationId


-- Get the list of Rxs already assigned/In the tote
create table #totegroups (
        ToteId varchar(24),
        GroupNum varchar(10),
        ToteGroupNum tinyint
) 

If @ToteId Is Not Null
   Insert Into
      #ToteGroups (ToteId, GroupNum, ToteGroupNum)
   Select Distinct
      OTA.ToteId, O.GroupNum, ToteGroupNum
   From
      OrderToteAssoc OTA With (NoLock)
      Inner Join OeOrder O With (NoLock) On O.OrderId = OTA.OrderId
      Left Join GovRx GR With (NoLock) On O.OrderId = GR.OrderId
   Where
      OTA.ToteId = @localToteId

--JLH: index after data inserted for join performance and optimzer cardinality estimates      
create index totegroups_bygroupnum on #totegroups(groupnum)

--Get OeData to a temp table
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
    , [NdcInvPool] = O.Ndc + '-' + Convert(varchar(30),IsNull(P.InvPool, 0))
Into  
    #OeData
From 
   OeOrder as O With (NoLock)
   Inner Join OeGroup as G With (NoLock) on G.GroupNum = O.GroupNum
   Inner Join OePriority PR With (NoLock) on PR.PriCode = O.PriCodeSys
   Left Join Pharmacy As P With (NoLock) On P.PharmacyId = O.PharmacyId
Where
   O.OrderStatus = 'Scheduled'
   And
   O.FillType = @localFillType


If @UseStockLocations = 1 
Begin
   If @FilteredLocationsOnly = 0
      Begin
         Insert Into #OeDataUserLocs
            Select * 
            From #OeData O
            Where Not Exists (Select 1 From #NdcInvPoolExclusionList X Where X.NdcInvPool = O.NdcInvPool)
      End

   Else
      Begin
         Insert Into #OeDataUserLocs
         Select O.*
         From #OeData As O
         Inner Join #NdcInvPoolInclusionList As I 
            On I.NdcInvPool = O.NdcInvPool
      End
End
Else
Begin
   Insert Into #OeDataUserLocs
   Select * 
   From #OeData 
End

--Create Index on OrderId
Create Index ByOrderId On #OeDataUserLocs(OrderId)

-- Select the Top X groups based on internal priority

--JLH: CTE approach to reduce logical reads
;with NextDeliveryDay as (
        select
                SysEndPharmacyId,
                min(case when WeekDayCode >= datepart(WeekDay, getdate()) then WeekDayCode end) ThisWeekday,
                min(WeekDayCode) FirstWeekDay
        from CfStoreDeliveryCourierCutOff with (nolock)
        group by SysEndPharmacyId
)
Select Distinct Top (@RetrieveTopXGroups)
     [Priority] =
        Case When O.PriCodeSys = 60 Or O.PriCodeSys = 30
             Then SubString(Convert(VarChar(20), O.PriFillByDtTm, 1), 1, 5) + Char(13) + '   ' + SubString(Convert(VarChar(20), O.PriFillByDtTm, 0), 13, 5) + ' ' + SubString(Convert(VarChar(20), O.PriFillByDtTm, 0), 18, 2)
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
Into
   #ResultTable
From
   #OeDataUserLocs O
   Left Join GovRx As GR With (NoLock) 
      On GR.OrderId = O.OrderId
   Left Join BfBatchFillRxAssoc As B With (NoLock) 
      On B.OrderId = O.OrderId
            And
         B.RemoveDtTm Is Null
   Left Join OrderToteAssoc As OTA With (NoLock)
      On OTA.OrderId = O.OrderId
   Left Join #ToteGroups As TG 
      On TG.GroupNum = O.GroupNum
         And ((TG.ToteGroupNum Is Null And GR.ToteGroupNum Is Null)
              Or
              (TG.ToteGroupNum Is Not Null And GR.ToteGroupNum = TG.ToteGroupNum))
   Left Join OeWorkflowStepInProgress As IPG With (NoLock) 
      On IPG.ObjectKey = (O.GroupNum + '-' + CAST(GR.ToteGroupNum As varchar))
   Left Join OeGroupCvyFillData OG With (NoLock)
      On OG.GroupNum = O.GroupNum
            And
         OG.ToteGroupNum = GR.ToteGroupNum
   Left Join SysEndPharmacy as EP With (NoLock) 
      On O.StoreNum = EP.StoreNum
            And
         IsNull(O.PharmacyId,0) = IsNull(EP.PharmacyId,0)
   left join NextDeliveryDay NDD on EP.ID = NDD.SysEndPharmacyId
   --JLH: this join eliminates the need for the fanout across weekdays in the predicate
   Left Join CfStoreDeliveryCourierCutOff CF With (NoLock)
      On EP.Id = CF.SysEndPharmacyId
Where
   -- Is not an in-progress fill at some station
   IPG.ObjectKey Is Null
   And
   ((@localBatchFillId Is Not Null And B.BatchFillId = @localBatchFillId) Or @localBatchFillId Is Null)
   And
   (-- If Rxs are assigned/In the passed in Tote ID or the Tote currently at the specified station
    --    Only consider those groups with Rxs assigned to that specific tote
    (Exists (Select 1 From #ToteGroups) And TG.ToteId Is Not Null And OTA.OrderToteStateCode = 'A')
    Or
    -- If Rxs are NOT assigned to the passed in Tote ID or the Tote currently at the specified station
    --    Only consider those groups with Rxs not yet assigned to any tote
    (Not Exists (Select 1 From #ToteGroups) And OTA.OrderId Is Null))
    And
    (@UseHubGroupFilter = 0 OR (@UseHubGroupFilter = 1 AND CF.DeliveryHub IN (select hub from #hubs)))
    /*
    ((
      (Select Count(Id) From CfStoreDeliveryCourierCutOff With (NoLock) where SysEndPharmacyId = (Select Id from SysEndPharmacy Where StoreNum = O.StoreNum And IsNull(PharmacyId,0) = IsNull(O.PharmacyId,0))) > 0 
      And
      CF.WeekDayCode =
         Case When (Select Count(Id) From CfStoreDeliveryCourierCutOff With (NoLock) Where SysEndPharmacyId = (Select Id from SysEndPharmacy With (NoLock) Where StoreNum = O.StoreNum And IsNull(PharmacyId,0) = IsNull(O.PharmacyId,0)) and WeekDayCode >= DATEPART(WEEKDAY, GETDATE())) >= 1
              Then (Select Top 1 WeekDayCode From CfStoreDeliveryCourierCutOff With (NoLock) Where SysEndPharmacyId = (Select Id from SysEndPharmacy With (NoLock) Where StoreNum = O.StoreNum And IsNull(PharmacyId,0) = IsNull(O.PharmacyId,0)) and WeekDayCode >= DATEPART(WEEKDAY, GETDATE()) Order By WeekDayCode Asc)
              Else (Select Top 1 WeekDayCode From CfStoreDeliveryCourierCutOff With (NoLock) Order By WeekDayCode Asc) End)
     Or (Select Count(Id) From CfStoreDeliveryCourierCutOff With (NoLock) where SysEndPharmacyId = (Select Id from SysEndPharmacy With (NoLock) Where StoreNum = O.StoreNum And IsNull(PharmacyId,0) = IsNull(O.PharmacyId,0))) <= 0)
    And
    (@UseHubGroupFilter = 0
     Or
     (@UseHubGroupFilter = 1 And CF.DeliveryHub In (Select Hub From #Hubs)))
     */
Group By
   O.PriCodeSys, O.PriFillByDtTm, O.PriDesc, O.GroupingCriteriaVal, O.NumOfRxs,
   CF.DeliveryHub, CF.DeliveryRoute, O.GroupNum, GR.ToteGroupNum, --JLH: O.PriCodeSys, this was a duplicate
   OG.StartingTotePopWorkflowStepCode
Order By
   MinPriInternal;


-- Order the resulting groups by the applicable column
With 
     DistinctGroups As (
        -- Distinct groups present in the result set
        Select Distinct 
           r.GroupNum
        From #ResultTable As r)
   , DistintGroupProducts As (
        -- Distinct products per group to avoid duplicate hits
        Select Distinct 
             o.GroupNum
           , o.ProductId
        From OeOrder As o With (NoLock)
        Inner Join DistinctGroups g On g.GroupNum = o.GroupNum)
   , InventoryFlags As (
        -- Inventory-based flags (one row per group)
        Select
             gp.GroupNum
           , IsColdStorage          = Max(Case When m.StorageCondCode IN ('F','R')     Then 1 Else 0 End)
           , IsControlledSubstance  = Max(Case When m.DeaClassCode > 0                 Then 1 Else 0 End)
           , IsPen                  = Max(Case When m.CrossContaminationCategory = '2' Then 1 Else 0 End)
           , IsSulfa                = Max(Case When m.CrossContaminationCategory = '3' Then 1 Else 0 End)
        From DistintGroupProducts gp
        Join vInvMasterWithUserOverrides As m With (NoLock)
          On m.ProductId = gp.ProductId
        Group By GP.GroupNum)
   , ProductFlags As (
        -- Flag table (one row per group)
        Select
              dgp.GroupNum
            , IsHazMat = Max(Case When FP.IsFlaggedFor = 'HAZMAT' Then 1 Else 0 End)
            , IsHazDsp = Max(Case When FP.IsFlaggedFor = 'HAZDSP' Then 1 Else 0 End)
            , IsHazShp = Max(Case When FP.IsFlaggedFor = 'HAZSHP' Then 1 Else 0 End)
        From DistintGroupProducts dgp
        Join InvFlaggedProducts As FP With (NoLock)
          On FP.ProductId = dgp.ProductId
        Group By dgp.GroupNum)
   , GroupingCriteria As (
        -- JLH: replace outer apply for single pass processing
        Select 
                DG.GroupNum,
                G.GroupingCriteriaVal + ISNULL('   (' + s.splitsuffix + ')','') FmtdGroupingCriteriaVal
        From DistinctGroups DG
        Inner Join OeGroup G with (nolock) on dg.groupnum = g.groupnum
        left join (
                select 
                        gsa.groupnum,
                        string_agg(sc.descrip,'; ') splitsuffix
                from OeGroupSplitCodeAssoc gsa with (nolock)
                inner join OeGroupSplitCode sc with (nolock) on gsa.splitcode = sc.code
                group by gsa.groupnum
        ) s on s.groupnum = dg.groupnum)
        /*
        JLH: the scalar function below forces RBAR processing...converted to left join and string_agg to allow for parallelism
        Outer Apply (
            Select dbo.fOrdGroupingCritValForDisplay(G.GroupNum) As FmtdGroupingCriteriaVal
        ) As X)
        */
   Select
         rt.*
       , gc.FmtdGroupingCriteriaVal
       , [IsColdStorage] = Coalesce(inf.IsColdStorage, 0)
       , [IsControlledSubstance] = Coalesce(inf.IsControlledSubstance, 0)
       , [IsHazMat] = Coalesce(pf.IsHazMat, 0)
       , [IsHazDsp] = Coalesce(pf.IsHazDsp, 0)
       , [IsHazShp] = Coalesce(pf.IsHazShp, 0)
       , [IsPen] = Coalesce(inf.IsPen, 0)
       , [IsSulfa] = Coalesce(inf.IsSulfa, 0)
   From #ResultTable As rt
   Left Join GroupingCriteria gc On gc.GroupNum = rt.GroupNum
   Left Join InventoryFlags inf On inf.GroupNum = rt.GroupNum
   Left Join ProductFlags pf On pf.GroupNum = rt.GroupNum
   Order By
        Case When @GroupByStartingTotePopWorkflowStepCode = 1 And StartingTotePopWorkflowStepCode = 'M' Then 0 Else 1 End
      , Case When @SortByColumn Not In ('Priority', 'Group', 'GroupSize', 'DeliveryHub', 'DeliveryRoute') Then MinPriInternal End
      --Sort by Priority ASC/Desc
      , Case When @SortByColumn = 'Priority' And @AscendingSort = 1 Then MinPriInternal End ASC
      , Case When @SortByColumn = 'Priority' And @AscendingSort = 0 Then MinPriInternal End Desc
      --Sort by GroupSize ASC/Desc
      , Case When @SortByColumn = 'GroupSize' And @AscendingSort = 1 Then NumOfRxs End ASC
      , Case When @SortByColumn = 'GroupSize' And @AscendingSort = 0 Then NumOfRxs End Desc
      --Sort by DeliveryHub ASC/Desc
      , Case When @SortByColumn = 'DeliveryHub' And @AscendingSort = 1 Then DeliveryHub End ASC
      , Case When @SortByColumn = 'DeliveryHub' And @AscendingSort = 0 Then DeliveryHub End Desc
      --Sort by DeliveryRoute ASC/Desc
      , Case When @SortByColumn = 'DeliveryRoute' And @AscendingSort = 1 Then DeliveryRoute End ASC
      , Case When @SortByColumn = 'DeliveryRoute' And @AscendingSort = 0 Then DeliveryRoute End Desc
      --Sort by Group ASC/Desc
      , Case When @SortByColumn = 'Group' And @AscendingSort = 1 Then GroupingCriteriaVal End ASC
      , Case When @SortByColumn = 'Group' And @AscendingSort = 0 Then GroupingCriteriaVal End Desc
      --Finally always sort by PriInternal in Ascending order
      , MinPriInternal Asc


Drop Table If Exists #ResultTable
                   , #Hubs
                   , #NdcInvPoolExclusionList
                   , #NdcInvPoolInclusionList
                   , #OeData
                   , #OeDataUserLocs
                   , #ToteGroups
                   
--End
set statistics io off
set statistics time off

/
