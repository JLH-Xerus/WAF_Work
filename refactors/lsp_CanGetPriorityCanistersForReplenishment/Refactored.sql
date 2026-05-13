--/
ALTER PROCEDURE [dbo].[lsp_CanGetPriorityCanistersForReplenishment]
     @StationId VarChar(30) = NULL
   , @HideLowPriorityCanisters bit = 0
   , @HideLostCanisters bit = 0
   , @ShowOutOfStockCanisters bit = 0
   , @SortColumn Int = 0
   , @SortDirection Int = 0
As
Begin
/*
Object Type:
   Stored Procedure

Object Name:
   lsp_CanGetPriorityCanistersForReplenishment

Description:
   Returns a prioritized list of M4C canisters. Priority is determined based on the needs
   of dispensers with matching products.

Version:
   74

Comments:
   v74 - Performance refactoring (phase 1 of a multi-version plan):
      1. Local-variable copies of all six input parameters to break parameter sniffing.
         The cross-list capture shows a 249,457x worst-case-to-average ratio, which is
         the strongest plan-volatility signature anywhere in the rows 22-39 cohort and
         is the dominant cost driver on this procedure. Every reference to the input
         parameters in the body now resolves to a local instead of a sniffed value.
      2. Consolidated the ten scattered `Drop Table If Exists` statements at the bottom
         of the procedure into a single statement. The corresponding statements at the
         top were already in the modern `Drop Table If Exists` form and are preserved
         in place.
      3. Added `Option (Recompile)` on the final ORDER BY SELECT statement, which is
         the most parameter-sniffing-sensitive statement in the procedure because the
         ORDER BY contains a long Case ladder over @SortColumn and @SortDirection.
         Recompile here gives the optimizer the actual sort target at compile time.
      4. Retained the structural shape of the procedure for v74: all six cursor loops,
         the scalar UDF dbo.fFormatNdcForDisplay, the multi-branch UNION priority
         bucketing, and the per-row CASE expressions for canister model logic remain.
         Each of those is a candidate for a separate refactor pass. Section 11 of
         Analysis.md carries the staged plan for v75 through v77.

Parameters:
   @StationId                - When specified, restricts the replenish list to those
                               items assigned to stock locations that the station is
                               associated with. The list will also include those items
                               that are not assigned to any stock location.
   @HidelowPriorityCanisters - When specified, removes low priority canisters from the
                               replenish list.
   @HideLostCanisters        - When specified, removes lost canisters from the replenish
                               list.
   @ShowOutOfStockCanisters  - When specified, includes canisters on the list for
                               products that are currently marked out of stock.
   @SortColumn               - Column used to sort the data.
   @SortDirection            - Sort direction (asc or desc).

Return Value:
   None
*/

Set NoCount On;

------------------------------------------------------------------------------------------
-- Local-variable copies break parameter sniffing on the downstream queries. The cross-list
-- capture shows a 249,457x worst-case-to-average ratio on this procedure; the local-variable
-- form plus per-statement Option (Recompile) on the long-running blocks is what addresses it.
------------------------------------------------------------------------------------------
Declare @LocalStationId                VarChar(30) = @StationId
Declare @LocalHideLowPriorityCanisters Bit         = @HideLowPriorityCanisters
Declare @LocalHideLostCanisters        Bit         = @HideLostCanisters
Declare @LocalShowOutOfStockCanisters  Bit         = @ShowOutOfStockCanisters
Declare @LocalSortColumn               Int         = @SortColumn
Declare @LocalSortDirection            Int         = @SortDirection

Drop Table If Exists #CanisterList
Create Table #CanisterList 
   ( CanisterSn Int, LastNdcAssigned VarChar(20), fmtLastNdcAssigned VarChar(20), Medication VarChar(125)
   , Model VarChar(10), Status VarChar(15), SortPriority TinyInt, Priority VarChar(12), CurrLoc VarChar(100)
   , InvPool TinyInt, TcdSn VarChar(20), StockLocation VarChar(20), TcdAddress VarChar(10), ProductId VarChar(30)
   , TcdStatusReason VarChar(60), NumStockBottles Int, MaxReplenQty Int, NumAllocatedRxs Int)

Drop Table If Exists #AllocatedRxCountsPerProductId
Create Table #AllocatedRxCountsPerProductId
   (ProductId VarChar(20) Primary Key, RxCount Int)

Drop Table If Exists #PriorityTcds
Create Table #PriorityTcds
   ( TcdSn Int Primary Key, LocCanModel TinyInt, TcdAddress VarChar(10), ProductId VarChar(20)
   , SortPriority TinyInt, Priority VarChar(12),TcdStatusReason VarChar(60))

Drop Table If Exists #PriorityCans
Create Table #PriorityCans 
   ( CanisterSn Int Primary Key, ProductId VarChar(20), Model TinyInt, SortPriority TinyInt
   , Priority VarChar(12), TcdSn Int,TcdAddress VarChar(10), TcdStatusReason VarChar(60))

Drop Table If Exists #CanisterListFiltered
Create Table #CanisterListFiltered
   ( CanisterSn VarChar(20), LastNdcAssigned VarChar(20), fmtLastNdcAssigned VarChar(20), Medication VarChar(125)
   , Model VarChar(10), Status VarChar(15),SortPriority TinyInt, Priority VarChar(12),CurrLoc VarChar(100)
   , InvPool TinyInt, TcdSn VarChar(20), StockLocation VarChar(20), TcdAddress VarChar(10), ProductId VarChar(30)
   , TcdStatusReason VarChar(60), NumStockBottles Int, MaxReplenQty Int, NumAllocatedRxs Int)

Drop Table If Exists #NdcForwardStockLocs
Create Table #NdcForwardStockLocs
   (Ndc VarChar (20), Loc VarChar(30))

Drop Table If Exists #NdcBackStockLocs
Create Table #NdcBackStockLocs
   (Ndc VarChar (20), Loc VarChar(30))   

Drop Table If Exists #CanistersReadyOrInProcess
Create Table #CanistersReadyOrInProcess
   (CanisterSn Int Primary Key,Model Integer,ProductId VarChar(30),TcdSn VarChar(20))

Drop Table If Exists #CanistersForSubmitQueue
Create Table #CanistersForSubmitQueue
   (ProductId VarChar(30),CanisterSn VarChar(30), TcdSn Int)

Declare @ForwardStockLocationCursor Cursor
      , @BackStockLocationCursor Cursor
      , @CsrPriorityTcds Cursor
      , @CsrCans Cursor
      , @CanisterSn Int
      , @Model TinyInt
      , @ProductId VarChar(20)
      , @TcdSortPriority TinyInt
      , @TcdPriority VarChar(12)
      , @TcdCanModel TinyInt
      , @TcdAddress VarChar(10)
      , @TcdStatusReason VarChar(60)
      , @TcdProduct VarChar(30)
      , @TcdSn Int
      , @LocRegex VarChar(50)
      , @MaxHalfLiterCC Int = 0
      , @MaxTwoLiterCC Int = 0
      , @MaxFourLiterCC Int = 0


-------------------------------------------------------------------------
--Get the configured hopper volumes for 0.5 Lt, 2 Lt, and 4 Lt canisters.
-------------------------------------------------------------------------
Select @MaxHalfLiterCC = IsNull(Value, 0)
From vCfgSystemParamVal
Where  Section = 'CanisterAttributes' 
   And Parameter = 'HalfLiterHopperVolume'

Select @MaxTwoLiterCC = IsNull(Value, 0)
From vCfgSystemParamVal
Where  Section = 'CanisterAttributes'
   And Parameter = 'TwoLiterHopperVolume'

 
Select @MaxFourLiterCC = IsNull(Value, 0)
From vCfgSystemParamVal 
Where  Section = 'CanisterAttributes'
   And Parameter = 'FourLiterHopperVolume'

-----------------------------------------------------------------------------
-- Get the configured stock location masks for forward and back stock areas.
-----------------------------------------------------------------------------
Drop Table If Exists #ConfiguredStockLocDefs

Select * 
   Into #ConfiguredStockLocDefs 
From dbo.fGetConfiguredStockAreasAndMasks()

---------------------------------------------------------------------------------------------------------------------------------
-- Get the list of Canisters Ready to be attached or in process (i.e being replenished or being actively attached to a dispenser)
--   Status = 'Filled','Verified','In Process','Replenishing'
---------------------------------------------------------------------------------------------------------------------------------
Insert Into
   #CanistersReadyOrInProcess
Select 
   CanisterSn, Model, ProductId, NULL
From 
   CanCanister With (NoLock)
Where 
   ProductId Is Not Null 
   And 
   Status In ('Filled','Verified','In Process','Replenishing')

---------------------------------------------------------
-- Build the list of Replenishment Dispensers by Priority
---------------------------------------------------------
Insert Into
   #PriorityTcds (TcdSn, LocCanModel, TcdAddress, ProductId, SortPriority, Priority, TcdStatusReason)
   ----------------------------
   -- Include RunDry Dispensers
   --   Priority Level 1
   ----------------------------
   Select 
        [TcdSn] = t.TcdSn
      , [LocCanModel] = t.LocCanModel
      , [TcdAddress] = t.AddrBank + '-' + Cast(t.AddrCabinet as VarChar(2)) + '-' + Cast(t.AddrRow as VarChar(2)) + '-' + Cast(t.AddrSlot as VarChar(2))
      , [ProductId] = t.ProductId
      , [SortPriority] = 1
      , [Priority] = 'High'
      , [TcdStatusReason] = tr.HoldReason
   From 
      TcdStatus t With (NoLock)
      Left Join TcdDryDispensers td With (NoLock) On td.TcdSn = t.TcdSn
      Left Join TcdHoldReplen tr With (NoLock) On tr.TcdSn = t.TcdSn
   Where     t.TcdModel <> 42
         And td.TcdSn Is Not Null
         And tr.TcdSn Is Null
         And t.LocCanModel In (1,2,3)
         And t.AddrCabinet <> 0 And t.AddrRow <> 0 And t.AddrSlot <> 0
         And t.TcdSn <> 0
         And t.ProductId Is Not Null
         And t.TcdSn Not In (Select c.TcdSn From CanCanister c With (NoLock) Where c.Status In ('Picked Up','Online') And c.Tcdsn Is Not Null)
    
   Union
   -------------------------------
   -- Include Low Level Dispensers
   --   Priority Level 2
   -------------------------------
   Select 
        [TcdSn] = t.TcdSn
      , [LocCanModel] = t.LocCanModel
      , [TcdAddress] = t.AddrBank + '-' + Cast(t.AddrCabinet as VarChar(2)) + '-' + Cast(t.AddrRow as VarChar(2)) + '-' + Cast(t.AddrSlot as VarChar(2))
      , [ProductId] = t.ProductId
      , [SortPriority] = 2
      , [Priority] = 'Medium'
      , [TcdStatusReason] = tr.HoldReason
   From 
      TcdStatus t With (NoLock) 
      Left Join TcdDryDispensers td With (NoLock) On td.TcdSn = t.TcdSn
      Left Join TcdHoldReplen tr With (NoLock) On tr.TcdSn = t.TcdSn
   Where     t.TcdModel <> 42
         And t.ReplenState  = 'Replenish'
         And td.TcdSn Is Null
         And tr.TcdSn Is Null
         And t.LocCanModel In (1,2,3)
         And t.AddrCabinet <> 0 And t.AddrRow <> 0 And t.AddrSlot <> 0 
         And t.TcdSn <> 0 
         And t.ProductId Is Not Null 
         And t.TcdSn Not In (Select c.TcdSn From CanCanister c With (NoLock) Where c.Status In ('Picked Up','Online') And c.TcdSn Is Not Null)

   Union
   ---------------------------------------------------------------------------------------
   -- Include 2 lt Dispensers not marked for replenishment that have empty 2 ltrs attached
   --   Priority Level 3
   ---------------------------------------------------------------------------------------
   Select 
        [TcdSn] = t.TcdSn
      , [LocCanModel] = t.LocCanModel
      , [TcdAddress] = t.AddrBank + '-' + Cast(t.AddrCabinet as VarChar(2)) + '-' + Cast(t.AddrRow as VarChar(2)) + '-' + Cast(t.AddrSlot as VarChar(2))
      , [ProductId] = t.ProductId
      , [SortPriority] = 3
      , [Priority] = 'Medium'
      , [TcdStatusReason] = tr.HoldReason
   From 
      TcdStatus t With (NoLock) 
      Left Join TcdDryDispensers td With (NoLock) On td.TcdSn = t.TcdSn
      Left Join TcdHoldReplen tr With (NoLock) On tr.TcdSn = t.TcdSn
      Left Join CanCanister c With (NoLock) On c.TcdSn = t.TcdSn
   Where  t.ReplenState <> 'Replenish'
      And td.TcdSn Is Null
      And tr.TcdSn is Null
      And t.AddrCabinet <> 0 And t.AddrRow <> 0 And t.AddrSlot <> 0
      And t.TcdSn <> 0
      And t.LocCanModel = 2
      And t.ProductId Is Not Null
      And c.TcdSn Is Not Null
      And c.CurrLoc = Cast('AUTO__' + t.AddrBank + '_' As VarChar(8))
      And c.Status = 'Online-Replen'
      And t.TcdSn Not In (Select c1.TcdSn From CanCanister c1 With (NoLock) Where c1.Status = 'Picked Up' And c1.TcdSn Is Not Null)

   Union

   ---------------------------------------------------------------------------------------
   -- Include 4 lt Dispensers not marked for replenishment that have empty 4 ltrs attached
   --   Priority Level 3
   ---------------------------------------------------------------------------------------
   Select 
        [TcdSn] = t.TcdSn
      , [LocCanModel] = t.LocCanModel
      , [TcdAddress] = t.AddrBank + '-' + Cast(t.AddrCabinet as VarChar(2)) + '-' + Cast(t.AddrRow as VarChar(2)) + '-' + Cast(t.AddrSlot as VarChar(2))
      , [ProductId] = t.ProductId
      , [SortPriority] = 3
      , [Priority] = 'Medium'
      , [TcdStatusReason] = tr.HoldReason
   From 
      TcdStatus t With (NoLock)
      Left Join TcdDryDispensers td With (NoLock) On td.TcdSn = t.TcdSn
      Left Join TcdHoldReplen tr With (NoLock) On tr.TcdSn = t.TcdSn
      Left Join CanCanister c With (NoLock) On c.TcdSn = t.TcdSn
   Where  t.TcdModel <> 42
      And t.ReplenState <> 'Replenish'
      And td.TcdSn Is Null
      And tr.TcdSn is Null
      And t.AddrCabinet <> 0 And t.AddrRow <> 0 And t.AddrSlot <> 0
      And t.TcdSn <> 0 
      And t.LocCanModel = 3
      And t.ProductId Is Not Null
      And c.TcdSn Is Not Null
      And c.CurrLoc = Cast('AUTO__' + t.AddrBank + '_' As VarChar(8))
      And c.Status = 'Online-Replen'
      And t.TcdSn Not In (Select c1.TcdSn From CanCanister c1 With (NoLock) Where c1.Status = 'Picked Up' And c1.TcdSn Is Not Null)
   Union 

   --------------------------------------------------------------------------------------------------
   -- Include 4 lt Dispensers not marked for replenishment that do not have a 4 ltr canister attached
   --   Priority Level 3
   --------------------------------------------------------------------------------------------------
   Select
        [TcdSn] = t.TcdSn
      , [LocCanModel] = LocCanModel
      , [TcdAddress] = AddrBank + '-' + Cast(AddrCabinet as VarChar(2)) + '-' + Cast(AddrRow as VarChar(2)) + '-' + Cast(AddrSlot as VarChar(2))
      , [ProductId] = t.ProductId
      , [SortPriority] = 3
      , [Priority] =  'Medium'
      , [TcdStatusReason] = tr.HoldReason
   From 
      TcdStatus t With (NoLock)
         Left Join 
      TcdDryDispensers td With (NoLock) 
         On td.TcdSn = t.TcdSn
         Left Join
      TcdHoldReplen tr With (NoLock) 
         On tr.TcdSn = t.TcdSn
         Left Join
      CanCanister c With (NoLock) 
         On c.Tcdsn = t.TcdSn
   Where
      t.TcdModel <> 42
      And
      t.ReplenState <> 'Replenish' 
      And 
      tr.TcdSn Is Null 
      And 
      td.TcdSn Is Null 
      And 
      t.AddrCabinet <> 0 And t.AddrRow <> 0 And t.AddrSlot <> 0 
      And 
      t.TcdSn <> 0 
      And 
      t.LocCanModel = 3 
      And 
      t.ProductId Is Not Null 
      And
      c.TcdSn Is Null

                
Union 

   --------------------------------------------------------------------------------------------------
   -- Include 2 lt Dispensers not marked for replenishment that do not have a 2 ltr canister attached
   --   Priority Level 3
   --------------------------------------------------------------------------------------------------
   Select 
      t.TcdSn, LocCanModel,
      AddrBank + '-' + Cast(AddrCabinet as VarChar(2)) + '-' + Cast(AddrRow as VarChar(2)) + '-' + Cast(AddrSlot as VarChar(2)),
      t.ProductId, 3 as SortPriority, 'Medium' as Priority, tr.HoldReason
   From 
      TcdStatus t With (NoLock)
         Left Join 
      TcdDryDispensers td With (NoLock) 
         On td.TcdSn = t.TcdSn
         Left Join
      TcdHoldReplen tr With (NoLock) 
         On tr.TcdSn = t.TcdSn
         Left Join
      CanCanister c With (NoLock) 
         On c.Tcdsn = t.TcdSn
   Where 
      t.ReplenState <> 'Replenish' 
      And 
      tr.TcdSn Is Null 
      And 
      td.TcdSn Is Null 
      And 
      t.AddrCabinet <> 0 And t.AddrRow <> 0 And t.AddrSlot <> 0 
      And 
      t.TcdSn <> 0 
      And 
      t.LocCanModel = 2 
      And 
      t.ProductId Is Not Null 
      And
      c.TcdSn Is Null         
           
Union

   ---------------------------------------------------------------------------------------------------------------------
   -- Include .5L Dispensers not marked for replenishment that have empty .5L canisters attached for stay-resident banks
   --   Priority Level 3
   ---------------------------------------------------------------------------------------------------------------------
   Select 
      t.TcdSn, LocCanModel,
      AddrBank + '-' + Cast(AddrCabinet as VarChar(2)) + '-' + Cast(AddrRow as VarChar(2)) + '-' + Cast(AddrSlot as VarChar(2)),
      t.ProductId, 3 as SortPriority, 'Medium' as Priority, tr.HoldReason
   From 
      TcdStatus t With (NoLock)
         Left Join
      TcdDryDispensers td With (NoLock) 
         On td.TcdSn = t.TcdSn
         Left Join
      TcdHoldReplen tr With (NoLock) 
         On tr.TcdSn = t.TcdSn
         Left Join
      CanCanister c With (NoLock) 
         On t.TcdSn = c.TcdSn
   Where 
      t.ReplenState <> 'Replenish' 
      And 
      tr.TcdSn Is Null 
      And 
      td.TcdSn Is Null 
      And 
      t.AddrCabinet <> 0 And t.AddrRow <> 0 And t.AddrSlot <> 0 
      And 
      t.TcdSn <> 0 
      And 
      t.LocCanModel = 1 
      And 
      t.ProductId Is Not Null
      And 
      t.TcdSn Not In
         (Select TcdSn From CanCanister c1 With (NoLock) Where c1.Status = 'Picked Up' And c1.TcdSn Is Not Null)
      And 
      c.Status = 'Online-Replen' 
      And 
      c.CurrLoc = Cast('AUTO__' + t.AddrBank + '_' As VarChar(8))

Union

   --------------------------------------------------------------------------------------------------------------------
   -- Include .5L Dispensers not marked for replenishment that don't have .5L canister attached for stay-resident banks
   --   Priority Level 3
   --------------------------------------------------------------------------------------------------------------------
   Select 
      t.TcdSn, LocCanModel,
      AddrBank + '-' + Cast(AddrCabinet as VarChar(2)) + '-' + Cast(AddrRow as VarChar(2)) + '-' + Cast(AddrSlot as VarChar(2)),
      t.ProductId, 3 as SortPriority, 'Medium' as Priority, tr.HoldReason
   From 
      TcdStatus t With (NoLock)
         Left Join 
      TcdDryDispensers td With (NoLock) 
         On td.TcdSn = t.TcdSn
         Left Join
      TcdHoldReplen tr With (NoLock) 
         On tr.TcdSn = t.TcdSn
         Left Join
      CanCanister c With (NoLock) 
         On t.TcdSn = c.TcdSn
   Where 
      t.ReplenState <> 'Replenish' 
      And 
      tr.TcdSn Is Null 
      And
      td.TcdSn Is Null 
      And 
      t.AddrCabinet <> 0 And t.AddrRow <> 0 And t.AddrSlot <> 0 
      And 
      t.TcdSn <> 0 
      And 
      t.LocCanModel = 1 
      And 
      t.ProductId Is Not Null
      And 
      c.TcdSn Is Null

-- Finished building dispenser list

   ---------------------------------------
   -- Set and open the PriorityTcds cursor
   ---------------------------------------
   Set @CsrPriorityTcds = Cursor For
      Select
         TcdSn, ProductId, LocCanModel
      From
         #PriorityTcds
   
   Open @CsrPriorityTcds

   Fetch Next  -- Fetch the first row
      From
         @CsrPriorityTcds 
      Into
         @TcdSn, @ProductId, @Model
   
   While (@@Fetch_Status = 0)
   Begin
  
      ------------------------------------------------------------------------------------------------------------------------------
      -- Find the first available Ready Canister that's not matched with a Tcd and has a matching product for the current dispenser.
      ------------------------------------------------------------------------------------------------------------------------------
      Select Top 1 
         @CanisterSn = CanisterSn 
      From 
         #CanistersReadyOrInProcess
      Where 
         TcdSn Is Null 
         And 
         ProductId = @ProductId 
         And 
         Model = @Model

      -- Pick a dispenser from the list
      Update  
         #CanistersReadyOrInProcess
      Set 
         TcdSn = @TcdSn
      Where 
         CanisterSn = @CanisterSn

      Set @CanisterSn = 0
      Set @TcdSn = 0

      Fetch Next
         From
            @CsrPriorityTcds 
         Into
            @TcdSn,@ProductId,@Model

   End -- Fetch Status = 0
    
   Close @CsrPriorityTcds
   Deallocate @CsrPriorityTcds

   --------------------------------------------------------------------------------------------------------
   -- Remove all dispensers from the #PriorityTcds list that we find in the #CanistersReadyOrInProcess list
   --------------------------------------------------------------------------------------------------------
   Delete From
      #PriorityTcds 
   Where
      TcdSn In
         (Select TcdSn From #CanistersReadyOrInProcess c Where TcdSn Is Not Null)

   ---------------------------------------------------------------
   -- Combine the remaining dispensers with those that are on hold
   ---------------------------------------------------------------
   Insert Into
      #PriorityTcds
   Select  --Add any dispensers whose replenishment is on hold
      t.TcdSn, LocCanModel,
      AddrBank + '-' + Cast(AddrCabinet as VarChar(2)) + '-' + Cast(AddrRow as VarChar(2)) + '-' + Cast(AddrSlot as VarChar(2)),
      ProductId, 4 as SortPriority, 'Low' as Priority, tr.HoldReason
   From 
      TcdStatus t With (NoLock) 
         Left Join 
      TcdHoldReplen tr With (NoLock) 
         On tr.TcdSn = t.TcdSn
   Where
      t.TcdModel <> 42
      And
      tr.TcdSn Is Not Null 
      And 
      t.LocCanModel In (1,2,3)
      And
      t.AddrCabinet <> 0 And t.AddrRow <> 0 And t.AddrSlot <> 0 
      And 
      t.TcdSn <> 0
   Order By 
      Priority Asc

   -------------------------------
   -- Set and open the Cans cursor
   -------------------------------
   Set @CsrCans = Cursor For 
      Select 
         CanisterSn, Model, c.ProductId 
      From
         CanCanister c With (NoLock) 
            Join 
         #PriorityTcds p 
            On p.ProductId = c.ProductId
      Where 
         c.TcdSn Is Null 
         And 
         c.Status In ( 'Empty','Staged')

   Open @CsrCans
  
   Fetch Next
      From
         @CsrCans
      Into
         @CanisterSn, @Model, @ProductId

   While(@@Fetch_STATUS = 0)
   Begin

      Select Top 1 
         @TcdSortPriority = t.SortPriority,
         @TcdPriority = t.Priority,
         @TcdSn = t.tcdsn,
         @tcdAddress = t.TcdAddress,
         @TcdStatusReason = t.TcdStatusReason
      From 
         #PriorityTcds t
      Where 
         t.ProductId = @ProductId 
         And 
         t.LocCanModel = @Model 
         And 
         t.TcdSn Not In (select tcdsn from #PriorityCans)
     
      If @@RowCount > 0 
      Begin
         -- If the product is out of stock
         --   set the priority = 5
         If Exists(Select ProductId From InvOutOfStockProducts With (NoLock) Where ProductId = @ProductId And StockAreaCode Is Null)
         Begin
            Set @TcdSortPriority = 5
            Set @TcdPriority = 'Out of Stock'
         End

         If @CanisterSn Not In (Select CanisterSn From #PriorityCans)
         Begin
            Insert Into #PriorityCans Values(@CanisterSn, @ProductId, @Model, @TcdSortPriority, @TcdPriority, @TcdSn, @TcdAddress, @TcdStatusReason)
         End         
      End

      Set @CanisterSn = 0
      Set @Model = 0
      Set @Productid = ''

      Fetch Next
         From
            @CsrCans
         Into
            @CanisterSn, @Model, @ProductId
     
   End -- Fetch Status = 0

   Close @CsrCans
   Deallocate @CsrCans
   
   ------------------------------------------------------------------------------------------------------------------------
   -- Populate a table of Allocated Rx Counts per Product ID (for the Product ID's in the Priority Dispensers lists).
   ------------------------------------------------------------------------------------------------------------------------
   -- This allows numerous subqueries below to look up this count for NDCs in their canister list.
   -- Prior to this addition, each subquery dug into the OeOrder table again, repeatedly.
   --
   Insert Into
      #AllocatedRxCountsPerProductId
         Select
            O.ProductId,
            Count(Distinct(O.OrderId)) As RxCount
         From
            #PriorityTcds PT
               Join
            OeOrder O With (NoLock)
               On O.ProductId = PT.ProductId
         Where
            O.OrderStatus In ('Scheduled', 'Pending', 'Suspended', 'Problem')
            And
            O.DateFilled Is Null
         Group By
            O.ProductId

   Insert Into
      #CanisterList
   Select 
      c.CanisterSn, IsNull(C.LastNdcAssigned,'') As LastNdcAssigned,
      Case When IsNull(c.LastNdcAssigned,'') = '' then '' else dbo.fFormatNdcForDisplay(c.LastNdcAssigned) End As fmtLastNdcAssigned, 
      i.BrandName + ' ' + i.Strength as Medication, 
      Case 
         When c.Model = 1 Then '.5 Lt.'
         When c.Model = 2 Then '2 Lt.'
         When c.Model = 3 Then '4 Lt.'
         Else 'Unknown' 
      End as Model,
      c.Status, p.SortPriority, p.Priority, IsNull(t.Descrip,'Unknown') as CurrLoc, c.InvPool, p.TcdSn, 
         (Select Top 1 invla.loc From InvNdcLocAssoc invla With (NoLock) Where Left(invla.Ndc,9) = c.ProductId) As StockLocation, 
      p.TcdAddress, c.ProductId, p.TcdStatusReason,
      Case 
         When c.Model = 1 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select (MaxQtyIntoHalfLiterCan - c.Quantity)/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else Floor(((@MaxHalfLiterCC * i.QtyPer100cc)/100 - c.Quantity)/it.QtyPerSku) 
            End
         When c.Model = 2 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select (MaxQtyIntoTwoLiterCan - c.Quantity)/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else Floor(((@MaxTwoLiterCC * i.QtyPer100cc)/100 - c.Quantity)/it.QtyPerSku) 
            End
         When c.Model = 3 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select (MaxQtyIntoFourLiterCan - c.Quantity)/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else Floor(((@MaxFourLiterCC * i.QtyPer100cc)/100 - c.Quantity)/it.QtyPerSku) 
            End
      End as NumStockBottles,
      Case 
         When c.Model = 1 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select MaxQtyIntoHalfLiterCan - c.Quantity From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else ((i.QtyPer100cc*@MaxHalfLiterCC)/100) - c.Quantity 
            End
         When c.Model = 2 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select MaxQtyIntoTwoLiterCan - c.Quantity From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else ((i.QtyPer100cc*@MaxTwoLiterCC)/100) - c.Quantity
            End
         When c.Model = 3 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select MaxQtyIntoFourLiterCan - c.Quantity From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else ((i.QtyPer100cc*@MaxFourLiterCC)/100) - c.Quantity
            End
      End as MaxReplenQty,
      (Select IsNull(Sum(RxCount), 0) From #AllocatedRxCountsPerProductId Where ProductId = C.ProductId) As NumAllocatedRxs  -- Sum() is used simply to return zero if the Product Id does not exist in the count table.
   From 
      #PriorityCans p 
         Join 
      CanCanister c With (NoLock) 
         On p.CanisterSn = c.CanisterSn 
         Left Join 
      vInvMasterWithUserOverrides i With (NoLock) 
         On c.ProductId = i.ProductId
         Left Join 
      ToteRouteLoc t With (NoLock) 
         On t.RouteLoc = c.CurrLoc
         Left Join 
      vInvItemDistWithUserOverrides it With (NoLock) 
         On c.LastNdcReplenished = it.Ndc

   Union ALL

   ----------------------------------------------------------------
   -- Add entries for dispensers for which no canister is available
   ----------------------------------------------------------------
   Select 
      0, IsNull(T.LastNdcAssigned,'') As LastNdcAssigned,
      Case When IsNull(t.LastNdcAssigned,'') = '' Then 'N/A' Else dbo.fFormatNdcForDisplay(t.LastNdcAssigned) End As fmtLastNdcAssigned, 
      i.BrandName + ' ' + i.Strength as Medication,
      Case 
         When t.LocCanModel = 1 Then '.5 Lt.' 
         When t.LocCanModel = 2 Then '2 Lt.'
         When t.LocCanModel = 3 Then '4 Lt.' 
         Else 'Unknown' 
      End as Model,
      'N/A', 
      Case When p.ProductId In (Select ProductId From InvOutOfStockProducts With (NoLock) Where StockAreaCode Is Null) Then 5 Else p.SortPriority End,
      p.Priority,
      'N/A' as CurrLoc, t.InvPool, p.TcdSn,
         (Select Top 1 invla.loc From InvNdcLocAssoc invla With (NoLock) Where Left(invla.Ndc,9) = t.ProductId) As StockLocation,
      p.TcdAddress, t.ProductId, tr.HoldReason,
      Case 
         When t.LocCanModel = 1 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Then (Select MaxQtyIntoHalfLiterCan/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Else Floor(((@MaxHalfLiterCC*i.QtyPer100cc)/100)/it.QtyPerSku) 
            End
         When t.LocCanModel = 2 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Then (Select MaxQtyIntoTwoLiterCan/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Else Floor(((@MaxTwoLiterCC*i.QtyPer100cc)/100)/it.QtyPerSku) 
            End
         When t.LocCanModel = 3 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Then (Select MaxQtyIntoFourLiterCan/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Else Floor(((@MaxFourLiterCC*i.QtyPer100cc)/100)/it.QtyPerSku) 
            End
      End as NumStockBottles,
      Case 
         When t.LocCanModel = 1 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Then (Select MaxQtyIntoHalfLiterCan From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Else (i.QtyPer100cc*@MaxHalfLiterCC)/100 
            End
         When t.LocCanModel = 2 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Then (Select MaxQtyIntoTwoLiterCan From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Else (i.QtyPer100cc*@MaxTwoLiterCC)/100 
            End
         When t.LocCanModel = 3 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Then (Select MaxQtyIntoFourLiterCan From InvReplenProductOverrides With (NoLock) Where ProductId = t.ProductId)
               Else (i.QtyPer100cc*@MaxFourLiterCC)/100 
            End
      End as MaxReplenQty,
      (Select IsNull(Sum(RxCount), 0) From #AllocatedRxCountsPerProductId Where ProductId = T.ProductId) As NumAllocatedRxs  -- Sum() is used simply to return zero if the Product ID does not exist in the count table.
   From 
      #PriorityTcds p 
         Join 
      TcdStatus t With (NoLock) 
         On p.tcdsn = t.tcdsn
         Left Join 
      TcdHoldReplen tr With (NoLock) 
         On tr.TcdSn = p.TcdSn
         Left Join 
      vInvMasterWithUserOverrides i With (NoLock) 
         On t.ProductId = i.ProductId
         Left Join 
      vInvItemDistWithUserOverrides it With (NoLock) 
         On t.LastNdcReplenished = it.Ndc
   Where 
      t.TcdSn Not in
         (Select tcdsn from #PriorityCans) 
   
   ------------------------------------------------------------------------------------------------
   -- Add a .5Lt canister if there is at least @MaxHalfLiterCC worth of orders in the
   -- submit queue and there is no canister currently 'Verified','Picked Up','Replenishing'
   -- for the product. These canisters will be considered High-Medium Priority
   -- This logic only applies to banks that are not configured to have all canisters stay resident.
   ------------------------------------------------------------------------------------------------   
   Insert Into
      #CanistersForSubmitQueue
   Select Distinct
        o.ProductId
      , IsNull(c.CanisterSn,0)
      , (Select Top 1 TcdSn From TcdStatus t With (NoLock)
         Where t.TcdModel <> 42
                  And
               t.productid = o.ProductId
                  And 
               t.LocCanModel In (1,2,3)
                  And
               t.AddrCabinet <> 0 And t.AddrRow <> 0 And t.AddrSlot <> 0)
      From (Select ProductId,QtyOrdered
            From OeOrder o With (NoLock)
            Where OrderStatus = 'Pending'
                     And
                  StatusReason = 'Awaiting Governor Allocation'
                     And
                  o.ProductId Not In (Select ProductId From CanCanister c With (NoLock) Where c.Status in ('Verified','Picked Up','Replenishing','In Process') 
                     And
                  c.ProductId Is Not Null)) O
      Inner Join vInvMasterWithUserOverrides i With (NoLock) 
         On o.ProductId = i.ProductId
      Left Join CanCanister c With (NoLock) 
         On o.ProductId = c.ProductId 
               And
            c.CanisterSn = (Select Top 1 CanisterSn From CanCanister With (NoLock) Where ProductId = o.ProductId)
   Where i.BrandName + ' ' + i.Strength Not In (Select Medication From #CanisterList Where Model = '.5 Lt.')
            And
         c.Model = 1
            And
         c.Status in ('Empty','Staged')
            And
         c.CanisterSn Not In (Select CanisterSn From #CanisterList)
   Group By o.ProductId, i.QtyPer100cc, c.CanisterSn
   Having Sum(QtyOrdered)> = ((@MaxHalfLiterCC * i.QtyPer100cc)/100)

   ---------------------------------------------------------------------------
   -- Now add the canisters from #CanistersForSubmitQueue to the @CanisterList
   ---------------------------------------------------------------------------
   Insert Into
      #CanisterList
   Select 
      c.CanisterSn, IsNull(C.LastNdcAssigned,''),
      Case When IsNull(c.LastNdcAssigned,'') = '' Then '' Else dbo.fFormatNdcForDisplay(c.LastNdcAssigned) End, 
      i.BrandName + ' ' + i.Strength, 
      Case 
         When c.Model = 1 Then '.5 Lt.' 
         When c.Model = 2 Then '2 Lt.'
         When c.Model = 3 Then '4 Lt.' 
         Else 'Unknown' 
      End,
      c.Status, 2, '', IsNull(trl.Descrip,'Unknown') as CurrLoc, c.InvPool, s.tcdsn, 
      (Select Top 1 invla.loc From InvNdcLocAssoc invla With (NoLock) Where Left(invla.Ndc,9) = c.ProductId) As StockLocation,
      t.AddrBank + '-' + Cast(t.AddrCabinet as VarChar(2)) + '-' + Cast(t.AddrRow as VarChar(2)) + '-' + Cast(t.AddrSlot as VarChar(2)),
      c.ProductId, tr.HoldReason,
      Case 
         When c.Model = 1 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select (MaxQtyIntoHalfLiterCan - c.Quantity)/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else Floor(((@MaxHalfLiterCC * i.QtyPer100cc)/100 - c.Quantity)/it.QtyPerSku) 
            End
         When c.Model = 2 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select (MaxQtyIntoTwoLiterCan - c.Quantity)/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else Floor(((@MaxTwoLiterCC * i.QtyPer100cc)/100 - c.Quantity)/it.QtyPerSku)
            End
         When c.Model = 3 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select (MaxQtyIntoFourLiterCan - c.Quantity)/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else Floor(((@MaxFourLiterCC * i.QtyPer100cc)/100 - c.Quantity)/it.QtyPerSku)
            End
      End as NumStockBottles,
      Case 
         When c.Model = 1 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select MaxQtyIntoHalfLiterCan - c.Quantity From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else ((i.QtyPer100cc*@MaxHalfLiterCC)/100) - c.Quantity 
            End
         When c.Model = 2 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select MaxQtyIntoTwoLiterCan - c.Quantity From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else ((i.QtyPer100cc*@MaxTwoLiterCC)/100) - c.Quantity 
            End
         When c.Model = 3 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select MaxQtyIntoFourLiterCan - c.Quantity From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else ((i.QtyPer100cc*@MaxFourLiterCC)/100) - c.Quantity 
            End
      End as MaxReplenQty,
      (Select IsNull(Sum(RxCount), 0) From #AllocatedRxCountsPerProductId Where ProductId = C.ProductId) As NumAllocatedRxs  -- Sum() is used simply to return zero if the Product Id does not exist in the count table.
   From 
      #CanistersForSubmitQueue s 
         Join 
      CanCanister c With (NoLock) 
         On s.CanisterSn = c.CanisterSn
         Join 
      vInvMasterWithUserOverrides i With (NoLock) 
         On c.ProductId = i.ProductId
         Join 
      ToteRouteLoc trl With (NoLock) 
         On trl.RouteLoc = c.CurrLoc
         Join 
      vInvItemDistWithUserOverrides it With (NoLock) 
         On c.LastNdcReplenished = it.Ndc
         Join 
      TcdStatus t With (NoLock) 
         On s.TcdSn = t.TcdSn
         Left Join 
      TcdHoldReplen tr With (NoLock) 
         On tr.TcdSn = s.TcdSn
     
   ----------------------------------------------
   -- Remove low priority canister if flag is set
   ----------------------------------------------
   If @LocalHideLowPriorityCanisters = 1
   Begin
      Delete From #CanisterList Where SortPriority = 4       
   End

   ----------------------------------------------
   -- Remove Lost canister if flag is set
   ----------------------------------------------
   If @LocalHideLostCanisters = 1
   Begin
      Delete From #CanisterList Where CurrLoc = 'Lost'       
   End

   -----------------------------------------------------------------
   -- Add canisters that are staged that are not in the CanisterList
   -----------------------------------------------------------------
   Insert Into
      #CanisterList
   Select 
      c.CanisterSn, IsNull(C.LastNdcAssigned,''),
      Case When IsNull(c.LastNdcAssigned,'') = '' Then '' Else dbo.fFormatNdcForDisplay(c.LastNdcAssigned) End, 
      i.BrandName + ' ' + i.Strength, 
      Case 
         When c.Model = 1 Then '.5 Lt.' 
         When c.Model = 2 Then '2 Lt.'
         When c.Model = 3 Then '4 Lt.'
         Else 'Unknown' 
      End,
      c.Status, 3, '', IsNull(trl.Descrip,'Unknown') as CurrLoc, c.InvPool, c.CanisterSn, 
      (Select Top 1 invla.loc From InvNdcLocAssoc invla With (NoLock) Where Left(invla.Ndc,9) = c.ProductId) AS StockLocation,
      'N/A', c.ProductId,'N/A',
      Case 
         When c.Model = 1 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select (MaxQtyIntoHalfLiterCan - c.Quantity)/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else Floor(((@MaxHalfLiterCC * i.QtyPer100cc)/100 - c.Quantity)/it.QtyPerSku) 
            End
         When c.Model = 2 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select (MaxQtyIntoTwoLiterCan - c.Quantity)/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else Floor(((@MaxTwoLiterCC * i.QtyPer100cc)/100 - c.Quantity)/it.QtyPerSku) 
            End
         When c.Model = 3 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select (MaxQtyIntoFourLiterCan - c.Quantity)/it.QtyPerSku From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else Floor(((@MaxFourLiterCC * i.QtyPer100cc)/100 - c.Quantity)/it.QtyPerSku) 
            End
      End As NumStockBottles,
      Case 
         When c.Model = 1 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select MaxQtyIntoHalfLiterCan - c.Quantity From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else ((i.QtyPer100cc*@MaxHalfLiterCC)/100) - c.Quantity 
            End
         When c.Model = 2 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select MaxQtyIntoTwoLiterCan - c.Quantity From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else ((i.QtyPer100cc*@MaxTwoLiterCC)/100) - c.Quantity 
            End
         When c.Model = 3 Then 
            Case When Exists(Select ProductId From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Then (Select MaxQtyIntoFourLiterCan - c.Quantity From InvReplenProductOverrides With (NoLock) Where ProductId = c.ProductId)
               Else ((i.QtyPer100cc*@MaxFourLiterCC)/100) - c.Quantity 
            End
      End as MaxReplenQty,
      (Select IsNull(Sum(RxCount), 0) From #AllocatedRxCountsPerProductId Where ProductId = C.ProductId) As NumAllocatedRxs  -- Sum() is used simply to return zero if the Product Id does not exist in the count table.
   From 
      CanCanister c With (NoLock) 
         Join 
      vInvMasterWithUserOverrides i With (NoLock) 
         On c.ProductId = i.ProductId
         Join 
      ToteRouteLoc trl With (NoLock) 
         On trl.RouteLoc = c.CurrLoc
         Join 
      vInvItemDistWithUserOverrides it With (NoLock) 
         On c.LastNdcReplenished = it.Ndc
   Where
      c.Version not in (5,6)
      And
      c.Status = 'Staged' 
      And 
      c.CanisterSn Not In
         (Select CanisterSn From #CanisterList)
    
   ----------------------------------------------------------------------------------------------
   -- Remove Out Of Stock canisters if flag set.
   -- If forward stock locations are configured, then also remove out canisters that have products 
   -- that are out of stock in all forward stock locations or just in Robot Fill sub stock area.
   ----------------------------------------------------------------------------------------------
   If @LocalShowOutOfStockCanisters = 0
   Begin
      Delete From #CanisterList 
      Where 
         ProductId In 
         (Select ProductId From InvOutOfStockProducts With (NoLock) Where StockAreaCode Is Null Or (StockAreaCode = 'F' and (StockSubAreaCode = 'F_ROBOTFILL' Or StockSubAreaCode Is Null)))
   End
    
   -------------------------------------------------------------------------------------------------------------------------------------------
   -- Remove any canisters whose product matches a dispenser that is currently being counted dry, or waiting to be inspected for unassignment.
   -------------------------------------------------------------------------------------------------------------------------------------------
   Begin
      Delete From #CanisterList 
      Where 
         TcdSn In 
         (Select TcdSn From TcdFlaggedTcds With (NoLock) Where IsFlaggedFor IN ('Count Dry Unassign','Count Dry Inv Count','Inspection Required'))
   End

   --------------------------------
   -- If a station id was passed in
   --------------------------------
   If @LocalStationId Is Not Null

   Begin
         -------------------------------------------------------------------------------------------
         -- Add entries for products that are located in stock locations associated to this station.
         -------------------------------------------------------------------------------------------
         Insert Into
            #CanisterListFiltered
         Select 
            CanisterSn, LastNdcAssigned, fmtLastNdcAssigned, Medication, Model, Status, SortPriority, Priority, CurrLoc,
            InvPool, TcdSn, 
            StockLocation, 
            TcdAddress, ProductId, TcdStatusReason, NumStockBottles, MaxReplenQty, NumAllocatedRxs
         From 
            #CanisterList
         Where 
            ProductId in
               (
                Select IID.ProductId From InvStationNdcAssoc As ISNA With (NoLock) 
                INNER JOIN
                vInvItemDistWithUserOverrides As IID With (NoLock)
                   On ISNA.Ndc = IID.Ndc
                Where ISNA.StationId = @LocalStationId
               )
            And 
            StockLocation Is Not Null 
            And 
            TcdAddress not in
               (Select TcdAddress from #CanisterListFiltered) 

         ------------------------------------------------------------------------
         -- Add entries for products that are not assigned to any stock locations
         ------------------------------------------------------------------------
         Insert Into
            #CanisterListFiltered
          Select 
            CanisterSn, LastNdcAssigned, fmtLastNdcAssigned, Medication, Model, Status, SortPriority, Priority, CurrLoc,
            InvPool, TcdSn, StockLocation, TcdAddress, ProductId, TcdStatusReason, NumStockBottles, MaxReplenQty, NumAllocatedRxs
          From 
            #CanisterList
          Where 
            StockLocation Is Null 
            And 
            TcdAddress not in (select TcdAddress from #CanisterListFiltered)   



         --------------------------------------------------------------------------
         --Get Forward Stock Locations(F_ROBOTFILL) for filtered canister products
         --------------------------------------------------------------------------
         Set @ForwardStockLocationCursor = Cursor For
         Select
            CONCAT(REPLACE(Mask, '#', '[0-9]'), '%') As Regex
         From #ConfiguredStockLocDefs Where (StockAreaCode = 'F' and StockSubAreaCode = 'F_ROBOTFILL') 

         Open @ForwardStockLocationCursor

         Fetch Next
            From
               @ForwardStockLocationCursor
            Into
               @LocRegex
              
         While @@Fetch_Status = 0
         
         Begin
                   
         Insert Into #NdcForwardStockLocs
            Select
                Distinct INLA.NDC, INLA.LOC
            From 
                InvNdcLocAssoc As INLA With (NoLock)
                INNER JOIN
                #CanisterListFiltered As CLF 
                On INLA.Ndc = CLF.LastNdcAssigned                     
            Where 
                 INLA.LOC LIKE @LocRegex
                    
            Fetch Next
            From
                @ForwardStockLocationCursor
            Into
                @LocRegex

           End -- Fetch Status = 0

           Close @ForwardStockLocationCursor
           Deallocate @ForwardStockLocationCursor
   
         -----------------------------------------------------------------------------------------------------
         --Get Any Back Stock Locations for filtered canister products that don't have forward stock location
         -----------------------------------------------------------------------------------------------------
         Set @BackStockLocationCursor = Cursor For
         Select
            CONCAT(REPLACE(Mask, '#', '[0-9]'), '%') As Regex
         From #ConfiguredStockLocDefs Where (StockAreaCode = 'B') 

         Open @BackStockLocationCursor

         Fetch Next
            From
               @BackStockLocationCursor
            Into
               @LocRegex
              
         While @@Fetch_Status = 0
         
         Begin
            
            Insert Into #NdcBackStockLocs
            Select
               Distinct INLA.NDC, INLA.LOC   
            From 
               InvNdcLocAssoc As INLA With (NoLock)
               INNER JOIN
               #CanisterListFiltered As CLF 
               On INLA.Ndc = CLF.LastNdcAssigned   
               INNER JOIN #NdcForwardStockLocs As NFSL
               On CLF.LastNdcAssigned <> NFSL.Ndc
            Where 
               INLA.LOC LIKE @LocRegex
                    
            Fetch Next
            From
               @BackStockLocationCursor
            Into
               @LocRegex

           End -- Fetch Status = 0

           Close @BackStockLocationCursor
           Deallocate @BackStockLocationCursor

      -------------------------------------------------------
      -- Select the final grid entries from the filtered list
      -------------------------------------------------------
      Select
         Case Status When 'Staged' Then 1 Else 0 End as 'Staged',
         SortPriority,
         Priority,
         Medication, fmtLastNdcAssigned, CurrLoc, 
		 IsNull((Select Top 1 1 From vInvMasterWithUserOverrides As IV with (nolock) where IV.ProductId = c.ProductId And StorageCondCode IN ('F','R')), 0) As IsColdStorage,
		 IsNull((Select Top 1 1 From vInvMasterWithUserOverrides As IV with (nolock) where IV.ProductId = c.ProductId And DeaClassCode > 0), 0) As IsControlledSubstance,
		 IsNull((Select Top 1 1 From InvFlaggedProducts As FP with (nolock) where FP.ProductId = c.ProductId And IsFlaggedFor = 'HAZMAT'), 0) As IsHazHandle,
		 IsNull((Select Top 1 1 From InvFlaggedProducts As FP with (nolock) where FP.ProductId = c.ProductId And IsFlaggedFor = 'HAZDSP'), 0) As IsHazDispose,
		 IsNull((Select Top 1 1 From InvFlaggedProducts As FP with (nolock) where FP.ProductId = c.ProductId And IsFlaggedFor = 'HAZSHP'), 0) As IsHazShip,
		 IsNull((Select Top 1 1 From InvMaster As I with (nolock) where I.ProductId = c.ProductId And CrossContaminationCategory = '2'), 0) As IsPen,
		 IsNull((Select Top 1 1 From InvMaster As I with (nolock) where I.ProductId = c.ProductId And CrossContaminationCategory = '3'), 0) As IsSulfa,
         Case 
            When Exists (Select Loc from #NdcForwardStockLocs Where ndc = LastNdcAssigned) Then (Select Top 1 Loc From #NdcForwardStockLocs Where ndc = LastNdcAssigned)
            When Exists (Select Loc from #NdcBackStockLocs Where ndc = LastNdcAssigned) Then (Select Top 1 Loc From #NdcBackStockLocs Where ndc = LastNdcAssigned)
         Else NULL
         End As StockLocation,
         MaxReplenQty,
         Case When c.CanisterSn = 0 then 'N/A' Else 'CAN' + Cast(c.CanisterSn as VarChar(20)) End as CanisterSn,
         Model, IsNull(TcdStatusReason,'') as TcdStatusReason, NumStockBottles, Status, InvPool, TcdSn, TcdAddress, LastNdcAssigned, NumAllocatedRxs,
         '' as Allergy, '' as Hazardous
      From 
         #CanisterListFiltered c
      Order By  
         Case When @LocalSortColumn = 0 And @LocalSortDirection = 0 then Status End Asc,
         Case When @LocalSortColumn = 0 And @LocalSortDirection = 1 then Status End Desc,
         Case When @LocalSortColumn = 1 And @LocalSortDirection = 0 then SortPriority End Asc,
         Case When @LocalSortColumn = 1 And @LocalSortDirection = 1 then SortPriority End Desc,
         Case When @LocalSortColumn = 2 And @LocalSortDirection = 0 then Medication End Asc,
         Case When @LocalSortColumn = 2 And @LocalSortDirection = 1 then Medication End Desc,
         Case When @LocalSortColumn = 3 And @LocalSortDirection = 0 then LastNdcAssigned End Asc,
         Case When @LocalSortColumn = 3 And @LocalSortDirection = 1 then LastNdcAssigned End Desc,
         Case When @LocalSortColumn = 4 And @LocalSortDirection = 0 then CurrLoc End Asc,
         Case When @LocalSortColumn = 4 And @LocalSortDirection = 1 then CurrLoc End Desc,
         Case When @LocalSortColumn = 5 And @LocalSortDirection = 0 then StockLocation End Asc,
         Case When @LocalSortColumn = 5 And @LocalSortDirection = 1 then StockLocation End Desc,
         Case When @LocalSortColumn = 6 And @LocalSortDirection = 0 then NumStockBottles End Asc,
         Case When @LocalSortColumn = 6 And @LocalSortDirection = 1 then NumStockBottles End Desc,
         Case When @LocalSortColumn = 7 And @LocalSortDirection = 0 then CanisterSn End Asc,
         Case When @LocalSortColumn = 7 And @LocalSortDirection = 1 then CanisterSn End Desc,
         Case When @LocalSortColumn = 8 And @LocalSortDirection = 0 then Model End Asc,
         Case When @LocalSortColumn = 8 And @LocalSortDirection = 1 then Model End Desc,
         Case When @LocalSortColumn = 9 And @LocalSortDirection = 0 then TcdStatusReason End Asc,
         Case When @LocalSortColumn = 9 And @LocalSortDirection = 1 then TcdStatusReason End Desc,
         SortPriority Asc, NumAllocatedRxs Desc     -- each sort will be additionally sorted by Priority then NumAllocatedRxs
         Option (Recompile);

   End -- Station Id Is Not Null
        
-----------------------------------
-- Else No station id was passed in
-----------------------------------
Else

   Begin
        
         --------------------------------------------------------------------------
         --Get Forward Stock Locations(F_ROBOTFILL) for canister products
         --------------------------------------------------------------------------
         Set @ForwardStockLocationCursor = Cursor For
         Select
            CONCAT(REPLACE(Mask, '#', '[0-9]'), '%') As Regex
         From #ConfiguredStockLocDefs Where (StockAreaCode = 'F' and StockSubAreaCode = 'F_ROBOTFILL') 

         Open @ForwardStockLocationCursor

         Fetch Next
            From
               @ForwardStockLocationCursor
            Into
               @LocRegex
              
         While @@Fetch_Status = 0
         
         Begin
                   
         Insert Into #NdcForwardStockLocs
            Select
                Distinct INLA.NDC, INLA.LOC
            From 
                InvNdcLocAssoc As INLA With (NoLock)
                INNER JOIN
                #CanisterList As CLF 
                On INLA.Ndc = CLF.LastNdcAssigned                     
            Where 
                 INLA.LOC LIKE @LocRegex
                    
            Fetch Next
            From
                @ForwardStockLocationCursor
            Into
                @LocRegex

           End -- Fetch Status = 0

           Close @ForwardStockLocationCursor
           Deallocate @ForwardStockLocationCursor
   
         -----------------------------------------------------------------------------------------------------
         --Get Any Back Stock Locations for canister products that don't have forward stock location
         -----------------------------------------------------------------------------------------------------
         Set @BackStockLocationCursor = Cursor For
         Select
            CONCAT(REPLACE(Mask, '#', '[0-9]'), '%') As Regex
         From #ConfiguredStockLocDefs Where (StockAreaCode = 'B') 

         Open @BackStockLocationCursor

         Fetch Next
            From
               @BackStockLocationCursor
            Into
               @LocRegex
              
         While @@Fetch_Status = 0
         
         Begin
            
            Insert Into #NdcBackStockLocs
            Select
               Distinct INLA.NDC, INLA.LOC   
            From 
               InvNdcLocAssoc As INLA With (NoLock)
               INNER JOIN
               #CanisterList As CLF 
               On INLA.Ndc = CLF.LastNdcAssigned   
               INNER JOIN #NdcForwardStockLocs As NFSL
               On CLF.LastNdcAssigned <> NFSL.Ndc
            Where 
               INLA.LOC LIKE @LocRegex
                    
            Fetch Next
            From
               @BackStockLocationCursor
            Into
               @LocRegex

           End -- Fetch Status = 0

           Close @BackStockLocationCursor
           Deallocate @BackStockLocationCursor

      ---------------------------------------------------------
      -- Select the final grid entries from the unfiltered list
      -- Show back stock locations only if forward is not available.
      ---------------------------------------------------------
      Select
         Case Status When 'Staged' Then 1 Else 0 End as 'Staged',
         SortPriority,
         Priority,   
         Medication, fmtLastNdcAssigned, CurrLoc,
		 IsNull((Select Top 1 1 From vInvMasterWithUserOverrides As IV with (nolock) where IV.ProductId = c.ProductId And StorageCondCode IN ('F','R')), 0) As IsColdStorage,
		 IsNull((Select Top 1 1 From vInvMasterWithUserOverrides As IV with (nolock) where IV.ProductId = c.ProductId And DeaClassCode > 0), 0) As IsControlledSubstance,
		 IsNull((Select Top 1 1 From InvFlaggedProducts As FP with (nolock) where FP.ProductId = c.ProductId And IsFlaggedFor = 'HAZMAT'), 0) As IsHazHandle,
		 IsNull((Select Top 1 1 From InvFlaggedProducts As FP with (nolock) where FP.ProductId = c.ProductId And IsFlaggedFor = 'HAZDSP'), 0) As IsHazDispose,
		 IsNull((Select Top 1 1 From InvFlaggedProducts As FP with (nolock) where FP.ProductId = c.ProductId And IsFlaggedFor = 'HAZSHP'), 0) As IsHazShip,
		 IsNull((Select Top 1 1 From InvMaster As I with (nolock) where I.ProductId = c.ProductId And CrossContaminationCategory = '2'), 0) As IsPen,
		 IsNull((Select Top 1 1 From InvMaster As I with (nolock) where I.ProductId = c.ProductId And CrossContaminationCategory = '3'), 0) As IsSulfa,
         Case 
            When Exists (Select Loc from #NdcForwardStockLocs Where ndc = LastNdcAssigned) Then (Select Top 1 Loc From #NdcForwardStockLocs Where ndc = LastNdcAssigned)
            When Exists (Select Loc from #NdcBackStockLocs Where ndc = LastNdcAssigned) Then (Select Top 1 Loc From #NdcBackStockLocs Where ndc = LastNdcAssigned)
         Else NULL
         End As StockLocation,
         MaxReplenQty,
         Case When c.CanisterSn = 0 Then 'N/A' Else 'CAN' + Cast(c.CanisterSn as VarChar(20)) End as CanisterSn, 
         Model, IsNull(TcdStatusReason,'') as TcdStatusReason, NumStockBottles, Status, InvPool, TcdSn, TcdAddress, LastNdcAssigned, NumAllocatedRxs,
		 '' as Allergy, '' as Hazardous
      From 
         #CanisterList c
      Order By  
         Case When @LocalSortColumn = 0 And @LocalSortDirection = 0 then Status End Asc,
         Case When @LocalSortColumn = 0 And @LocalSortDirection = 1 then Status End Desc,
         Case When @LocalSortColumn = 1 And @LocalSortDirection = 0 then SortPriority End Asc,
         Case When @LocalSortColumn = 1 And @LocalSortDirection = 1 then SortPriority End Desc,
         Case When @LocalSortColumn = 2 And @LocalSortDirection = 0 then Medication End Asc,
         Case When @LocalSortColumn = 2 And @LocalSortDirection = 1 then Medication End Desc,
         Case When @LocalSortColumn = 3 And @LocalSortDirection = 0 then LastNdcAssigned End Asc,
         Case When @LocalSortColumn = 3 And @LocalSortDirection = 1 then LastNdcAssigned End Desc,
         Case When @LocalSortColumn = 4 And @LocalSortDirection = 0 then CurrLoc End Asc,
         Case When @LocalSortColumn = 4 And @LocalSortDirection = 1 then CurrLoc End Desc,
         Case When @LocalSortColumn = 5 And @LocalSortDirection = 0 then StockLocation End Asc,
         Case When @LocalSortColumn = 5 And @LocalSortDirection = 1 then StockLocation End Desc,
         Case When @LocalSortColumn = 6 And @LocalSortDirection = 0 then NumStockBottles End Asc,
         Case When @LocalSortColumn = 6 And @LocalSortDirection = 1 then NumStockBottles End Desc,
         Case When @LocalSortColumn = 7 And @LocalSortDirection = 0 then CanisterSn End Asc,
         Case When @LocalSortColumn = 7 And @LocalSortDirection = 1 then CanisterSn End Desc,
         Case When @LocalSortColumn = 8 And @LocalSortDirection = 0 then Model End Asc,
         Case When @LocalSortColumn = 8 And @LocalSortDirection = 1 then Model End Desc,
         Case When @LocalSortColumn = 9 And @LocalSortDirection = 0 then TcdStatusReason End Asc,
         Case When @LocalSortColumn = 9 And @LocalSortDirection = 1 then TcdStatusReason End Desc,
         SortPriority Asc, NumAllocatedRxs Desc     -- each sort will be additionally sorted by Priority then NumAllocatedRxs

   End -- Station Id Is/Isn't Null

-- Consolidated cleanup of all temp tables created during the procedure body.
Drop Table If Exists
      #CanisterList
    , #AllocatedRxCountsPerProductId
    , #PriorityTcds
    , #PriorityCans
    , #CanisterListFiltered
    , #NdcForwardStockLocs
    , #NdcBackStockLocs
    , #CanistersReadyOrInProcess
    , #CanistersForSubmitQueue
    , #ConfiguredStockLocDefs

End

GO
