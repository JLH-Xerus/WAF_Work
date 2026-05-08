--/
CREATE PROCEDURE lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs 
	@StoreNum VarChar(19),
	@NextDayOrderStartTime VarChar(10)
As
/*
Object Type:
   Stored Procedure

Name:
   lsp_SrtGetNumOfStoresWithAndWithoutSorterLocs

Version:
   6

Description:
   Retrieves the number of stores with and without sorter locations.

Parameters:
   Input:
      @StoreNum                  -
      @NextDayOrderStartTime     -
   Output:
      None

Return Value:
   None

Comments:
   None
*/
Set NoCount On

IF OBJECT_ID('tempdb..#StoreForCurrentDayOrders') IS NOT NULL
	DROP TABLE #StoreForCurrentDayOrders

Declare @CurrentDate DateTime
Set @CurrentDate = Convert(Date,GetDate())

Begin

	-----------------------------------------------------------------------------------------------------------------------------------
	-- Find the stores for the "current day" Orders for non-refrigerated products that have not been sorted yet by the package sorter
	-- and place them in temp table #CurrentDayOrders
	-----------------------------------------------------------------------------------------------------------------------------------
	Select
		Distinct O.StoreNum
	Into #StoreForCurrentDayOrders
	From
		OeOrder As O With (NOLOCK)
			Join
		OeOrderSecondaryData As O2 With (NOLOCK)
			On O.OrderId = O2.OrderId And O.HistoryDtTm = O2.HistoryDtTm
			Left Join 
		vInvMasterWithUserOverrides As I With (NOLOCK)
			On O.ProductId = I.ProductId
	Where
		(TRIM(O.StoreNum) = TRIM(@StoreNum) Or @StoreNum = '')
			And
		I.StorageCondCode <> 'R'   -- (Not refrigerated)
			And
		--Order's Promised Date/Time is LESS than CurrentDate + 1 day + INI[OrderGovernor]NextDayOrderStartTime
		O2.PlanPickUpDtTm < DATEADD(day,1,@CurrentDate) + @NextDayOrderStartTime
			And
		O.OrderStatus In
		(
			'Pending','Scheduled','Problem',
			'Out For Central Fill','Split-DD','Split-MV',
			'Split/MV','Counting','Suspended','Partial Counted',
			'Counted','Vial Wait','Partial Filled','Filled',
			'Checked','Verifying','Verified','In Bin',
			'Ready For Pick Up','Ready To Ship','Ship Pending'
		)
			And Not
		-- Exclude Rxs in any "Pending" status prior to "Pending; Awaiting Governor Allocation" status.                                      
		(O.OrderStatus = 'Pending' And Not (O.StatusReason = 'Awaiting Governor Allocation'))

	--------------------------------------------------------------------------------
	-- Main Query
	--   Get number of stores per each priority as defined by each Union statement
	--------------------------------------------------------------------------------
	Select
		Priority, NumOfStores
	From
	(
		--Unassigned store, Active Orders in the system (VERY HIGH PRIORITY)
		Select
			1 As Priority, Count(*) As NumOfStores
		From
			(
				-- All existing stores that are NOT assigned to sorter locations and there are active Orders for these stores
				Select
					Distinct SEP.StoreNum
				From
					SysEndPharmacy As SEP With (NOLOCK)
				Where
					(TRIM(SEP.StoreNum) = TRIM(@StoreNum) Or @StoreNum = '')
				And SEP.StoreNum Not In 
				(
					Select Distinct SEP.StoreNum
					From SrtSorterLoc AS SL With (NOLOCK) 
					Left Join SysEndPharmacyShpCarrierAssoc AS SPSC With (NoLock) ON SPSC.CarrierCode = SL.CarrierCode
					Left Join SysEndPharmacy AS SEP With (NoLock) ON SEP.Id = SPSC.SysEndPharmacyId
					Where SEP.StoreNum Is Not Null
				)					
				And SEP.StoreNum In (Select StoreNum From #StoreForCurrentDayOrders)
			) As T1

		Union

			--Assigned store, No tote assigned (HIGH PRIORITY)
			Select
				2 As Priority, Count(*) As NumOfStores
			From
				(
					Select Distinct SEP.StoreNum As StoreNum
					From	  SrtSorterLoc As SL With (NOLOCK)
					Left JOIN SysEndPharmacyShpCarrierAssoc AS SPSC With (NoLock) ON SPSC.CarrierCode = SL.CarrierCode
					Left JOIN SysEndPharmacy AS SEP With (NoLock) On SEP.Id = SPSC.SysEndPharmacyId 
					Where 
						(TRIM(SEP.StoreNum) = TRIM(@StoreNum) Or @StoreNum = '')
					And SL.CarrierCode Is Not Null
					And SL.HasTote = 0
				) As T2

		Union

			--Assigned store, Full Tote (MEDIUM PRIORITY)
			Select
				3 As Priority, Count(*) As NumOfStores
			From
				(
					Select Distinct SEP.StoreNum
					From SrtSorterLoc As SL With (NOLOCK)
					Left Join SysEndPharmacyShpCarrierAssoc AS SPSC With (NoLock) ON SPSC.CarrierCode = SL.CarrierCode
					Left Join SysEndPharmacy As SEP With (NoLock) On SEP.Id = SPSC.SysEndPharmacyId
					Where
						(TRIM(SEP.StoreNum) = Trim(@StoreNum) Or @StoreNum = '')
					And SL.CarrierCode Is Not Null
					And SEP.StoreNum Is Not Null
					And SL.IsFull = 1
				) As T3

		Union

			--Unassigned store, No active Orders in the system (LOW PRIORITY)
			Select
				4 As Priority, Count(*) As NumOfStores
			From
				(
					-- All existing stores that are NOT assigned to sorter locations and there are NO active Orders for these stores
					Select Distinct SEP.StoreNum
					From SysEndPharmacy As SEP With (NOLOCK)
					Where
						(TRIM(SEP.StoreNum) = TRIM(@StoreNum) Or @StoreNum = '')
					And SEP.StoreNum Not In (Select StoreNum From #StoreForCurrentDayOrders )
					AND	SEP.StoreNum Not In 
					(
						Select Distinct SEP.StoreNum
						From SrtSorterLoc AS SL With (NOLOCK) 
						Left Join SysEndPharmacyShpCarrierAssoc AS SPSC With (NoLock) ON SPSC.CarrierCode = SL.CarrierCode
						Left Join SysEndPharmacy AS SEP With (NoLock) ON SEP.Id = SPSC.SysEndPharmacyId
						Where SEP.StoreNum Is Not Null
					)
							
				) As T4

	) As MainQuery

	Order By
		Priority

End

/
