--/
CREATE PROCEDURE lsp_SrtGetShipToteIfExists 
	@ShipToteId varchar(22)
As
------------------------------------------------------------------------------------------------------------------------------------------------------
-- Procedure:
--        lsp_SrtGetShipToteIfExists
--
-- Version:
--        26
--
-- Description:
--        Retrieve a shipping tote's data that is pertinent to the tote's display status on the Package Sorter Status form.
--
-- Parameters:
--        @ShipToteId        - The ID of the shipping tote for which to retreive the data.
--
-- Return Value:
--        None
--        The data is returned as a recordset of 1 row.
--
-- Comments:
--        This stored procedure is called by the Package Sorter Status display.
--
------------------------------------------------------------------------------------------------------------------------------------------------------
Set NoCount On

Declare @HasExceptionPackages Bit
Declare @ServiceCode VarChar(50)
Declare @ShipToteIdentifier varchar(22)


-- If the scanned ShipToteId is an ABC Tote Id the scanned value is stored in the StaticToteBarcode fiel
If Exists
	(
		Select Id From SrtShipTote With (NoLock)
		Where StaticToteBarcode = @ShipToteId
	)
	Set @ShipToteIdentifier = (
		Select Id From SrtShipTote With (NoLock)
		Where StaticToteBarcode = @ShipToteId)	
Else
	Set @ShipToteIdentifier = @ShipToteId

---------------------------------------------------------------------------------
-- Retrieve an indication of whether or not the tote contains exception packages.
---------------------------------------------------------------------------------
If Exists
	(
		Select
			*
		From
			SrtShipToteShipmentAssoc TS With (NoLock)
				Join
			OeOrderShipmentAssoc OS With (NoLock)
					On OS.ShipmentId = TS.ShipmentId
				Join
			OeOrder O With (NoLock)
				On O.OrderId = OS.OrderId And O.HistoryDtTm = OS.HistoryDtTm
		Where
			TS.ShipToteId = @ShipToteIdentifier
			And
			O.OrderStatus = 'Problem'  -- Active Rxs in "Problem" Status are considered Exceptions.

		Union

		Select
			*
		From
			SrtShipToteShipmentAssoc TS With (NoLock)
				Join
			OeOrderShipmentAssoc OS With (NoLock)
					On OS.ShipmentId = TS.ShipmentId
				Join
			OeOrderHistory O With (NoLock)
				On O.OrderId = OS.OrderId And O.HistoryDtTm = OS.HistoryDtTm
		Where
			TS.ShipToteId = @ShipToteIdentifier
			And
			O.OrderStatus = 'Canceled'  -- Active Rxs in "Canceled" Status are considered Exceptions.
	)
	Set @HasExceptionPackages = 1
Else
	Set @HasExceptionPackages = 0

-------------------------------------------------------------------
-- Retrieve the Service Code associated with packages in this tote.
-------------------------------------------------------------------
Select
	Top 1
		@ServiceCode = S.ServiceCode
From
	SrtShipTote T With (NoLock)
		Join
	SrtShipToteShipmentAssoc TS With (NoLock)
		On TS.ShipToteId = T.Id
		Join
	ShpShipment S With (NoLock)
			On S.Id = TS.ShipmentId
Where
	T.Id = @ShipToteIdentifier
	And
	T.CarrierCode Is Not Null

------------------------------------------------
-- Retrieve and return the Shipping Tote's Data.
------------------------------------------------
Set NoCount Off

-- First check if Tote is in SrtShipTote, if it's not check ShpManifest
If Exists
	(
		Select T.Id
		From
			SrtShipTote T With (NoLock) 
		--		Left Join
		--	ShpCarrier C WITH (NoLock)
		--		On C.Code = T.CarrierCode
		--		Left Join
		--	SrtSorterLoc L With (NoLock)
		--		On L.ShipToteId = T.Id
		Where
			T.Id = @ShipToteIdentifier
	)
	Begin
		Select
			T.Id,
			RTrim(IsNull(T.CarrierCode, '')) As CarrierCode,
			IsNull(C.Name, '') as CarrierName,
			T.Status,
			T.NumOfPkgs,
			IsNull(L.Id,'') As SorterLocId,
			@HasExceptionPackages As HasExceptionPackages,
			IsNull(T.ServiceCode, '') As ServiceCode,
			IsNull(T.TargetSorterLocId, '') As TargetSorterLocId,
			IsNull(T.DeliveryToteId, '') As DeliveryToteId
		From
			SrtShipTote T With (NoLock) 
				Left Join
			ShpCarrier C WITH (NoLock)
				On C.Code = T.CarrierCode
				Left Join
			SrtSorterLoc L With (NoLock)
				On L.ShipToteId = T.Id
		Where
			T.Id = @ShipToteIdentifier
	End
Else
	Begin
		Select
			SM.ShipToteId As Id,
			RTrim(IsNull(SM.CarrierCode, '')) As CarrierCode,
			IsNull(SC.Name, '') as CarrierName,
			'' As Status,
			(Select Count(ShipToteId) From ShpManifest With (NoLock) Where ShipToteId = @ShipToteIdentifier) As NumOfPkgs,
			'' As SorterLocId,
			@HasExceptionPackages As HasExceptionPackages,
			IsNull(SM.ServiceCode, '') As ServiceCode,
			'' As TargetSorterLocId,
			IsNull(SM.DeliveryToteId, '') As DeliveryToteId
		From
			ShpManifest SM With (NoLock)
				Left Join
			ShpCarrier SC With (NoLock)
				On SC.Code = SM.CarrierCode
		Where
			SM.ShipToteId = @ShipToteIdentifier
	End

Select
	ShipmentId
From
	SrtShipToteShipmentAssoc
Where
	ShipToteId = @ShipToteIdentifier


/
