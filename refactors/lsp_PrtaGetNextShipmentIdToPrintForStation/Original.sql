/****** Object:  StoredProcedure [dbo].[lsp_PrtaGetNextShipmentIdToPrintForStation]    Script Date: 4/5/2026 8:50:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE Procedure [dbo].[lsp_PrtaGetNextShipmentIdToPrintForStation]
   @StationId VarChar(8)
As
/*
Object Type:
   Stored Procedure

Name:
   lsp_PrtaGetNextShipmentIdToPrintForStation

Version:
   17

Description:
   Return the next shipment Id to print paperwork for given the station Id.

Parameters:
   Input:
      @StationId         - The station Id of the workstation where the shipment has been received at.
   Output:
      None

Return Value:
   None

Comments:
   Each query in this stored procedure is force to run serially using the "MaxDop 1" hint.
	This is done to avoid mass parallel processing when SQL Server chooses a parallel query plan.
	In at least one fielded system, the parallel plan caused the query to run over 1 second and cause the following frequent error:
	   "The query processor could not start the necessary thread resources for parallel query execution."
*/
Set NoCount On
IF OBJECT_ID('tempdb..#AllRxsArrivedForShipmentId') IS NOT NULL DROP TABLE #AllRxsArrivedForShipmentId
IF OBJECT_ID('tempdb..#AllItemsStoredinBins') IS NOT NULL DROP TABLE #AllItemsStoredinBins
IF OBJECT_ID('tempdb..#TopShipmentToPrint') IS NOT NULL DROP TABLE #TopShipmentToPrint
IF OBJECT_ID('tempdb..#ShipmentIdInBin') IS NOT NULL DROP TABLE #ShipmentIdInBin
IF OBJECT_ID('tempdb..#TopShipmentIdInBin') IS NOT NULL DROP TABLE #TopShipmentIdInBin
IF OBJECT_ID('tempdb..#TopShipmentIdInManPack') IS NOT NULL DROP TABLE #TopShipmentIdInManPack
IF OBJECT_ID('tempdb..#TopShipmentIdInSemiPack') IS NOT NULL DROP TABLE #TopShipmentIdInSemiPack
IF OBJECT_ID('tempdb..#TopShipmentIdInAutoPack') IS NOT NULL DROP TABLE #TopShipmentIdInAutoPack
IF OBJECT_ID('tempdb..#CvyRxsArrivedForShipmentId') IS NOT NULL DROP TABLE #CvyRxsArrivedForShipmentId

------------------------------------------------------------------
-- Get Shipment Ids for orders stored in a Bin at this station.
------------------------------------------------------------------
select
	osa.ShipmentId,
	Count(O.orderId) as RxCount
Into
	#ShipmentIdInBin
From 
	OeOrder o With (nolock)
		Left Join
	OeOrderShipmentAssoc osa With (NOLOCK)
		On o.OrderId = osa.OrderId And o.HistoryDtTm = osa.HistoryDtTm
		left Join
	OrdWillCallBin owb With (NOLOCK)
		On o.OrderId = owb.OrderId
		Join
	ShpStationWillCallBinAssoc SB With (NOLOCK)
		On owb.BinId Like (SB.BinIdPrefix + '%')
		Left Join
	PwkPrinterTray ppt With (NOLOCK)
		On ppt.ShipmentId = osa.ShipmentId
Where 
	SB.StationId = @StationId
		And 
	(osa.ShipmentId Is Not Null And ppt.Printer Is Null)
		And
	o.OrderStatus <> 'Problem'
Group By 
	osa.ShipmentId			  
Option (MaxDop 1);

-----------------------------------------------------------------------------------------
-- Get minimum PriInternal value for each ShipmentId
-----------------------------------------------------------------------------------------

Select b.Shipmentid, b.RxCount, Min(o.PriInternal) as PriInternal
Into #TopShipmentIdInBin
From #ShipmentIdInBin b
	Join OeOrderShipmentAssoc s With (NOLOCK) On b.ShipmentId = s.ShipmentId
	Join OeOrder o With (NOLOCK) On s.OrderId = o.OrderId
Group By 
	b.Shipmentid, b.RxCount
Order By 
	Min(o.PriInternal) asc
Option (MaxDop 1);

-----------------------------------------------------------------------------------------
-- Get Shipment Ids from CvyManPackStationToteQueue (checking for this station ID)
-----------------------------------------------------------------------------------------
Select
	OSA.ShipmentId,
	Min(O.PriInternal) As PriInternal,
	Count(O.OrderId) As RxCount
Into
	#TopShipmentIdInManPack
From 
	CvyManPackStationToteQueue CMP With (NoLock)
		Join
	OrderToteAssoc OTA With (NoLock)
		On OTA.ToteId = CMP.ToteId And OTA.OrderToteStateCode = 'I'
		Join
	OeOrder O With (NoLock)
		On O.OrderId = OTA.OrderId
		Join
	OeOrderShipmentAssoc OSA With (NoLock)
		On OSA.OrderId = O.OrderId And OSA.HistoryDtTm = O.HistoryDtTm
		Left Join
	PwkPrinterTray PPT With (NoLock)
		On PPT.ShipmentId = OSA.ShipmentId
Where
	CMP.StationId = @StationId
		And
	PPT.Printer Is Null                               -- Exclude shipments/packages already having print in tray.
		And
	O.OrderStatus Not In ('Problem', 'Ship Pending')
Group By
	OSA.ShipmentId,
	PriInternal
Order By 
	PriInternal Asc
Option (MaxDop 1);

--------------------------------------------------------------------------------------
--  Totes from CvySemiAutoPackStationToteQueue (checking for this StationId)
--------------------------------------------------------------------------------------
select 
	OSA.ShipmentId,
	Min(O.PriInternal) As PriInternal,
	Count(O.OrderId) As RxCount
Into
	#TopShipmentIdInSemiPack
from 
	CvySemiAutoPackStationToteQueue CSP with (nolock)
		Join
	OrderToteAssoc OTA With (NoLock)
		On OTA.ToteId = CSP.ToteId And OTA.OrderToteStateCode = 'I'
		Join
	OeOrder O With (NoLock)
		On O.OrderId = OTA.OrderId
		Join
	OeOrderShipmentAssoc OSA With (NoLock)
		On OSA.OrderId = O.OrderId And OSA.HistoryDtTm = O.HistoryDtTm
		Left Join
	PwkPrinterTray PPT With (NoLock)
		On PPT.ShipmentId = OSA.ShipmentId
Where
	CSP.StationId = @StationId
		And
	PPT.Printer Is Null                               -- Exclude shipments/packages already having print in tray.
		And
	O.OrderStatus Not In ('Problem', 'Ship Pending')
Group By
	OSA.ShipmentId,
	PriInternal
Order By 
	PriInternal Asc
Option (MaxDop 1);

----------------------------------------------------------------------------------
-- Totes from CvyAutoPackStationVialQueue (checking for this StationId)
----------------------------------------------------------------------------------
Select
	OSA.ShipmentId,
	Min(O.PriInternal) As PriInternal,
	Count(OSA.OrderId) As RxCount
Into
	#TopShipmentIdInAutoPack
From 
	CvyAutoPackStationVialQueue CAP With (NoLock)
		Join
	OeOrder O With (NoLock)
		On O.OrderId = CAP.OrderId And O.HistoryDtTm = CAP.HistoryDtTm
		Join
	OeOrderShipmentAssoc OSA With (NoLock)
		On OSA.OrderId = O.OrderId And OSA.HistoryDtTm = O.HistoryDtTm
		Left Join
	PwkPrinterTray PPT With (NoLock)
		On PPT.ShipmentId = OSA.ShipmentId
Where
	CAP.StationId = @StationId
		And
	PPT.Printer Is Null                                                 -- Only include shipments/packages not yet having print to tray.
		And
	O.OrderStatus Not In ('Problem', 'Ship Pending')                    -- Only include count of Rxs not in status Problem or Ship Pending.
Group By
	OSA.ShipmentId
Order By
	PriInternal Asc
Option (MaxDop 1);

----------------------------------------------------------------------------
-- Get the all shipments that has all Rx arrived for this station, from the 3 temp tables. 
----------------------------------------------------------------------------
select 
	ShipmentId, 
	Min(t.PriInternal) As PriInternal, 
	Sum(t.RxCount) as SumOfAllRxsArrivedForShipmentId
Into 
	#CvyRxsArrivedForShipmentId
From 
	(
		Select * From #TopShipmentIdInManPack With (NoLock)
		union all
		Select * From #TopShipmentIdInSemiPack With (NoLock)
		union all
		Select * From #TopShipmentIdInAutoPack With (NoLock)
	) t
Group by 
	t.ShipmentId
Option (MaxDop 1);

----------------------------------------------------------------------------
-- Get the shipments that has all Rx currently stored
-- This may happen when a shipment contains:
--    3 total items
--    2 of which are Stored
--    Non-Stored item is cancelled
----------------------------------------------------------------------------
Select 
	t.ShipmentId, 
	t.PriInternal, 
	t.RxCount As SumOfAllRxsArrivedForShipmentId
Into
	#AllItemsStoredinBins
From 
    #TopShipmentIdInBin t With (NoLock)
	Left Join OeWorkflowStepInProgress As WFS With (NOLOCK) On WFS.ObjectKey = STR(t.ShipmentId)
Where 
((Select COUNT(OSA.Orderid) from OeOrderShipmentAssoc As OSA with (NoLock) Left Join 
	  OeOrderHistory As OH With (NoLock) On OSA.OrderId = OH.OrderID And OSA.HistoryDtTm = OH.HistoryDtTm where ShipmentId = t.ShipmentId And IsNull(OH.OrderStatus,'') <> 'Canceled') = t.RxCount)
    And WFS.ObjectKey Is NULL
Order By PriInternal Asc
Option (MaxDop 1);

----------------------------------------------------------------------------
-- Get the shipments that has all Rx arrived for this station, having remaining Rxs stored away in Bin from the TopShipmentIdInBin Table
----------------------------------------------------------------------------
Select 
	c.ShipmentId, 
	c.PriInternal,
	Sum(c.SumOfAllRxsArrivedForShipmentId+ IsNULL(t.RxCount,0)) As SumOfAllRxsArrivedForShipmentId 
Into
	#AllRxsArrivedForShipmentId
From 
	#CvyRxsArrivedForShipmentId c  With (NoLock)
	Left Join #TopShipmentIdInBin t With (NoLock) On c.ShipmentId = t.ShipmentId
	Left Join OeWorkflowStepInProgress As WFS With (NOLOCK) On WFS.ObjectKey = STR(c.ShipmentId)
Where WFS.ObjectKey Is NULL
Group by  
	c.ShipmentId, c.PriInternal
Option (MaxDop 1);

----------------------------------------------------------------------------
-- Get the highest priority shipment for this station from the #AllRxsArrivedForShipmentId temp table
----------------------------------------------------------------------------
select Top 1 
	ShipmentId, 
	SumOfAllRxsArrivedForShipmentId as RxCount
Into 
	#TopShipmentToPrint 
From 
	(
		Select * From #AllRxsArrivedForShipmentId With (NoLock) 
			Union all
		Select * From #AllItemsStoredinBins With (NoLock)
	 ) tt
Group by 
	ShipmentId, SumOfAllRxsArrivedForShipmentId, PriInternal
Having
((Select COUNT(OSA.Orderid) from OeOrderShipmentAssoc As OSA with (NoLock) Left Join 
	  OeOrderHistory As OH With (NoLock) On OSA.OrderId = OH.OrderID And OSA.HistoryDtTm = OH.HistoryDtTm where ShipmentId = tt.ShipmentId And IsNull(OH.OrderStatus,'') <> 'Canceled') = SUM(tt.SumOfAllRxsArrivedForShipmentId))
Order By 
	PriInternal asc
Option (MaxDop 1);

-----------------------------------------------------------------------------------------
-- Get all of the orders in the group, so that Documents for the shipment can be printed.
-----------------------------------------------------------------------------------------
select
	OSA.ShipmentId,
	O.OrderId,
	O.HistoryDtTm,
	OTA.ToteId,
	OPA.PoNum,
	PPT.Printer,
	PPT.TrayNum 
from
	OeOrder O with (nolock)
		left join
	OrderToteAssoc OTA with (nolock)
		on OTA.OrderId = O.OrderId
		left join
	OeOrderShipmentAssoc OSA with (nolock)
		on OSA.OrderId = O.OrderId and OSA.HistoryDtTm = O.HistoryDtTm
		left join
	OeOrderPoNumAssoc OPA with (nolock)
		on O.OrderId = OPA.OrderId
		left join
	PwkPrinterTray PPT with (nolock)
		on PPT.ShipmentId = OSA.ShipmentId
where
	OSA.ShipmentId = (select ShipmentId from #TopShipmentToPrint with (nolock))
Order By 
	O.PriInternal asc
Option (MaxDop 1);
GO
