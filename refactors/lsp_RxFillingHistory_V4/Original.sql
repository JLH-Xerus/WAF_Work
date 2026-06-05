/****** Object:  StoredProcedure [dbo].[lsp_RxFillingHistory_V4]    Script Date: 5/12/2026 11:42:27 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[lsp_RxFillingHistory_V4]
AS
BEGIN
--PWD-ORSYMPH01

/*

Object Type:
   Stored Procedure

Name:
   lsp_RxFillingHistory_V4


Version:
   1

Description:
   Replaces the inline code used to replace a report previously written  to help stabilize the query plan 

*/


Set NoCount On  

Declare  @StartDate DateTime, @EndDate DateTime 

--Set @StartDate = '<<StartDate>>'
--Set @EndDate = '<<EndDate>>'

Select @StartDate = Cast(Value As datetime) From SysProperty With (NoLock) Where Keyword = 'CustomTask.DSCSAReport' 
--  Insert Into SysProperty (Keyword, Value)  Values ('CustomTask.DSCSAReport','11/25/2024 07:00:00')
Set @EndDate = DateAdd(Hour, DateDiff(hour,0,GetDate()),0)
 

-- Drop Temp Tables if they Exist  
If Object_Id('TempDb..#Unsorted') Is Not Null  
   Drop Table #Unsorted  
If Object_Id('TempDb..#LatestFilled') Is Not Null  
   Drop Table #LatestFilled 
If Object_Id('TempDb..#Grouped') Is Not Null  
   Drop Table #Grouped  
If Object_Id('TempDb..#GTinDisp') Is Not Null  
   Drop Table #GTinDisp
If Object_Id('TempDb..#SerialNums') Is Not Null  
   Drop Table #SerialNums  
If Object_Id('TempDb..#OtherColumns') Is Not Null  
   Drop Table #OtherColumns 
If Object_Id('TempDb..#MultiVialOrders') Is Not Null  
   Drop Table #MultiVialOrders 
If Object_Id('TempDb..#YetToBeFilledMultiVials') Is Not Null  
   Drop Table #YetToBeFilledMultiVials 
   

-- Get all Rxs that were filled during the specified time period, 
-- but only return the latest date filled.
-- This prevents duplicate records for the same Rx that may have been filled multiple times,
-- or that were cancelled and refilled. Only the latest action for each Rx will be returned.
-- Only the latest action will be used to determine if an Rx was Cancelled or not.
-- This applies only to the specified time period; an Rx could be Cancelled in this time period
-- and Filled later, but it will be listed as Cancelled for this query/report.

Select MAX(DateFilled) MaxDateFilled, RxNum
Into #LatestFilled
From (
   Select DateFilled, RxNum
   From  
      OeOrder With (NoLock) 
   Where
      DateFilled > @StartDate And DateFilled <= @EndDate
   Union
   Select DateFilled, RxNum
   From  
      OeOrderHistory With (NoLock) 
   Where
      DateFilled > @StartDate And DateFilled <= @EndDate
) AllRx
Group By RxNum

-- Get additonal info for each Rx 

Select  
   LF.MaxDateFilled, --O.DateFilled,
   O.RxNum,
   O.OrderId,
   O.RefillNum,
   O.Ndc,
   O.StoreNum,
   O.HistoryDtTm,
   TcdSn As RepTcdSn,  
   OLC.LotCode,
   OLC.ExpireDate,
   ILN.SerialNum,
   ILN.Gtin
   
Into #Unsorted
From  
   #LatestFilled LF
   Join
   OeOrder As O With (NoLock) on O.RxNum=LF.RxNum and O.DateFilled=LF.MaxDateFilled 
      Join
   OePatientCust As PC With (NoLock) On O.PatCustId = PC.PatCustId
      Join
   OeGroup as G With (NoLock) On O.GroupNum = G.GroupNum
      Left JOIN 
   OeOrderTcdAssoc As OT With (NoLock)
	  On OT.OrderId = O.OrderId and OT.HistoryDtTm = O.HistoryDtTm
      Left JOIN
   SecUser As S1 With (NoLock) On O.OprFilledBy = S1.OprId
      Left JOIN
   SecUser As S2 With (NoLock) On O.OprVerifiedBy = S2.OprId
      Left JOIN
   Pharmacy as P with (NoLock) on O.PharmacyId = P.PharmacyId
      Left JOIN
   InvPool as I with (NoLock) on P.InvPool = I.Id
      Left JOIN
   OeLotcode as OLC With (NoLock) on O.OrderId = OLC.OrderId and O.HistoryDtTm = OLC.HistoryDtTm
     Left JOIN
   OeSerialNumGTIN as OSG With (NoLock) on O.OrderId = OSG.OrderId and O.HistoryDtTm = OSG.HistoryDtTm
     left JOIN
   InvLpnNdc as ILN With (NoLock) on OSG.InvLpnNdcId = ILN.Id and ILN.LotCode = OLC.LotCode
Where
   OLC.LotCode != 'DISCARD_DATE'
   AND
   O.OrderStatus <> 'Split/MV' 
   And
   O.OrderStatus <> 'Canceled'
      
UNION  

Select  
   LF.MaxDateFilled, --O.DateFilled,  
   O.RxNum,  
   O.OrderId,
   O.RefillNum,  
   O.Ndc,
   O.StoreNum,
   O.HistoryDtTm,
   TcdSn As RepTcdSn,  
   OLC.LotCode, 
   OLC.ExpireDate,  
   ILN.SerialNum,
   ILN.Gtin 
From  
   #LatestFilled LF
   Join
   OeOrderHistory As O With (NoLock) on O.RxNum=LF.RxNum and O.DateFilled=LF.MaxDateFilled 
      Join
   OePatientCust As PC With (NoLock)  
      On O.PatCustId = PC.PatCustId  
      Join 
   OeGroup As G With (NoLock)  
      On O.GroupNum = G.GroupNum
	  Left JOIN 
   OeOrderTcdAssoc As OT With (NoLock)
	  On OT.OrderId = O.OrderId and OT.HistoryDtTm = O.HistoryDtTm
      Left JOIN 
   SecUser As S1 With (NoLock)  
      On O.OprFilledBy = S1.OprId  
      Left JOIN 
   SecUser As S2 With (NoLock)  
      On O.OprVerifiedBy = S2.OprId  
      Left JOIN 
   Pharmacy as P with (NoLock) on O.PharmacyId = P.PharmacyId
      Left JOIN 
   InvPool as I with (NoLock) on P.InvPool = I.Id
      Left JOIN 
   OeLotcode as OLC With (NoLock) on O.OrderId = OLC.OrderId and O.HistoryDtTm = OLC.HistoryDtTm
     Left JOIN 
   OeSerialNumGTIN as OSG With (NoLock) on O.OrderId = OSG.OrderId and O.HistoryDtTm = OSG.HistoryDtTm
     Left JOIN 
   InvLpnNdc as ILN With (NoLock) on OSG.InvLpnNdcId = ILN.Id and ILN.LotCode = OLC.LotCode
Where  
   OLC.LotCode != 'DISCARD_DATE'
   AND
   O.OrderStatus <> 'Split/MV'
   And 
   O.OrderStatus <> 'Canceled' 
   And 
   Not (O.OrderStatus = 'Canceled' And O.StatusReason = 'Undo of Pick Up or Shipment')  
       
--Check if any multi vials are filled 
--Remove Multi vials if any of them are not yet filled 
--Add missing multi vial info if all of them are filled
 
Select 
	Distinct Left(OrderId, Len(OrderId) - 3) OrderId
Into
	#MultiVialOrders
From 
	#Unsorted
Where 
	OrderId Like '%[^0-9]%'

--Select * from #MultiVialOrders

--remove multi vial rxs which has Yet to be filled vials

Select 
	* 
Into 
	#YetToBeFilledMultiVials 
From (
	Select 
		Left(O.OrderId, Len(O.OrderId) - 3) OrderId
	From 
		OeOrder As O With(NoLock) 
	Inner Join 
		#MultiVialOrders M 
		On O.OrderId Like M.OrderId+ '%-[0-9][0-9]'
	Where
		O.DateFilled Is Null
		And O.OrderStatus <> 'Canceled' 
	Union 
	Select 
		Left(O.OrderId, Len(O.OrderId) - 3) OrderId 
	From 
		OeOrderHistory As O With(NoLock) 
	Inner Join 
		#MultiVialOrders M 
		On O.OrderId Like M.OrderId + '%-[0-9][0-9]'
	Where 
		O.DateFilled Is Null
		And O.OrderStatus <> 'Canceled' 
		And Not (O.OrderStatus = 'Canceled' And O.StatusReason = 'Undo of Pick Up or Shipment')) A 
	
Delete From #MultiVialOrders  
Where OrderId In (Select OrderId From #YetToBeFilledMultiVials) 

--Delete multivial data from unsorted table 
Delete From #Unsorted 
Where OrderId Like '%[^0-9]%'

--Insert Multivial data of Rxs with all filled vials into Unsorted table
Insert Into #Unsorted
	Select  
	   O.DateFilled,
	   O.RxNum,
	   Left(O.OrderId, Len(O.OrderId) - 3) OrderId,
	   O.RefillNum,
	   O.Ndc,
	   O.StoreNum,
	   O.HistoryDtTm,
	   TcdSn As RepTcdSn,  
	   OLC.LotCode,
	   OLC.ExpireDate,
	   ILN.SerialNum,
	   ILN.Gtin
	From  
	   (Select O.* 
		From
			#MultiVialOrders M
			Join
			OeOrder As O With (NoLock) On O.OrderId Like M.OrderId + '-[0-9][0-9]'
			Where
			O.DateFilled Is Not Null
	  	    And
			O.OrderStatus <> 'Canceled') As O
		  Join
	   OePatientCust As PC With (NoLock) On O.PatCustId = PC.PatCustId
		  Join
	   OeGroup as G With (NoLock) On O.GroupNum = G.GroupNum
		  Left JOIN 
	   OeOrderTcdAssoc As OT With (NoLock)
		  On OT.OrderId = O.OrderId and OT.HistoryDtTm = O.HistoryDtTm
		  Left JOIN
	   SecUser As S1 With (NoLock) On O.OprFilledBy = S1.OprId
		  Left JOIN
	   SecUser As S2 With (NoLock) On O.OprVerifiedBy = S2.OprId
		  Left JOIN
	   Pharmacy as P with (NoLock) on O.PharmacyId = P.PharmacyId
		  Left JOIN
	   InvPool as I with (NoLock) on P.InvPool = I.Id
		  Left JOIN
	   OeLotcode as OLC With (NoLock) on O.OrderId = OLC.OrderId and O.HistoryDtTm = OLC.HistoryDtTm
		 Left JOIN
	   OeSerialNumGTIN as OSG With (NoLock) on O.OrderId = OSG.OrderId and O.HistoryDtTm = OSG.HistoryDtTm
		 left JOIN
	   InvLpnNdc as ILN With (NoLock) on OSG.InvLpnNdcId = ILN.Id and ILN.LotCode = OLC.LotCode
	Where
	   OLC.LotCode != 'DISCARD_DATE'

	UNION  

	Select  
	   O.DateFilled,  
	   O.RxNum,  
	   Left(O.OrderId, Len(O.OrderId) - 3) OrderId,
	   O.RefillNum,  
	   O.Ndc,
	   O.StoreNum,
	   O.HistoryDtTm,
	   TcdSn As RepTcdSn,  
	   OLC.LotCode, 
	   OLC.ExpireDate,  
	   ILN.SerialNum,
	   ILN.Gtin 
	From  
	   (Select O.* 
		From
			#MultiVialOrders M
			Join
			OeOrderHistory As O With (NoLock) On O.OrderId Like M.OrderId + '-[0-9][0-9]'
			Where  
	  	    O.DateFilled Is Not Null
		    And 
		    O.OrderStatus <> 'Canceled' 
		    And Not (O.OrderStatus = 'Canceled' And O.StatusReason = 'Undo of Pick Up or Shipment')) As O
		  Join
	   OePatientCust As PC With (NoLock)  
		  On O.PatCustId = PC.PatCustId  
		  Join 
	   OeGroup As G With (NoLock)  
		  On O.GroupNum = G.GroupNum
		  Left JOIN 
	   OeOrderTcdAssoc As OT With (NoLock)
		  On OT.OrderId = O.OrderId and OT.HistoryDtTm = O.HistoryDtTm
		  Left JOIN 
	   SecUser As S1 With (NoLock)  
		  On O.OprFilledBy = S1.OprId  
		  Left JOIN 
	   SecUser As S2 With (NoLock)  
		  On O.OprVerifiedBy = S2.OprId  
		  Left JOIN 
	   Pharmacy as P with (NoLock) on O.PharmacyId = P.PharmacyId
		  Left JOIN 
	   InvPool as I with (NoLock) on P.InvPool = I.Id
		  Left JOIN 
	   OeLotcode as OLC With (NoLock) on O.OrderId = OLC.OrderId and O.HistoryDtTm = OLC.HistoryDtTm
		 Left JOIN 
	   OeSerialNumGTIN as OSG With (NoLock) on O.OrderId = OSG.OrderId and O.HistoryDtTm = OSG.HistoryDtTm
		 Left JOIN 
	   InvLpnNdc as ILN With (NoLock) on OSG.InvLpnNdcId = ILN.Id and ILN.LotCode = OLC.LotCode
	Where  
	   OLC.LotCode != 'DISCARD_DATE'

--Select Distinct OrderId From #Unsorted where OrderId  in (Select OrderId from #MultiVialOrders)	


--Get SerialNums grouped by RxNum,LotCode, ExpireDate

Select  
   RxNum,
   Max(OrderId) OrderId,
   LotCode + '_' + Convert(varchar(10),Min(ExpireDate) ,20) + '_' + COALESCE((Select STRING_AGG(Value, ',')  From (Select Distinct Value From String_Split(STRING_AGG(Cast(COALESCE(SerialNum,'') As varchar(max)), ','),',')Where value <> '') As SerialNum ),'') LotCode_ExpireDate_SerialNum  
Into #SerialNums
From #Unsorted
Group By RxNum,LotCode

--Get Gtin, DispenserSN group by RxNum

Select  
   RxNum,
   Max(OrderId) OrderId,
   --COALESCE((Select STRING_AGG(Value, ',')  From (Select Distinct Value From String_Split(STRING_AGG(COALESCE(Cast(Gtin As varchar(max)), ''), ','),',') Where value <> '') As Gtin),'') Gtin,
   Min(Gtin) Gtin,
   COALESCE((Select STRING_AGG(Value, ',')  From (Select Distinct Value From String_Split(STRING_AGG(COALESCE(Cast(RepTcdSn As varchar(max)), ''), ','),',') Where value <> '') As RepTcdSn),'') DispenserSN
Into #GTinDisp
From #Unsorted
Group By RxNum

--All Other columns group by rxnum

Select  
   Max(MaxDateFilled) As DateFilled,  
   RxNum,
   Max(OrderId) As OrderId,
   Max(RefillNum) As RefillNum, 
   Max(Ndc) As Ndc,
   Max(StoreNum) As StoreNum,
   MAX(HistoryDtTm) as HistoryDtTm
Into #OtherColumns
From #Unsorted
Group By RxNum
-- We need to group by RxNum 

Select  
   DateFilled,  
   A.RxNum,
   A.OrderId,
   RefillNum, 
   Ndc,
   StoreNum,
   HistoryDtTm,
   (Select STRING_AGG(Value, ';')  From (Select Distinct Value From String_Split(STRING_AGG(COALESCE(Cast(LotCode_ExpireDate_SerialNum As varchar(max)), ''), ';'),';') Where value <> '') As LotCode_ExpireDate_SerialNum) LotCode_ExpireDate_SerialNum, 
   Min(Gtin) Gtin,
   (Select STRING_AGG(Value, ',')  From (Select Distinct Value From String_Split(STRING_AGG(COALESCE(Cast(C.DispenserSN As varchar(max)), ''), ','),',') Where value <> '') As DispenserSN) DispenserSN
Into #Grouped
From #OtherColumns A
Left Join #SerialNums B
On A.RxNum = B.RxNum
Left Join #GTinDisp C
On A.RxNum = C.RxNum
Group By DateFilled,  
   A.RxNum,
   A.OrderId,
   RefillNum, 
   Ndc,
   StoreNum,
   HistoryDtTm

Select 
	DateFilled,
	OrderId,
	Ndc,
	StoreNum,
	LotCode_ExpireDate_SerialNum,
	Gtin,
	DispenserSN,
	(Select Top 1 ExpireDate From OeLotCode OL With (Nolock) Where OL.OrderId = G.OrderId And OL.HistoryDtTm = G.HistoryDtTm Order By ExpireDate) DisacardDate
From #Grouped G
Where DateFilled > @StartDate And DateFilled <= @EndDate
Order By DateFilled, OrderId

--Select * from #Grouped Where OrderId in (Select OrderId from #MultiVialOrders)

-- Drop Temp Tables if they Exist  
If Object_Id('TempDb..#Unsorted') Is Not Null  
   Drop Table #Unsorted  
If Object_Id('TempDb..#LatestFilled') Is Not Null  
   Drop Table #LatestFilled 
If Object_Id('TempDb..#Grouped') Is Not Null  
   Drop Table #Grouped  
If Object_Id('TempDb..#GTinDisp') Is Not Null  
   Drop Table #GTinDisp
If Object_Id('TempDb..#SerialNums') Is Not Null  
   Drop Table #SerialNums 
If Object_Id('TempDb..#OtherColumns') Is Not Null  
   Drop Table #OtherColumns 
If Object_Id('TempDb..#MultiVialOrders') Is Not Null  
   Drop Table #MultiVialOrders 
If Object_Id('TempDb..#YetToBeFilledMultiVials') Is Not Null  
   Drop Table #YetToBeFilledMultiVials 
   

END             
GO
