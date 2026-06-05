/****** Object:  StoredProcedure [dbo].[lsp_RxFillingHistory_V4]    Script Date: 5/12/2026 9:35:07 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   Procedure [dbo].[lsp_RxFillingHistory_V4]
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
 
SET NOCOUNT ON 
Declare  @StartDate DateTime, @EndDate DateTime     
--Set @StartDate = '<<StartDate>>'  
--Set @EndDate = '<<EndDate>>'    
Select @StartDate = Cast(Value As datetime) From SysProperty With (NoLock) Where Keyword = 'CustomTask.DSCSAReport'   
--select @StartDate
--  Insert Into SysProperty (Keyword, Value)  Values ('CustomTask.DSCSAReport','11/25/2024 07:00:00')  
Set @EndDate = DateAdd(Hour, DateDiff(hour,0,GetDate()),0)  
--select @EndDate
Select MAX(DateFilled) MaxDateFilled, RxNum  
Into #LatestFilled  From 
	(     
		Select DateFilled, RxNum     From          OeOrder With (NoLock)      Where        DateFilled > @StartDate And DateFilled <= @EndDate     
		Union     
		Select DateFilled, RxNum     From          OeOrderHistory With (NoLock)      Where        DateFilled > @StartDate And DateFilled <= @EndDate 
	) AllRx  
	Group By RxNum    

Select       
	LF.MaxDateFilled, 
	--O.DateFilled,     
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
	From #LatestFilled LF     
	Join     
	OeOrder As O With (NoLock) on O.RxNum=LF.RxNum 
	and O.DateFilled=LF.MaxDateFilled         
	Join     OePatientCust As PC With (NoLock) On O.PatCustId = PC.PatCustId        
	Join     OeGroup as G With (NoLock) On O.GroupNum = G.GroupNum        
	Left JOIN      OeOrderTcdAssoc As OT With (NoLock)     On OT.OrderId = O.OrderId 
	and OT.HistoryDtTm = O.HistoryDtTm        
	Left JOIN     SecUser As S1 With (NoLock) On O.OprFilledBy = S1.OprId        
	Left JOIN     SecUser As S2 With (NoLock) On O.OprVerifiedBy = S2.OprId        
	Left JOIN     Pharmacy as P with (NoLock) on O.PharmacyId = P.PharmacyId        
	Left JOIN     InvPool as I with (NoLock) on P.InvPool = I.Id        
	Left JOIN     OeLotcode as OLC With (NoLock) on O.OrderId = OLC.OrderId and O.HistoryDtTm = OLC.HistoryDtTm       Left JOIN     OeSerialNumGTIN as OSG With (NoLock) on O.OrderId = OSG.OrderId 
	and O.HistoryDtTm = OSG.HistoryDtTm       
	left JOIN     InvLpnNdc as ILN With (NoLock) on OSG.InvLpnNdcId = ILN.Id and ILN.LotCode = OLC.LotCode  Where     OLC.LotCode != 'DISCARD_DATE'     AND     O.OrderStatus <> 'Split/MV'      And     O.OrderStatus <> 'Canceled'          
union
Select       
	LF.MaxDateFilled, 
	--O.DateFilled,       
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
	From       #LatestFilled LF     
	Join     OeOrderHistory As O With (NoLock) on O.RxNum=LF.RxNum and O.DateFilled=LF.MaxDateFilled         
	Join     OePatientCust As PC With (NoLock)          On O.PatCustId = PC.PatCustId          
	Join      OeGroup As G With (NoLock)          On O.GroupNum = G.GroupNum     
	Left JOIN      OeOrderTcdAssoc As OT With (NoLock)     On OT.OrderId = O.OrderId and OT.HistoryDtTm = O.HistoryDtTm        
	Left JOIN      SecUser As S1 With (NoLock)          On O.OprFilledBy = S1.OprId          
	Left JOIN      SecUser As S2 With (NoLock)          On O.OprVerifiedBy = S2.OprId          
	Left JOIN      Pharmacy as P with (NoLock) on O.PharmacyId = P.PharmacyId        
	Left JOIN      InvPool as I with (NoLock) on P.InvPool = I.Id        
	Left JOIN      OeLotcode as OLC With (NoLock) on O.OrderId = OLC.OrderId and O.HistoryDtTm = OLC.HistoryDtTm       
	Left JOIN      OeSerialNumGTIN as OSG With (NoLock) on O.OrderId = OSG.OrderId and O.HistoryDtTm = OSG.HistoryDtTm       
	Left JOIN      InvLpnNdc as ILN With (NoLock) on OSG.InvLpnNdcId = ILN.Id and ILN.LotCode = OLC.LotCode  Where       OLC.LotCode != 'DISCARD_DATE'     AND     O.OrderStatus <> 'Split/MV'     And      O.OrderStatus <> 'Canceled'      And      Not (O.OrderStatus = 'Canceled' And O.StatusReason = 'Undo of Pick Up or Shipment')   
 
END 
GO
