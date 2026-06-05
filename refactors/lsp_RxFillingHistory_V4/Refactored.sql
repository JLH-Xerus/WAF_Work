SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[lsp_RxFillingHistory_V4]
As
Begin
--PWD-ORSYMPH01

/*

Object Type:
   Stored Procedure

Name:
   lsp_RxFillingHistory_V4

Version:
   2

Description:
   DSCSA filling history report. Returns one row per Rx filled since the
   CustomTask.DSCSAReport watermark, with lot, expiration, serial number,
   GTIN, and dispenser serial detail.

   v2 refactor (2026-06-05):
   - Multi-vial LIKE prefix joins replaced with one scan per order table and an
     equality join on the derived base OrderId.
   - UNION replaced with UNION ALL everywhere; downstream aggregation already dedupes.
   - Removed four left-joined tables that contributed no output columns
     (SecUser x2, Pharmacy, InvPool). OePatientCust and OeGroup inner joins
     converted to EXISTS filters.
   - Aggregate, split, distinct, re-aggregate STRING_AGG round trips replaced with
     dedupe-first aggregation.
   - #Grouped built with 1:1 joins instead of a redundant GROUP BY re-aggregation.
   - Legacy Object_Id temp table checks replaced with DROP TABLE IF EXISTS.
   - Removed unreachable Canceled/StatusReason predicate.
   - Output contract unchanged: same columns, same names (including the DisacardDate
     spelling), same dedupe and empty-string/NULL semantics.

*/


Set NoCount On

Declare @StartDate DateTime, @EndDate DateTime

Select @StartDate = Cast(Value As datetime) From SysProperty With (NoLock) Where Keyword = 'CustomTask.DSCSAReport'
Set @EndDate = DateAdd(Hour, DateDiff(Hour, 0, GetDate()), 0)


-- v2: DROP TABLE IF EXISTS replaces the legacy Object_Id checks
Drop Table If Exists #Unsorted
Drop Table If Exists #LatestFilled
Drop Table If Exists #Grouped
Drop Table If Exists #GTinDisp
Drop Table If Exists #SerialNums
Drop Table If Exists #OtherColumns
Drop Table If Exists #MultiVialOrders
Drop Table If Exists #YetToBeFilledMultiVials


-- Latest fill per Rx in the window.
-- v2: UNION ALL. The MAX / GROUP BY on the outer query already collapses duplicates,
-- so the UNION distinct sort was paying for the same dedupe twice.

Select Max(DateFilled) MaxDateFilled, RxNum
Into #LatestFilled
From (
   Select DateFilled, RxNum
   From OeOrder With (NoLock)
   Where DateFilled > @StartDate And DateFilled <= @EndDate
   Union All
   Select DateFilled, RxNum
   From OeOrderHistory With (NoLock)
   Where DateFilled > @StartDate And DateFilled <= @EndDate
) AllRx
Group By RxNum

-- v2: RxNum is unique after the GROUP BY; the clustered index gives the joins
-- below a keyed, ordered probe side and accurate cardinality.
Create Unique Clustered Index CX_LatestFilled On #LatestFilled (RxNum)


-- Detail rows for each latest-filled Rx.
-- v2 changes in this statement:
--   SecUser (x2), Pharmacy, and InvPool left joins removed: no output column referenced them.
--   OePatientCust / OeGroup inner joins converted to EXISTS: they contributed no columns and
--   acted only as existence filters. EXISTS cannot multiply rows.
--   OeLotcode made an explicit inner join: the WHERE predicate on OLC.LotCode already
--   discarded outer rows, so the LEFT keyword was misleading the reader, not the optimizer.
--   UNION ALL between the OeOrder and OeOrderHistory branches: cross-branch duplicates
--   are absorbed by the dedupe-first aggregation below.

Select
   LF.MaxDateFilled,
   O.RxNum,
   O.OrderId,
   O.RefillNum,
   O.Ndc,
   O.StoreNum,
   O.HistoryDtTm,
   OT.TcdSn As RepTcdSn,
   OLC.LotCode,
   OLC.ExpireDate,
   ILN.SerialNum,
   ILN.Gtin
Into #Unsorted
From
   #LatestFilled LF
   Join
   OeOrder As O With (NoLock) On O.RxNum = LF.RxNum And O.DateFilled = LF.MaxDateFilled
   Left Join
   OeOrderTcdAssoc As OT With (NoLock) On OT.OrderId = O.OrderId And OT.HistoryDtTm = O.HistoryDtTm
   Join
   OeLotcode As OLC With (NoLock) On OLC.OrderId = O.OrderId And OLC.HistoryDtTm = O.HistoryDtTm
   Left Join
   OeSerialNumGTIN As OSG With (NoLock) On OSG.OrderId = O.OrderId And OSG.HistoryDtTm = O.HistoryDtTm
   Left Join
   InvLpnNdc As ILN With (NoLock) On ILN.Id = OSG.InvLpnNdcId And ILN.LotCode = OLC.LotCode
Where
   OLC.LotCode != 'DISCARD_DATE'
   And O.OrderStatus <> 'Split/MV'
   And O.OrderStatus <> 'Canceled'
   And Exists (Select 1 From OePatientCust PC With (NoLock) Where PC.PatCustId = O.PatCustId)
   And Exists (Select 1 From OeGroup G With (NoLock) Where G.GroupNum = O.GroupNum)

Union All

-- v2: removed the unreachable predicate
-- Not (O.OrderStatus = 'Canceled' And O.StatusReason = 'Undo of Pick Up or Shipment');
-- it can never change the result behind OrderStatus <> 'Canceled'.
Select
   LF.MaxDateFilled,
   O.RxNum,
   O.OrderId,
   O.RefillNum,
   O.Ndc,
   O.StoreNum,
   O.HistoryDtTm,
   OT.TcdSn As RepTcdSn,
   OLC.LotCode,
   OLC.ExpireDate,
   ILN.SerialNum,
   ILN.Gtin
From
   #LatestFilled LF
   Join
   OeOrderHistory As O With (NoLock) On O.RxNum = LF.RxNum And O.DateFilled = LF.MaxDateFilled
   Left Join
   OeOrderTcdAssoc As OT With (NoLock) On OT.OrderId = O.OrderId And OT.HistoryDtTm = O.HistoryDtTm
   Join
   OeLotcode As OLC With (NoLock) On OLC.OrderId = O.OrderId And OLC.HistoryDtTm = O.HistoryDtTm
   Left Join
   OeSerialNumGTIN As OSG With (NoLock) On OSG.OrderId = O.OrderId And OSG.HistoryDtTm = O.HistoryDtTm
   Left Join
   InvLpnNdc As ILN With (NoLock) On ILN.Id = OSG.InvLpnNdcId And ILN.LotCode = OLC.LotCode
Where
   OLC.LotCode != 'DISCARD_DATE'
   And O.OrderStatus <> 'Split/MV'
   And O.OrderStatus <> 'Canceled'
   And Exists (Select 1 From OePatientCust PC With (NoLock) Where PC.PatCustId = O.PatCustId)
   And Exists (Select 1 From OeGroup G With (NoLock) Where G.GroupNum = O.GroupNum)


-- Multi-vial reconciliation.
-- Multi-vial OrderIds carry a -NN suffix; the base id is Left(OrderId, Len(OrderId) - 3).
-- Remove multi-vial orders with any vial still unfilled; re-insert every vial of fully
-- filled multi-vial orders under the base OrderId.

Select
   Distinct Left(OrderId, Len(OrderId) - 3) OrderId
Into
   #MultiVialOrders
From
   #Unsorted
Where
   OrderId Like '%[^0-9]%'

-- v2: the original joined the order tables to #MultiVialOrders with
-- O.OrderId Like M.OrderId + '%-[0-9][0-9]', a non-sargable prefix match that nested-loop
-- scanned the order table once per multi-vial order. Replaced with a single scan of each
-- order table restricted to suffixed OrderIds, then an equality join on the derived base id.
-- This also tightens a latent cross-match: the embedded % let base id ABC match ABC123-01.

Select Distinct V.BaseOrderId As OrderId
Into #YetToBeFilledMultiVials
From (
   Select Left(OrderId, Len(OrderId) - 3) As BaseOrderId
   From OeOrder With (NoLock)
   Where OrderId Like '%-[0-9][0-9]'
     And DateFilled Is Null
     And OrderStatus <> 'Canceled'
   Union All
   Select Left(OrderId, Len(OrderId) - 3)
   From OeOrderHistory With (NoLock)
   Where OrderId Like '%-[0-9][0-9]'
     And DateFilled Is Null
     And OrderStatus <> 'Canceled'
) V
Join #MultiVialOrders M On M.OrderId = V.BaseOrderId

Delete From #MultiVialOrders
Where OrderId In (Select OrderId From #YetToBeFilledMultiVials)

-- Drop per-vial rows from the detail set; qualifying orders are re-inserted in full below.
Delete From #Unsorted
Where OrderId Like '%[^0-9]%'

-- v2: same equality-join replacement and the same join trims as the main detail build.
Insert Into #Unsorted
Select
   O.DateFilled,
   O.RxNum,
   Left(O.OrderId, Len(O.OrderId) - 3) OrderId,
   O.RefillNum,
   O.Ndc,
   O.StoreNum,
   O.HistoryDtTm,
   OT.TcdSn,
   OLC.LotCode,
   OLC.ExpireDate,
   ILN.SerialNum,
   ILN.Gtin
From
   (Select OO.*
    From
       OeOrder As OO With (NoLock)
       Join
       #MultiVialOrders M On M.OrderId = Left(OO.OrderId, Len(OO.OrderId) - 3)
    Where
       OO.OrderId Like '%-[0-9][0-9]'
       And OO.DateFilled Is Not Null
       And OO.OrderStatus <> 'Canceled') As O
   Left Join
   OeOrderTcdAssoc As OT With (NoLock) On OT.OrderId = O.OrderId And OT.HistoryDtTm = O.HistoryDtTm
   Join
   OeLotcode As OLC With (NoLock) On OLC.OrderId = O.OrderId And OLC.HistoryDtTm = O.HistoryDtTm
   Left Join
   OeSerialNumGTIN As OSG With (NoLock) On OSG.OrderId = O.OrderId And OSG.HistoryDtTm = O.HistoryDtTm
   Left Join
   InvLpnNdc As ILN With (NoLock) On ILN.Id = OSG.InvLpnNdcId And ILN.LotCode = OLC.LotCode
Where
   OLC.LotCode != 'DISCARD_DATE'
   And Exists (Select 1 From OePatientCust PC With (NoLock) Where PC.PatCustId = O.PatCustId)
   And Exists (Select 1 From OeGroup G With (NoLock) Where G.GroupNum = O.GroupNum)

Union All

Select
   O.DateFilled,
   O.RxNum,
   Left(O.OrderId, Len(O.OrderId) - 3) OrderId,
   O.RefillNum,
   O.Ndc,
   O.StoreNum,
   O.HistoryDtTm,
   OT.TcdSn,
   OLC.LotCode,
   OLC.ExpireDate,
   ILN.SerialNum,
   ILN.Gtin
From
   (Select OO.*
    From
       OeOrderHistory As OO With (NoLock)
       Join
       #MultiVialOrders M On M.OrderId = Left(OO.OrderId, Len(OO.OrderId) - 3)
    Where
       OO.OrderId Like '%-[0-9][0-9]'
       And OO.DateFilled Is Not Null
       And OO.OrderStatus <> 'Canceled') As O
   Left Join
   OeOrderTcdAssoc As OT With (NoLock) On OT.OrderId = O.OrderId And OT.HistoryDtTm = O.HistoryDtTm
   Join
   OeLotcode As OLC With (NoLock) On OLC.OrderId = O.OrderId And OLC.HistoryDtTm = O.HistoryDtTm
   Left Join
   OeSerialNumGTIN As OSG With (NoLock) On OSG.OrderId = O.OrderId And OSG.HistoryDtTm = O.HistoryDtTm
   Left Join
   InvLpnNdc As ILN With (NoLock) On ILN.Id = OSG.InvLpnNdcId And ILN.LotCode = OLC.LotCode
Where
   OLC.LotCode != 'DISCARD_DATE'
   And Exists (Select 1 From OePatientCust PC With (NoLock) Where PC.PatCustId = O.PatCustId)
   And Exists (Select 1 From OeGroup G With (NoLock) Where G.GroupNum = O.GroupNum)


-- Lot-level rollup: LotCode_ExpireDate_SerialNum per (RxNum, LotCode).
-- v2: the original aggregated serials into a CSV, split the CSV with STRING_SPLIT, deduped
-- the fragments, and re-aggregated. Dedupe the rows first, aggregate once.
-- IsNull(..., '') preserves the v1 trailing-underscore form when a lot has no serials.

Select
   LG.RxNum,
   LG.LotCode + '_' + Convert(varchar(10), LG.MinExpireDate, 20) + '_' + IsNull(SL.SerialList, '') As LotCode_ExpireDate_SerialNum
Into #SerialNums
From (
   Select RxNum, LotCode, Min(ExpireDate) As MinExpireDate
   From #Unsorted
   Group By RxNum, LotCode
) LG
Left Join (
   Select RxNum, LotCode, STRING_AGG(Cast(SerialNum As varchar(max)), ',') As SerialList
   From (Select Distinct RxNum, LotCode, SerialNum From #Unsorted Where SerialNum <> '') DS
   Group By RxNum, LotCode
) SL On SL.RxNum = LG.RxNum And SL.LotCode = LG.LotCode


-- Rx-level GTIN and dispenser serials.
-- v2: same dedupe-first replacement. Min(Gtin) preserved from v1 (the STRING_AGG variant
-- was already commented out in v1).

Select
   A.RxNum,
   A.Gtin,
   IsNull(DS.DispenserSN, '') As DispenserSN
Into #GTinDisp
From (
   Select RxNum, Min(Gtin) As Gtin
   From #Unsorted
   Group By RxNum
) A
Left Join (
   Select RxNum, STRING_AGG(Cast(RepTcdSn As varchar(max)), ',') As DispenserSN
   From (Select Distinct RxNum, RepTcdSn From #Unsorted Where RepTcdSn <> '') D
   Group By RxNum
) DS On DS.RxNum = A.RxNum


-- One row per Rx for the non-aggregated columns. Unchanged from v1.

Select
   Max(MaxDateFilled) As DateFilled,
   RxNum,
   Max(OrderId) As OrderId,
   Max(RefillNum) As RefillNum,
   Max(Ndc) As Ndc,
   Max(StoreNum) As StoreNum,
   Max(HistoryDtTm) As HistoryDtTm
Into #OtherColumns
From #Unsorted
Group By RxNum


-- Final per-Rx assembly.
-- v2: #OtherColumns and #GTinDisp are one row per RxNum, and #SerialNums strings are unique
-- within an Rx because each carries its LotCode prefix, so the v1 GROUP BY plus a second
-- aggregate/split/distinct/re-aggregate round trip was a no-op. Plain joins with one
-- STRING_AGG replace it. NullIf preserves v1's empty-to-NULL behavior for DispenserSN.

Select
   A.DateFilled,
   A.RxNum,
   A.OrderId,
   A.RefillNum,
   A.Ndc,
   A.StoreNum,
   A.HistoryDtTm,
   L.LotCode_ExpireDate_SerialNum,
   C.Gtin,
   NullIf(C.DispenserSN, '') As DispenserSN
Into #Grouped
From
   #OtherColumns A
   Left Join (
      Select RxNum, STRING_AGG(Cast(LotCode_ExpireDate_SerialNum As varchar(max)), ';') As LotCode_ExpireDate_SerialNum
      From #SerialNums
      Group By RxNum
   ) L On L.RxNum = A.RxNum
   Left Join
   #GTinDisp C On C.RxNum = A.RxNum


-- Final output. Column list, names (including the DisacardDate spelling), filter, and
-- ordering unchanged from v1. The correlated lookup is kept as-is; see Analysis.md
-- Section 11 for the follow-up on it.

Select
   DateFilled,
   OrderId,
   Ndc,
   StoreNum,
   LotCode_ExpireDate_SerialNum,
   Gtin,
   DispenserSN,
   (Select Top 1 ExpireDate From OeLotCode OL With (NoLock) Where OL.OrderId = G.OrderId And OL.HistoryDtTm = G.HistoryDtTm Order By ExpireDate) DisacardDate
From #Grouped G
Where DateFilled > @StartDate And DateFilled <= @EndDate
Order By DateFilled, OrderId


-- Cleanup
Drop Table If Exists #Unsorted
Drop Table If Exists #LatestFilled
Drop Table If Exists #Grouped
Drop Table If Exists #GTinDisp
Drop Table If Exists #SerialNums
Drop Table If Exists #OtherColumns
Drop Table If Exists #MultiVialOrders
Drop Table If Exists #YetToBeFilledMultiVials


End
GO
