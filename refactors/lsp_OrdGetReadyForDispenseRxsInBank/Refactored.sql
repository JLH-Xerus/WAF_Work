--/
ALTER PROCEDURE [dbo].[lsp_OrdGetReadyForDispenseRxsInBank]
   @AddrBank                 VarChar(2),
   @ReturnFillingRelatedData Bit = 0
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_OrdGetReadyForDispenseRxsInBank

Version:
   21

Comments:
   v21 - Performance refactoring:
      1. Local-variable copies of both input parameters.
      2. Consolidated the seven `If Object_Id ... Drop Table` blocks into a single
         `Drop Table If Exists` at entry and a matching one at exit.
      3. Collapsed the two reads of OeOrder for the upstream-Rx scan (one for the
         specified bank, one for the unassigned bank '-') into a single read with
         `AddrBank In (@LocalAddrBank, '-')`. The two reads had identical predicate
         structure aside from the bank value.
      4. Replaced the two-step "min PriInternal per product then re-join for the
         OrderId" pattern with a single ROW_NUMBER() window function. The v20 form
         needed two temp tables (the min-priority-per-product set and the joined-back
         set) to express what a single window function does in one pass.
      5. Collapsed #CountedRxsWithTopUpstreamRxData01 and #CountedRxsWithTopUpstreamRxData02
         into a single SELECT. The v20 form built one temp table to compute the upstream
         priority and a second to compute the "Act On As" priority and the make-way Order
         ID. Both columns are derived expressions over the same row set; one SELECT
         expresses both.
      6. Pre-computed the @UpstreamQueuedRxGroupNotYetCountedAdjustedBoostVal lookup
         once and used it in the consolidated SELECT.
      7. Added supporting indexes on the temp tables that participate in downstream joins.
      8. Added Option (Recompile) on the consolidated SELECTs to support accurate
         selectivity estimates against the local-variable parameters.
*/

Set NoCount On

------------------------------------------------------------------------------------------
-- Local variables.
------------------------------------------------------------------------------------------
Declare @LocalAddrBank                 VarChar(2) = @AddrBank
Declare @LocalReturnFillingRelatedData Bit         = @ReturnFillingRelatedData

Drop Table If Exists
      #CountedRxs
    , #UpstreamCandidates
    , #TopUpstreamPerProduct
    , #CountedRxsWithTopUpstream

------------------------------------------------------------------------------------------
-- One-time lookup of the upstream-Rx boost adjustment value.
------------------------------------------------------------------------------------------
Declare @UpstreamQueuedRxGroupNotYetCountedAdjustedBoostVal Char(1)

If Exists
(
    Select 1
    From vCfgSystemParamVal With (NoLock)
    Where Section   = 'RxPriority'
      And Parameter = 'BoostInternalPriOfRxsInGroupWhenRxGoesCountingAndFilled'
      And Value     = 'True'
)
    Set @UpstreamQueuedRxGroupNotYetCountedAdjustedBoostVal = '7'
Else
    Set @UpstreamQueuedRxGroupNotYetCountedAdjustedBoostVal = '9'

------------------------------------------------------------------------------------------
-- Counted Rxs in the specified bank.
------------------------------------------------------------------------------------------
Select
      OrderId
    , ProductId
    , PriInternal
    , QtyOrdered
Into #CountedRxs
From OeOrder With (NoLock)
Where AddrBank = @LocalAddrBank
  And FillType = 'A'
  And OrderId Not Like '%<[1-2]>'
  And Not OrderStatus Like 'Split_MV'
  And
  (
      OrderStatus In ('Counted', 'Partial Counted')
      Or (OrderStatus = 'Split-DD' And StatusReason = 'Counted')
  )
Option (Recompile);

Create Index IX_CountedRxs_Product On #CountedRxs(ProductId) Include (OrderId, PriInternal, QtyOrdered)

------------------------------------------------------------------------------------------
-- Upstream candidate Rxs for those Counted Products. Combined into one read across the
-- specified bank and the unassigned bank ('-'). The two v20 reads had identical structure
-- aside from the AddrBank value.
------------------------------------------------------------------------------------------
Select
      O.ProductId
    , O.PriInternal
    , O.OrderId
Into #UpstreamCandidates
From OeOrder O With (NoLock)
Where O.AddrBank In (@LocalAddrBank, '-')
  And O.FillType = 'A'
  And O.OrderId Not Like '%<[1-2]>'
  And
  (
      O.OrderStatus = 'Scheduled'
      Or (O.OrderStatus = 'Split-DD' And O.StatusReason = 'Scheduled')
  )
  And Exists
  (
      Select 1
      From #CountedRxs C
      Where C.ProductId = O.ProductId
  )
Option (Recompile);

Create Index IX_Upstream_Product On #UpstreamCandidates(ProductId, PriInternal) Include (OrderId)

------------------------------------------------------------------------------------------
-- Top upstream candidate per product via a single ROW_NUMBER() pass.
-- The v20 form built min-PriInternal-per-product and then re-joined; one window function
-- expresses both steps.
------------------------------------------------------------------------------------------
;With Ranked As
(
    Select
          ProductId
        , PriInternal
        , OrderId
        , [Rk] = Row_Number() Over (Partition By ProductId Order By PriInternal Asc, OrderId Asc)
    From #UpstreamCandidates
)
Select ProductId, PriInternal, OrderId
Into #TopUpstreamPerProduct
From Ranked
Where Rk = 1

Create Index IX_TopUpstream_Product On #TopUpstreamPerProduct(ProductId) Include (PriInternal, OrderId)

------------------------------------------------------------------------------------------
-- Counted Rxs joined to their top upstream candidate, with the "Act On As" priority and
-- the "make way for upstream" Order ID computed in a single SELECT.
------------------------------------------------------------------------------------------
Select
      C.OrderId
    , C.ProductId
    , C.QtyOrdered
    , C.PriInternal
    , [TopPriUpstreamQueuedRxOrderId]               = IsNull(B.OrderId, 'None')
    , [TopPriUpstreamQueuedRxAdjustedPriInternal]   = AdjustedUpstream
    , [ActOnAsPriInternal]                          = Case When AdjustedUpstream < C.PriInternal Then AdjustedUpstream Else C.PriInternal End
    , [FillToMakeWayForUpstreamRxOrderId]           = Case When AdjustedUpstream < C.PriInternal Then B.OrderId Else Null End
Into #CountedRxsWithTopUpstream
From #CountedRxs C
Left Join #TopUpstreamPerProduct B
    On B.ProductId = C.ProductId
Cross Apply
(
    Select
        [AdjustedUpstream] = IsNull
        (
            Left(B.PriInternal, 1)
            + Case SubString(B.PriInternal, 2, 1)
                  When '9' Then @UpstreamQueuedRxGroupNotYetCountedAdjustedBoostVal
                  Else SubString(B.PriInternal, 2, 1)
              End
            + SubString(B.PriInternal, 3, 22)
          , '999999999999999999'
        )
) X
Option (Recompile);

Create Index IX_CRTU_ActOn On #CountedRxsWithTopUpstream(ActOnAsPriInternal, PriInternal) Include (OrderId)

------------------------------------------------------------------------------------------
-- Final SELECT, one of two shapes.
------------------------------------------------------------------------------------------
Set NoCount Off

If @LocalReturnFillingRelatedData = 0
Begin
    Select
          F.OrderId
        , F.ProductId
        , F.FillToMakeWayForUpstreamRxOrderId
        , F.QtyOrdered
        , INV.QtyPer100Cc
        , [ProductVolumeInDrams]   = Cast(((F.QtyOrdered * 100.0) / INV.QtyPer100Cc) * 0.2705 As Decimal(9,3))
        , [OverFlowLabelRequired]  = Case When OFR.IsFlaggedFor = 'Prt Ovrflw Lbl' Then 1 Else 0 End
        , F.ActOnAsPriInternal
        , F.PriInternal
    From #CountedRxsWithTopUpstream F
    Inner Join vInvMasterWithUserOverrides INV With (NoLock)
        On INV.ProductId = F.ProductId
    Left Join OeFlaggedRxs OFR With (NoLock)
        On OFR.OrderId = F.OrderId
End
Else
Begin
    Select
          C.OrderId, C.FillToMakeWayForUpstreamRxOrderId
        , O.RxNum, O.OrderStatus
        , dbo.fSqlFormatDecimalValueForDisplay(O.QtyOrdered) As QtyOrdered
        , O.AddrBank, O.RefillNum, O.PriInternal
        , O.Ndc, O.ProductId, O.Medication, O.StatusReason, O.FillType
        , O.NumLabelsPrinted, O.GroupNum, O.PriCodeSys
    From #CountedRxsWithTopUpstream C
    Inner Join OeOrder O With (NoLock)
        On O.OrderId = C.OrderId
    Order By
          C.ActOnAsPriInternal
        , C.PriInternal
End

Drop Table If Exists
      #CountedRxs
    , #UpstreamCandidates
    , #TopUpstreamPerProduct
    , #CountedRxsWithTopUpstream

End
GO
