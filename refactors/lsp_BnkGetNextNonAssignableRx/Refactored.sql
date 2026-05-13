--/
ALTER PROCEDURE [dbo].[lsp_BnkGetNextNonAssignableRx]
As
Begin
/*
Object Type:
   Stored Procedure

Object Name:
   lsp_BnkGetNextNonAssignableRx

Version:
   12

Comments:
   v12 - Performance refactoring:
      1. Consolidated the two TcdStatus + BnkConfiguredBanks + BnkAllBanks scans
         (#IncludedBankProducts and #ExcludedBankProducts) into a single scan
         #BankProducts with an IsExcluded column. The original scanned the same
         three-table join twice with identical predicates aside from the IsExcluded
         filter.
      2. Added supporting indexes on every temp table that participates in a
         downstream join.
      3. The two source sets for the final UNION (#RxsNotInAnyBank and
         #RxsOnlyInExclBank) are disjoint by construction: a single Bank-Unassigned
         Rx is either "not in any bank" or "only in an excluded bank," not both.
         UNION ALL is therefore semantically equivalent to UNION and avoids the
         implicit DISTINCT.
      4. Added Option (Recompile) on the long-running SELECTs. The procedure carries
         no input parameters, so the benefit is plan-quality against current data
         shape rather than against parameter variance.
*/

Set NoCount On

Drop Table If Exists
      #BankUnassignedRxs
    , #RxsInAnyBank
    , #RxsNotInAnyBank
    , #BankProducts
    , #RxsOnlyInExclBank

------------------------------------------------------------------------------------------
-- Scheduled, Auto-Fill, Bank-Unassigned Rxs.
------------------------------------------------------------------------------------------
Select
      O.OrderId
    , O.PriInternal
    , O.ProductId
    , O2.RequestedFillMethodCode
Into #BankUnassignedRxs
From OeOrder O With (NoLock)
Inner Join OeOrderSecondaryData O2 With (NoLock)
    On O2.OrderId = O.OrderId
Where
(
    O.OrderStatus = 'Scheduled'
    Or (O.OrderStatus = 'Problem'
        And O.PendingOrderStatus = 'Scheduled'
        And O.StatusReason Not Like '%Filling method not available')
)
  And O.FillType = 'A'
  And O.AddrBank = '-'
  And (O2.RequestedFillMethodCode In ('A', 'S') Or O2.RequestedFillMethodCode Is Null)
  And O.LastStatusChgDtTm < DateAdd(Second, -10, GetDate())
Option (Recompile);

Create Index IX_BUR_Product On #BankUnassignedRxs(ProductId) Include (OrderId, PriInternal, RequestedFillMethodCode)

------------------------------------------------------------------------------------------
-- Bank Unassigned Rxs whose product resides in any cabinet bank dispenser supporting
-- the requested fill method.
------------------------------------------------------------------------------------------
Select OrderId, PriInternal, AddrBank, SupportedFillMethod, RequestedFillMethodCode, TcdSn
Into #RxsInAnyBank
From
(
    Select
          O.OrderId
        , O.PriInternal
        , T.AddrBank
        , [SupportedFillMethod] = Case BA.AutoFillTypeCode
                                    When 'O' Then 'A'
                                    When 'H' Then 'S'
                                    Else Null
                                  End
        , O.RequestedFillMethodCode
        , T.TcdSn
    From #BankUnassignedRxs O
    Inner Join TcdStatus T With (NoLock)
        On T.ProductId = O.ProductId
    Inner Join BnkAllBanks BA With (NoLock)
        On BA.AddrBank = T.AddrBank
    Group By O.OrderId, O.PriInternal, T.AddrBank, BA.AutoFillTypeCode, O.RequestedFillMethodCode, T.TcdSn
) Combined
Where SupportedFillMethod = RequestedFillMethodCode
   Or RequestedFillMethodCode Is Null
Option (Recompile);

Create Index IX_RIAB_Order On #RxsInAnyBank(OrderId)

------------------------------------------------------------------------------------------
-- Bank-Unassigned Rxs whose product is NOT in any qualifying cabinet bank dispenser.
------------------------------------------------------------------------------------------
Select
      O.OrderId
    , [NonAssignableReason] = 'NotInAnyBank'
    , O.PriInternal
    , O.RequestedFillMethodCode
Into #RxsNotInAnyBank
From #BankUnassignedRxs O
Left Join #RxsInAnyBank InAny
    On InAny.OrderId = O.OrderId
Where InAny.TcdSn Is Null
Option (Recompile);

------------------------------------------------------------------------------------------
-- One consolidated scan over TcdStatus + BnkConfiguredBanks + BnkAllBanks produces
-- both the included and excluded product-bank sets. The original procedure did this
-- with two near-identical SELECTs that differed only in the IsExcluded filter.
------------------------------------------------------------------------------------------
Select
      T.ProductId
    , [SupportedFillMethod] = Case BA.AutoFillTypeCode
                                When 'O' Then 'A'
                                When 'H' Then 'S'
                                Else Null
                              End
    , T.AddrBank
    , [IsExcluded] = B.IsExcluded
Into #BankProducts
From TcdStatus T With (NoLock)
Inner Join BnkConfiguredBanks B With (NoLock)
    On T.AddrBank = B.AddrBank
Inner Join BnkAllBanks BA With (NoLock)
    On BA.AddrBank = T.AddrBank
Where T.ProductId Is Not Null
Group By T.ProductId, T.AddrBank, BA.AutoFillTypeCode, B.IsExcluded
Option (Recompile);

Create Index IX_BP_Product On #BankProducts(ProductId) Include (SupportedFillMethod, AddrBank, IsExcluded)

------------------------------------------------------------------------------------------
-- Bank-Unassigned Rxs whose product only resides in an Excluded cabinet bank.
------------------------------------------------------------------------------------------
Select
      O.OrderId
    , [NonAssignableReason] = 'OnlyInExcludedBank'
    , O.PriInternal
    , O.RequestedFillMethodCode
Into #RxsOnlyInExclBank
From #BankUnassignedRxs O
Left Join #BankProducts Ex
    On Ex.ProductId = O.ProductId And Ex.IsExcluded = 1
Left Join #BankProducts Incl
    On Incl.ProductId = O.ProductId And Incl.IsExcluded = 0
Where Not
(
    (Incl.AddrBank Is Not Null And O.RequestedFillMethodCode = Incl.SupportedFillMethod)
    Or (Ex.AddrBank Is Null)
    Or (Ex.AddrBank Is Not Null And O.RequestedFillMethodCode Is Not Null
        And O.RequestedFillMethodCode <> Ex.SupportedFillMethod)
)
  And Ex.AddrBank Is Not Null
Option (Recompile);

------------------------------------------------------------------------------------------
-- Top-priority non-assignable Rx. UNION ALL is safe because the two source sets are
-- disjoint: a Bank-Unassigned Rx is either "not in any bank" (no matching product in
-- TcdStatus) or "only in excluded bank" (product in TcdStatus but only in IsExcluded=1).
------------------------------------------------------------------------------------------
Select Top 1
      OrderId
    , NonAssignableReason
    , PriInternal
From
(
    Select OrderId, NonAssignableReason, PriInternal From #RxsNotInAnyBank
    Union All
    Select OrderId, NonAssignableReason, PriInternal From #RxsOnlyInExclBank
) AS NonAssignable
Order By PriInternal, OrderId
Option (Recompile);

Drop Table If Exists
      #BankUnassignedRxs
    , #RxsInAnyBank
    , #RxsNotInAnyBank
    , #BankProducts
    , #RxsOnlyInExclBank

End
GO
