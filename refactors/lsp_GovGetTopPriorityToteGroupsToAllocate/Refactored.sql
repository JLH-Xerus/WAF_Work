--/
ALTER PROCEDURE [dbo].[lsp_GovGetTopPriorityToteGroupsToAllocate]
   @ExcludeSingleToteGroups       Bit = 0,
   @ExcludeGroupToteGroups        Bit = 0,
   @ExcludeConsolidatedToteGroups Bit = 0,
   @ExcludeSingleVialGroups       Bit = 0,
   @ExcludeGroupVialGroups        Bit = 0,
   @ExcludeSorterIdList           dbo.udtTableOfVarChars Readonly
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_GovGetTopPriorityToteGroupsToAllocate

Version:
   3

Comments:
   v3 - Performance refactoring:
      1. Local-variable copies of all five bit parameters.
      2. Replaced the @GroupsToAllocate table variable with a #GroupsToAllocate temp
         table, indexed for the final SELECT. Table variables do not carry meaningful
         statistics, which compounds the parameter-sniffing issue the cross-list
         capture flags.
      3. Pre-materialized the "no blocking Rx" check into #BlockedPairs once. The
         original procedure repeated a ~10-line Not Exists subquery FIVE TIMES across
         the five Insert blocks. v3 computes the set of (GroupNum, ToteGroupNum) pairs
         that have a blocking Rx (any Rx in Pending+Awaiting-Division or Problem+
         Pending-Awaiting-Division status), and each Insert filters it out via a clean
         Left Join + IS NULL probe.
      4. Pre-materialized the HasAllocatedRx Exists check into #AllocatedPairs once
         instead of executing it as a correlated subquery in the final SELECT.
      5. Added Option (Recompile) on each Insert and on the final SELECT. The cross-list
         capture's "plan instability" classification on this procedure is consistent
         with the five-branch Insert structure compiling against the first call's
         parameter combination and reusing that plan.
      6. The five Insert blocks remain structurally distinct because they differ in
         the OrderTypeCode filter, the NumOfItems calculation, the SorterId projection,
         and the GROUP BY shape. Consolidating them into a single Insert with CASE-
         driven shapes would multiply rather than reduce optimizer work.
*/

Set NoCount On

------------------------------------------------------------------------------------------
-- Local variables. Inputs are normalized to 0 if NULL.
------------------------------------------------------------------------------------------
Declare @LocalExcludeSingleToteGroups       Bit = IsNull(@ExcludeSingleToteGroups, 0)
Declare @LocalExcludeGroupToteGroups        Bit = IsNull(@ExcludeGroupToteGroups, 0)
Declare @LocalExcludeConsolidatedToteGroups Bit = IsNull(@ExcludeConsolidatedToteGroups, 0)
Declare @LocalExcludeSingleVialGroups       Bit = IsNull(@ExcludeSingleVialGroups, 0)
Declare @LocalExcludeGroupVialGroups        Bit = IsNull(@ExcludeGroupVialGroups, 0)

Declare @CfgAllocateXGroupsEachPass         Int
Declare @CfgAllocatedToteQueueHandlingMode  VarChar(max)

Select @CfgAllocateXGroupsEachPass = Convert(Int, [Value])
From vCfgSystemParamVal With (NoLock)
Where Section = 'OrderGovernor' And Parameter = 'AllocateXGroupsEachPass'

Select @CfgAllocatedToteQueueHandlingMode = [Value]
From vCfgComputerParamVal With (NoLock)
Where Computer = HOST_NAME() And Section = 'OrderGovernor' And Parameter = 'AllocatedToteQueueHandlingMode'

------------------------------------------------------------------------------------------
-- Temp tables.
------------------------------------------------------------------------------------------
Drop Table If Exists #AwaitingAllocationRxs, #BlockedPairs, #GroupsToAllocate, #AllocatedPairs

Create Table #GroupsToAllocate
(
    GroupNum       VarChar(10),
    ToteGroupNum   TinyInt,
    OrderTypeCode  Char(2),
    PriInternal    VarChar(40),
    SorterId       Int,
    NumOfItems     Int
)

------------------------------------------------------------------------------------------
-- Rxs queued for allocation.
------------------------------------------------------------------------------------------
Select
      O.OrderId
    , O.GroupNum
    , O.PriInternal
Into #AwaitingAllocationRxs
From OeOrder O With (NoLock)
Where O.OrderStatus = 'Pending'
  And O.StatusReason = 'Awaiting Governor Allocation'
  And O.OrderId Not Like '%[(<][1-8][)>]'
Option (Recompile);

Create Index IX_AAR_OrderId On #AwaitingAllocationRxs(OrderId) Include (GroupNum, PriInternal)
Create Index IX_AAR_GroupNum On #AwaitingAllocationRxs(GroupNum)

------------------------------------------------------------------------------------------
-- Pre-materialize the "blocking Rx" set once. The original procedure embedded this
-- check as a 10-line Not Exists subquery in FIVE separate Insert blocks. A (GroupNum,
-- ToteGroupNum) pair is blocked if it has any Rx whose status is Pending-Awaiting-
-- Governor-Division/Tote-Grouping or Problem-Pending-Awaiting-Governor-Division/Tote-
-- Grouping. Each Insert below joins to this set and filters via IS NULL.
------------------------------------------------------------------------------------------
Select Distinct
      O.GroupNum
    , G.ToteGroupNum
Into #BlockedPairs
From GovRx G With (NoLock)
Inner Join OeOrder O With (NoLock)
    On O.OrderId = G.OrderId
Where
(
    (O.OrderStatus = 'Pending' And O.StatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping'))
    Or
    (O.OrderStatus = 'Problem' And O.PendingOrderStatus = 'Pending'
        And O.PendingStatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping'))
)
Option (Recompile);

Create Index IX_BP_Pair On #BlockedPairs(GroupNum, ToteGroupNum)

------------------------------------------------------------------------------------------
-- Tote-group branches.
------------------------------------------------------------------------------------------
If @CfgAllocatedToteQueueHandlingMode = 'Separate'
Begin

    If @LocalExcludeSingleToteGroups = 0 And Exists (Select 1 From GovRx With (NoLock) Where OrderTypeCode = 'ST')
    Begin
        Insert Into #GroupsToAllocate (GroupNum, ToteGroupNum, OrderTypeCode, PriInternal, SorterId, NumOfItems)
        Select Top (@CfgAllocateXGroupsEachPass)
              A.GroupNum
            , G.ToteGroupNum
            , G.OrderTypeCode
            , Min(A.PriInternal) As MinPriInternal
            , [SorterId]   = 0
            , [NumOfItems] = 1
        From #AwaitingAllocationRxs A
        Inner Join GovRx G With (NoLock)
            On G.OrderId = A.OrderId
        Left Join #BlockedPairs BP
            On BP.GroupNum = A.GroupNum And BP.ToteGroupNum = G.ToteGroupNum
        Where G.OrderTypeCode = 'ST'
          And BP.GroupNum Is Null
        Group By A.GroupNum, G.ToteGroupNum, G.OrderTypeCode
        Order By MinPriInternal
        Option (Recompile);
    End

    If @LocalExcludeGroupToteGroups = 0 And Exists (Select 1 From GovRx With (NoLock) Where OrderTypeCode = 'GT')
    Begin
        Insert Into #GroupsToAllocate (GroupNum, ToteGroupNum, OrderTypeCode, PriInternal, SorterId, NumOfItems)
        Select Top (@CfgAllocateXGroupsEachPass)
              A.GroupNum
            , G.ToteGroupNum
            , G.OrderTypeCode
            , Min(A.PriInternal) As MinPriInternal
            , [SorterId]   = 0
            , [NumOfItems] = 1
        From #AwaitingAllocationRxs A
        Inner Join GovRx G With (NoLock)
            On G.OrderId = A.OrderId
        Left Join #BlockedPairs BP
            On BP.GroupNum = A.GroupNum And BP.ToteGroupNum = G.ToteGroupNum
        Where G.OrderTypeCode = 'GT'
          And BP.GroupNum Is Null
        Group By A.GroupNum, G.ToteGroupNum, G.OrderTypeCode
        Order By MinPriInternal
        Option (Recompile);
    End

End
Else
Begin

    If @LocalExcludeConsolidatedToteGroups = 0 And Exists (Select 1 From GovRx With (NoLock) Where OrderTypeCode In ('ST', 'GT'))
    Begin
        Insert Into #GroupsToAllocate (GroupNum, ToteGroupNum, OrderTypeCode, PriInternal, SorterId, NumOfItems)
        Select Top (@CfgAllocateXGroupsEachPass)
              A.GroupNum
            , G.ToteGroupNum
            , G.OrderTypeCode
            , Min(A.PriInternal) As MinPriInternal
            , [SorterId]   = 0
            , [NumOfItems] = 1
        From #AwaitingAllocationRxs A
        Inner Join GovRx G With (NoLock)
            On G.OrderId = A.OrderId
        Left Join #BlockedPairs BP
            On BP.GroupNum = A.GroupNum And BP.ToteGroupNum = G.ToteGroupNum
        Where G.OrderTypeCode In ('ST', 'GT')
          And BP.GroupNum Is Null
        Group By A.GroupNum, G.ToteGroupNum, G.OrderTypeCode
        Order By MinPriInternal
        Option (Recompile);
    End

End

------------------------------------------------------------------------------------------
-- Group vial (GV) groups.
------------------------------------------------------------------------------------------
If @LocalExcludeGroupVialGroups = 0 And Exists (Select 1 From GovRx With (NoLock) Where OrderTypeCode = 'GV')
Begin
    Insert Into #GroupsToAllocate (GroupNum, ToteGroupNum, OrderTypeCode, PriInternal, SorterId, NumOfItems)
    Select Top (@CfgAllocateXGroupsEachPass)
          A.GroupNum
        , G.ToteGroupNum
        , G.OrderTypeCode
        , Min(A.PriInternal) As MinPriInternal
        , [SorterId]   = 0
        , [NumOfItems] = Count(G.OrderId)
    From #AwaitingAllocationRxs A
    Inner Join GovRx G With (NoLock)
        On G.OrderId = A.OrderId
    Left Join #BlockedPairs BP
        On BP.GroupNum = A.GroupNum And BP.ToteGroupNum = G.ToteGroupNum
    Where G.OrderTypeCode = 'GV'
      And BP.GroupNum Is Null
    Group By A.GroupNum, G.ToteGroupNum, G.OrderTypeCode
    Order By MinPriInternal
    Option (Recompile);
End

------------------------------------------------------------------------------------------
-- Single vial (SV) groups. Two branches differ only in whether the sorter exclusion
-- list is applied; the WHERE shape is otherwise identical.
------------------------------------------------------------------------------------------
If @LocalExcludeSingleVialGroups = 0 And Exists (Select 1 From GovRx With (NoLock) Where OrderTypeCode = 'SV')
Begin
    If Exists (Select 1 From @ExcludeSorterIdList)
    Begin
        Insert Into #GroupsToAllocate (GroupNum, ToteGroupNum, OrderTypeCode, PriInternal, SorterId, NumOfItems)
        Select Top (@CfgAllocateXGroupsEachPass)
              A.GroupNum
            , G.ToteGroupNum
            , G.OrderTypeCode
            , Min(A.PriInternal) As MinPriInternal
            , G.SorterId
            , [NumOfItems] = 1
        From #AwaitingAllocationRxs A
        Inner Join GovRx G With (NoLock)
            On G.OrderId = A.OrderId
        Left Join @ExcludeSorterIdList X
            On X.[Value] = G.SorterId
        Left Join #BlockedPairs BP
            On BP.GroupNum = A.GroupNum And BP.ToteGroupNum = G.ToteGroupNum
        Where G.OrderTypeCode = 'SV'
          And X.[Value] Is Null
          And BP.GroupNum Is Null
        Group By A.GroupNum, G.ToteGroupNum, G.OrderTypeCode, G.SorterId
        Order By MinPriInternal
        Option (Recompile);
    End
    Else
    Begin
        Insert Into #GroupsToAllocate (GroupNum, ToteGroupNum, OrderTypeCode, PriInternal, SorterId, NumOfItems)
        Select Top (@CfgAllocateXGroupsEachPass)
              A.GroupNum
            , G.ToteGroupNum
            , G.OrderTypeCode
            , Min(A.PriInternal) As MinPriInternal
            , G.SorterId
            , [NumOfItems] = 1
        From #AwaitingAllocationRxs A
        Inner Join GovRx G With (NoLock)
            On G.OrderId = A.OrderId
        Left Join #BlockedPairs BP
            On BP.GroupNum = A.GroupNum And BP.ToteGroupNum = G.ToteGroupNum
        Where G.OrderTypeCode = 'SV'
          And BP.GroupNum Is Null
        Group By A.GroupNum, G.ToteGroupNum, G.OrderTypeCode, G.SorterId
        Order By MinPriInternal
        Option (Recompile);
    End
End

Create Index IX_GTA_Pair On #GroupsToAllocate(GroupNum, ToteGroupNum)

------------------------------------------------------------------------------------------
-- Pre-materialize the HasAllocatedRx set once. The original ran this as a correlated
-- Exists subquery in the final SELECT.
------------------------------------------------------------------------------------------
Select Distinct
      O.GroupNum
    , G.ToteGroupNum
Into #AllocatedPairs
From OeOrder O With (NoLock)
Inner Join GovRx G With (NoLock)
    On G.OrderId = O.OrderId
Inner Join #GroupsToAllocate A
    On A.GroupNum = O.GroupNum And A.ToteGroupNum = G.ToteGroupNum
Where G.RxCvyStateCode = '0'
Option (Recompile);

Create Index IX_AP_Pair On #AllocatedPairs(GroupNum, ToteGroupNum)

------------------------------------------------------------------------------------------
-- Final SELECT.
------------------------------------------------------------------------------------------
Select
      A.GroupNum
    , A.ToteGroupNum
    , A.OrderTypeCode
    , A.PriInternal
    , A.SorterId
    , A.NumOfItems
    , [HasAllocatedRx] = Case When AP.GroupNum Is Not Null Then 1 Else 0 End
From #GroupsToAllocate A
Left Join #AllocatedPairs AP
    On AP.GroupNum = A.GroupNum And AP.ToteGroupNum = A.ToteGroupNum
Option (Recompile);

Drop Table If Exists #AwaitingAllocationRxs, #BlockedPairs, #GroupsToAllocate, #AllocatedPairs

Return @@Error

End
GO
