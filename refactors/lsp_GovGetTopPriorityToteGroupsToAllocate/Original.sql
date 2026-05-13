/****** Object:  StoredProcedure [dbo].[lsp_GovGetTopPriorityToteGroupsToAllocate]    Script Date: 4/5/2026 8:50:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create   Procedure [dbo].[lsp_GovGetTopPriorityToteGroupsToAllocate]
   @ExcludeSingleToteGroups bit = 0,
   @ExcludeGroupToteGroups bit = 0,
   @ExcludeConsolidatedToteGroups bit = 0,
   @ExcludeSingleVialGroups bit = 0,
   @ExcludeGroupVialGroups bit = 0,
   @ExcludeSorterIdList dbo.udtTableOfVarChars READONLY
As
/*
Object Type:
   Stored Procedure

Name:
   lsp_GovGetTopPriorityToteGroupsToAllocate

Version:
   2

Description:
   Retrieve the top priority tote groups to be allocated from the queued Rx groups.

Parameters:
   Input:
      @ExcludeSingleToteGroups            - Whether or not to exclude any single tote (ST) groups from being returned.
      @ExcludeGroupToteGroups             - Whether or not to exclude any group tote (GT) groups from being returned.
      @ExcludeConsolidatedToteGroups      - When configured with "AllocatedToteQueueHandlingMode" set to "Consolidated", whether or not to exclude any single tote (ST) or group tote (GT) groups from being returned.
      @ExcludeSingleVialGroups            - Whether or not to exclude any single vial (SV) groups from being returned.
      @ExcludeGroupVialGroups             - Whether or not to exclude any group vial (GV) groups from being returned.
      @ExcludeSorterIdList                - List of sorter IDs to exclude for single-vial groups.
   Output:
      None

Return Value:
   0     = Success
   < 0   = Value of @@Error

Comments:
   None
*/

-- Normalize inputs
If @ExcludeSingleToteGroups Is Null
   Set @ExcludeSingleToteGroups = 0
If @ExcludeGroupToteGroups Is Null
   Set @ExcludeGroupToteGroups = 0
If @ExcludeConsolidatedToteGroups Is Null
   Set @ExcludeConsolidatedToteGroups = 0
If @ExcludeSingleVialGroups Is Null
   Set @ExcludeSingleVialGroups = 0
If @ExcludeGroupVialGroups Is Null
   Set @ExcludeGroupVialGroups = 0

Declare @GroupsToAllocate Table (GroupNum varchar(10), ToteGroupNum tinyint, OrderTypeCode char(2), PriInternal varchar(40), SorterId int, NumOfItems int)

Declare @CfgAllocateXGroupsEachPass int, @CfgAllocatedToteQueueHandlingMode varchar(max)
Select @CfgAllocateXGroupsEachPass = Convert(int, [Value]) From vCfgSystemParamVal with (nolock) where Section = 'OrderGovernor' And Parameter = 'AllocateXGroupsEachPass'
Select @CfgAllocatedToteQueueHandlingMode = [Value] From vCfgComputerParamVal with (nolock) where Computer = HOST_NAME() And Section = 'OrderGovernor' And Parameter = 'AllocatedToteQueueHandlingMode'

-- Retrieve a set of Rxs that are queued for allocation
Drop Table If Exists #AwaitingAllocationRxs
Select
   O.OrderId, O.GroupNum, O.PriInternal
Into
   #AwaitingAllocationRxs
From
   OeOrder As O with (nolock)
Where
   O.OrderStatus = 'Pending'
   And O.StatusReason = 'Awaiting Governor Allocation'
   And O.OrderId Not Like '%[(<][1-8][)>]'   -- Exclude split-DD portion Rxs


If @CfgAllocatedToteQueueHandlingMode = 'Separate'
   Begin

      If @ExcludeSingleToteGroups = 0 And Exists (Select 1 From GovRx with (nolock) where OrderTypeCode = 'ST')
         Begin
            -- Insert any ready single tote (ST) groups
            Insert Into
               @GroupsToAllocate
            Select Top (@CfgAllocateXGroupsEachPass)
               A.GroupNum, G.ToteGroupNum, G.OrderTypeCode, Min(A.PriInternal) As MinPriInternal, 0 As SorterId, 1 As NumOfItems
            From
               #AwaitingAllocationRxs As A
               Join GovRx As G with (nolock) On G.OrderId = A.OrderId
            Where
               G.OrderTypeCode = 'ST'
               And Not Exists (  -- An Rx does NOT exist for the Group + Tote Group which is NOT at least in a "Pending" status with reason "Awaiting Governor Allocation"
                  Select 1
                  From GovRx As G2 with (nolock)
                  Join OeOrder As O2 with (nolock) On O2.OrderId = G2.OrderId
                  Where
                     O2.GroupNum = A.GroupNum
                     And G2.ToteGroupNum = G.ToteGroupNum
                     And (
                        (O2.OrderStatus = 'Pending' And (O2.StatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping')))
                        Or
                        (O2.OrderStatus = 'Problem' And (O2.PendingOrderStatus = 'Pending' And (O2.PendingStatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping'))))
                     )
               )
            Group By
               A.GroupNum, G.ToteGroupNum, G.OrderTypeCode
            Order By
               MinPriInternal
         End

      If @ExcludeGroupToteGroups = 0 And Exists (Select 1 From GovRx with (nolock) where OrderTypeCode = 'GT')
         Begin
            -- Insert any ready group tote (GT) groups
            Insert Into
               @GroupsToAllocate
            Select Top (@CfgAllocateXGroupsEachPass)
               A.GroupNum, G.ToteGroupNum, G.OrderTypeCode, Min(A.PriInternal) As MinPriInternal, 0 As SorterId, 1 As NumOfItems
            From
               #AwaitingAllocationRxs As A
               Join GovRx As G with (nolock) On G.OrderId = A.OrderId
            Where
               G.OrderTypeCode = 'GT'
               And Not Exists (  -- An Rx does NOT exist for the Group + Tote Group which is NOT at least in a "Pending" status with reason "Awaiting Governor Allocation"
                  Select 1
                  From GovRx As G2 with (nolock)
                  Join OeOrder As O2 with (nolock) On O2.OrderId = G2.OrderId
                  Where
                     O2.GroupNum = A.GroupNum
                     And G2.ToteGroupNum = G.ToteGroupNum
                     And (
                        (O2.OrderStatus = 'Pending' And (O2.StatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping')))
                        Or
                        (O2.OrderStatus = 'Problem' And (O2.PendingOrderStatus = 'Pending' And (O2.PendingStatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping'))))
                     )
               )
            Group By
               A.GroupNum, G.ToteGroupNum, G.OrderTypeCode
            Order By
               MinPriInternal
         End

   End
Else  -- "Consolidated"
   Begin

      If @ExcludeConsolidatedToteGroups = 0 And Exists (Select 1 From GovRx with (nolock) where OrderTypeCode In ('ST','GT'))
         Begin
            -- Insert any ready single tote (ST) or group tote (GT) groups
            Insert Into
               @GroupsToAllocate
            Select Top (@CfgAllocateXGroupsEachPass)
               A.GroupNum, G.ToteGroupNum, G.OrderTypeCode, Min(A.PriInternal) As MinPriInternal, 0 As SorterId, 1 As NumOfItems
            From
               #AwaitingAllocationRxs As A
               Join GovRx As G with (nolock) On G.OrderId = A.OrderId
            Where
               G.OrderTypeCode In ('ST','GT')
               And Not Exists (  -- An Rx does NOT exist for the Group + Tote Group which is NOT at least in a "Pending" status with reason "Awaiting Governor Allocation"
                  Select 1
                  From GovRx As G2 with (nolock)
                  Join OeOrder As O2 with (nolock) On O2.OrderId = G2.OrderId
                  Where
                     O2.GroupNum = A.GroupNum
                     And G2.ToteGroupNum = G.ToteGroupNum
                     And (
                        (O2.OrderStatus = 'Pending' And (O2.StatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping')))
                        Or
                        (O2.OrderStatus = 'Problem' And (O2.PendingOrderStatus = 'Pending' And (O2.PendingStatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping'))))
                     )
               )
            Group By
               A.GroupNum, G.ToteGroupNum, G.OrderTypeCode
            Order By
               MinPriInternal
         End

   End

If @ExcludeGroupVialGroups = 0 And Exists (Select 1 From GovRx with (nolock) where OrderTypeCode = 'GV')
   Begin
      -- Insert any ready group vial (GV) groups
      Insert Into
         @GroupsToAllocate
      Select Top (@CfgAllocateXGroupsEachPass)
         A.GroupNum, G.ToteGroupNum, G.OrderTypeCode, Min(A.PriInternal) As MinPriInternal, 0 As SorterId, Count(G.OrderId) As NumOfItems
      From
         #AwaitingAllocationRxs As A
         Join GovRx As G with (nolock) On G.OrderId = A.OrderId
      Where
         G.OrderTypeCode = 'GV'
         And Not Exists (  -- An Rx does NOT exist for the Group + Tote Group which is NOT at least in a "Pending" status with reason "Awaiting Governor Allocation"
            Select 1
            From GovRx As G2 with (nolock)
            Join OeOrder As O2 with (nolock) On O2.OrderId = G2.OrderId
            Where
               O2.GroupNum = A.GroupNum
               And G2.ToteGroupNum = G.ToteGroupNum
               And (
                  (O2.OrderStatus = 'Pending' And (O2.StatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping')))
                  Or
                  (O2.OrderStatus = 'Problem' And (O2.PendingOrderStatus = 'Pending' And (O2.PendingStatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping'))))
               )
         )
      Group By
         A.GroupNum, G.ToteGroupNum, G.OrderTypeCode
      Order By
         MinPriInternal
   End

If @ExcludeSingleVialGroups = 0 And Exists (Select 1 From GovRx with (nolock) where OrderTypeCode = 'SV')
   Begin

      If Exists (Select 1 From @ExcludeSorterIdList)
         Begin
            -- Insert any ready single vial (SV) groups
            Insert Into
               @GroupsToAllocate
            Select Top (@CfgAllocateXGroupsEachPass)
               A.GroupNum, G.ToteGroupNum, G.OrderTypeCode, Min(A.PriInternal) As MinPriInternal, G.SorterId, 1 As NumOfItems
            From
               #AwaitingAllocationRxs As A
               Join GovRx As G with (nolock) On G.OrderId = A.OrderId
               Left Join @ExcludeSorterIdList X On X.[Value] = G.SorterId
            Where
               G.OrderTypeCode = 'SV'
               And X.[Value] Is Null   -- Sorter ID is not present in the excluded list
               And Not Exists (        -- An Rx does NOT exist for the Group + Tote Group which is NOT at least in a "Pending" status with reason "Awaiting Governor Allocation"
                  Select 1
                  From GovRx As G2 with (nolock)
                  Join OeOrder As O2 with (nolock) On O2.OrderId = G2.OrderId
                  Where
                     O2.GroupNum = A.GroupNum
                     And G2.ToteGroupNum = G.ToteGroupNum
                     And (
                        (O2.OrderStatus = 'Pending' And (O2.StatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping')))
                        Or
                        (O2.OrderStatus = 'Problem' And (O2.PendingOrderStatus = 'Pending' And (O2.PendingStatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping'))))
                     )
               )
            Group By
               A.GroupNum, G.ToteGroupNum, G.OrderTypeCode,  G.SorterId
            Order By
               MinPriInternal
         End
      Else
         Begin
            -- Insert any ready single vial (SV) groups
            Insert Into
               @GroupsToAllocate
            Select Top (@CfgAllocateXGroupsEachPass)
               A.GroupNum, G.ToteGroupNum, G.OrderTypeCode, Min(A.PriInternal) As MinPriInternal, G.SorterId, 1 As NumOfItems
            From
               #AwaitingAllocationRxs As A
               Join GovRx As G with (nolock) On G.OrderId = A.OrderId
            Where
               G.OrderTypeCode = 'SV'
               And Not Exists (  -- An Rx does NOT exist for the Group + Tote Group which is NOT at least in a "Pending" status with reason "Awaiting Governor Allocation"
                  Select 1
                  From GovRx As G2 with (nolock)
                  Join OeOrder As O2 with (nolock) On O2.OrderId = G2.OrderId
                  Where
                     O2.GroupNum = A.GroupNum
                     And G2.ToteGroupNum = G.ToteGroupNum
                     And (
                        (O2.OrderStatus = 'Pending' And (O2.StatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping')))
                        Or
                        (O2.OrderStatus = 'Problem' And (O2.PendingOrderStatus = 'Pending' And (O2.PendingStatusReason In ('Awaiting Governor Division', 'Awaiting Governor Tote Grouping'))))
                     )
               )
            Group By
               A.GroupNum, G.ToteGroupNum, G.OrderTypeCode, G.SorterId
            Order By
               MinPriInternal
         End

   End


Select
   A.*,
   Case When Exists (
      Select 1
      From OeOrder O with (nolock)
      Join GovRx G with (nolock) On G.OrderId = O.OrderId And G.ToteGroupNum = A.ToteGroupNum
      Where O.GroupNum = A.GroupNum And G.RxCvyStateCode = '0'
   ) Then 1 Else 0 End As HasAllocatedRx
From
   @GroupsToAllocate A


Return @@Error

GO
