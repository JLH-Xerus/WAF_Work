--/
ALTER PROCEDURE [dbo].[lsp_ImgGetListOfTopXImagesToMove]
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_ImgGetListOfTopXImagesToMove

Version:
   7

Description:
   Pull the configured number of rows from ImgImage table and relevant tables to export Shipped Rx and Verified Canister
   Images to file system

Parameters:
   Input:
      None
   Output:
      None

Return Value:
   DataSet

Comments:
   v7 - Performance refactoring:
      1. Extracted TOP subquery into local variable for rowgoal optimization.
         The original TOP (SELECT ... FROM vCfgSystemParamVal) subquery prevented
         the optimizer from estimating cardinality for the rowgoal operator; a local
         variable allows it to sniff the actual value.
      2. Replaced LEFT JOIN + OR across two join paths with UNION of two
         focused INNER JOIN branches (Shipped Rx path / Verified Canister path).
         The original pattern (LEFT JOIN all four association tables, then filter
         with OR on OrderStatus/Status) forced the optimizer into a single plan
         that could not seek efficiently on either path independently.
      3. UNION (not UNION ALL) deduplicates images that qualify through both paths.
      4. Retained FORCESEEK hints on association tables pending post-deployment
         validation; optimizer should now choose seeks naturally with INNER JOINs.
      5. Retained vImgImage view reference (UNION ALL over ImgImage + ImgImage_IntId)
         for forward compatibility; view can be replaced with ImgImage directly
         if ImgImage_IntId remains empty.
*/

Set NoCount On

-- Pull the configured batch size into a local variable so the optimizer
-- can sniff the value and apply rowgoal optimization to the TOP operator.
Declare @MaxImages Int

Select @MaxImages = Convert(Int, [Value])
From vCfgSystemParamVal With (NoLock)
Where Section = 'Operation'
         And
      Parameter = 'MaxNumberOfImagesToPullInASingleRunFromBgWk'

-- Two INNER JOIN branches replace the original LEFT JOIN + OR pattern.
-- Each branch independently seeks through its association table, and
-- the optimizer can apply rowgoal to each branch via the TOP value.

Select Top (@MaxImages)
      Id
    , ContentCode
From (
    ------------------------------------------------------------------
    -- Branch 1: Images associated with a Shipped Rx order
    ------------------------------------------------------------------
    Select
          i.Id
        , i.ContentCode
    From vImgImage As i With (NoLock)
    Inner Join ImgRxImgAssoc As IR With (NoLock, FORCESEEK) On i.Id = IR.ImgId
    Inner Join OeOrderHistory As OH With (NoLock) On IR.OrderId = OH.OrderId
    Where i.ImageData Is Not Null
             And
          (i.IsMovedToFile Is Null Or i.IsMovedToFile <> 1)
             And
          i.ArchivedDtTm Is Null
             And
          OH.OrderStatus = 'Shipped'

    Union

    ------------------------------------------------------------------
    -- Branch 2: Images associated with a Verified Canister
    ------------------------------------------------------------------
    Select
          i.Id
        , i.ContentCode
    From vImgImage As i With (NoLock)
    Inner Join ImgCanImgAssoc As IC With (NoLock, FORCESEEK) On i.Id = IC.ImgId
    Inner Join CanCanister As C With (NoLock) On IC.CanisterSn = C.CanisterSn
    Where i.ImageData Is Not Null
             And
          (i.IsMovedToFile Is Null Or i.IsMovedToFile <> 1)
             And
          i.ArchivedDtTm Is Null
             And
          C.Status = 'Verified'
) Combined
Order By Id Asc

End
GO
/
