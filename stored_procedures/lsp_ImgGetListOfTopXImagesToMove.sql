--/
CREATE PROCEDURE lsp_ImgGetListOfTopXImagesToMove
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
      1. Extracted TOP subquery into local variable for rowgoal optimization
      2. Replaced LEFT JOIN + OR across two join paths with UNION of two
         focused INNER JOIN branches (Shipped Rx path / Verified Canister path)
      3. UNION (not UNION ALL) deduplicates images that qualify through both paths
      4. Retained FORCESEEK hints on association tables pending post-deployment
         validation; optimizer should now choose seeks naturally with INNER JOINs
      5. Retained vImgImage view reference (UNION ALL over ImgImage + ImgImage_IntId)
         for forward compatibility; view can be replaced with ImgImage directly
         if ImgImage_IntId remains empty
*/

-- Pull the configured batch size into a local variable so the optimizer
-- can sniff the value and apply rowgoal optimization to the TOP operator
Declare @MaxImages Int

Select @MaxImages = Convert(Int, [Value])
From vCfgSystemParamVal
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
    From vImgImage As i With (NOLOCK)
    Join ImgRxImgAssoc As IR With (NOLOCK, FORCESEEK) On i.Id = IR.ImgId
    Join OeOrderHistory As OH With (NOLOCK) On IR.OrderId = OH.OrderId
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
    From vImgImage As i With (NOLOCK)
    Join ImgCanImgAssoc As IC With (NOLOCK, FORCESEEK) On i.Id = IC.ImgId
    Join CanCanister As C With (NOLOCK) On IC.CanisterSn = C.CanisterSn
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


/
