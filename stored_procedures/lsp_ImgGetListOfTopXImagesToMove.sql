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
   6

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
   None
*/

Select 
   Top (Select 
            Convert(Int, [Value])
        From vCfgSystemParamVal
        Where Section = 'Operation'
                 And
              Parameter = 'MaxNumberOfImagesToPullInASingleRunFromBgWk')
     i.Id
   , i.ContentCode
From vImgImage As i With (NOLOCK)
Left Join ImgRxImgAssoc As IR With (NOLOCK, FORCESEEK) On i.Id = Ir.ImgId 
Left Join OeOrderHistory As OH With (NOLOCK) On IR.OrderId = OH.OrderId 
Left Join ImgCanImgAssoc as IC With (NOLOCK, FORCESEEK) On i.Id = IC.ImgId
Left Join CanCanister as C With (NOLOCK) On IC.CanisterSn = C.CanisterSn
Where i.ImageData Is Not Null
         And
      (i.IsMovedToFile Is Null Or i.IsMovedToFile <> 1)
         And
      i.ArchivedDtTm Is NULL and (i.Id = IR.ImgId Or i.Id = IC.ImgId)
		 And 
	  (Oh.OrderStatus = 'Shipped' or C.Status = 'Verified')
Order By i.Id Asc
End


/
