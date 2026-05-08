--/
CREATE PROCEDURE lsp_RxvGetRxsQueuedForVerification 
     @FillTypeFilter dbo.udtTableOfVarChars READONLY
   , @LocationFilter dbo.udtTableOfVarChars READONLY
As
Begin
/*
Object Type:
   Stored Procedure

Name:
   lsp_RxvGetRxsQueuedForVerification

Version:
   7

Description:
   Retrieve all applicable Rx's that are queued for verification. 
   This script will ignore Rxs that are not currently under audit, do not require visual inspection, and are autofill when Canister Verification is enabled

Parameters:
   Input:
      @FillTypeFilter      - Table of varchars containing applicable fill types to filter the resulting Rxs by
      @LocationFilter      - Table of varchars containing applicable stock location prefixes to filter the resulting Rxs by
                                (stock location prefixes should be already be appended with "%" at the time they are passed into this stored procedure)
   Output:
      None

Return Value:
   0     = Successful
   < 0   = Value of @@Error

Comments:
   This stored procedure was modified as part of WI 34282 to improve performance.
*/

Drop Table 
   If Exists #QueuedRxs
           , #LocationNdcs
           , #AuditRxs
           , #RxList
           , #SemiAutoFillBanks

-- RxInheritsCanisterVerification
Declare @CfgRxInheritsCanisterVerification As Bit = 0

--RxVerificationMode
Declare @CfgRxDigitalVerificationMode As Varchar(20) = ''

------------------------------------------------------------------------------------------
-- Query for the System-Wide config param value [RxVerification] @CfgRxInheritsCanisterVerification
------------------------------------------------------------------------------------------
Select
   @CfgRxInheritsCanisterVerification = IIF(SW.Value = 'True', 1, 0)
From
   vCfgSystemParamVal As SW With (NoLock)
Where
   SW.Section = 'RxVerification'
      And
   SW.Parameter = 'RxInheritsCanisterVerification'

------------------------------------------------------------------------------------------------
-- Query for the System-Wide config param value [RxVerification] DigitalVerificationMode
------------------------------------------------------------------------------------------------
Select @CfgRxDigitalVerificationMode = SW.Value
From
   vCfgSystemParamVal As SW With (NoLock)
Where
   SW.Section = 'RxVerification'
   And SW.Parameter = 'DigitalVerificationMode'
   
Declare @FillTypeFilterExists As BIT = 0

Select @FillTypeFilterExists = 1 From @FillTypeFilter

-- Get the list of Semi-Auto Filling Cabinets
Select Distinct
     [Bank] = BCfg.AddrBank
   , [FillType] = BType.Descrip
Into #SemiAutoFillBanks
FROM BnkConfiguredBanks As BCfg With (NoLock)
Inner Join BnkAllBanks As BAll With (NoLock) 
   On BAll.AddrBank = BCfg.AddrBank
Inner Join BnkAutoFillType As BType With (NoLock)
   On BAll.AutoFillTypeCode = BType.Code
Where BCfg.IsExcluded = 0
         And
      BType.Code = 'H'

-- Get the list of Rxs with a Count Audit
select 
     C.OrderId
   , CR.IsVisualOnly
Into 
   #AuditRxs
From  CaAuditRx As C With (nolock)
Inner Join CaAuditRule As CR With (NoLock)
   On CR.Id = C.AuditRuleId

------------------------------------------------------------------------------------------
-- Add In Rxs That 
--    - Are Slated for Count Audit no matter the fill type
--    - Are NOT Filled by Dispensers
--    - Are filled by dispensers ONLY in a Semi-Auto Filling Cabinet
--    - Are filled by a robotic pod ONLY if "Rx Inherits Canister Verification" parameter is disabled
------------------------------------------------------------------------------------------
Select
      O.OrderId
    , HistoryDtTm
    , DateFilled
    , PriInternal
    , Ndc
Into
   #RxList
From OeOrder As O With (NoLock)
Left Join OeWorkflowStepInProgress As OW With (NoLock)
   On O.OrderId = OW.ObjectKey
Left Join #AuditRxs As A 
   On A.OrderId = O.OrderId
Left Join @FillTypeFilter AS F
   On O.FillType = F.[Value]
Where O.OrderStatus = 'Filled'
         And
      O.DateFilled Is Not Null               -- Rx has been filled
         And
      O.DateVerified Is Null                 -- Rx has not yet been verified
         And
      OW.ObjectKey Is Null                    -- Rx is not currently being processed at a station
         And 
      (A.IsVisualOnly = 1
       Or (A.IsVisualOnly Is Null
           And (@CfgRxInheritsCanisterVerification = 0
                Or ((O.FillType <> 'A')
                    Or
                    (O.FillType = 'A' And O.AddrBank In (Select Bank From #SemiAutoFillBanks))
                   )
               )
          )
      )  -- Exclude auto fills if they are not queued for audit and we are inheriting canister verification
         And
      (@FillTypeFilterExists = 0 Or F.[Value] Is Not Null)   -- No Filter present or if there is fill-type matched the filter

-- Select applicable Rxs queued for verification
Select Distinct
     O.OrderId
   , O.DateFilled
   , [PriInternalShort] = Left(O.PriInternal, 4)
   , O.Ndc
Into
   #QueuedRxs
From
   #RxList As O
   Left Join ImgRxImgAssoc As IR With (NoLock) 
      On O.OrderId = IR.OrderId
            And
         IR.HistoryDtTm = O.HistoryDtTm
   Left Join ImgImage As I With (NoLock)
      On I.Id = IR.ImgId
Where
   I.ContentCode In (2,3)-- VIAL_CONTENTS-IMAGE(2) ; RX_PACKAGE-IMAGE(3)
   Or @CfgRxDigitalVerificationMode = 'Local'

Insert Into
   #QueuedRxs
Select Distinct
     O.OrderId
   , O.DateFilled
   , [PriInternalShort] = Left(O.PriInternal, 4)
   , O.Ndc
From
   #RxList As O
   Inner Join ImgRxImgAssoc As IR With (NoLock) 
      On O.OrderId = IR.OrderId
            And
         IR.HistoryDtTm = O.HistoryDtTm
   Inner Join ImgImage_IntId As I With (NoLock)
      On I.Id = IR.ImgId
Where
   I.ContentCode In (2,3)-- VIAL_CONTENTS-IMAGE(2) ; RX_PACKAGE-IMAGE(3)


-- If stock location filters were specified in order to filter the Rx NDC's
If Exists (Select 1 From @LocationFilter)
   Begin
      -- Retrieve a list of NDCs to filter results by
      Select Distinct
         L.Ndc
      Into
         #LocationNdcs
      From
         @LocationFilter As F
         Inner Join InvNdcLocAssoc As L With (NoLock)
            On L.Loc Like F.[Value]

      Select Distinct
           OrderId
         , DateFilled
         , PriInternalShort
      From
         #QueuedRxs As Q
         Inner Join #LocationNdcs As N
            On N.Ndc = Q.Ndc
      Order By DateFilled
   End
Else
   Begin
      -- Else, no stock location filters were specified
      Select Distinct
           OrderId
         , DateFilled
         , PriInternalShort
      From
         #QueuedRxs
      Order By DateFilled
   End

Drop Table
   If Exists #QueuedRxs
           , #LocationNdcs
           , #AuditRxs
           , #RxList
           , #SemiAutoFillBanks

Return @@Error
End


/
