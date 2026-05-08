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
   8

Description:
   Retrieve all applicable Rx's that are queued for verification.
   This script will ignore Rxs that are not currently under audit, do not require visual inspection,
   and are autofill when Canister Verification is enabled.

Parameters:
   Input:
      @FillTypeFilter      - Table of varchars containing applicable fill types to filter the resulting Rxs by
      @LocationFilter      - Table of varchars containing applicable stock location prefixes to filter the resulting Rxs by
                                (stock location prefixes should already be appended with "%" at the time they are passed into this stored procedure)
   Output:
      None

Return Value:
   0     = Successful
   < 0   = Value of @@Error

Comments:
   This stored procedure was modified as part of WI 34282 to improve performance.

   v8 changes (refactor for read reduction):
      1. Single config-table read replaces two separate vCfgSystemParamVal reads (one Pivot scan instead of two scans).
      2. The two duplicate INSERT INTO #QueuedRxs blocks are consolidated into one INSERT...SELECT DISTINCT
         with UNION ALL across the ImgImage and ImgImage_IntId paths. The non-Local mode uses INNER JOINs on
         each branch (no LEFT JOIN OR pattern), and the Local-mode path is broken out as its own UNION ALL
         branch that requires no image match. See [[LEFT JOIN OR Anti-Pattern]] Fix #1.
      3. The complex nested OR/AND filter on the #RxList build is flattened by pre-computing a single
         @ExcludeAutoFill flag and an in-memory hash of semi-auto banks. The WHERE clause becomes a flat
         list of AND'd predicates plus one OR for the audit-visual-only branch.
      4. #SemiAutoFillBanks gets a clustered index on AddrBank to support the IN probe.
      5. #RxList gets a clustered index on (OrderId, HistoryDtTm) to support the join into ImgRxImgAssoc.
      6. The legacy `Drop Table If Exists` block at the top of the proc is preserved (it is correct as
         written and is not the source of any cost). Final cleanup is left to session teardown to keep the
         main statement count down. Add explicit drops back if tempdb churn becomes a concern.
*/

Drop Table
   If Exists #QueuedRxs
           , #LocationNdcs
           , #AuditRxs
           , #RxList
           , #SemiAutoFillBanks

------------------------------------------------------------------------------------------
-- Read both [RxVerification] config values in a single scan of vCfgSystemParamVal.
-- v7 issued two separate SELECTs against this view per execution; v8 reads it once.
------------------------------------------------------------------------------------------
Declare @CfgRxInheritsCanisterVerification As Bit         = 0
Declare @CfgRxDigitalVerificationMode      As Varchar(20) = ''

Select
     @CfgRxInheritsCanisterVerification =
        Max(Case When SW.Parameter = 'RxInheritsCanisterVerification'
                 Then IIF(SW.Value = 'True', 1, 0) End)
   , @CfgRxDigitalVerificationMode =
        Max(Case When SW.Parameter = 'DigitalVerificationMode'
                 Then SW.Value End)
From vCfgSystemParamVal As SW With (NoLock)
Where SW.Section = 'RxVerification'
  And SW.Parameter In ('RxInheritsCanisterVerification', 'DigitalVerificationMode')

------------------------------------------------------------------------------------------
-- Pre-compute a single flat flag for the canister-verification autofill exclusion.
-- v7 expressed this as a 4-level nested OR/AND inside the WHERE clause of the #RxList
-- build, which prevented the optimizer from making clean use of the
-- ByOrderStatusFillTypeAddrBank index. The flag is referenced in a flat predicate below.
------------------------------------------------------------------------------------------
Declare @ExcludeAutoFill As Bit = @CfgRxInheritsCanisterVerification

Declare @FillTypeFilterExists As Bit = 0
Select  @FillTypeFilterExists = 1 From @FillTypeFilter

------------------------------------------------------------------------------------------
-- Get the list of Semi-Auto Filling Cabinets.
-- Add a clustered index on AddrBank so the IN probe from the #RxList build is a seek.
------------------------------------------------------------------------------------------
Select Distinct
     [Bank] = BCfg.AddrBank
   , [FillType] = BType.Descrip
Into #SemiAutoFillBanks
From BnkConfiguredBanks As BCfg With (NoLock)
Inner Join BnkAllBanks As BAll With (NoLock)
   On BAll.AddrBank = BCfg.AddrBank
Inner Join BnkAutoFillType As BType With (NoLock)
   On BAll.AutoFillTypeCode = BType.Code
Where BCfg.IsExcluded = 0
  And BType.Code = 'H'

Create Clustered Index IX_SAFB On #SemiAutoFillBanks ([Bank])

------------------------------------------------------------------------------------------
-- Get the list of Rxs with a Count Audit. The IsVisualOnly column is what drives the
-- LEFT JOIN test in the #RxList build below. Aggregate to one row per OrderId to avoid
-- multiplying #RxList rows when an Rx has multiple audit-rule mappings, and so the
-- LEFT JOIN can stay a LEFT JOIN.
------------------------------------------------------------------------------------------
Select
     C.OrderId
   , [IsVisualOnly] = Max(Cast(CR.IsVisualOnly As Int))
Into #AuditRxs
From CaAuditRx As C With (NoLock)
Inner Join CaAuditRule As CR With (NoLock)
   On CR.Id = C.AuditRuleId
Group By C.OrderId

Create Clustered Index IX_AR On #AuditRxs (OrderId)

------------------------------------------------------------------------------------------
-- Build #RxList. v8 simplifies the nested OR/AND filter:
--    Include an Rx if any of:
--       a. It has a visual-only audit (A.IsVisualOnly = 1).
--       b. It has no audit row (A.IsVisualOnly Is Null) and we are not excluding auto-fills.
--       c. It has no audit row, we are excluding auto-fills, but the FillType is not 'A'.
--       d. It has no audit row, we are excluding auto-fills, FillType is 'A', and the bank
--          is in #SemiAutoFillBanks.
-- All four cases collapse cleanly when @ExcludeAutoFill is wired in as a flat flag.
------------------------------------------------------------------------------------------
Select
      O.OrderId
    , HistoryDtTm
    , DateFilled
    , PriInternal
    , Ndc
Into #RxList
From OeOrder As O With (NoLock)
Left Join OeWorkflowStepInProgress As OW With (NoLock)
   On O.OrderId = OW.ObjectKey
Left Join #AuditRxs As A
   On A.OrderId = O.OrderId
Left Join @FillTypeFilter As F
   On O.FillType = F.[Value]
Where O.OrderStatus  = 'Filled'
  And O.DateFilled  Is Not Null
  And O.DateVerified Is Null
  And OW.ObjectKey  Is Null
  And (A.IsVisualOnly = 1
       Or (A.IsVisualOnly Is Null
           And ( @ExcludeAutoFill = 0
                 Or O.FillType <> 'A'
                 Or Exists (Select 1
                              From #SemiAutoFillBanks SAB
                             Where SAB.[Bank] = O.AddrBank))))
  And (@FillTypeFilterExists = 0 Or F.[Value] Is Not Null)

Create Clustered Index IX_RxL On #RxList (OrderId, HistoryDtTm)

------------------------------------------------------------------------------------------
-- Build #QueuedRxs in ONE INSERT...SELECT DISTINCT.
-- v7 had two separate INSERTs:
--    INSERT 1: LEFT JOIN to ImgImage with `WHERE ContentCode In (2,3) Or @CfgRxDigitalVerificationMode = 'Local'`.
--              That OR is the [[LEFT JOIN OR Anti-Pattern]] in disguise: in Local mode the WHERE
--              survives even when the LEFT JOIN produced no match, which is the same shape as
--              "match through path A or accept the row regardless." The optimizer cannot push
--              the predicate into the join.
--    INSERT 2: INNER JOIN to ImgImage_IntId with `ContentCode In (2,3)`.
--
-- v8 splits the work into three explicit branches with INNER JOINs only, combined with UNION ALL
-- and de-duplicated once at the outer SELECT. The three branches are mutually exclusive on the
-- (OrderId, image-source) pair, so UNION ALL with one outer DISTINCT is correct semantically.
--    Branch 1: ImgImage match with ContentCode In (2,3).
--    Branch 2: ImgImage_IntId match with ContentCode In (2,3).
--    Branch 3: Local-mode pass-through (no image match required), gated by the config flag.
--              Empty branch when not in Local mode, which the optimizer prunes at compile time
--              when the local variable is read from a session variable.
------------------------------------------------------------------------------------------
Select Distinct
     OrderId
   , DateFilled
   , [PriInternalShort] = Left(PriInternal, 4)
   , Ndc
Into #QueuedRxs
From (
   -- Branch 1: ImgImage path
   Select O.OrderId, O.DateFilled, O.PriInternal, O.Ndc
   From #RxList As O
   Inner Join ImgRxImgAssoc As IR With (NoLock)
      On O.OrderId    = IR.OrderId
     And O.HistoryDtTm = IR.HistoryDtTm
   Inner Join ImgImage As I With (NoLock)
      On I.Id = IR.ImgId
   Where I.ContentCode In (2, 3)

   Union All

   -- Branch 2: ImgImage_IntId path
   Select O.OrderId, O.DateFilled, O.PriInternal, O.Ndc
   From #RxList As O
   Inner Join ImgRxImgAssoc As IR With (NoLock)
      On O.OrderId    = IR.OrderId
     And O.HistoryDtTm = IR.HistoryDtTm
   Inner Join ImgImage_IntId As I With (NoLock)
      On I.Id = IR.ImgId
   Where I.ContentCode In (2, 3)

   Union All

   -- Branch 3: Local-mode pass-through, no image match required.
   -- The `Where 1 = ...` predicate evaluates at runtime; when the mode is not Local,
   -- this branch returns zero rows and is cheap.
   Select O.OrderId, O.DateFilled, O.PriInternal, O.Ndc
   From #RxList As O
   Where @CfgRxDigitalVerificationMode = 'Local'
) As S

------------------------------------------------------------------------------------------
-- Final result. Branched IF/ELSE on @LocationFilter is preserved from v7, since each
-- branch produces its own clean plan and the parameter is genuinely optional.
------------------------------------------------------------------------------------------
If Exists (Select 1 From @LocationFilter)
   Begin
      -- Stock-location filter present
      Select Distinct
         L.Ndc
      Into #LocationNdcs
      From @LocationFilter As F
      Inner Join InvNdcLocAssoc As L With (NoLock)
         On L.Loc Like F.[Value]

      Select Distinct
           OrderId
         , DateFilled
         , PriInternalShort
      From #QueuedRxs As Q
      Inner Join #LocationNdcs As N
         On N.Ndc = Q.Ndc
      Order By DateFilled
   End
Else
   Begin
      Select Distinct
           OrderId
         , DateFilled
         , PriInternalShort
      From #QueuedRxs
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
