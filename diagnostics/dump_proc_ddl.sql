/*
   Object Type:
      Diagnostic query (read-only, no DML)

   Purpose:
      Extract the body (DDL) of a list of stored procedures so they can be
      analyzed offline. Used to seed the cross-list expense-candidate batch
      (tracking sheet rows 22 to 39) which were not committed to the workspace
      stored_procedures folder.

   How to run:
      Execute against any one MFC database (Tolleson recommended for parity
      with the other extracts). Read-only and safe on production.

      The result set has one row per procedure with the full body text in
      the body_text column. In SSMS, right-click the result, Save As, and
      paste each one into stored_procedures/<ProcName>.sql, or save the
      whole grid and split.

      Alternatively, in DBVis, expand any row whose body_text was truncated
      and copy the full text from the cell editor.

   Output:
      object_name | object_id | body_length_chars | body_text

   Reusability:
      Parameterized via the @Procs list at the top. Adjust that list when
      analyzing other procs that need DDL extraction.
*/

Set NoCount On;

Declare @Procs Table (ProcName SysName Primary Key);

Insert Into @Procs (ProcName) Values
   -- Cross-list expense candidates (tracking sheet rows 22 to 39)
   ('lsp_TcdGetNextCountLauncherRx'),
   ('lsp_PwkGetVerifiedRxsWithManualPriority'),
   ('lsp_PrtaGetNextShipmentIdToPrintForStation'),
   ('lsp_CanGetPriorityCanistersForReplenishment'),
   ('lsp_PkgGetReadyToPackOrders'),
   ('lsp_OrdSetGroupPriBoost'),
   ('lsp_BnkGetNextRxToAssignToBank_DispenserBalancing'),
   ('lsp_OrdGetReadyForDispenseRxsInBank'),
   ('lsp_TcdGetTcdsThatCanCountProduct'),
   ('lsp_GovGetTopPriorityToteGroupsToAllocate'),
   ('lsp_SrtGenerateAndReturnSorterToteLocationStats'),
   ('lsp_OrdGetRxNumsMedsFromPo'),
   ('lsp_GovGetSingleVialSorterCounts'),
   ('lsp_DbDeleteNonLeafletOeOrderTextDocument'),
   ('lsp_BnkGetNextNonAssignableRx'),
   ('lsp_OrdSetPriInternalSubPriForRx'),
   ('lsp_OeOrderTextDocumentsClassify'),
   ('lsp_TcdGetOnlyOneTcdOnlineExistsInBankInvPoolAndItHasDualDispPortionRx');

Select
     p.ProcName                                         As object_name
   , o.object_id                                        As object_id
   , Len(Object_Definition(o.object_id))                As body_length_chars
   , Object_Definition(o.object_id)                     As body_text
From @Procs p
Left Join sys.objects o
       On o.name = p.ProcName
      And o.type In ('P', 'PC')
Order By p.ProcName;

-- If any row returns NULL for object_id, that proc does not exist in this
-- database under the given name. Verify the spelling against sys.procedures
-- or sys.objects.
