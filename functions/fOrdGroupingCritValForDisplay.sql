--/
CREATE FUNCTION fOrdGroupingCritValForDisplay 
   (@GroupNum VarChar(10))
Returns
   varchar(max)
As
Begin
/*
Object Type:
   User-defined function

Name:
   fOrdGroupingCritValForDisplay

Version:
   3

Description:
   Returns the formatted grouping criteria value (appended with any split code reasons) of an Rx group for display.

Parameters:
   Input:
      @GroupNum      - The GroupNum of the Rx group to return the formatted grouping criteria value for.
   Output:
      None

Return Value:
   The Rx groups GroupingCriteriaVal appended with any applicable split reason descriptions.

Comments:
   None
*/


   Declare @GroupingCritVal varchar(120)

   Select
      @GroupingCritVal = GroupingCriteriaVal
   From
      OeGroup with (nolock)
   Where
      GroupNum = @GroupNum


   -- If the Rx group has been split
   If Exists (Select 1 From OeGroupSplitCodeAssoc with (nolock) where GroupNum = @GroupNum)
      Begin

         Declare @Suffix varchar(max)

         -- Build the list of SplitCode descriptions associated with the Rx group
         Select
            @Suffix = Coalesce(@Suffix + '; ', '') + S.Descrip
         From
            OeGroupSplitCodeAssoc As GSA with (nolock)
            Join OeGroupSplitCode As S with (nolock) On S.Code = GSA.SplitCode
         Where
            GSA.GroupNum = @GroupNum

         -- Return the GroupingCriteriaVal appended with the list of SplitCode descriptions
         Set @GroupingCritVal = @GroupingCritVal + '   (' + @Suffix + ')'

      End

   Return @GroupingCritVal

End

/
