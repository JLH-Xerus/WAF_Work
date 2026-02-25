--/
CREATE PROCEDURE lsp_DbInsertOrUpdateSysProperty 
	@Keyword VarChar(50),
	@Value VarChar(255)
As
/*
Object Type:
	Stored Procedure

Name:
	lsp_DbInsertOrUpdateSysProperty

Version:
	1

Description:
	Insert or update a SysProperty Keyword/Value pair entry.

Parameters:
	Input:
		@Keyword     - The Keyword for which to store the new or revised Value.
		@Value       - The new or revised Value to store for the Keyword.
	Output:
		None

Return Value:
	@Rc - 0 = Successful; Non-0 = Failed

Comments:
	None
*/
	If Exists (Select * From SysProperty Where Keyword = @Keyword)
		Update SysProperty Set Value = @Value Where Keyword = @Keyword
	Else
		Insert Into SysProperty (Keyword, Value) Values (@Keyword, @Value)

	Return @@Error

/
