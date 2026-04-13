--/
ALTER PROCEDURE [dbo].[lsp_SrtGetSortationItems]
    @Barcode varchar(100) = NULL,
    @SortationCode varchar(30) = NULL
AS
------------------------------------------------------------------------------------------------------------------------------------------------------
-- Procedure:
--      lsp_SrtGetSortationItems
--
-- Version:
--      3
--
-- Description:
--      Retrieves one or more Sortation Items based on the input parameters.
--      If no parameters are provided, retrieves all records from the table.
--
-- Parameters:
--      @Barcode            - Barcode for the Sortable Item (optional)
--      @SortationCode      - Sortation Code (optional)
--
-- Comments:
--      v3 - Performance refactoring:
--         Replaced catch-all OR pattern:
--            WHERE (@Barcode IS NULL OR Barcode = @Barcode)
--              AND (@SortationCode IS NULL OR SortationCode = @SortationCode)
--         with IF/ELSE branching to allow the optimizer to generate a seekable
--         plan for each parameter combination. The original pattern forced a
--         table/index scan on every call because the optimizer could not
--         short-circuit the IS NULL check at compile time.
--
--         With only 2 optional parameters (4 combinations), explicit branching
--         is the simplest and most maintainable approach. Each branch produces
--         a stable cached plan that uses index seeks when parameters are provided.
--
--         Expected impact: ~95K reads/exec (scan) -> <10 reads/exec (seek) for
--         single-row lookups by Barcode or SortationCode on a 4-column table.
--
------------------------------------------------------------------------------------------------------------------------------------------------------
BEGIN
    SET NOCOUNT ON;

    If @Barcode Is Not Null And @SortationCode Is Not Null
    Begin
        -- Both parameters provided: seek on Barcode + SortationCode
        Select
            Barcode,
            SortationCode
        From
            SrtSortationItem With (NoLock)
        Where
            Barcode = @Barcode
            And SortationCode = @SortationCode
    End
    Else If @Barcode Is Not Null
    Begin
        -- Barcode only: seek on Barcode
        Select
            Barcode,
            SortationCode
        From
            SrtSortationItem With (NoLock)
        Where
            Barcode = @Barcode
    End
    Else If @SortationCode Is Not Null
    Begin
        -- SortationCode only: seek on SortationCode
        Select
            Barcode,
            SortationCode
        From
            SrtSortationItem With (NoLock)
        Where
            SortationCode = @SortationCode
    End
    Else
    Begin
        -- No parameters: return all rows (scan is correct here)
        Select
            Barcode,
            SortationCode
        From
            SrtSortationItem With (NoLock)
    End
END
GO
/
