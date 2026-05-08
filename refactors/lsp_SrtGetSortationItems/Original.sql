--/
CREATE PROCEDURE lsp_SrtGetSortationItems 
    @Barcode varchar(100) = NULL,
    @SortationCode varchar(30) = NULL
AS
------------------------------------------------------------------------------------------------------------------------------------------------------
-- Procedure:
--      lsp_SrtGetSortationItems
--
-- Version:
--		2
--
-- Description:
--      Retrieves one or more Sortation Items based on the input parameters.
--      If no parameters are provided, retrieves all records from the table.
--
-- Parameters:
--      @Barcode            - Barcode for the Sortable Item (optional)
--      @SortationCode      - Sortation Code (optional)
--
------------------------------------------------------------------------------------------------------------------------------------------------------
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        Barcode,
        SortationCode
    FROM
        SrtSortationItem WITH (NOLOCK)
    WHERE 
        (@Barcode IS NULL OR Barcode = @Barcode) AND
        (@SortationCode IS NULL OR SortationCode = @SortationCode)
END

/
