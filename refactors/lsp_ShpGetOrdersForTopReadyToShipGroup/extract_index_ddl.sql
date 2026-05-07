/*
   Object Type:
      Diagnostic query (read-only, no DML)

   Purpose:
      Extract the full index definition (clustered + nonclustered) for the six
      tables touched by lsp_ShpGetOrdersForTopReadyToShipGroup. The Tolleson
      schema dump (Tolleson_PA_tables.sql) only contains clustered primary keys;
      this query supplies the missing nonclustered key columns and INCLUDEs
      that the v25 plan references (specifically OeOrder.ByGroupNum,
      OeOrder.ByOrderId, OeOrder.ByOrderStatus, OeOrderHistory.ByGroupNum).

   How to run:
      Execute against any one MFC database (Tolleson recommended for parity
      with the existing STATS IO/TIME capture). Read-only; safe on production.

   Output:
      One row per index across the six tables, with key columns ordered by
      key ordinal and INCLUDE columns listed separately. Filtered index
      predicates are surfaced for any filtered indexes.

   Where to put the output:
      Save the result grid to
         MFCs/Tolleson/Tolleson_PA_indexes.sql   (or similar)
      so future refactor sessions can read index DDL without re-running this.

   Reusability:
      Parameterized via the @Tables list at the top. Adjust that list when
      analyzing other procs.
*/

Set NoCount On;

Declare @Tables Table (TableName SysName Primary Key);

-- Adjust this list per analysis target
Insert Into @Tables (TableName) Values
   ('OeOrder'),
   ('OeOrderHistory'),
   ('OeOrderCurrHistoryDtTm'),
   ('OeGroup'),
   ('OeOrderShipmentAssoc'),
   ('OeOrderPoNumAssoc');

Select
     s.name                         As schema_name
   , t.name                         As table_name
   , i.name                         As index_name
   , i.index_id                     As index_id
   , i.type_desc                    As index_type
   , i.is_unique                    As is_unique
   , i.is_primary_key               As is_primary_key
   , i.has_filter                   As has_filter
   , i.filter_definition            As filter_predicate
   , i.fill_factor                  As fill_factor
   -- Key columns in order, with DESC marker if the column sorts descending
   , Stuff((
        Select ', ' + c.name
                  + Case When ic.is_descending_key = 1 Then ' DESC' Else '' End
        From sys.index_columns ic
        Inner Join sys.columns c
            On c.object_id = ic.object_id
           And c.column_id = ic.column_id
        Where ic.object_id     = i.object_id
          And ic.index_id      = i.index_id
          And ic.is_included_column = 0
        Order By ic.key_ordinal
        For Xml Path('')), 1, 2, '')   As key_columns
   -- INCLUDE columns (no order semantics)
   , Stuff((
        Select ', ' + c.name
        From sys.index_columns ic
        Inner Join sys.columns c
            On c.object_id = ic.object_id
           And c.column_id = ic.column_id
        Where ic.object_id     = i.object_id
          And ic.index_id      = i.index_id
          And ic.is_included_column = 1
        Order By ic.index_column_id
        For Xml Path('')), 1, 2, '')   As included_columns
   -- Row count & size for context (helps prioritize index changes)
   , p.rows                         As row_count
   , Cast(au.total_pages * 8.0 / 1024.0 As Decimal(12, 2)) As size_mb
From sys.indexes i
Inner Join sys.tables  t On t.object_id = i.object_id
Inner Join sys.schemas s On s.schema_id = t.schema_id
Inner Join @Tables    tl On tl.TableName = t.name
Left  Join sys.partitions p
       On p.object_id = i.object_id And p.index_id = i.index_id
       And p.partition_number = 1
Left  Join sys.allocation_units au
       On au.container_id = p.partition_id
       And au.type_desc = 'IN_ROW_DATA'
Where i.type > 0   -- skip heaps; we only want clustered + nonclustered indexes
Order By
     t.name
   , i.index_id;
