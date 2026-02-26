/*
Support script: loop lsp_DbDeleteOldRxData over a wide date range in smaller chunks.

Requires lsp_DbDeleteOldRxData to support:
  @HistoryDtTmFrom, @HistoryDtTmTo (inclusive/exclusive)
  @HistoryRowsDeleted OUTPUT
  @AcceptRejectRowsDeleted OUTPUT
*/

ALTER PROCEDURE "dbo"."lsp_DbPurgeHistoryData_Driver"
     @WideFromDtTm DateTime         -- inclusive
   , @WideToDtTm DateTime           -- exclusive
   , @ChunkDays Int = 7             -- days per chunk (use 1-30 typically)

   , @NumOfRowsBlockSize Int = 100000
   , @MaxToDeletePerExec Int = 25000000   -- passed as @MaxToDelete to the worker proc (caps rows per exec)
   , @MaxExecsPerChunk   Int = 1000       -- safety guard
As
Begin
   Set NoCount On;

   If @WideToDtTm <= @WideFromDtTm
      Throw 51001, '@WideToDtTm must be greater than @WideFromDtTm.', 1;

   If @ChunkDays < 1
      Throw 51002, '@ChunkDays must be >= 1.', 1;

   Declare
        @ChunkFrom DateTime = @WideFromDtTm
      , @ChunkTo DateTime
      , @Execs Int
      , @HistDel Int
      , @ArDel Int;

   While @ChunkFrom < @WideToDtTm
      Begin
         Set @ChunkTo = DateAdd(Day, @ChunkDays, @ChunkFrom);
         If @ChunkTo > @WideToDtTm Set @ChunkTo = @WideToDtTm;

         Print '=== Purging window [' + Convert(VarChar(30), @ChunkFrom, 120) + ', ' + Convert(VarChar(30), @ChunkTo, 120) + ') ===';

         Set @Execs = 0;

         While 1 = 1
            Begin
               Set @Execs += 1;
               If @Execs > @MaxExecsPerChunk
                  Throw 51003, 'MaxExecsPerChunk exceeded for a chunk. Possible hot range or predicate mismatch.', 1;

               Set @HistDel = 0;
               Set @ArDel = 0;

               Exec dbo.lsp_DbPurgeHistoryData
                         @OlderThanXDays = 1              -- ignored because window params are provided
                       , @NumOfRowsBlockSize = @NumOfRowsBlockSize
                       , @MaxToDelete = @MaxToDeletePerExec
                       , @HistoryDtTmFrom = @ChunkFrom
                       , @HistoryDtTmTo   = @ChunkTo
                       , @HistoryRowsDeleted = @HistDel Output
                       , @AcceptRejectRowsDeleted = @ArDel Output;

               Print '   Pass ' + Cast(@Execs As VarChar(12))
                     + ' deleted: History=' + Cast(IsNull(@HistDel,0) As VarChar(12))
                     + '; AcceptReject=' + Cast(IsNull(@ArDel,0) As VarChar(12))
                     + '   (' + Convert(VarChar(30), GetDate(), 120) + ')';

               If IsNull(@HistDel,0) = 0 And IsNull(@ArDel,0) = 0
                  Break;
            End

         Set @ChunkFrom = @ChunkTo;
      End
End
