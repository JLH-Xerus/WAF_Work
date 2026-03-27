--/
CREATE PROCEDURE lsp_PmssIwebGetTopQueuedTrx 
As 

/*
Object Type:
   Stored Procedure

Name:
   lsp_PmssIwebGetTopQueuedTrx

Version:
   2

Description:
   Get the oldest Queued Fill or Cancel [Rx/Order] request from the PMSS Captured XML Transactions table.

Parameters:
   Input:
      None

   Output:
      None

Return Value:
   None

Comments:
   For use with PMSS IA Web Services interface only.
*/

Select Top 1
  Id, TrxKey, Trx, TrxStream
From
  PmssCapturedXmlTrxs With (NoLock)
Where
  TrxStateCode = 'Q' And
  Trx In ('fillRequest','cancelRxRequest','cancelOrderRequest')
Order By
  Id


/
