-- Smoke test for the scaffolding.
-- Should produce two PRINT messages in the .messages.txt output,
-- proving the InfoMessage handler is wired up correctly.

PRINT 'Hello from 01_smoke_test.sql';
PRINT CONCAT('Server time is: ', CONVERT(varchar(30), SYSDATETIME(), 121));
