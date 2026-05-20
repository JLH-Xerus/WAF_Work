PRINT 'Hello from 01_smoke_test.sql';
PRINT CONCAT('Server time is: ', CONVERT(varchar(30), SYSDATETIME(), 121));

SELECT TOP 10 name, type_desc, create_date
FROM   sys.objects
ORDER  BY create_date DESC;
