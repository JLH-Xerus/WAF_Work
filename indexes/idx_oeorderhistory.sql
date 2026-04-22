USE [PharmAssist]
GO
SET ANSI_PADDING ON
GO

/****** Object:  Index [PK_OeOrderHistory] - Drop and Recreate on Partition Scheme ******/
ALTER TABLE [dbo].[OeOrderHistory] DROP CONSTRAINT [PK_OeOrderHistory]
GO

ALTER TABLE [dbo].[OeOrderHistory] ADD CONSTRAINT [PK_OeOrderHistory] PRIMARY KEY CLUSTERED 
(
	[OrderId] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 92, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

/****** Object:  Index [ByDateFilled] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByDateFilled] ON [dbo].[OeOrderHistory]
(
	[DateFilled] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = ON, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

SET ANSI_PADDING ON
GO

/****** Object:  Index [ByGroupNum] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByGroupNum] ON [dbo].[OeOrderHistory]
(
	[GroupNum] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = ON, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 92, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

/****** Object:  Index [ByHistoryDtTm] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByHistoryDtTm] ON [dbo].[OeOrderHistory]
(
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = ON, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 92, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

/****** Object:  Index [ByLastModifiedDtTm] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByLastModifiedDtTm] ON [dbo].[OeOrderHistory]
(
	[LastModifiedDtTm] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = ON, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

SET ANSI_PADDING ON
GO

/****** Object:  Index [ByOrderStatus] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByOrderStatus] ON [dbo].[OeOrderHistory]
(
	[OrderStatus] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = ON, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 92, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

/****** Object:  Index [ByPatCustId] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByPatCustId] ON [dbo].[OeOrderHistory]
(
	[PatCustId] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = ON, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 92, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

SET ANSI_PADDING ON
GO

/****** Object:  Index [ByRxNumRefillNum] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByRxNumRefillNum] ON [dbo].[OeOrderHistory]
(
	[RxNum] ASC,
	[RefillNum] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = ON, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 92, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO
