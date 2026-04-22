USE [PharmAssist]
GO
SET ANSI_PADDING ON
GO

/****** Object:  Index [PK_OeOrderSecondaryData] - Partition Aligned ******/
ALTER TABLE [dbo].[OeOrderSecondaryData] ADD CONSTRAINT [PK_OeOrderSecondaryData] PRIMARY KEY CLUSTERED 
(
	[OrderId] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

SET ANSI_PADDING ON
GO

/****** Object:  Index [ByExtOrderNum] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByExtOrderNum] ON [dbo].[OeOrderSecondaryData]
(
	[ExtOrderNum] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 92, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

/****** Object:  Index [ByHistoryDtTm] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByHistoryDtTm] ON [dbo].[OeOrderSecondaryData]
(
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 92, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

/****** Object:  Index [ByLastModifiedDtTm] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByLastModifiedDtTm] ON [dbo].[OeOrderSecondaryData]
(
	[LastModifiedDtTm] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

SET ANSI_PADDING ON
GO

/****** Object:  Index [ByPrevRxNum] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [ByPrevRxNum] ON [dbo].[OeOrderSecondaryData]
(
	[PrevRxNum] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 92, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO

/****** Object:  Index [BySourceRxId] - Partition Aligned ******/
CREATE NONCLUSTERED INDEX [BySourceRxId] ON [dbo].[OeOrderSecondaryData]
(
	[SourceRxId] ASC,
	[HistoryDtTm] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 92, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [ps_historydttm]([HistoryDtTm])
GO
