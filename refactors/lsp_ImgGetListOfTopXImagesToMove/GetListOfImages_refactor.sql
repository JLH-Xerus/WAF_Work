use pharmassist
go

set statistics io on
set statistics time on

Declare @MaxImages Int

Select @MaxImages = Convert(Int, [Value])
From vCfgSystemParamVal With (NoLock)
Where Section = 'Operation'
And Parameter = 'MaxNumberOfImagesToPullInASingleRunFromBgWk'

select
	i.id,i.contentcode
into #qualifyingImages
from vImgImage i with (nolock)
where i.ImageData is not null
and (i.IsMovedToFile is null or i.IsMovedToFile <> 1)
and i.ArchivedDtTm is null

create clustered index byQIId on #qualifyingImages (id)

select top (@maxImages) id, contentcode
from #qualifyingImages qi
where exists (
		select 1 from dbo.ImgRxImgAssoc ir with (nolock)
		inner join dbo.oeorderhistory oh With (NoLock) on ir.OrderId = oh.OrderId
		where ir.ImgId = qi.id
		and oh.orderstatus = 'Shipped'
	)
or exists (
		select 1 from ImgCanImgAssoc ic with (nolock)
		inner join CanCanister c with (nolock) on ic.CanisterSn = c.CanisterSn
		where ic.ImgId = qi.Id and c.status = 'Verified'
	)

drop table #qualifyingImages

set statistics io off
set statistics time off
