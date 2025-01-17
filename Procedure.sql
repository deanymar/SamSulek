USE [SamSulekData]
GO
/****** Object:  StoredProcedure [dbo].[UpdateData]    Script Date: 29-Jun-24 6:46:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER procedure [dbo].[UpdateData]
as
truncate table [SamSulekDW].[dbo].[MuslcesWorked]
truncate table [SamSulekDW].[dbo].[SamSulekInfo]
truncate table [SamSulekDW].[dbo].[VideoData]

insert into [SamSulekDW].[dbo].[MuslcesWorked]
select *
from (
SELECT  T0.video_id,T0.publishedAt,
case 
	when value like '%chest%' then 'Chest'
	when value like '%delts%' or value like '%side%' or value like '%shoulders%' or value like '%rear%' then 'Shoulders'
	when  value like '%cardio%' then 'Cardio'
	when value like '%ham%' or value like '%quad%' or value like '%legs%' then 'Legs'
	when  value like '%calves%'then 'Calves'
	when value like '%bi%' then 'Bicep'
	When value like '%tri%' then 'Triceps'
	when value like '%arms%' then 'Arms'
	when value like '%fore%' then 'Forearms'
	when value like '%back%' then 'Back'
	end as MusclesWorked
FROM VideoData T0
    CROSS APPLY STRING_SPLIT(title, ' ')
	
	) Z
	where Z.MusclesWorked is not null
	group by z.publishedAt,z.video_id,Z.MusclesWorked
	order by publishedAt desc
	
insert into [SamSulekDW].[dbo].[SamSulekInfo]
select * 
from [dbo].[ChannelData]

insert into [SamSulekDW].[dbo].[VideoData]
SELECT
    video_id,
    title,
    publishedAt,
    publishedDay,
    viewCount,
    likeCount,
    commentCount,
    durationMinutes
from [dbo].[VideoData]



