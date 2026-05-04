select
u.name, 
u.email, 
u.nik, 
g.name as 'title',
SUM(TIME_TO_SEC(tus.video_duration)) AS 'duration',
100 AS 'progress',
MAX(tus.updated_at) as 'last_updated',
'Video' as 'type',
'Growth Path' as 'platform'
from trx_user_growthpaths tug
left join growthpaths g on g.id = tug.growthpath_id
left join users u on u.id = tug.user_id 
left join trx_user_silabuses tus on tus.growthpath_id = g.id and tus.user_id = u.id 
group by u.name, u.email, u.nik, g.name;
