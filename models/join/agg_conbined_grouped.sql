select
date,
network,
os	,
campaign,
cid,
sum(installs) as installs,
sum(applications) as applications,
sum(mynumber) as mynumbers,
sum(confirmnumbers) as confirmnumbers,
sum(complete) as completes,
sum(cost) as cost
from  {{ref('agg_conbined_union')}}
group by
date,
network,
os	,
campaign,
cid
order by date