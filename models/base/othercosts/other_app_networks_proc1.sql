select 
date,
network,
campaign,
"null" as adgroup,
os,
installs,
applications,
mynumber,
confirmnumbers,
complete,
cost
from (
  select 
  date,
  network,
  campaign,
  os,
  installs,
  applications,
  mynumber,
  confirmnumbers,
  complete,
  (coalesce(installs,0) * install_unit_cost
  + coalesce(applications,0) * application_unit_cost
  + coalesce(complete,0) * completion_cost) as cost,
  ROW_NUMBER() OVER(PARTITION BY date, network, os, campaign,cid ORDER BY diff) AS row_num
  from
    (
    select
    date,
    adjust.network,
    campaign,    
    cid,
    adjust.os,
    installs,
    applications,
    mynumber,
    confirmnumbers,
    complete,
    install_unit_cost,
    application_unit_cost,
    completion_cost,
    date_diff(adjust.date, cost.date_from, DAY) as diff
    from {{ ref('adjust_proc2')}} adjust inner join {{ ref('cost_mapping_proc')}} cost
    on (
      adjust.network = cost.network and adjust.os = cost.os and adjust.date >= cost.date_from 
    )
  )
) where row_num = 1 and cost > 0
order by date desc , os,network, campaign