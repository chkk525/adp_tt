select 
date,
  network,
  campaign,
  "n/a" as adgroup,
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
  0 as installs,
  0 as applications,
  0 as mynumber,
  0 as confirmnumbers,
  0 as complete,
  (installs * install_unit_cost 
  + applications * application_unit_cost
  + complete * completion_cost) as cost,
  ROW_NUMBER() OVER(PARTITION BY date, network, os, campaign,cid ORDER BY diff) AS row_num
  from
    (
    select
    date,
    gacvs.network,
    campaign,
    cid,
    gacvs.os,
    installs,
    applications,
    mynumber,
    confirmnumbers,
    complete,
    install_unit_cost,
    application_unit_cost,
    completion_cost,
    date_diff(gacvs.date, cost.date_from, DAY) as diff
    from 
        {{ref('ga_proc2')}} gacvs 
        inner join {{ref('cost_mapping_proc')}} cost
        on (
            gacvs.network = cost.network and gacvs.os = cost.os and gacvs.date >= cost.date_from 
        )
  )
) where row_num = 1 and cost > 0