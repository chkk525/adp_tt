with adjust_proc2 as (
  select
  *,
  case
    when lower(campaign) like "%cid%" then  SAFE.SUBSTR(campaign, STRPOS(lower(campaign),"cid")+3,5)
    when lower(adgroup) like "%cid%" then  SAFE.SUBSTR(adgroup, STRPOS(lower(adgroup),"cid")+3,5)  
    else "n/a" 
  end as cid,
  case
    when lower(campaign) like "%cid%" then  SAFE.SUBSTR(campaign, 1, STRPOS(lower(campaign),"cid")+7)
    when lower(adgroup) like "%cid%" then  SAFE.SUBSTR(adgroup, 1,STRPOS(lower(adgroup),"cid")+7)  
    else campaign
  end as campaign_short
  from {{ref('adjust_proc')}}
),

adjust_network_mappings2 as (
  select 
    adjust_network as network,
    network as network2
  from {{ref('adjust_network_mappings_proc')}}    
)


select
    date,
    network,
    os,
    campaign,
    cid,
    sum(installs) as installs,
    sum(applications) as applications,
    sum(mynumber) as mynumber,
    sum(confirmnumbers) as confirmnumbers,
    sum(complete) as complete,
    sum(cost) as cost
from(
    select 
        date,  
        case 
            when network2 is null then network
            else network2
        end as network , 
    os,
    campaign_short as campaign,
    cid,
    installs,
    applications,
    mynumber,
    confirmnumbers,
    complete, 
    0 as cost
    from
      adjust_proc2 left outer join adjust_network_mappings2 using (network)
)
group by 
     date, network, os, campaign, cid
