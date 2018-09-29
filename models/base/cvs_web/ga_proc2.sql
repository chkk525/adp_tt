with gacvs_proc2 as (
select
  *,
  case
    when lower(campaign) like "%cid%" then  SAFE.SUBSTR(campaign, STRPOS(lower(campaign),"cid")+3,5)
    when lower(adgroup) like "%cid%" then  SAFE.SUBSTR(adgroup, STRPOS(lower(adgroup),"cid")+3,5)  
    else "n/a" 
  end as cid,
  case
    when lower(campaign) like "%cid%" then  SAFE.SUBSTR(campaign, 1, STRPOS(lower(campaign),"cid")+8)
    when lower(adgroup) like "%cid%" then  SAFE.SUBSTR(adgroup, 1,STRPOS(lower(adgroup),"cid")+8)  
    else campaign
  end as campaign_short
  from {{ref ('ga_proc')}}    
),

ga_network_mappings_proc2 as (
  select 
    ga_network as network,
    network as network2
  from {{ref('ga_network_mappings_proc')}}    
)

select 
date,  
case 
    when network2 is null then network
    else network2
end as network ,  
case 
    when lower(campaign_short) like "%_ios_%" then "ios"
    when lower(campaign_short) like "%_android_%" then "android"
    when lower(campaign_short) like "%_web_%" then "web"
    else "web"
end as os,
campaign_short as campaign,
cid, 
 0 as installs,
 applications,
 0 as mynumber, 
 0 as confirmnumbers, 
 0 as complete,
 0 as cost
from
  gacvs_proc2 left outer join ga_network_mappings_proc2 using (network)
