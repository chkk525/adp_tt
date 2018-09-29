with fbads_proc2 as (
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
  from {{ref ('fbads_proc')}}    
)


select 
 date,  
 "Facebook" as network , 
 campaign_short as campaign,
 case 
   when lower(campaign_short) like "%_ios_%" then "ios"
   when lower(campaign_short) like "%_android_%" then "android"
   when lower(campaign_short) like "%_web_%" then "web"
   else "n/a"
  end as os,
 cid, 
 0 as installs,
 0 as applications,
 0 as mynumber, 
 0 as confirmnumbers, 
 0 as complete,
 cost
from
  fbads_proc2
order by date desc 