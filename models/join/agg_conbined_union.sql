SELECT
date,  network , os, campaign, cid, installs, applications, mynumber, confirmnumbers, complete, cost
,"adjust" as datasource 
FROM
{{ref('agg_ads_union')}}

UNION ALL

SELECT
date,  network , os, campaign, cid, installs, applications, mynumber, confirmnumbers, complete, cost
,"fbads" as datasource 
FROM
{{ref('agg_cvs_union')}}
 