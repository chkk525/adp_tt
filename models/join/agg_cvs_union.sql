SELECT
date,  network , os, campaign, cid, installs, applications, mynumber, confirmnumbers, complete, cost
,"adjust" as datasource 
FROM
{{ref('adjust_proc2')}}
 
UNION ALL

SELECT
date,  network , os, campaign, cid, installs, applications, mynumber, confirmnumbers, complete, cost
,"ga_proc2" as datasource 
FROM
{{ref('ga_proc2')}}
