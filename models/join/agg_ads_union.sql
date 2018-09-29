SELECT
date,  network , os, campaign, cid, installs, applications, mynumber, confirmnumbers, complete, cost
,"fbads" as datasource 
FROM
{{ref('fbads_proc2')}}
 
UNION ALL

SELECT
date,  network , os, campaign, cid, installs, applications, mynumber, confirmnumbers, complete, cost
,"gads" as datasource 
FROM
{{ref('gads_proc2')}}

UNION ALL

SELECT
date, network , os, campaign, cid, installs, applications, mynumber, confirmnumbers, complete, cost
, "asa" as datasource
FROM
    {{ref
('asa_proc2')}}

UNION ALL

SELECT
date,  network , os, campaign, cid, installs, applications, mynumber, confirmnumbers, complete, cost
,"twads" as datasource 
FROM
{{ref('twads_proc2')}}

UNION ALL

SELECT
date,  network , os, campaign, cid, installs, applications, mynumber, confirmnumbers, complete, cost
,"other_web" as datasource 
FROM
{{ref('other_web_networks_proc2')}}

UNION ALL

SELECT
date,  network , os, campaign, cid, installs, applications, mynumber, confirmnumbers, complete, cost
,"other_app" as datasource 
FROM
{{ref('other_app_networks_proc2')}}