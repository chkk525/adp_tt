SELECT
   date,
   network,
   campaign,
   adgroup,
   os,
   installs,
   applications,
   mynumber,
   confirmnumbers,
   complete
FROM
(
 SELECT
   date,
   network,
   campaign,
   adgroup,
   os,
   installs,
   applications,
   mynumber,
   confirmnumbers,
   complete,
   time_of_entry,
   first_value(time_of_entry) over (partition by date,network,campaign, adgroup,os order by time_of_entry desc) lv
 FROM `{{ target.project }}.agency_data_pipeline.adjust` 
)
WHERE time_of_entry = lv and date > date '2017-01-01'