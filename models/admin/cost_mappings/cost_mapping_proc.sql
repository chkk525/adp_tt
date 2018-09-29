 SELECT
network,
os,
date_from,
install_unit_cost,
application_unit_cost,
completion_cost
FROM
(
  SELECT
    *,
    first_value(time_of_entry) over (
        partition by network, os, date_from order by time_of_entry desc) lv
  FROM `{{ target.project }}.agency_data_pipeline.cost_mapping` 
) WHERE time_of_entry = lv