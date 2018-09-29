SELECT
distinct adjust_network, network 
FROM
(
  SELECT
    *,
    first_value(time_of_entry) over (partition by adjust_network, network order by time_of_entry desc) lv
	FROM `{{ target.project }}.agency_data_pipeline.adjust_network_mappings` 
)
WHERE time_of_entry = lv