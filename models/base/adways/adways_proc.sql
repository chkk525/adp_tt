select
date,
network as network,
campaign as campaign,
campaign as adgroup,
cost as cost
from (
select
*,
first_value(time_of_entry ) over (partition by date, network, campaign order by time_of_entry desc) lv
from `{{ target.project }}.agency_data_pipeline.adways` 
) where time_of_entry = lv