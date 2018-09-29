select
date,
campaign,
"n/a" as adgroup,
cost
from (
select
*,
first_value(time_of_entry ) over (partition by date, campaign order by time_of_entry desc) lv
from `{{ target.project }}.agency_data_pipeline.twads` 
) where time_of_entry = lv