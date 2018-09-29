select
date,
campaign_name as campaign,
ad_set_name as adgroup,
amount_spent as cost
from (
select
*,
first_value(time_of_entry ) over (partition by date, campaign_name, ad_set_name order by time_of_entry desc) lv
from `{{ target.project }}.agency_data_pipeline.fbads` 
) where time_of_entry = lv