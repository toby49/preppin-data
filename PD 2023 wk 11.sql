with cte as (
select customer
    ,(address_long)/(180/pi()) as address_long_rad
    ,(address_lat)/(180/pi()) as address_lat_rad
    ,branch
    ,(branch_long)/(180/pi()) as branch_long_rad
    ,(branch_lat)/(180/pi()) as branch_lat_rad
    ,round((3963 * acos((sin(address_lat_rad) * sin(branch_lat_rad)) + cos(address_lat_rad) * cos(branch_lat_rad) * cos(branch_long_rad - address_long_rad))),2) as distance_from_branch
    ,row_number() over(partition by customer order by distance_from_branch) as rnk
from pd2023_wk11_dsb_customer_locations 
full outer join pd2023_wk11_dsb_branches
)

select branch
    ,branch_long_rad
    ,branch_lat_rad
    ,distance_from_branch
    ,row_number() over(partition by branch order by distance_from_branch) as Customer_priority
    ,customer
    ,address_long_rad
    ,address_lat_rad
from cte
where rnk = 1