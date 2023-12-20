//use CTE to reference the pivot to our first unpivot 
with unpivoted as (
select customer_id
    , split_part(category, '___', 1) as Device
    , split_part(category, '___', 2) as Rating
    , Value
from (
    select * 
    from pd2023_wk06_dsb_customer_survey
) as source //using a source query to reference the unpivot
    
    unpivot( Value for category in (MOBILE_APP___EASE_OF_USE, MOBILE_APP___EASE_OF_ACCESS, MOBILE_APP___NAVIGATION, MOBILE_APP___LIKELIHOOD_TO_RECOMMEND, MOBILE_APP___OVERALL_RATING, ONLINE_INTERFACE___EASE_OF_USE, ONLINE_INTERFACE___EASE_OF_ACCESS, ONLINE_INTERFACE___NAVIGATION, ONLINE_INTERFACE___LIKELIHOOD_TO_RECOMMEND, ONLINE_INTERFACE___OVERALL_RATING
    )) as pvt
)

, CTE as(
select * 
from unpivoted
    pivot (SUM(VALUE) for DEVICE in ('MOBILE_APP', 'ONLINE_INTERFACE')) AS P 
//we need to give pivot alias and then state column names in order to remove quotation marks in 'moblie' and 'online'
    (CUSTOMER_ID
    ,RATING
    ,MOBILE_APP
    ,ONLINE_INTERFACE)
where rating != 'OVERALL_RATING'
)

, preferences as (
select Customer_ID
    ,avg(mobile_app)
    ,avg(online_interface)
    , case 
    when avg(mobile_app)-avg(online_interface) >= 2 then 'Mobile Superfan' 
    when avg(mobile_app)-avg(online_interface)>=1 then 'Mobile Fan'
     when avg(mobile_app)-avg(online_interface)<=-2 then 'Online Interface Superfan'
    when avg(mobile_app)-avg(online_interface)<= -1 then 'Online Interface Fan'
    else 'Neutral'
    end as Preference
from CTE
group by customer_ID
)

select preference
    ,round((Count(distinct customer_id)/(select count(distinct customer_id) from preferences))*100, 1)as Percent_of_total
from preferences
group by preference

;select* from unpivoted