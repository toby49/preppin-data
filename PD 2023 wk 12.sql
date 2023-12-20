with cte as (
select max(year) over(order by row_num) as year 
    ,date
    ,bank_holiday
    -- ,date_from_parts(year, month, day)
from pd2023_wk12_uk_bank_holidays
)

, bank_hols as (
select bank_holiday
    , (date || '-' || year)::date as date
from cte 
WHERE LENGTH(TRIM(bank_holiday)) > 0
)

, dates_with_flag as (
select to_date(c.date, 'dd/mm/yyyy') as join_date
    ,new_customers
    ,bank_holiday
    ,date_part('weekday', join_date) as weekday
    ,case
        when bank_holiday is not null 
        or weekday = 6 or weekday =0 then 1
        else 0 end as reporting_flag
from pd2023_wk12_new_customers as c
left join bank_hols as h on join_date=h.date
)

//create lookup table with all non reporting days

, non_reporting as (
select distinct join_date as non_reporting_date
from dates_with_flag
where reporting_flag = 1 
)

//create lookup table with all reporting days

, reporting as (
select distinct join_date as non_reporting_date
from dates_with_flag
where reporting_flag = 0 
)

//to find next reporting date we find the minimum date > that comes after every non_reporting date

, next_day as (

select non_reporting_date
    ,min(join_date) as next_reporting_date
from dates_with_flag as f
inner join non_reporting as n on f.join_date > n.non_reporting_date
where reporting_flag = 0 
group by non_reporting_date
)

//for uk data: 

, uk as (
select coalesce(next_reporting_date, join_date) as date
    ,sum(new_customers) as uk_new_customers
    ,monthname(dateadd('day', 1, coalesce(next_reporting_date, join_date))) || '-' || date_part('year', dateadd('day', 1, coalesce(next_reporting_date, join_date))) as reporting_month
    ,row_number() over(partition by reporting_month order by date) as reporting_day
from dates_with_flag as f 
left join next_day as n on n.non_reporting_date = f.join_date
where reporting_month != 'Jan-2024'
group by date
)

//before joining ROI data, give aliases: 

,roi as (
select reporting_month as roi_reporting_month
    ,reporting_day as roi_reporting_day
    ,new_customers as roi_new_customers
    ,date(reporting_date, 'dd/mm/yyyy') as roi_reporting_date
from pd2023_wk12_roi_new_customers
)

select *
from uk
full outer join roi on uk.date = roi_reporting_date
