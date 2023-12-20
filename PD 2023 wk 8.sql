with CTE as(
select * 
    ,1 as mnth
from pd2023_wk08_01
union all 
select *
    ,2 as mnth
from pd2023_wk08_02
union all
select * 
    ,3 as mnth
from pd2023_wk08_03
union all 
select *
    ,4 mnth
from pd2023_wk08_04
union all
select * 
    ,5 mnth
from pd2023_wk08_05
union all 
select *
    ,6 mnth
from pd2023_wk08_06
union all
select * 
    , 7 mnth
from pd2023_wk08_07
union all 
select *
    ,8 mnth
from pd2023_wk08_08
union all
select * 
    ,9 mnth
from pd2023_wk08_09
union all 
select *
    ,10 mnth
from pd2023_wk08_10
union all
select * 
    ,11 mnth
from pd2023_wk08_11
union all 
select *
    ,12 mnth
from pd2023_wk08_12
)
, formatted as (
select date_from_parts(2023, mnth, 01) as file_date
    ,ticker
    ,sector
    ,market
    ,stock_name
    ,substr(market_cap,2,length(market_cap)-2)::double *
    case 
        when right(market_cap, 1)='B' then 1000000000
        when right(market_cap, 1)='M' then 1000000
        end as market_capitalisation
    ,replace(purchase_price, '$')::double as price
from cte
where market_cap != 'n/a'
)
, ranked as (
select case 
        when market_capitalisation<100000000 then 'Small'
        when market_capitalisation<1000000000 then 'Medium'
        when market_capitalisation <100000000000 then 'Large'
        else 'Huge'
        end as market_cap_cat
    ,case 
        when price <25000 then 'Low'
        when price <50000 then 'Medium'
        when price <75000 then 'High'
        when price <100000 then 'Very High'
        end as price_cat
    ,file_date
    ,ticker
    ,sector
    ,market
    ,stock_name
    ,market_capitalisation
    ,price
    ,rank() over(partition by file_date, price_cat, market_cap_cat order by price desc) as rnk
from formatted
)
select *
from ranked 
where rnk <=5
//incorrect number of rows
