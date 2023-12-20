with CTE as (
select 
    split_part(transaction_code, '-', 1) as bank
    ,monthname(date(transaction_date, 'dd/mm/yyyy hh24:mi:ss')) as mnth
    ,sum(value) as total_value
    , rank() over(partition by monthname(date(transaction_date, 'dd/mm/yyyy hh24:mi:ss')) order by sum(value) desc) as rnk
from pd2023_wk01
group by mnth, bank
)
,avg_rank as (
select 
    bank
    , avg(rnk) as avg_rnk_per_bank
from CTE
group by bank
)
,avg_value_by_rank as (
select 
    rnk
    , avg(total_value) as avg_value
from cte
group by rnk
)
select 
    mnth
    ,cte.bank
    ,total_value
    ,cte.rnk
    ,avg_value
    ,avg_rnk_per_bank
    
from CTE
inner join avg_rank as r on r.bank=cte.bank
inner join avg_value_by_rank as av on av.rnk=cte.rnk
