//instead of pivoting we can create two queries, one for inflows, one for outflows (value will be -ve) then union
with cte as( 
select account_to as account_id
    ,transaction_date
    ,value
    ,balance
from pd2023_wk07_transaction_detail as d
inner join pd2023_wk07_transaction_path as p on p.transaction_id = d.transaction_id
inner join pd2023_wk07_account_information as i on i.account_number=p.account_to
where cancelled_ = 'N'
and balance_date = '2023-01-31'
//balance_date filter helps us bring in the starting balance

union all 

select account_from as account_id
    ,transaction_date
    ,(value)*-1 as value 
    ,balance
from pd2023_wk07_transaction_detail as d
inner join pd2023_wk07_transaction_path as p on p.transaction_id = d.transaction_id
inner join pd2023_wk07_account_information as i on i.account_number=p.account_from
where cancelled_ = 'N'
and balance_date = '2023-01-31'

union all 

select account_number as account_id
    ,balance_date as transaction_date
    ,null as value
    ,balance
from pd2023_wk07_account_information
)

//calculate running sum of value, ordering by date and value if multiple transactions on the same day

select account_id
    ,transaction_date
    ,value as transaction_value
    ,sum(coalesce(value,0)) over (partition by account_id order by transaction_date, value desc) + balance as running_sum_of_balance
    //^^^ this is a window operation therefore we do not need a group by statement
    //coalesce() is like zn() in tableau
    ,balance
from cte
order by account_id, transaction_date, value desc