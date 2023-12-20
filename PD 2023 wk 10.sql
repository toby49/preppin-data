//we can create parameters using the follwing query
SET SELECTED_DATE = '2023-02-01';

with input as (
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
    ,sum(coalesce(value,0)) over (partition by account_id order by transaction_date, value desc) + balance as balance
    //^^^ this is a window operation therefore we do not need a group by statement
    //coalesce() is like zn() in tableau
from cte
order by account_id, transaction_date, value desc
)

, daily as (
select account_id
    ,transaction_date
    ,sum(transaction_value) as transaction_value
from input
group by account_id, transaction_date
)

// to find balances for each day of transaction: 
    //need to create CTE so that we can query rownumber (filter to final balance for every transaction)

, balances as(
select *
    , row_number() over(partition by account_id, transaction_date order by transaction_value) as rn
from input
)

, daily_summary as (
select b.account_id
    ,b.transaction_date
    ,d.transaction_value
    ,balance
from balances as b
inner join daily as d on d.account_id = b.account_id and d.transaction_date = b.transaction_date
where rn=1
)

//Scaffold the data so that each account has a row for each day between 31st jan and 14th feb
    //we can using recursive CTE logic:
    -- with numbers as (
    -- select 1 as n 
    -- union all 
    -- select n + 1
    -- from numbers 
    -- where n < 10 
    -- )

    -- select * from numbers

//we also need a distinct list of account numbers to then bring in to this cte so that we have a row for every date for every account.
,accountnumbers as (

select distinct account_id 
from input
)

,numbers as (

select account_id 
    ,'2023-01-31'::date as n
from accountnumbers

union all 

select account_id
    ,dateadd('day', 1, n)
from numbers 
where n < '2023-02-14'::date
)

, daily_view as(
select n.account_id
    ,n.n as transaction_date
    ,s.transaction_value as transaction_value1
    ,s.balance
    ,b.balance as balance1
    ,datediff('day', b.transaction_date, n.n) as datediff
    ,row_number() over (partition by n.account_id, n.n order by datediff('day', b.transaction_date, n.n)) as rn
from numbers as n 
left join daily_summary as s on n.account_id = s.account_id and n.n=s.transaction_date
inner join balances as b on b.account_id=n.account_id and b.transaction_date<= n.n
order by n.account_id , n.n
)

select account_id
    , transaction_date
    , transaction_value1 as transaction_value
    , balance1 as balance
from daily_view
where rn =1 
//this is where we add the parameter filter using '$'+parameter
and transaction_date=$selected_date;


