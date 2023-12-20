with info as (
select account_number
    , account_type
    , value as account_holder_id
    , balance_date
    , balance
from pd2023_wk07_account_information,LATERAL SPLIT_TO_TABLE(account_holder_id, ', ')
where account_holder_id is not null 
)

, personal as(
select h.account_holder_id
    , name
    ,date_of_birth
    , first_line_of_address
    ,account_number
    ,account_type
    ,balance_date
    ,balance
    , '0'||contact_number as telephone
from pd2023_wk07_account_holders as h
inner join info on info.account_holder_id=h.account_holder_id
)

select p.transaction_id
    ,account_to
    ,transaction_date
    ,value
    ,personal.account_number
    ,account_type
    ,Balance_date
    ,personal.balance
    ,Name
    ,date_of_birth
    ,telephone
    ,first_line_of_address
from pd2023_wk07_transaction_detail as d
inner join pd2023_wk07_transaction_path as p on p.transaction_id=d.transaction_id
inner join personal on personal.account_number=p.account_from
where cancelled_='N'
and value>1000
and account_type != 'Platinum'

