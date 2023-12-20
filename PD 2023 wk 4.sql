// Union the data: 

With CTE as (
SELECT *, 'pd2023_wk04_january' AS Table_Name
FROM pd2023_wk04_january
UNION ALL
SELECT *, 'pd2023_wk04_february' AS Table_Name
FROM pd2023_wk04_february
UNION ALL
SELECT *, 'pd2023_wk04_march' AS Table_Name
FROM pd2023_wk04_march
UNION ALL
SELECT *, 'pd2023_wk04_april' AS Table_Name
FROM pd2023_wk04_april
UNION ALL
SELECT *, 'pd2023_wk04_may' AS Table_Name
FROM pd2023_wk04_may
UNION ALL
SELECT *, 'pd2023_wk04_june' AS Table_Name
FROM pd2023_wk04_june
UNION ALL
SELECT *, 'pd2023_wk04_july' AS Table_Name
FROM pd2023_wk04_july
UNION ALL
SELECT *, 'pd2023_wk04_august' AS Table_Name
FROM pd2023_wk04_august
UNION ALL
SELECT *, 'pd2023_wk04_september' AS Table_Name
FROM pd2023_wk04_september
UNION ALL
SELECT *, 'pd2023_wk04_october' AS Table_Name
FROM pd2023_wk04_october
UNION ALL
SELECT *, 'pd2023_wk04_november' AS Table_Name
FROM pd2023_wk04_november
UNION ALL
SELECT *, 'pd2023_wk04_december' AS Table_Name
FROM pd2023_wk04_december
)

// Parse joining date:

, PRE_PIVOT as(
Select 
    ID,
    demographic,
    DATE_FROM_PARTS(2023, date_part('month',date(SPLIT_PART(TABLE_NAME, '_', 3), 'MMMM')), JOINING_DAY) AS JOINING_DATE,
    value
from CTE 
)

// Pivot:
// 3rd CTE 'Post Pivot' needed in order to query RN since row_number() is a window operation
, Post_Pivot as (
select 
    id,
    joining_date,
    ethnicity,
    account_type,
    date_of_birth::date as Date_of_Birth,
    // To find duplicate columns, add a row ID restarting for every customer
    row_number() OVER(Partition by id ORDER by joining_date ASC) as rn
from PRE_PIVOT
PIVOT (MAX(value) FOR DEMOGRAPHIC IN ('Ethnicity', 'Account Type','Date of Birth')) as P
    (id,
    joining_date,
    ethnicity,
    account_type,
    date_of_birth)
)
//Below removes duplicates
select 
    id,
    joining_date,
    account_type,
    date_of_birth,
    ethnicity
    from Post_Pivot
where rn=1

//Could also just aggregate/ min(joindate) to remove duplicates instead of the row number window function:

-- from pre_pivot
--     pivot (max(value) for Demographic in ('Ethnicity', 'Account Type', 'Date of Birth')) as P
--         (id,
--     joining_date,
--     ethnicity,
--     account_type,
--     date_of_birth)
-- group by id, ethnicity, account_type, date_of_birth
