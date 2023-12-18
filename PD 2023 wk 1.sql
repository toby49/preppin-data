SELECT Split_part(transaction_code, '-', 1) AS Bank,

--Renaming online or in-person values       
       CASE
         WHEN online_or_in_person = 1 THEN 'Online'
         WHEN online_or_in_person = 2 THEN 'In-Person'
       end                                  AS ONLINE_OR_IN_PERSON,

--Renaming dates (dayofweek function returns a number)       
       CASE Date_part(dayofweek, To_date(transaction_date,
                                 'dd/mm/yyyy hh24:mi:ss'))
         WHEN 0 THEN 'Monday'
         WHEN 1 THEN 'Tuesday'
         WHEN 2 THEN 'Wednesday'
         WHEN 3 THEN 'Thursday'
         WHEN 4 THEN 'Friday'
         WHEN 5 THEN 'Saturday'
         WHEN 6 THEN 'Sunday'
       end                                  AS TRANSACTION_DATE

FROM   pd2023_wk01
LIMIT  100;


--Output 1:
SELECT Split_part(transaction_code, '-', 1) AS Bank,
       Sum(value)                           AS Total_Value
FROM   pd2023_wk01
GROUP  BY bank;

--Output 2:
SELECT Split_part(transaction_code, '-', 1) AS Bank,
       CASE
         WHEN online_or_in_person = 1 THEN 'Online'
         WHEN online_or_in_person = 2 THEN 'In-Person'
       end                                  AS ONLINE_OR_IN_PERSON,
       CASE Date_part(dayofweek, To_date(transaction_date,
                                 'dd/mm/yyyy hh24:mi:ss'))
         WHEN 0 THEN 'Monday'
         WHEN 1 THEN 'Tuesday'
         WHEN 2 THEN 'Wednesday'
         WHEN 3 THEN 'Thursday'
         WHEN 4 THEN 'Friday'
         WHEN 5 THEN 'Saturday'
         WHEN 6 THEN 'Sunday'
       end                                  AS TRANSACTION_DAY,
       Sum(value)                           AS Total_Value
FROM   pd2023_wk01
GROUP  BY Bank,
          ONLINE_OR_IN_PERSON,
          TRANSACTION_DAY;

--Output 3:
SELECT Split_part(transaction_code, '-', 1) AS Bank,
       customer_code,
       Sum(value)                           AS Total_Value
FROM   pd2023_wk01
GROUP  BY 1,
          2; 
