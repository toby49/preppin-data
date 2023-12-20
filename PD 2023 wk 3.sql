WITH cte AS
(
         SELECT
                  CASE online_or_in_person
                           WHEN 1 THEN 'Online'
                           WHEN 2 THEN 'In-Person'
                  END                                                                   AS transaction_type,
                  Date_part(quarter, To_date(transaction_date, 'dd/mm/yyyy hh24:mi:ss'))AS quarter,
                  Sum(value)                                                            AS total_value
         FROM     pd2023_wk01
         WHERE    LEFT(transaction_code, 3) = 'DSB'
         GROUP BY transaction_type,
                  quarter )
SELECT     online_or_in_person,
           Replace(t.quarter, 'Q', '')::int AS quarter,
           target
FROM       pd2023_wk03_targets AS t UNPIVOT (target FOR quarter IN (q1,
                                                                    q2,
                                                                    q3,
                                                                    q4))
INNER JOIN cte
ON         cte.transaction_type=t.online_or_in_person
AND        cte.quarter = replace(t.quarter, 'Q', '')::int