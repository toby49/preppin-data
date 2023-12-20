SELECT * 
FROM
pd2023_wk02_swift_codes
limit 10;

SELECT
transaction_id,
'GB' || check_digits || swift_code || REPLACE(SORT_CODE,'-','') || account_number as IBAN
FROM PD2023_WK02_TRANSACTIONS as T
INNER JOIN pd2023_wk02_swift_codes as S
on S.BANK = t.bank