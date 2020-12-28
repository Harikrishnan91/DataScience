-- Setting the preferences of sql workbench 8.0.21
-- Server Used : MySQL Server
SET SQL_SAFE_UPDATES = 0;
SET sql_mode='';
-- Step 1 : Create appropriate tables with Date, Closing Price and 20 Day MA, 50 Day MA
-- Alter the table to add a new column stock_date with type as date
-- The reason to add a new column is if the default date column is selected as Date type while creating the table,
-- None of the records are inserted.
alter table BAJAJ_AUTO
add stock_date date;

-- Perform an update on column stock_date  using str_to_date function. The str_to_Date function accepts two
-- values namely the column name which needs to be converted and the format the date is present in that column
-- In our case, the date follows the pattern DD-MONTH-YYYY

update BAJAJ_AUTO set stock_date = str_to_date(`date`, '%d-%M-%Y');

-- CREATE table such that the moving average of previous 19 rows and 49 rows 
-- respectively are considered

-- On inspecting the source table ,the dates are given in descending order i.e. from 2018 to 2015
-- Hence it is imperative to sort them first in ascending order to compute the moving averages
-- We use the capacity of the window functions to achieve the same
-- As seen below we fetch the average of Close Price for a range of preceding 19 rows 
-- and the current row to calculate the 20 Day MA. Same principle applies to 50 Day MA except 
-- the number of preceding days is 49 instead of 19.

drop table if exists BAJAJ1;
create table BAJAJ1
  as (select stock_date as "Date", `Close Price` as  "Close Price" , 
  avg(`Close Price`) over (order by stock_date asc rows 19 preceding) as "20 Day MA",
  avg(`Close Price`) over (order by stock_date asc rows 49 preceding) as "50 Day MA",
  row_number()     over (order by stock_date ) as "rownum"
  from BAJAJ_AUTO);
 
 -- Query the created table
SELECT * FROM BAJAJ1;

-- It is better to ignore the first 50 rows  rather than trying to deal with NULL or by filling it with average value as that would make no practical sense.
-- Hence deleting those rows to ignore them in further analysis.

delete  from BAJAJ1 where rownum < 50;
commit;

-- Dropping the column rownum  using Alter and Drop command
alter table BAJAJ1 drop column rownum;


-- Alter the table to add a new column stock_date with type as date
alter table EICHER
add stock_date date;

-- Perform an update on column stock_date  using str_to_date function. The str_to_Date function accepts two
-- values namely the column name which needs to be converted and the format the date is present in that column
-- In our case, the date follows the pattern DD-MONTH-YYYY
update EICHER set stock_date = str_to_date(`date`, '%d-%M-%Y');

-- CREATE table such that the moving average of previous 19 rows and 49 rows 
-- respectively are considered
create table EICHER1
  as (select stock_date as "Date", `Close Price` as  "Close Price" , 
  avg(`Close Price`) over (order by stock_date asc rows 19 preceding) as "20 Day MA",
  avg(`Close Price`) over (order by stock_date asc rows 49 preceding) as "50 Day MA",
  row_number()     over (order by stock_date ) as "rownum"
  from EICHER);
  
SELECT * FROM EICHER1;

-- It is better to ignore the first 50 rows  rather than trying to deal with NULL or by filling it with average value as that would make no practical sense.
-- Hence deleting those rows to ignore them in further analysis.

delete  from EICHER1 where rownum < 50;
commit;

-- Dropping the column rownum  using Alter and Drop command
alter table EICHER1 drop column rownum;


-- Alter the table to add a new column stock_date with type as date
alter table HERO
add stock_date date;

-- Perform an update on column stock_date  using str_to_date function. The str_heroto_Date function accepts two
-- values namely the column name which needs to be converted and the format the date is present in that column
-- In our case, the date follows the pattern DD-MONTH-YYYY
update HERO set stock_date = str_to_date(`date`, '%d-%M-%Y');

-- CREATE table such that the moving average of previous 19 rows and 49 rows 
-- respectively are considered
create table HERO1
  as (select stock_date as "Date", `Close Price` as  "Close Price" , 
  avg(`Close Price`) over (order by stock_date asc rows 19 preceding) as "20 Day MA",
  avg(`Close Price`) over (order by stock_date asc rows 49 preceding) as "50 Day MA",
  row_number()     over (order by stock_date ) as "rownum"
  from HERO);
  
SELECT * FROM HERO1;

-- It is better to ignore the first 50 rows  rather than trying to deal with NULL or by filling it with average value as that would make no practical sense.
-- Hence deleting those rows to ignore them in further analysis.

delete  from HERO1 where rownum < 50;
commit;

-- Dropping the column rownum using Alter and Drop command
alter table HERO1 drop column rownum;

-- Alter the table to add a new column stock_date with type as date
alter table INFOSYS
add stock_date date;

-- Perform an update on column stock_date  using str_to_date function. The str_to_Date function accepts two
-- values namely the column name which needs to be converted and the format the date is present in that column
-- In our case, the date follows the pattern DD-MONTH-YYYY
update INFOSYS set stock_date = str_to_date(`date`, '%d-%M-%Y');

-- CREATE table such that the moving average of previous 19 rows and 49 rows 
-- respectively are considered
create table INFOSYS1
  as (select stock_date as "Date", `Close Price` as  "Close Price" , 
  avg(`Close Price`) over (order by stock_date asc rows 19 preceding) as "20 Day MA",
  avg(`Close Price`) over (order by stock_date asc rows 49 preceding) as "50 Day MA",
  row_number()     over (order by stock_date ) as "rownum"
  from INFOSYS);
  
SELECT * FROM INFOSYS1;

-- It is better to ignore the first 50 rows  rather than trying to deal with NULL or by filling it with average value as that would make no practical sense.
-- Hence deleting those rows to ignore them in further analysis.

delete  from INFOSYS1 where rownum < 50;
commit;

-- Dropping the column rownum using Alter and Drop command
alter table INFOSYS1 drop column rownum;

-- Alter the table to add a new column stock_date with type as date
alter table TCS
add stock_date date;

-- Perform an update on column stock_date  using str_to_date function. The str_to_Date function accepts two
-- values namely the column name which needs to be converted and the format the date is present in that column
-- In our case, the date follows the pattern DD-MONTH-YYYY
update TCS set stock_date = str_to_date(`date`, '%d-%M-%Y');

-- CREATE table such that the moving average of previous 19 rows and 49 rows 
-- respectively are considered
create table TCS1
  as (select stock_date as "Date", `Close Price` as  "Close Price" , 
  avg(`Close Price`) over (order by stock_date asc rows 19 preceding) as "20 Day MA",
  avg(`Close Price`) over (order by stock_date asc rows 49 preceding) as "50 Day MA",
  row_number()     over (order by stock_date ) as "rownum"
  from TCS);
  
SELECT * FROM TCS1;

-- It is better to ignore the first 50 rows  rather than trying to deal with NULL or by filling it with average value as that would make no practical sense.
-- Hence deleting those rows to ignore them in further analysis.

delete  from TCS1 where rownum < 50;
commit;

-- Dropping the column rownum using Alter and Drop command
alter table TCS1 drop column rownum;

-- Alter the table to add a new column stock_date with type as date
alter table TVS
add stock_date date;

-- Perform an update on column stock_date  using str_to_date function. The str_to_Date function accepts two
-- values namely the column name which needs to be converted and the format the date is present in that column
-- In our case, the date follows the pattern DD-MONTH-YYYY
update TVS set stock_date = str_to_date(`date`, '%d-%M-%Y');

-- CREATE table such that the moving average of previous 19 rows and 49 rows 
-- respectively are considered
create table TVS1
  as (select stock_date as "Date", `Close Price` as  "Close Price" , 
  avg(`Close Price`) over (order by stock_date asc rows 19 preceding) as "20 Day MA",
  avg(`Close Price`) over (order by stock_date asc rows 49 preceding) as "50 Day MA",
  row_number()     over (order by stock_date ) as "rownum"
  from TVS);
  
SELECT * FROM TVS1;

-- It is better to ignore the first 50 rows  rather than trying to deal with NULL or by filling it with average value as that would make no practical sense.
-- Hence deleting those rows to ignore them in further analysis.

delete  from TVS1 where rownum < 50;
commit;

-- Dropping the column rownum using Alter and Drop command
alter table TVS1 drop column rownum;

-- Step 2:
-- Create a master table containing the date and close price of all the six stocks.
-- The master table is created using joining all the tables by performing a inner join
-- using the common column (Date) across tables. Then the date, each stock's closing price are
-- fetched and inserted into the master table. 
-- The keyword "as" is used to alias the fetched column names.

create table STOCK_MASTER as(
select `Date`,b.`Close Price` as "Bajaj",tcs.`Close Price` as "TCS",
tvs.`Close Price` as "TVS",inf.`Close Price` as "Infosys",
ei.`Close Price` as "Eicher",h.`Close Price` as "Hero"
from BAJAJ1 b
inner join TCS1 tcs
using (`Date`)
inner join TVS1 tvs
using (`Date`)
inner join INFOSYS1 inf
using (`Date`)
inner join EICHER1 ei
using (`Date`)
inner join HERO1 h
using (`Date`));

-- commit the transaction
commit;

-- Step 3:
-- Use the table created in Step(1) to generate buy and sell signal. 
-- Store this in another table named 'bajaj2'. Perform this operation for all stocks.

-- Query Decoded below:
-- create a common table expression stock_temp_table which stores date, close price and term flag
-- The term flag takes value Y if 20 day moving average is greater than 50 day Moving average, else it takes the value of N
-- The columns in CTE stock_temp_table is passed on to main query
-- Using window functions first_value and nth_value (2nd argument indicates the nth value that needs to be considered), we decide whether the 20 Day Moving average has crossed over 50 day Moving average, indicating as buy.
-- On the other hand if the 50 day MA cross over 20 day MA, then signal is set as Sell
-- If niether of sell/buy are there, then the signal is set as Hold

create table BAJAJ2 as 
with stock_temp_table as(
select `Date` as "stock_date",`Close Price` AS "close_price",
	CASE WHEN `20 Day MA` > `50 Day MA` THEN 'Y'
    ELSE 'N'
    END AS "term_flag"
from BAJAJ1
)
select 
		stock_date AS "Date",
		close_price AS "Close Price",
		case 
        when first_value(term_flag) over (order by stock_date rows between 1 preceding and 0 following) = nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) then  'Hold'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'Y' then 'Buy'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'N' then 'Sell'
        else 'Hold'
        end
        AS "Signal" 
	FROM stock_temp_table;

-- The same operation is performed except for the change in table names (here the eicher stock is considered)
create table EICHER2 as 
with stock_temp_table as(
select `Date` as "stock_date",`Close Price` AS "close_price",
	CASE WHEN `20 Day MA` > `50 Day MA` THEN 'Y'
    ELSE 'N'
    END AS "term_flag"
from EICHER1
)
select 
		stock_date AS "Date",
		close_price AS "Close Price",
		case 
        when first_value(term_flag) over (order by stock_date rows between 1 preceding and 0 following) = nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) then  'Hold'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'Y' then 'Buy'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'N' then 'Sell'
        else 'Hold'
        end
        AS "Signal" 
	FROM stock_temp_table;
    
-- The same operation is performed except for the change in table names (here the hero stock is considered)

create table HERO2 as 
with stock_temp_table as(
select `Date` as "stock_date",`Close Price` AS "close_price",
	CASE WHEN `20 Day MA` > `50 Day MA` THEN 'Y'
    ELSE 'N'
    END AS "term_flag"
from HERO1
)
select 
		stock_date AS "Date",
		close_price AS "Close Price",
		case 
        when first_value(term_flag) over (order by stock_date rows between 1 preceding and 0 following) = nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) then  'Hold'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'Y' then 'Buy'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'N' then 'Sell'
        else 'Hold'
        end
        AS "Signal" 
	FROM stock_temp_table;

-- The same operation is performed except for the change in table names (here the infosys stock is considered)

create table INFOSYS2 as 
with stock_temp_table as(
select `Date` as "stock_date",`Close Price` AS "close_price",
	CASE WHEN `20 Day MA` > `50 Day MA` THEN 'Y'
    ELSE 'N'
    END AS "term_flag"
from INFOSYS1
)
select 
		stock_date AS "Date",
		close_price AS "Close Price",
		case 
        when first_value(term_flag) over (order by stock_date rows between 1 preceding and 0 following) = nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) then  'Hold'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'Y' then 'Buy'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'N' then 'Sell'
        else 'Hold'
        end
        AS "Signal" 
	FROM stock_temp_table;

-- The same operation is performed except for the change in table names (here the tcs stock is considered)

create table TCS2 as 
with stock_temp_table as(
select `Date` as "stock_date",`Close Price` AS "close_price",
	CASE WHEN `20 Day MA` > `50 Day MA` THEN 'Y'
    ELSE 'N'
    END AS "term_flag"
from TCS1
)
select 
		stock_date AS "Date",
		close_price AS "Close Price",
		case 
        when first_value(term_flag) over (order by stock_date rows between 1 preceding and 0 following) = nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) then  'Hold'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'Y' then 'Buy'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'N' then 'Sell'
        else 'Hold'
        end
        AS "Signal" 
	FROM stock_temp_table;

-- The same operation is performed except for the change in table names (here the tvs stock is considered)

create table TVS2 as 
with stock_temp_table as(
select `Date` as "stock_date",`Close Price` AS "close_price",
	CASE WHEN `20 Day MA` > `50 Day MA` THEN 'Y'
    ELSE 'N'
    END AS "term_flag"
from TVS1
)
select 
		stock_date AS "Date",
		close_price AS "Close Price",
		case 
        when first_value(term_flag) over (order by stock_date rows between 1 preceding and 0 following) = nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) then  'Hold'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'Y' then 'Buy'
        when nth_value(term_flag,2) over (order by stock_date rows between 1 preceding and 0 following) = 'N' then 'Sell'
        else 'Hold'
        end
        AS "Signal" 
	FROM stock_temp_table;

COMMIT;

-- Step 4: Create a User defined function, that takes the date as input and returns the signal for that particular day (Buy/Sell/Hold) for the Bajaj stock.

delimiter $$

create function stock_function( stock_dt date)
returns varchar(10)
deterministic
begin

declare flag varchar(10);

select `Signal` into flag 
from BAJAJ2
where `Date` = stock_dt;

return flag;

end $$

delimiter ;

-- Test function created
select stock_function('2015-03-13'); 

