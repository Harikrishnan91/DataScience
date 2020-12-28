-- Problem Description:
/*
The New York City Taxi & Limousine Commission (TLC) has provided a dataset of trips made by the taxis in the New York City. The detailed trip-level data is more than just a vast list of taxi pickup and drop off coordinates.  

The records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations (location coordinates of the starting and ending points), trip distances, itemized fares, rate types, payment types, driver-reported passenger counts etc. The data used was collected and provided to the NYC Taxi and Limousine Commission (TLC) by technology providers authorized under the Taxicab & Livery Passenger Enhancement Programs (TPEP/LPEP).

The dataset was created by aggregating the various records. It provides precise location coordinates for where the trip started and ended, timestamps for when the trip started and ended, plus a few other variables including fare amount, payment method, and distance travelled.
*/

-- Checking whether the data base exist before creating one
drop database harikrishnan_newark_taxi;
-- Create a new database
create database harikrishnan_newark_taxi;
-- Specify to use the created DB for further queries
use harikrishnan_newark_taxi;

-- IMPORTANT: BEFORE CREATING ANY TABLE, MAKE SURE YOU RUN THIS COMMAND 
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar;

-- PARTITION THE DATA  
-- IMPORTANT: BEFORE PARTITIONING ANY TABLE, MAKE SURE YOU RUN THESE COMMANDS 
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

-- create the initial table
create external table harikrishnan_newark_taxi.newyork_taxi(VendorID int,tpep_pickup_datetime timestamp,tpep_dropoff_datetime timestamp,
passenger_count int,trip_distance double,RatecodeID int,store_and_fwd_flag string,PULocationID int,DOLocationID int,
payment_type int,fare_amount double,extra double,mta_tax double,tip_amount double,tolls_amount double,
improvement_surcharge double,total_amount double)
row format delimited fields terminated by ','
location '/common_folder/nyc_taxi_data/'
tblproperties ("skip.header.line.count"="1");

--Retrieve the first 10 rows
select * from harikrishnan_newark_taxi.newyork_taxi limit 10;

--Basic Data Checks


-- 1. How many records has each TPEP provider provided? Write a query that summarises the number of records of each provider.
select count(*) as Number_of_Rides,vendorid from harikrishnan_newark_taxi.newyork_taxi group by vendorid;

-- Result of the above query indicates the Vendor "Verifone Inc" (Vendor ID : 2) has a count of 647183 rides whereas the 
-- vendor Creative Technologies (Vendor ID:1)  have served for 527286 rides. It is understood that Verifone Inc has served more than
-- Creative Technologies by around 10%

-- 2. The data provided is for months November and December only. Check whether the data is consistent, and if not, identify the data quality issues. Mention all data quality issues in comments.

--First fetch the list of column names
desc harikrishnan_newark_taxi.newyork_taxi;

-- Check whether any column for a given row has null value

select max(vendorid),max(tpep_pickup_datetime),max(tpep_dropoff_datetime),max(passenger_count),
max(trip_distance),max(ratecodeid),max(store_and_fwd_flag),max(pulocationid),
max(dolocationid),max(payment_type),max(fare_amount),max(extra),max(mta_tax),
max(tip_amount),max(tolls_amount),max(improvement_surcharge),max(total_amount) from harikrishnan_newark_taxi.newyork_taxi
UNION ALL
select min(vendorid),min(tpep_pickup_datetime),min(tpep_dropoff_datetime),min(passenger_count),
min(trip_distance),min(ratecodeid),min(store_and_fwd_flag),min(pulocationid),
min(dolocationid),min(payment_type),min(fare_amount),min(extra),min(mta_tax),
min(tip_amount),min(tolls_amount),min(improvement_surcharge),min(total_amount) from harikrishnan_newark_taxi.newyork_taxi;

-- As seen from the query result, all the columns provides the min and max result indicating that no null rows/cells are present
-- The result of the above minmax query is shown below
--VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount
-- (First row indicates maximum value, while the second row indicates minimum value)
-- 2,2018-01-01 00:04:00.0,2019-04-24 19:21:00.0,9,126.41,99,Y,265,265,4,650.0,4.8,11.4,450.0,895.89,1.0,928.19
-- 1,2003-01-01 00:58:00.0,2003-01-01 01:28:00.0,0,0.0,1,N,1,1,1,-200.0,-10.6,-0.5,-1.16,-5.76,-0.3,-200.8

-- Analysis from above result
-- 1. The pick up time ranges from 2003 to 2018 indicating a span of 15 years data. Of this we are interested only in Nov and December of 2017.
-- 2. Hence we need to partition the data by month for optimized performance.
-- 3. Trip distance column has a minimum value of zero which is strange and should be an erroneous data.
-- 4. Minimum of Amount columns (fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) has negative values
-- indicating erroneous values
-- 5. Minimum of passenger_count is 0 which is wrong.

select distinct(ratecodeid) from harikrishnan_newark_taxi.newyork_taxi;
-- Above distinct query on Rate Code ID reveals that ratecode id has a value of 99 which is different from what's mentioned in data dictonary

select distinct(payment_type) from harikrishnan_newark_taxi.newyork_taxi;
-- As seen from above distinct query on payment type, only the Credit card,Cash,No charge,Dispute payment types are used in the entire dataset
-- The payment types Unknown and Voided Trips that is mentioned in the data dictonary is not present thoroughout the dataset.

select vendorid, count(vendorid) from harikrishnan_newark_taxi.newyork_taxi
where month(tpep_pickup_datetime) not in (11,12) or year(tpep_pickup_datetime) not in (2017) 
group by vendorid;

-- As seen from above query which indicates which vendor's data has most inconsistency in terms of date range, it is revealed
-- that vendor 2 has 14 such records which neither is in month of Nov/Dec nor in the year of 2017.

select vendorid, count(vendorid) from harikrishnan_newark_taxi.newyork_taxi
where passenger_count = 0 
group by vendorid;

-- In terms of passeneger count, vendor 1 (6813 records) has more erroneous data than vendor 2 (11 records). Here passenger_count = 0
-- is considered as erroneous record.

select vendorid, count(vendorid) from harikrishnan_newark_taxi.newyork_taxi
where trip_distance = 0 
group by vendorid;

-- In terms of trip distance, vendor 1 (4217 records) has more erroneous data than vendor 2 (3185 records). Here trip distance = 0
-- is considered as erroneous record.

select vendorid, count(vendorid) from harikrishnan_newark_taxi.newyork_taxi
where ratecodeid = 99
group by vendorid;

-- With respect to Rate Code ID, vendor 1 (8 records) has more erroneous data than vendor 2 (1 record). Here Rate Code ID = 99
-- is considered as erroneous record.

select vendorid, count(vendorid) from harikrishnan_newark_taxi.newyork_taxi
where fare_amount < 0 or extra < 0 or mta_tax < 0 or tip_amount < 0 or tolls_amount < 0 or improvement_surcharge < 0 or total_amount < 0
group by vendorid;

-- Taking the amount column, vendor 2 has around 558 erroneous records and vendor 1 has around 1 erroneous record
-- On an overall level across columns, vendor 2 has more erroneous values across columns than vendor 1

-- Create a ORC Partioned Table
-- First drop the table 
drop table harikrishnan_newark_taxi.newyork_month_partitioned;

-- Create external table with partitioning by vendor ID and month
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar;

create external table harikrishnan_newark_taxi.newyork_month_partitioned(tpep_pickup_datetime timestamp,tpep_dropoff_datetime timestamp,
passenger_count int,trip_distance double,RatecodeID int,store_and_fwd_flag string,PULocationID int,DOLocationID int,
payment_type int,fare_amount double,extra double,mta_tax double,tip_amount double,tolls_amount double,
improvement_surcharge double,total_amount double) partitioned by (vendorid int,mnth int)
stored as orc location '/user/harikrishnan.sekar_astrazeneca/Newyork Taxi'
tblproperties ("orc.compress"="SNAPPY");

-- As part of above query, we create an external table by partitioning based on vendor and month columns.
-- It is important to note that the data is compressed in optimized row columnar (ORC) format with SNAPPY as the compression algorithm
-- The compressed data is stored in the accessible path /user/harikrishnan.sekar_astrazeneca/Newyork Taxi

insert overwrite table harikrishnan_newark_taxi.newyork_month_partitioned partition(vendorid,mnth)
select 
tpep_pickup_datetime,
tpep_dropoff_datetime,
passenger_count,
trip_distance,
RatecodeID,
store_and_fwd_flag,
PULocationID,
DOLocationID,
payment_type,
fare_amount,
extra,
mta_tax,
tip_amount,
tolls_amount,
improvement_surcharge,
total_amount,
vendorid,
month(tpep_pickup_datetime) mnth
from  harikrishnan_newark_taxi.newyork_taxi
where  (tpep_pickup_datetime >='2017-11-01 00:00:00.0' and tpep_pickup_datetime<'2018-01-01 00:00:00.0') and
(tpep_dropoff_datetime > tpep_pickup_datetime) and
(passenger_count > 0 ) and
(trip_distance > 0) and 
(ratecodeid!=99) and
( fare_amount> 0 ) and
 (extra in (0,0.5,1) ) and
 (mta_tax in (0,0.5)) and 
(tip_amount >= 0) and
( tolls_amount >= 0) and
( improvement_surcharge in (0,0.3)) and
(total_amount>0 ) ;

-- We insert data into the created partition table by passing certain filters to eliminate the erroneous rows.
-- The filter considered are The pick up date time should be greater than 1st November 2017 00:00 and less than 1st Jan 2018 00:00
-- and the passenger count should be greater than zero for a given ride
-- and trip distance should be greater than zero
-- and rate code ID is not equal to 99 (as 99 is not present in data dictionary)
-- and all the amount/surcharge/tax columns have value greater or equal to zero
-- Do note for extra, as per data dictionary, the only accepted values are either 0 or 0.5 or 1
-- Similarly for mta_tax, accepted values are 0 or 0.5 and for improvement surcharge the values can take either 0 or 0.3



select count(*) from harikrishnan_newark_taxi.newyork_month_partitioned;
-- Fetch the count of records from the newly created partioned table which shows the total records as 1153605. 
-- Comparing with the original table, we have filtered 20964 rows, which is 1.78% of the total data.

-- Analysis-I

-- 1. Compare the overall average fare per trip for November and December.

select avg(fare_amount) as Avg_Fare_Amt,avg(total_amount) as Avg_Total_Amt,mnth 
from harikrishnan_newark_taxi.newyork_month_partitioned
group by mnth;

-- It is evident from the query result that overall average fare amount and overall average total amount of November 2017  is greater than December 2017

-- 2. Explore the â€˜number of passengers per tripâ€™ - how many trips are made by each level of â€˜Passenger_countâ€™? 
-- Do most people travel solo or with other people?

select count(*), passenger_count from  harikrishnan_newark_taxi.newyork_month_partitioned
group by passenger_count;

-- From the result obtained it is evident that most riders prefer solo travel followed by group of 2 and then group of 5
-- On the basis of these insights, we can say that the new york taxi associations can arrange more compact/sedan cars and SUV which
-- have 5+1 seater for maximum revenue. The order of count is as follows: 1>2>5>3>6>4>7

--3. Which is the most preferred mode of payment?

select count(*) as Cnt , payment_type from harikrishnan_newark_taxi.newyork_month_partitioned
group by payment_type
order by cnt desc;

-- Most preferred payment type is Credit card followed by cash and then "no charge" followed by dispute

-- 4. What is the average tip paid per trip? Compare the average tip with the 25th, 50th and 75th percentiles 
-- and comment whether the â€˜average tipâ€™ is a representative statistic (of the central tendency) of â€˜tip amount paidâ€™. 

select avg(tip_amount) as avg_tip, percentile_approx(tip_amount,0.25) as 25_perc_tip,
percentile_approx(tip_amount,0.5) as 50_perc_tip,percentile_approx(tip_amount,0.75) as 75_perc_tip
from harikrishnan_newark_taxi.newyork_month_partitioned;

-- As seen from the query result the average tip is around 1.83 dollars whereas the 25th percentile is 0, 50th percentile is 1.36
-- and 75th percentile is 2.45 dollars
-- Hence we can conclude that the â€˜average tipâ€™ is NOT a representative statistic of the central tendency (50th Percentile) of â€˜tip amount paidâ€™

-- 5. Explore the â€˜Extraâ€™ (charge) variable - what fraction of total trips have an extra charge is levied?

select count(*) from harikrishnan_newark_taxi.newyork_month_partitioned;
-- To identify the fraction, we first need to fetch the total number of rows. The total number of rows is 1153605

select round(count(*)*100/1153605,2) as cnt_of_trips, extra
from harikrishnan_newark_taxi.newyork_month_partitioned
group by extra
having extra > 0;

-- The above query fetches the count of trips for each category of extra charges in a percentage basis.comment
-- That is we divide the respective category count by total number of records and then multiply it by 100 to convert into percentages.
-- 31.19% of trips have an extra charge of 0.5 dollar whereas 14.95% of the trips have an extra charge of 1 dollar
-- So cumulatively 46.14% of the trips levy extra charge

-- Analysis 2:

-- 1. What is the correlation between the number of passengers on any given trip, and the tip paid per trip? 
-- Do multiple travellers tip more compared to solo travellers?

select corr(passenger_count,tip_amount) 
from harikrishnan_newark_taxi.newyork_month_partitioned;

-- Correlation Coefficent Value : -0.0053

-- There is no correlation between passenger count and the tip amount. The same can also be verified by plotting scatter plot
-- against the passeneger count column versus the tip amount column as shown in the query below

select passenger_count,tip_amount
from harikrishnan_newark_taxi.newyork_month_partitioned;


-- 2. Segregate the data into five segments of â€˜tip paidâ€™: [0-5), [5-10), [10-15) , [15-20) and >=20. 
-- Calculate the percentage share of each bucket

select segment, round((count(*)*100/1153605),2) as Fraction
from (select
case when (tip_amount>= 0 and tip_amount<5)   then '[0-5]' 
     when (tip_amount>=5 and tip_amount<10)  then '[5-10]' 
     when (tip_amount>=10 and tip_amount<15) then '[10-15]'
     when (tip_amount>=15 and tip_amount<20) then '[15-20]'
     when (tip_amount>=20)                   then '>=20' end segment
     from harikrishnan_newark_taxi.newyork_month_partitioned) Temp 
     group by segment
     order by Fraction desc;

-- As seen from above query result, 92% of the trips have been tipped less than 5 dollars whereas 6% of the trips have been tipped
-- with anything greater or equal than 5 dollars but less than 10 dollars and the remainder fraction of tipping (2%) falls under the
-- [10-15] segment. The percentage share of segment [15-20] and '>=20' is almost negligible

-- 3. Which month has a greater average â€˜speedâ€™ - November or December?

select round(avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)),2) as Avg_Speed,
mnth from harikrishnan_newark_taxi.newyork_month_partitioned
group by mnth
order by Avg_Speed desc;

-- The average speed is computed by average of trip distance divided by time. The time in hours is calculated by differencing 
-- drop time with pick up time as a unix timestamp second and then dividing by 3600 to convert to hour. As seen from the result, 
-- the average speed hovers around 11.07 Miles/Hour for December
-- whereas at 10.97 Miles/Hour for November. The average speed of December slightly edges ahead of November.

-- 4. Analyse the average speed of the most happening days of the year.
-- The following are considered as the most happening days (25th December and 31stDecember)
-- Any trip that started on these days will be taken for analysis

-- Let's first compute the overall average

select round(avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)),2) as Avg_Speed
from harikrishnan_newark_taxi.newyork_month_partitioned;

-- The overall average speed is 11.02 miles/hour

select round(avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)),2) as Avg_Speed_Happening from harikrishnan_newark_taxi.newyork_month_partitioned 
where date_format(tpep_pickup_datetime,'dd') in ('25','31') and mnth = 12;

-- As fetched from 25th and 31st December, the average speed turns out to be 14.01 miles/hour which is greater than the 
-- overall average. It is bit strange as those are considered the most happening days of the year.
-- From the result, we can say that for that year, most newyorkers preferred to stay indoors thereby the traffic is very less
-- and hence the result of increased average speed than the overall average speed.