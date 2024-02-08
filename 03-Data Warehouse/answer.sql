
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `crafty-hall-410615.ny_taxi.external_green_taxi`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-410615-green-taxi-bucket/green_taxi_week3.parquet']
);

-- To create Materialized table, you can click `ny_taxi` dataset and click the create table button. 

-- Question1 
select count(1) from  `crafty-hall-410615.ny_taxi.external_green_taxi`; --840402
	
-- Question2 
select count(distinct(PULocationID))from `crafty-hall-410615.ny_taxi.external_green_taxi`; --0B
select count(distinct(PULocationID))from `crafty-hall-410615.ny_taxi.mat_green_taxi`; --6.41MB

--Question3
select count(*) from `crafty-hall-410615.ny_taxi.external_green_taxi` where fare_amount  =0; -- 1622

--Question4
-- best strategy to make an optimized table :: always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
-- Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE OR REPLACE TABLE `crafty-hall-410615.ny_taxi.mat_green_taxi_partitioned`
PARTITION BY lpep_pickup_date
CLUSTER BY pu_location_id AS (
  SELECT * FROM `crafty-hall-410615.ny_taxi.mat_green_taxi`
);



--Question 5 

select distinct(pu_location_id) from `crafty-hall-410615.ny_taxi.mat_green_taxi` 
where  lpep_pickup_date between DATE('2022-06-01')  and  DATE(DATETIME'2022-06-30');--12.82

select distinct(pu_location_id) from `crafty-hall-410615.ny_taxi.mat_green_taxi_partitioned` 
where  lpep_pickup_date between DATE('2022-06-01')  and  DATE(DATETIME'2022-06-30');--1.12

-- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

--Question 6 
-- GCP Bucket

--Question 7 
-- It is best practice in Big Query to always cluster your data:
--  False


--(Bonus: Not worth points) Question 8:
--No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
select count(*) from `crafty-hall-410615.ny_taxi.mat_green_taxi` -- 0 B 
select count(*) from `crafty-hall-410615.ny_taxi.mat_green_taxi` where fare_amount  =0; -- 6.41Mb 
/*
The total number of record is a known data, it is stored in 'Storage Info' , so no extra cost is needed for checking row count. 
*/