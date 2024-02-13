-- Homework 3

-- Setup: Creating an external table using Green Taxi Trip Records for 2022
CREATE OR REPLACE EXTERNAL TABLE `basic-dispatch-413405.ny_taxi.external_green_tripdata`
OPTIONS (
	format = 'PARQUET',
	uris = ['gs://dtc-data-lake-hw3-aa/green/green_tripdata_2022-*.parquet']
);

-- Check green trip data
SELECT * FROM `basic-dispatch-413405.ny_taxi.external_green_tripdata` LIMIT 10;

-- Setup: Create a non partitioned table from external table
CREATE OR REPLACE TABLE `basic-dispatch-413405.ny_taxi.green_tripdata_non_partitioned` AS
SELECT * FROM `basic-dispatch-413405.ny_taxi.external_green_tripdata`;

-- Question 1
SELECT COUNT(*) AS total_count FROM `basic-dispatch-413405.ny_taxi.green_tripdata_non_partitioned`;

-- Question 2
SELECT COUNT(DISTINCT PULocationID) FROM `basic-dispatch-413405.ny_taxi.external_green_tripdata`;
SELECT COUNT(DISTINCT PULocationID) FROM `basic-dispatch-413405.ny_taxi.green_tripdata_non_partitioned`;

-- Question 3
SELECT COUNT(*) FROM `basic-dispatch-413405.ny_taxi.green_tripdata_non_partitioned`
WHERE fare_amount = 0;

-- Question 4
CREATE OR REPLACE TABLE `basic-dispatch-413405.ny_taxi.green_tripdata_partitoned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM `basic-dispatch-413405.ny_taxi.external_green_tripdata`;

CREATE OR REPLACE TABLE `basic-dispatch-413405.ny_taxi.green_tripdata_partitoned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `basic-dispatch-413405.ny_taxi.external_green_tripdata`;

-- Question 5
SELECT COUNT(DISTINCT PULocationID) FROM `basic-dispatch-413405.ny_taxi.green_tripdata_non_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT COUNT(DISTINCT PULocationID) FROM `basic-dispatch-413405.ny_taxi.green_tripdata_partitoned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

