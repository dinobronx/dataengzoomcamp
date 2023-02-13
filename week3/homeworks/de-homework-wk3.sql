-- Homework wk 3
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `alien-handler-376020.dezoomhomework.wk3fhv`
OPTIONS (
  format='csv',
  uris = ['gs://de-homework-wk3/data/fhv_tripdata_2019-01.csv.gz',
          'gs://de-homework-wk3/data/fhv_tripdata_2019-02.csv.gz', 
          'gs://de-homework-wk3/data/fhv_tripdata_2019-03.csv.gz', 
          'gs://de-homework-wk3/data/fhv_tripdata_2019-04.csv.gz',
          'gs://de-homework-wk3/data/fhv_tripdata_2019-05.csv.gz',
          'gs://de-homework-wk3/data/fhv_tripdata_2019-06.csv.gz',
          'gs://de-homework-wk3/data/fhv_tripdata_2019-07.csv.gz',
          'gs://de-homework-wk3/data/fhv_tripdata_2019-08.csv.gz',
          'gs://de-homework-wk3/data/fhv_tripdata_2019-09.csv.gz',
          'gs://de-homework-wk3/data/fhv_tripdata_2019-10.csv.gz',
          'gs://de-homework-wk3/data/fhv_tripdata_2019-11.csv.gz',
          'gs://de-homework-wk3/data/fhv_tripdata_2019-12.csv.gz']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `alien-handler-376020.dezoomhomework.wk3fhv-nonpartitioned` AS
SELECT * FROM `alien-handler-376020.dezoomhomework.wk3fhv`;

SELECT COUNT(*) FROM `alien-handler-376020.dezoomhomework.wk3fhv`;
--43244696

-- from external table
SELECT DISTINCT (Affiliated_base_number)
FROM `alien-handler-376020.dezoomhomework.wk3fhv`;

-- from bq table
SELECT DISTINCT (Affiliated_base_number)
FROM `alien-handler-376020.dezoomhomework.wk3fhv-nonpartitioned`;


-- both null in pulocation and dolocation, 717748
SELECT COUNT(*)
FROM `alien-handler-376020.dezoomhomework.wk3fhv-nonpartitioned`
WHERE PUlocationID is null AND DOlocationID is null;

--- Creating partition and cluster table
CREATE OR REPLACE TABLE `alien-handler-376020.dezoomhomework.wk3fhv-partionedclustered`
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS 
SELECT * FROM `alien-handler-376020.dezoomhomework.wk3fhv-nonpartitioned`;



-- from non partitioned bq table
SELECT DISTINCT (Affiliated_base_number)
FROM `alien-handler-376020.dezoomhomework.wk3fhv-nonpartitioned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- from partition and clustered bq table
SELECT DISTINCT (Affiliated_base_number)
FROM `alien-handler-376020.dezoomhomework.wk3fhv-partionedclustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';