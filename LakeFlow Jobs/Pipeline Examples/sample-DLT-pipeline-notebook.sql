-- Databricks notebook source
CREATE CATALOG streaming_sql;
USE CATALOG streaming_sql;
USE SCHEMA default; 

-- COMMAND ----------

CREATE
OR REFRESH STREAMING TABLE taxi_raw_records (
  CONSTRAINT valid_distance EXPECT (trip_distance > 0.0) ON VIOLATION DROP ROW
) AS
SELECT
  *
FROM
  STREAM(samples.nyctaxi.trips)

-- COMMAND ----------

CREATE
OR REFRESH LIVE TABLE total_fare_amount_by_week AS
SELECT
  date_trunc("week", tpep_pickup_datetime) as week,
  SUM(fare_amount) as total_amount
FROM
  live.taxi_raw_records
GROUP BY
  week

-- COMMAND ----------

CREATE
OR REFRESH LIVE TABLE max_distance_by_week AS
SELECT
  date_trunc("week", tpep_pickup_datetime) as week,
  MAX(trip_distance) as max_distance
FROM
  live.taxi_raw_records
GROUP BY
  week
