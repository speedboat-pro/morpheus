-- Databricks notebook source
-- Produce all weekdays between two dates
> CREATE FUNCTION weekdays(start DATE, end DATE)
    RETURNS TABLE(day_of_week STRING, day DATE)
    RETURN SELECT extract(DAYOFWEEK_ISO FROM day), day
             FROM (SELECT sequence(weekdays.start, weekdays.end)) AS T(days)
                  LATERAL VIEW explode(days) AS day
             WHERE extract(DAYOFWEEK_ISO FROM day) BETWEEN 1 AND 5;

-- COMMAND ----------

-- Return all weekdays
> SELECT weekdays.day_of_week, day
    FROM weekdays(DATE'2022-01-01', DATE'2022-01-14');

-- COMMAND ----------

-- Return weekdays for date ranges originating from a LATERAL correlation
> SELECT weekdays.*
    FROM VALUES (DATE'2020-01-01'),
                (DATE'2021-01-01'),
                (DATE'2022-01-01') AS starts(start),
         LATERAL weekdays(start, start + INTERVAL '7' DAYS);

