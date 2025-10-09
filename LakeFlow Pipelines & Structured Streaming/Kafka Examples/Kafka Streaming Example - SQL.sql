-- Databricks notebook source
CREATE OR REPLACE STREAMING TABLE raw_space_events
(
  CONSTRAINT timestamp_not_null EXPECT (timestamp IS NOT NULL)
)
AS
  SELECT offset, timestamp, value::string as msg
   FROM STREAM read_kafka(
    bootstrapServers => 'kafka.gcn.nasa.gov:9092',
    subscribe => 'gcn.classic.text.SWIFT_POINTDIR',
    startingOffsets => 'earliest',

    -- params kafka.sasl.oauthbearer.client.id
    `kafka.sasl.mechanism` => 'OAUTHBEARER',
    `kafka.security.protocol` => 'SASL_SSL',
    `kafka.sasl.oauthbearer.token.endpoint.url` => 'https://auth.gcn.nasa.gov/oauth2/token', 
    `kafka.sasl.login.callback.handler.class` => 'kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler',


    `kafka.sasl.jaas.config` =>  
         '
          kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required
          clientId="..."
          clientSecret="..." ;         
         '
  );

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW split_events
COMMENT "Split Swift event message into individual rows"
AS
  WITH extracted_key_values AS (
    SELECT
      timestamp,
      split_part(line, ':', 1) AS key,
      TRIM(SUBSTRING(line, INSTR(line, ':') + 1)) AS value
    FROM (
      SELECT
        timestamp,
        explode(split(msg, '\\n')) AS line 
      FROM (LIVE.raw_space_events)
    )
    WHERE line != ''
  ),
  pivot_table AS (
    SELECT *
    FROM (
      SELECT key, value, timestamp
      FROM extracted_key_values
    )
    PIVOT (
      MAX(value) FOR key IN ('TITLE', 'NOTICE_DATE', 'NOTICE_TYPE', 'NEXT_POINT_RA', 'NEXT_POINT_DEC', 'NEXT_POINT_ROLL', 'SLEW_TIME', 'SLEW_DATE', 'OBS_TIME', 'TGT_NAME', 'TGT_NUM', 'MERIT', 'INST_MODES', 'SUN_POSTN', 'SUN_DIST', 'MOON_POSTN', 'MOON_DIST', 'MOON_ILLUM', 'GAL_COORDS', 'ECL_COORDS', 'COMMENTS')
    )
  )
  SELECT timestamp, TITLE, CAST(NOTICE_DATE AS TIMESTAMP) AS NOTICE_DATE, NOTICE_TYPE, NEXT_POINT_RA, NEXT_POINT_DEC, NEXT_POINT_ROLL, SLEW_TIME, SLEW_DATE, OBS_TIME, TGT_NAME, TGT_NUM, CAST(MERIT AS DECIMAL) AS MERIT, INST_MODES, SUN_POSTN, SUN_DIST, MOON_POSTN, MOON_DIST, MOON_ILLUM, GAL_COORDS, ECL_COORDS, COMMENTS
  FROM pivot_table
