-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Who can access this table?

-- COMMAND ----------

SELECT DISTINCT(grantee) AS `ACCESSIBLE BY`
FROM system.information_schema.table_privileges
WHERE table_schema = '{{schema_name}}' AND table_name = '{{table_name}}'
  UNION
    SELECT table_owner
    FROM system.information_schema.tables
    WHERE table_schema = '{{schema_name}}' AND table_name = '{{table}}'
  UNION
    SELECT DISTINCT(grantee)
    FROM system.information_schema.schema_privileges
    WHERE schema_name = '{{schema_name}}'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Which users accessed a table within the last day?

-- COMMAND ----------

SELECT
  user_identity.email as `User`,
  IFNULL(request_params.full_name_arg,
    request_params.name)
    AS `Table`,
    action_name AS `Type of Access`,
    event_time AS `Time of Access`
FROM system.access.audit
WHERE (request_params.full_name_arg = '{{catalog.schema.table}}'
  OR (request_params.name = '{{table_name}}'
  AND request_params.schema_name = '{{schema_name}}'))
  AND action_name
    IN ('createTable','getTable','deleteTable')
  AND event_date > now() - interval '1 day'
ORDER BY event_date DESC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Which tables did a user access?

-- COMMAND ----------

SELECT
        action_name as `EVENT`,
        event_time as `WHEN`,
        IFNULL(request_params.full_name_arg, 'Non-specific') AS `TABLE ACCESSED`,
        IFNULL(request_params.commandText,'GET table') AS `QUERY TEXT`
FROM system.access.audit
WHERE user_identity.email = '{{User}}'
        AND action_name IN ('createTable',
'commandSubmit','getTable','deleteTable')
        -- AND datediff(now(), event_date) < 1
        -- ORDER BY event_date DESC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## What permissions have changed on securable objects?

-- COMMAND ----------

SELECT event_time, user_identity.email, request_params.securable_type, request_params.securable_full_name, request_params.changes
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name = 'updatePermissions'
ORDER BY 1 DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## What are the most recently run notebooks?

-- COMMAND ----------

SELECT event_time, user_identity.email, request_params.commandText
FROM system.access.audit
WHERE action_name = `runCommand`
ORDER BY event_time DESC
LIMIT 100
