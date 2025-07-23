-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Troubleshooting Tips
-- MAGIC This query provides troubleshooting tips based on common issues encountered in Delta Live Table (DLT) pipelines. It categorizes issues and provides actionable advice to resolve them effectively.

-- COMMAND ----------

SELECT 
    'invalid' AS IssueType,
    CASE 
        WHEN 'invalid' LIKE '%invalid%' THEN 'Check if the pipeline name or configurations are correct.'
        ELSE 'Refer to the pipeline logs for more details.'
    END AS TroubleshootingTips
UNION ALL
SELECT 
    'catalog' AS IssueType,
    CASE 
        WHEN 'catalog' LIKE '%catalog%' THEN 'Ensure the catalog name matches the expected configuration.'
        ELSE 'Refer to the pipeline logs for more details.'
    END AS TroubleshootingTips
UNION ALL
SELECT 
    'target' AS IssueType,
    CASE 
        WHEN 'target' LIKE '%target%' THEN 'Verify the target schema exists and is accessible.'
        ELSE 'Refer to the pipeline logs for more details.'
    END AS TroubleshootingTips
UNION ALL
SELECT 
    'permissions' AS IssueType,
    CASE 
        WHEN 'permissions' LIKE '%permissions%' THEN 'Check workspace and schema permissions for this pipeline.'
        ELSE 'Refer to the pipeline logs for more details.'
    END AS TroubleshootingTips
