# Databricks notebook source
# MAGIC %md
# MAGIC # Create a route optimized serving endpoint
# MAGIC
# MAGIC This notebook demonstrates how to create a route optimized serving endpoint using Python.
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC Databricks Runtime for Machine Learning version 14.3 LTS or above.

# COMMAND ----------

import urllib.request
import json
 
class EndpointApiClient:
  def __init__(self, base_url, token):
    self.base_url = base_url
    self.token = token
  
  def create_inference_endpoint(self, endpoint_name, served_models, traffic = None):
    data = {"name": endpoint_name, "config": { "served_entities": served_models }, "route_optimized": True}
    return self._post('api/2.0/serving-endpoints', data)
 
  def _post(self, uri, body):
    json_body = json.dumps(body)
    json_bytes = json_body.encode('utf-8')
    headers = { 'Authorization': f'Bearer {self.token}', "Content-Type": "application/json" }
 
    url = f'{self.base_url}/{uri}'
    req = urllib.request.Request(url, data=json_bytes, headers=headers)
    response = urllib.request.urlopen(req)
    return json.load(response)


# COMMAND ----------

# MAGIC %md
# MAGIC You can use the following to configure the `EndpointAPIClient` with your personal access token and workspace URL.

# COMMAND ----------

# Add a personal access token
pat_token = "" 

# Add workspace URL
workspace_url = "" # Example: https://my-workspace.cloud.databricks.com

client = EndpointApiClient(workspace_url, pat_token)

# COMMAND ----------

# MAGIC %md
# MAGIC You can use the following to create a serving endpoint.

# COMMAND ----------

endpoint_name = ""
models = [{
  "entity_name": "",
  "entity_version": "",
  "workload_size": "",
  "workload_type": "",
  "scale_to_zero_enabled": ,
}]

endpoint = client.create_inference_endpoint(endpoint_name, models)

# COMMAND ----------

endpoint
