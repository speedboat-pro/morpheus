# ![][image1]

# CLASSROOM NOTES

[Resources](#resources)

[General Usage](#general-usage)

[Clusters and configuration](#clusters-and-configuration)

[Serverless](#serverless)

[Regions, Accounts and Workspaces](#regions,-accounts-and-workspaces)

[Notebooks](#notebooks)

[API reference](#api-reference)

[Spark](#spark)

[DataFrames/PySpark General](#dataframes/pyspark-general)

[User Defined Functions](#user-defined-functions)

[Structured Streaming](#structured-streaming)

[Delta Lake](#delta-lake)

[Databricks](#databricks)

[Articles from DBSQL SME Engineering – Medium](#articles-from-dbsql-sme-engineering-–-medium)

[General](#general)

[Declarative Pipelines (DLP) formerly known as  Delta Live Tables (DLT)](#declarative-pipelines-\(dlp\)-formerly-known-as-delta-live-tables-\(dlt\))

[Unity Catalog (UC)](#unity-catalog-\(uc\))

[General](#general-1)

[Data Product Features](#data-product-features)

[Clean Room](#clean-room)

[Delta Sharing](#delta-sharing)

[Performance Optimization:](#performance-optimization:)

[Delta Lake](#delta-lake-1)

[Photon/ Databricks SQL (DBSQL)](#photon/-databricks-sql-\(dbsql\))

[Migration Tools](#migration-tools)

[Apache Spark/Spark SQL](#apache-spark/spark-sql)

[Approximate Functions:](#approximate-functions:)

[Data Analyst (SQL)](#data-analyst-\(sql\))

[Databricks SQL](#databricks-sql)

[Alerts](#alerts)

[Dashboards](#dashboards)

[Genie Spaces/ Databricks IQ](#genie-spaces/-databricks-iq)

[Data Warehouse Design](#data-warehouse-design)

[Power BI](#power-bi)

[Data Engineering:](#data-engineering:)

[Schemas and Tables](#schemas-and-tables)

[Row Level Security (RLS) & Column Level Masking (CLM)](#row-level-security-\(rls\)-&-column-level-masking-\(clm\))

[Looker Connectivity](#looker-connectivity)

[Machine Learning](#machine-learning)

[General](#general-2)

[Data Prep for Machine Learning](#data-prep-for-machine-learning)

[EDA](#eda)

[dbutils.data.summarize()](#dbutils.data.summarize\(\)-dataframe’s-summary\(\)-method)  
 [DataFrame’s summary() method](#dbutils.data.summarize\(\)-dataframe’s-summary\(\)-method)

[Cross Validation](#cross-validation)

[SparkML](#sparkml)

[StringIndexer class](#stringindexer-class-onehotencoder-class-vectorassembler-class-standardscaler-class-robustscaler-class-randomsplit-method-imputer-class-pipeline-class)   
[OneHotEncoder class](#stringindexer-class-onehotencoder-class-vectorassembler-class-standardscaler-class-robustscaler-class-randomsplit-method-imputer-class-pipeline-class)  
[VectorAssembler class](#stringindexer-class-onehotencoder-class-vectorassembler-class-standardscaler-class-robustscaler-class-randomsplit-method-imputer-class-pipeline-class)  
[StandardScaler class](#stringindexer-class-onehotencoder-class-vectorassembler-class-standardscaler-class-robustscaler-class-randomsplit-method-imputer-class-pipeline-class)  
[RobustScaler class](#stringindexer-class-onehotencoder-class-vectorassembler-class-standardscaler-class-robustscaler-class-randomsplit-method-imputer-class-pipeline-class)  
[randomSplit method](#stringindexer-class-onehotencoder-class-vectorassembler-class-standardscaler-class-robustscaler-class-randomsplit-method-imputer-class-pipeline-class)  
[Imputer class](#stringindexer-class-onehotencoder-class-vectorassembler-class-standardscaler-class-robustscaler-class-randomsplit-method-imputer-class-pipeline-class)  
[Pipeline class](#stringindexer-class-onehotencoder-class-vectorassembler-class-standardscaler-class-robustscaler-class-randomsplit-method-imputer-class-pipeline-class)

[Feature Store](#feature-store)

[Model Development](#model-development)

[AutoML & Its Algorithms](#automl-&-its-algorithms)

[Deep Learning & Databricks](#deep-learning-&-databricks)

[Hyperopt](#hyperopt)

[Optuna](#optuna)

[Model Operations (MLOps)](#model-operations-\(mlops\))

[MLOps Definition and Benefits | Databricks](#mlops-definition-and-benefits-|-databricks-inference-tables-for-monitoring-and-debugging-models-databricks-mlops-stacks-mlops-workflows-on-databricks-monitor-metric-tables---azure-databricks-best-practices-for-operational-excellence)  
[Inference tables for monitoring and debugging models](#mlops-definition-and-benefits-|-databricks-inference-tables-for-monitoring-and-debugging-models-databricks-mlops-stacks-mlops-workflows-on-databricks-monitor-metric-tables---azure-databricks-best-practices-for-operational-excellence)  
[Databricks MLOps Stacks](#mlops-definition-and-benefits-|-databricks-inference-tables-for-monitoring-and-debugging-models-databricks-mlops-stacks-mlops-workflows-on-databricks-monitor-metric-tables---azure-databricks-best-practices-for-operational-excellence)  
[MLOps workflows on Databricks](#mlops-definition-and-benefits-|-databricks-inference-tables-for-monitoring-and-debugging-models-databricks-mlops-stacks-mlops-workflows-on-databricks-monitor-metric-tables---azure-databricks-best-practices-for-operational-excellence)    
[Monitor metric tables \- Azure Databricks](#mlops-definition-and-benefits-|-databricks-inference-tables-for-monitoring-and-debugging-models-databricks-mlops-stacks-mlops-workflows-on-databricks-monitor-metric-tables---azure-databricks-best-practices-for-operational-excellence)  
[Best practices for operational excellence](#mlops-definition-and-benefits-|-databricks-inference-tables-for-monitoring-and-debugging-models-databricks-mlops-stacks-mlops-workflows-on-databricks-monitor-metric-tables---azure-databricks-best-practices-for-operational-excellence)

[DataOps](#dataops)

[How does Databricks support CI/CD for machine learning?](#how-does-databricks-support-ci/cd-for-machine-learning?)

[Generative AI Reference](#generative-ai-reference)

[Blogs and General Resources](#blogs-and-general-resources)

[Databricks Documentation](#databricks-documentation)

[GenAI Solution Development](#genai-solution-development)

[Data preparation](#data-preparation)

[Vector Search](#vector-search)

[Retrieval Augmented Generation (RAG)](#retrieval-augmented-generation-\(rag\))

[GenAI Application Development](#genai-application-development)

[Build an unstructured data pipeline for RAG](#build-an-unstructured-data-pipeline-for-rag)

[Mastering RAG Chunking Techniques for Enhanced Document Processing](#mastering-rag-chunking-techniques-for-enhanced-document-processing)

[Semantic Chunking for RAG](#semantic-chunking-for-rag)

[MTEB Leaderboard \- a Hugging Face Space by mteb](#mteb-leaderboard---a-hugging-face-space-by-mteb)

[Apps](#apps)

[Introducing Databricks Apps](#introducing-databricks-apps)

[Python](#python)

[Databricks LLM APIS](#databricks-llm-apis)

[Prompt Engineering/LangChain](#prompt-engineering/langchain)

[Agents](#agents)

[OpenAI's Bet on a Cognitive Architecture](#openai's-bet-on-a-cognitive-architecture)

[GenAI Evaluation & Governance](#genai-evaluation-&-governance)

[Data Legality & Guardrails](#data-legality-&-guardrails)

[Securing & Governing AI Systems](#securing-&-governing-ai-systems)

[Evaluating LLMs](#evaluating-llms)

[GenAI Deployment & Monitoring](#genai-deployment-&-monitoring)

[Batch Inference](#batch-inference)

[Realtime Model Deployment](#realtime-model-deployment)

[Online Monitoring](#online-monitoring)

[Orchestration and CI/CD](#orchestration-and-ci/cd)

[Workflows, Tasks  & CI/CD:](#workflows,-tasks-&-ci/cd:)

[General](#general-3)

[Databricks Asset Bundles (DAB)](#databricks-asset-bundles-\(dab\))

[DBT](#dbt)

[Lakehouse Monitoring](#lakehouse-monitoring)

[Repos/DevOps](#repos/devops)

[Secrets:](#secrets:)

[Blog and Technical Articles](#blog-and-technical-articles)

# Resources {#resources}

## General Usage  {#general-usage}

[Tutorials | Databricks](https://www.databricks.com/resources/demos/tutorials)   
[Databricks pricing](https://www.databricks.com/product/pricing)   
[Databricks architecture overview](https://docs.databricks.com/en/getting-started/overview.html)

### Clusters and configuration {#clusters-and-configuration}

Managing Compute: [Manage compute](https://docs.databricks.com/en/compute/clusters-manage.html#start-a-cluster)   
Cluster Configuration: [Compute | Databricks on AWS](https://docs.databricks.com/user-guide/clusters/index.html)  
AWS: [Compute configuration reference | Databricks on AWS](https://docs.databricks.com/en/compute/configure.html)  
Azure: [Compute configuration reference \- Azure Databricks | Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/compute/configure)  
GoogleCloud: [Compute configuration reference | Databricks on Google Cloud](https://docs.gcp.databricks.com/en/compute/configure.html)  
User Guide: [Databricks data engineering](https://docs.databricks.com/user-guide/index.html#user-guide)    
Guide: [Databricks administration introduction](https://docs.databricks.com/administration-guide/index.html#administration-guideAdministration)    
Ganglia UI: [View compute metrics](https://docs.databricks.com/en/compute/cluster-metrics.html)  
Release Notes: [Databricks release notes](https://docs.databricks.com/release-notes/index.html#release-notes)  
Databricks Runtime (DBR) Releases: [Databricks Runtime release notes versions and compatibility](https://docs.databricks.com/en/release-notes/runtime/index.html#)   
Cluster configuration details  
	[Compute configuration reference](https://docs.databricks.com/aws/en/compute/configure#compute-log-delivery) \- cluster log delivery

#### Serverless {#serverless}

[Serverless compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)   
[Serverless Security](https://www.databricks.com/trust/security-features/serverless-security)  
[Attribute serverless costs to departments and users with budget policies](https://www.databricks.com/blog/attribute-serverless-costs-departments-and-users-budget-policies)

### Regions, Accounts and Workspaces {#regions,-accounts-and-workspaces}

[Get started: Account and workspace setup](https://docs.databricks.com/user-guide/getting-started.html)   
[Databricks clouds and regions](https://docs.databricks.com/en/resources/supported-regions.html)  
[Authentication for Databricks tools and APIs](https://docs.databricks.com/en/dev-tools/auth/index.html)   
[What are workspace files?](https://docs.databricks.com/en/files/workspace.html#)  
[Azure Active Directory integration](https://docs.databricks.com/en/admin/users-groups/scim/aad.html)  
[The Filestore folder](https://docs.databricks.com/en/_extras/notebooks/source/filestore.html)

### Notebooks {#notebooks}

User Guide / Notebooks: [Introduction to Databricks notebooks](https://docs.databricks.com/user-guide/notebooks/index.html)   
Importing notebooks \- Supported Formats: [Manage notebooks | Databricks on AWS](https://docs.databricks.com/notebooks/notebooks-manage.html#notebook-external-formats)   
[Developing Code in Notebooks](https://docs.databricks.com/en/notebooks/notebooks-code.html#develop-notebooks)  
	[Notebook Markdown Cheat Sheet](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1395231203492935/2328178954927609/4227492383157538/latest.html)  
	[Databricks widgets](https://docs.databricks.com/en/notebooks/widgets.html)   
[Notebook & Widget limitations](https://docs.databricks.com/en/notebooks/notebook-limitations.html#databricks-widgets)  
[**%run** vs. **dbutils.notebook.run**](https://docs.databricks.com/_extras/notebooks/source/pandas-to-pandas-api-on-spark-in-10-minutes.html)  
[Dbutils documentation](https://docs.databricks.com/user-guide/dev-tools/dbutils.html)

You can upload here https://nike.ent.box.com/f/5268b260354646ba88e2f41b0cda0c3c

Guardrails to guide answers dependent on additional data of the user, like country

## API reference {#api-reference}

### Spark {#spark}

#### 	DataFrames/PySpark General  {#dataframes/pyspark-general}

WholeStageCodeGen [spark-sql-whole-stage-codegen.adoc](https://github.com/rakeshdrk/mastering-apache-spark-book/blob/master/spark-sql-whole-stage-codegen.adoc)  
[PySpark 3.5.2 documentation](https://spark.apache.org/docs/latest/api/python/index.html)  
[Getting Started — PySpark 3.5.2 documentation](https://spark.apache.org/docs/latest/api/python/getting_started/index.html)  
[Data format options](https://docs.databricks.com/en/query/formats/index.html) Delta/CSV/ORC/Parquet/JSON etc  
	Details on FAILFAST/PERMISSIVE/DROPMALFORMED in each file format  
[Model semi-structured data](https://docs.databricks.com/aws/en/semi-structured)  
[DQX](https://databrickslabs.github.io/dqx/) a data quality framework for DLT and PySpark   
[Advanced Spark configurations](https://spark.apache.org/docs/latest/configuration.html)  
[Adaptive query execution](https://docs.databricks.com/en/optimizations/aqe.html)  
[Spark SQL Data Types](https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html)  
[Spark Built-In JDBC Drivers](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

* DB2  
* MariaDB  
* MS Sql  
* Oracle  
* PostgreSQL

  Date time functions:

  [Symbolic pattern letters for date and timestamp parsing and formatting](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)

  [Databricks blog post on Spark 3.0 changing data types](https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html)

		[Generic Load/Save Functions \- Spark 3.5.2 Documentation](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables)  
[From/to pandas and PySpark DataFrames](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/pandas_pyspark.html#pyspark)   
		[Convert between PySpark and pandas DataFrames](https://docs.databricks.com/en/pandas/pyspark-pandas-conversion.html) Covers PyArrow & Arrow  
		[pandas function APIs](https://docs.databricks.com/en/pandas/pandas-function-apis.html) mapInPandas & applyInPandas  
		[Best Practices — PySpark 3.5.3 documentation](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/best_practices.html) Pandas API on Spark

#### User Defined Functions {#user-defined-functions}

Seeing custom UDF: [SQL UDF \- USER](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-show-functions.html)  
Python UDF Decorator: [https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html)   
Pandas UDF Decorator: [https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.pandas\_udf.htm](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.pandas_udf.html)  
Vectorized UDF Blog post: [https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)   
Vectorized UDF Documentation: [https://spark.apache.org/docs/latest/api/python/user\_guide/sql/arrow\_pandas.html?highlight=arrow](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html?highlight=arrow)  

#### Structured Streaming {#structured-streaming}

[pyspark.sql.streaming.DataStreamReader](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html)   
[pyspark.sql.streaming.DataStreamWriter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html)   
[pyspark.sql.streaming.](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html)[StreamingQuery](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html)  
	[Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)  
	[Introducing transformWithState in Apache Spark™ Structured Streaming](https://www.databricks.com/blog/introducing-transformwithstate-apache-sparktm-structured-streaming)

### Delta Lake {#delta-lake}

[What is Delta Lake?](https://docs.databricks.com/en/delta/index.html)  
[Delta Lake Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html)  
	[table\_changes table-valued function](https://docs.databricks.com/aws/en/sql/language-manual/functions/table_changes)   
[DESCRIBE HISTORY](https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html)   
[Work with Delta Lake table history](https://docs.databricks.com/en/delta/history.html)  
[Clone a table on Databricks](https://docs.databricks.com/delta/clone.html)    
	[Dr. Alan Dennis of Speedboat’s article on data copying](https://www.linkedin.com/posts/alandennis_databricks-shallow-and-deep-clone-activity-7273329890001653760-Fq1a?utm_source=share&utm_medium=member_desktop&rcm=ACoAAAEOcdwBCgKE1Q6Lh8uL1KMdrISKEUf9XFI)  
[What are deletion vectors?](https://docs.databricks.com/en/delta/deletion-vectors.html)   
[Delta Lake Deletion Vectors](https://delta.io/blog/2023-07-05-deletion-vectors/)   
[Polars integration with Delta Lake](https://github.com/delta-io/delta-examples/blob/26e66c3af2d8d9449b4ed03053235cb7274d2317/notebooks/polars/read-delta-table-polars.ipynb) \- note: Polars is not a Spark native/worker aware API  
[GDPR and CCPA compliance with Delta Lake](https://learn.microsoft.com/en-us/azure/databricks/security/privacy/gdpr-delta)  
[Primary Key and Foreign Key constraints are GA and now enable faster queries](https://www.databricks.com/blog/primary-key-and-foreign-key-constraints-are-ga-and-now-enable-faster-queries)  
[Delta Lake generated columns | Databricks Documentation](https://docs.databricks.com/aws/en/delta/generated-columns#use-identity-columns-in-delta-lake)   
[Configure Delta Lake to control data file size](https://docs.databricks.com/en/delta/tune-file-size.html)

### Databricks {#databricks}

#### Articles from [DBSQL SME Engineering – Medium](https://medium.com/dbsql-sme-engineering)  {#articles-from-dbsql-sme-engineering-–-medium}

	[Databricks System Table Deep Dive — Data Tagging Design Patterns](https://medium.com/dbsql-sme-engineering/databricks-system-table-deep-dive-data-tagging-design-patterns-652718842b82)  
[Having your cake & eating it, too: Best Performance, User Experience, & TCO on DBSQL](https://medium.com/dbsql-sme-engineering/having-your-cake-and-eating-it-too-best-performance-user-experience-and-tco-on-databricks-e5557f52e193)    
[Tune Query Performance in Databricks SQL with the Query Profile](https://medium.com/dbsql-sme-engineering/tune-query-performance-in-databricks-sql-with-the-query-profile-439196b18f47)  
[Data Governance – DBSQL SME Engineering](https://medium.com/dbsql-sme-engineering/governance/home)  
[Fine-grained Access Control with Permission Table in Databricks SQL](https://medium.com/dbsql-sme-engineering/fine-grained-access-control-with-permission-table-in-databricks-sql-e6a24d1e1b6e)  
[Understanding Data Access Patterns with Unity Catalog Lineage](https://medium.com/dbsql-sme-engineering/understanding-data-access-patterns-with-unity-catalog-lineage-52eb4a632700)   
[1 Trillion Row Challenge on Databricks SQL](https://medium.com/dbsql-sme-engineering/1-trillion-row-challenge-on-databricks-sql-41a82fac5bed)  
[Query external Iceberg tables created and managed by Databricks with Snowflake](https://medium.com/dbsql-sme-engineering/query-external-iceberg-tables-created-and-managed-by-databricks-with-snowflake-e4d537d09e31) 

#### General {#general}

Client AppDev: DBConnect  
[DBConnect Github Repo](https://github.com/databricks-demos/dbconnect-examples)  
[Using DBConnect in PyCharn](https://docs.databricks.com/en/dev-tools/databricks-connect/python/pycharm.html)   
		Alteryx Integration  
			[Alteryx to Spark Converter](https://alteryx2spark-jxbkewujwixs7547z4iaru.streamlit.app/)  
[Alteryx Analytics Cloud With Databricks](https://www.alteryx.com/blog/alteryx-analytics-cloud-with-databricks)  
System Tables  
[Billing Assistant and System Tables](https://docs.databricks.com/en/admin/system-tables/index.html#which-system-tables-are-available)  
[Monitor account activity with system tables](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/)

* [Audit Logs](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/audit-logs)  
* [Billable Usage](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing)  
* [Table & Column Lineage](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/lineage)  
* [Data Marketplace analytics](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/marketplace)  
* [Clusters](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/compute)  
* [List Prices](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/pricing)  
* [Jobs & Tasks](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/jobs)  
* [Warehouse events](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/warehouse-events)

  API:

  [Databricks CLI commands](https://docs.databricks.com/en/dev-tools/cli/commands.html)

  [DButils commands](https://docs.databricks.com/en/dev-tools/databricks-utils.html)  

  [Databricks REST API](https://docs.databricks.com/api/latest/index.html#rest-api-2-0) 

  [Connecting to data sources](https://docs.databricks.com/en/connect/index.html)

  [Query JSON strings](https://docs.databricks.com/en/semi-structured/json.html)

  [UNDROP TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-undrop-table.html)


  #### Declarative Pipelines (DLP) formerly known as  Delta Live Tables (DLT) {#declarative-pipelines-(dlp)-formerly-known-as-delta-live-tables-(dlt)}

  [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp#gsc.tab=0) 

  	Python reference: [Lakeflow Declarative Pipelines Python language reference](https://docs.databricks.com/en/delta-live-tables/python-ref.html)

  	SQL reference: [Lakeflow Declarative Pipelines SQL language reference](https://docs.databricks.com/en/delta-live-tables/sql-ref.html)

  Expectations: [Manage data quality with pipeline expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)

  [DLT pipeline development made simple with notebooks](https://www.databricks.com/blog/dlt-pipeline-development-made-simple-notebooks)

  [Lakeflow Declarative Pipelines Pricing | Databricks](https://www.databricks.com/product/pricing/lakeflow-declarative-pipelines) 

  [Blog on reviewing DLT logs programmatically and building data quality reports](https://docs.google.com/document/d/15YKyX_fqN96LVM4uL9CL5t9289teMXGWD9c63eBRdSo/edit)

  [The AUTO CDC APIs: Simplify change data capture with Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/cdc)

  [Optimize stateful processing in Lakeflow Declarative Pipelines with watermarks](https://docs.databricks.com/aws/en/ldp/stateful-processing)

  [Incremental refresh for materialized views](https://docs.databricks.com/aws/en/optimizations/incremental-refresh) 

  [Run an update in Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/updates#development-mode)

  [Monitor Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/observability#query-the-event-log)

  #### Unity Catalog (UC)  {#unity-catalog-(uc)}

  ##### General  {#general-1}

  [What is Unity Catalog?](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) 

  [Use Unity Catalog with your Delta Live Tables pipelines](https://docs.databricks.com/en/delta-live-tables/unity-catalog.html) 	

  [Manage privileges in Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html) 

  [What is Unity Catalog?](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#managed-storage)  
  [User-Defined Functions (UDFs):](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html) 	

  [External Locations](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-external-locations)

  [Managed vs. external volumes](https://docs.databricks.com/aws/en/volumes/managed-vs-external)	

  [Lakehouse Federation](https://docs.databricks.com/en/query-federation/index.html)

  	[Run federated queries on Google BigQuery](https://docs.databricks.com/aws/en/query-federation/bigquery)

  [Announcing General Availability of Lakehouse Federation](https://www.databricks.com/blog/announcing-general-availability-lakehouse-federation)

  [Enable a workspace for Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/enable-workspaces.html) 

  [Monitor usage with system tables](https://docs.databricks.com/en/admin/system-tables/index.html) 

  [Create a Unity Catalog metastore](https://docs.databricks.com/en/data-governance/unity-catalog/create-metastore.html)

  [Apply tags to Unity Catalog securable objects](https://docs.databricks.com/en/database-objects/tags.html) 

  [Constraints on Databricks](https://docs.databricks.com/en/tables/constraints.html) SQL Only \- no PySpark API 

  ##### Data Product Features {#data-product-features}

  ###### *Clean Room*	 {#clean-room}

  * [Blog: What’s New with Data Sharing and Collaboration](https://www.databricks.com/blog/whats-new-data-sharing-and-collaboration)  
    * [Keynote \+ Demo: Clean Rooms](https://youtu.be/uB0n4IZmS34?feature=shared&t=4617)  
    * [Getting Started with Databricks Clean Rooms](https://www.youtube.com/watch?v=rmTdQ_ojj0A&t=2s)  
    * [Collaboration with Databricks Clean Rooms and PETs](https://www.youtube.com/watch?v=GsahvF-j888&t=2s)  
    * [Secure Data and AI Collaboration with Databricks Clean Rooms](https://www.youtube.com/watch?v=27kEu5Z2k0c&t=1s)  
    * [What's New with Data Sharing and Collaboration](https://www.youtube.com/watch?v=M1zrt1ZJexE&t=2s)

    ###### *Delta Sharing* {#delta-sharing}

			[Monitor and manage Delta Sharing egress costs (for providers)](https://docs.databricks.com/en/delta-sharing/manage-egress.html) 

Structured Streaming:  
[Autoloader](https://www.databricks.com/blog/2020/02/24/introducing-databricks-ingest-easy-data-ingestion-into-delta)  
[Compare Auto Loader file detection modes](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-detection-modes)    
[What is Auto Loader directory listing mode?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/directory-listing-mode)  
[What is Auto Loader file notification mode?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode)   
[Schema Evolution](https://docs.databricks.com/en/ingestion/auto-loader/schema.html)  
[Apply watermarks to control data processing thresholds](https://docs.databricks.com/en/structured-streaming/watermarks.html)  
[Configure Structured Streaming batch size on Databricks](https://docs.databricks.com/en/structured-streaming/batch-size.html)    
[Spark Streaming \- Spark 3.5.0 Documentation](https://spark.apache.org/docs/3.5.3/streaming-programming-guide.html#input-dstreams-and-receivers)   
[File metadata column (**\_metadata.\*** in PySpark/Spark SQL)](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)

#### Performance Optimization: {#performance-optimization:}

##### Delta Lake {#delta-lake-1}

[Delta Lake Liquid Clustering](https://delta.io/blog/liquid-clustering/)   
[Announcing General Availability of Predictive Optimization](https://www.databricks.com/blog/announcing-general-availability-predictive-optimization)  
[Announcing GA of Predictive I/O for Updates: Faster DML Queries, Right Out of the Box](https://www.databricks.com/blog/announcing-ga-predictive-io-updates-faster-dml-queries)  
[Top 5 Databricks Performance Tips](https://www.databricks.com/blog/2022/03/10/top-5-databricks-performance-tips.html)  
[Comprehensive Guide to Optimize Databricks, Spark and Delta Lake Workloads](https://www.databricks.com/discover/pages/optimize-data-workloads-guide)   
Delta Lake & Statistics:  
	[ANALYZE TABLE…COMPUTE STATISTICS](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-analyze-table.html) (COMPUTE…DELTA STATISTIC is stored in delta log)  
 	[Data Skipping/Default First-32 stats](https://docs.databricks.com/en/delta/data-skipping.html#specify-delta-statistics-columns)  
[Use liquid clustering/incompatibility with partitioning/Z-Order for Delta Tables](https://docs.databricks.com/en/delta/clustering.html)  
[Difference between Liquid clustering and Z-ordering](https://community.databricks.com/t5/data-engineering/difference-between-liquid-clustering-and-z-ordering/td-p/82262)   
[Announcing Automatic Liquid Clustering | Databricks Blog](https://www.databricks.com/blog/announcing-automatic-liquid-clustering)   
[OPTIMIZE | Databricks on AWS](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html)   
[VACUUM | Databricks on AWS](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html)  
[Details on Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)  
[When to partition tables on Databricks](https://docs.databricks.com/en/tables/partitions.html)  
[Predictive optimization for Unity Catalog managed tables](https://docs.databricks.com/en/optimizations/predictive-optimization.html)  
[Isolation levels and write conflicts on Databricks](https://docs.databricks.com/aws/en/optimizations/isolation-level#gsc.tab=0)   
[What are ACID guarantees on Databricks?](https://docs.databricks.com/aws/en/lakehouse/acid) 

##### Photon/ Databricks SQL (DBSQL) {#photon/-databricks-sql-(dbsql)}

		[Query profile](https://docs.databricks.com/en/sql/user/queries/query-profile.html)  
		[Medium article from databricks on tuning query performance](https://medium.com/dbsql-sme-engineering/tune-query-performance-in-databricks-sql-with-the-query-profile-439196b18f47#id_token=eyJhbGciOiJSUzI1NiIsImtpZCI6ImE1MGY2ZTcwZWY0YjU0OGE1ZmQ5MTQyZWVjZDFmYjhmNTRkY2U5ZWUiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJhenAiOiIyMTYyOTYwMzU4MzQtazFrNnFlMDYwczJ0cDJhMmphbTRsamRjbXMwMHN0dGcuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJhdWQiOiIyMTYyOTYwMzU4MzQtazFrNnFlMDYwczJ0cDJhMmphbTRsamRjbXMwMHN0dGcuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJzdWIiOiIxMTQxMTk5MTg1NDE3ODg4NzYxODQiLCJlbWFpbCI6ImFyY2hhZXVzZGVzaWduQGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJuYmYiOjE3MjkxODg1NTMsIm5hbWUiOiJBbmRyZXcgTWlua2luIiwicGljdHVyZSI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0tNelBMa3BJTVlpaHVscDM3Y1dORFRJRnl1dDVPSHRpNmhLX0pSYjhrcWlzandoNEk9czk2LWMiLCJnaXZlbl9uYW1lIjoiQW5kcmV3IiwiZmFtaWx5X25hbWUiOiJNaW5raW4iLCJpYXQiOjE3MjkxODg4NTMsImV4cCI6MTcyOTE5MjQ1MywianRpIjoiYzQ5MTc4YWRmNmU1YjNhOTgwYzY2MWU4OWYxNWVmODliYTAzZGEzNyJ9.T5Ec3_o_l__c3mLFyhtDBpMWdDpUKHcxRDCe2LNxP2XtP8XyuhFIRCfPKMSGmQ3pBbqr-kB0fReHEx0lRH2gs-rK9JXhkwK5tRKa64ZSmJabuHU_Mia7KsyaVJAgACu2wwzwVxy0TTB2K_Uqeh9k_HvUoXMrcGB9rrnjU-K5nhO1vnPv3rQ4-0IWFPEeau7ADKlMmyBemRl8xbs140Zx6LN2_LPMLSMh38a-1L5cK2r3T0L659su_T8I8NbB7ulRSKaZokHmzl7mnO6jG-nvp-IRWhAYIZXjazSUEKoMq1rppjT8cdg8rhaq1A9jOCNy8k8CM-dnkTIhGVVmxWcpdg)  
		[TPC-DS Price/Performance record holder](https://www.tpc.org/tpcds/results/tpcds_price_perf_results5.asp?resulttype=all&version=3)  
[TPC-DS Performance record holder](https://www.tpc.org/tpcds/results/tpcds_perf_results5.asp?resulttype=all&version=)   
[EXPLAIN | Databricks Documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-explain)  
	[What is Photon? | Databricks on AWS](https://docs.databricks.com/aws/en/compute/photon) 

###### 		*Migration Tools* {#migration-tools}

		[SQLGlot \- a no-dependency SQL parser, transpiler](https://github.com/tobymao/sqlglot/tree/main)   
		[GitHub \- Rremorph: Cross-compiler and Data Reconciler into Databricks Lakehouse](https://github.com/databrickslabs/remorph)  
[DBSQL MIgration Helpers](https://github.com/CodyAustinDavis/dbsql_sme/tree/main/Migration%20Helpers/10-migrations)   
[Observability Lakeview Dashboards](https://github.com/CodyAustinDavis/dbsql_sme/tree/main/Observability%20Dashboards%20and%20DBA%20Resources/Observability%20Lakeview%20Dashboard%20Templates)  

##### Apache Spark/Spark SQL {#apache-spark/spark-sql}

[How to Profile PySpark](https://www.databricks.com/blog/how-profile-pyspark)  
[Debugging with the Apache Spark UI](https://docs.databricks.com/en/compute/troubleshooting/debugging-spark-ui.html)  
[Spark UI Simulator](https://github.com/databricks-academy/spark-ui-simulator)  
	[Spill Listener \- Spark UI](https://github.com/databricks-academy/spark-ui-simulator/tree/main/experiment-1596/v002-S/screenshots)  
	[Salt Join \- Spark UI](https://selectfrom.dev/spark-performance-tuning-skewness-part-2-9f50c765a87e)  
[Hive UDF for legacy pre-Spark SQL conversions](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF)[Syntax, Partitioning Join & Skew hints](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-hints.html)  
[Comprehensive Guide to Optimize Databricks, Spark and Delta Lake Workload](https://www.databricks.com/discover/pages/optimize-data-workloads-guide)  
[Query variant data](https://docs.databricks.com/en/semi-structured/variant.html)

##### Approximate Functions: {#approximate-functions:}

	[Apache Spark ❤️ Apache DataSketches: New Sketch-Based Approximate Distinct Counting](https://www.databricks.com/blog/apache-spark-3-apache-datasketches-new-sketch-based-approximate-distinct-counting)  
[Details on Approximate Count Logic implementation in Spark](https://github-wiki-see.page/m/swoop-inc/spark-alchemy/wiki/Spark-HyperLogLog-Functions)

* [approx\_count\_distinct aggregate function](https://docs.databricks.com/en/sql/language-manual/functions/approx_count_distinct.html)  
* [approx\_percentile aggregate function](https://docs.databricks.com/en/sql/language-manual/functions/approx_percentile.html)  
* [approx\_top\_k aggregate function](https://docs.databricks.com/en/sql/language-manual/functions/approx_top_k.html)

#### Data Analyst (SQL) {#data-analyst-(sql)}

##### Databricks SQL {#databricks-sql}

[SQL Warehouse Types](https://docs.databricks.com/en/admin/sql/warehouse-types.html)  
[Databricks SQL Pricing](https://www.databricks.com/product/pricing/databricks-sql)  
[Introducing the New SQL Editor](https://www.databricks.com/blog/introducing-new-sql-editor)

##### Alerts {#alerts}

		[What are Databricks SQL alerts?](https://docs.databricks.com/en/sql/user/alerts/index.html) 

##### Dashboards {#dashboards}

[Learn the basics of the Databricks Data Intelligence Platform](https://www.databricks.com/learn/training/databricks-fundamentals)  
[Embed a dashboard](https://docs.databricks.com/en/dashboards/embed.html)  
[Work with dashboard parameters](https://docs.databricks.com/en/dashboards/parameters.html)  
[Use query-based parameters](https://docs.databricks.com/en/dashboards/tutorials/query-based-params.html)  
[Dashboards in notebooks](https://docs.databricks.com/aws/en/notebooks/dashboards#present-a-notebook-dashboard) 

##### Genie Spaces/ Databricks IQ {#genie-spaces/-databricks-iq}

[Onboarding your new AI/BI Genie | Databricks Blog](https://www.databricks.com/blog/onboarding-your-new-aibi-genie)  
[Add comments to data and AI assets](https://docs.databricks.com/en/comments/index.html)   
[Create visualizations with Databricks Assistant](https://docs.databricks.com/en/dashboards/tutorials/create-w-db-assistant.html).  
[Work with an AI/BI Genie space](https://docs.databricks.com/en/genie/index.html)   
[Building Confidence in Your Genie Space with Benchmarks and Ask for Review](https://www.databricks.com/blog/building-confidence-your-genie-space-benchmarks-and-ask-review)

##### Data Warehouse Design {#data-warehouse-design}

		[Building the Data Lakehouse](https://www.databricks.com/resources/ebook/building-the-data-lakehouse) Book by Bill Inmon, inventor of the enterprise data warehouse 

##### Power BI  {#power-bi}

		[Connect to semantic layer partners using Partner Connect](https://docs.databricks.com/en/partner-connect/semantic-layer.html)  
[R\&D Optimization With Knowledge Graphs](https://www.databricks.com/solutions/accelerators/rd-optimization-with-knowledge-graphs) 

#### Data Engineering: {#data-engineering:}

##### Schemas and Tables {#schemas-and-tables}

Schemas and Tables \- Databricks Docs: [https://docs.databricks.com/user-guide/tables.html](https://docs.databricks.com/user-guide/tables.html)   
Managed and Unmanaged Tables: [https://docs.databricks.com/user-guide/tables.html\#managed-and-unmanaged-tables](https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables)   
Creating a Table with the UI: [https://docs.databricks.com/user-guide/tables.html\#create-a-table-using-the-ui](https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui)   
Create a Local Table: [https://docs.databricks.com/user-guide/tables.html\#create-a-local-table](https://docs.databricks.com/user-guide/tables.html#create-a-local-table)   
[Databricks SQL/Dataframe API Join types](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-join.html)

##### Row Level Security (RLS) & Column Level Masking (CLM)  {#row-level-security-(rls)-&-column-level-masking-(clm)}

[Using Table-bound Functions](https://docs.databricks.com/en/tables/row-and-column-filters.html)  
[Using dynamic views](https://docs.databricks.com/en/views/dynamic.html)

##### Looker Connectivity {#looker-connectivity}

	[Looker to Databricks](https://cloud.google.com/looker/docs/db-config-databricks)  
	[Databricks to Looker](https://docs.databricks.com/en/partners/bi/looker.html)

NOT NULL/CHECK: [Constraints on Databricks](https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint)   
[CREATE VIEW](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-view.html)  
[CREATE TABLE \[USING\]](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)  
[Ingest data into a Databricks lakehouse](https://docs.databricks.com/en/ingestion/index.html)   
[Iceberg & Uniform Best Practices](https://docs.databricks.com/en/lakehouse-architecture/interoperability-and-usability/best-practices.html)  
[read\_files table-valued function](https://docs.databricks.com/en/sql/language-manual/functions/read_files.html)   
Details on [Photon](https://docs.databricks.com/en/compute/photon.html) \- Accelerates Delta and Parquet tables  
[CTAS with no data movement](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html) (CREATE TABLE...USING)  
[External tables](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-tables.html)

### Machine Learning {#machine-learning}

#### General {#general-2}

[MLflow Documentation](https://mlflow.org/docs/latest/index.html)  
	[MLflow — Manage Lifecycle of ML. Platform for Complete Machine Learning](https://medium.datadriveninvestor.com/mlflow-manage-lifecycle-of-ml-projects-f10760bf54d)  
	MLflow [Tutorials and Examples](https://mlflow.org/docs/latest/tutorials-and-examples/index.html)   
	[How To (Immensely) Optimize Your Machine Learning Development and Operations with MLflow](https://www.dailydoseofds.com/how-to-immensely-optimize-your-machine-learning-development-and-operations-with-mlflow/)  
	[Tutorial: End-to-end ML models on Databricks](https://docs.databricks.com/aws/en/mlflow/end-to-end-example)   
	[Automatic Logging with MLflow Tracking](https://mlflow.org/docs/latest/tracking/autolog.html)  
[MLflow Flavors](https://mlflow.org/docs/latest/models.html?highlight=flavors#built-in-model-flavors)    
[MLflow Flavors and PyFunc](https://mlflow.org/docs/latest/traditional-ml/creating-custom-pyfunc/part1-named-flavors.html?gad_source=1&gclid=Cj0KCQiA2oW-BhC2ARIsADSIAWoppZAzZzx9J6ihS5JQ-6Kfwj7YWE3NXNwg32T1X5cSpUupAp4eHl4aAj8QEALw_wcB) 

   
[Kaggle](https://www.kaggle.com/)   
	[Learn Intro to Machine Learning | Kaggle](https://www.kaggle.com/learn/intro-to-machine-learning) 

#### Data Prep for Machine Learning {#data-prep-for-machine-learning}

##### EDA {#eda}

###### [***dbutils.data.summarize()***](https://docs.databricks.com/en/dev-tools/databricks-utils.html#data-utility-dbutilsdata) 	*DataFrame’s **summary()** method* {#dbutils.data.summarize()-dataframe’s-summary()-method}

[Matplotlib](https://matplotlib.org/) core Python visualization  
[Seaborn core visualization](https://seaborn.pydata.org/)  
[YData Profiling](https://docs.profiling.ydata.ai/latest/) 

##### Cross Validation {#cross-validation}

[SparkML cross validation](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.tuning.CrossValidator.html)   
[sklearn cross validation](https://scikit-learn.org/stable/modules/cross_validation.html)

##### 

##### SparkML {#sparkml}

##### [**StringIndexer** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.StringIndexer.html#pyspark.ml.feature.StringIndexer)  [**OneHotEncoder** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.OneHotEncoder.html#pyspark.ml.feature.OneHotEncoder) [**VectorAssembler** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.VectorAssembler.html) [**StandardScaler** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.StandardScaler.html) [**RobustScaler** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.RobustScaler.html) [**randomSplit** method](http://randomSplit) [**Imputer** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.Imputer.html) [**Pipeline** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.Pipeline.html) {#stringindexer-class-onehotencoder-class-vectorassembler-class-standardscaler-class-robustscaler-class-randomsplit-method-imputer-class-pipeline-class}

##### Feature Store {#feature-store}

	[Feature engineering and serving](https://docs.databricks.com/en/machine-learning/feature-store/index.html)   
	[Unity Catalog & Feature Store](https://docs.databricks.com/en/machine-learning/feature-store/uc/feature-tables-uc.html#upgrade-a-workspace-feature-table-to-unity-catalog)  
	[Work with feature tables](https://docs.databricks.com/en/machine-learning/feature-store/uc/feature-tables-uc.html#use-existing-uc-table)   
[The Comprehensive Guide to Feature Stores](https://www.databricks.com/resources/ebook/the-comprehensive-guide-to-feature-stores?_ga=2.30329279.1288486691.1728575218-674923341.1701646724)   
[Publish features to an online store](https://docs.databricks.com/en/machine-learning/feature-store/publish-features.html)  
[Automatic feature lookup with Databricks Model Serving](https://docs.databricks.com/en/machine-learning/feature-store/automatic-feature-lookup.html)  
[Feature Engineering and Workspace Feature Store Python API](https://docs.databricks.com/en/machine-learning/feature-store/python-api.html)     
[Use features to train models](https://docs.databricks.com/aws/en/machine-learning/feature-store/train-models-with-feature-store)

#### Model Development {#model-development}

##### 	AutoML & Its Algorithms {#automl-&-its-algorithms}

	[Sci-Kit (sklearn)](https://scikit-learn.org/stable/index.html)   
		[Pipeline — scikit-learn 1.6.1 documentation](https://scikit-learn.org/stable/modules/generated/sklearn.pipeline.Pipeline.html)   
	[XGBoost](https://xgboost.readthedocs.io/en/latest/python/python_api.html#xgboost.XGBRegressor)  
	[LightGBM](https://lightgbm.readthedocs.io/en/latest/pythonapi/lightgbm.LGBMRegressor.html)  
[What is AutoML?](https://docs.databricks.com/en/machine-learning/automl/index.html)   
[Forecasting (serverless) with AutoML | Databricks](https://docs.databricks.com/aws/en/machine-learning/train-model/serverless-forecasting) 

##### Deep Learning & Databricks {#deep-learning-&-databricks}

	[HorovodRunner: distributed deep learning with Horovod](https://docs.databricks.com/en/archive/machine-learning/train-model/horovod-runner.html)   
[What is Ray on Databricks?](https://docs.databricks.com/en/machine-learning/ray/index.html)  
[Example code](https://github.com/aelhelouDB/ray-on-databricks-examples/blob/main/1-HPO-ML-Training-Optuna/01_hpo_optuna_ray_train.py)    
	[Connect Ray and Spark on Databricks](https://docs.databricks.com/en/machine-learning/ray/connect-spark-ray.html)   
[Save Apache Spark DataFrames as TFRecord files](https://docs.databricks.com/en/machine-learning/load-data/tfrecords-save-load.html)   
[Adapt single node PyTorch to distributed deep learning](https://docs.databricks.com/en/archive/machine-learning/train-model/mnist-pytorch.html)  
[Load data using Mosaic Streaming](https://docs.databricks.com/en/machine-learning/load-data/streaming.html)

##### Hyperopt {#hyperopt}

[Algorithms](https://hyperopt.github.io/hyperopt/#algorithms)  
[Parameter Expressions](https://hyperopt.github.io/hyperopt/getting-started/search_spaces/#parameter-expressions)  
[How (Not) to Tune Models With Hyperopt | Databricks Blog](https://www.databricks.com/blog/2021/04/15/how-not-to-tune-your-model-with-hyperopt.html)  
​​[Hyperopt best practices documentation from Databricks](https://docs.databricks.com/applications/machine-learning/automl-hyperparam-tuning/hyperopt-best-practices.html)  
[Best Practices for Hyperparameter Tuning with MLflow](https://www.databricks.com/session/best-practices-for-hyperparameter-tuning-with-mlflow) (talk)  
[Advanced Hyperparameter Optimization for Deep Learning with MLflow](https://www.databricks.com/session/advanced-hyperparameter-optimization-for-deep-learning-with-mlflow)  
[Scaling Hyperopt to Tune Machine Learning Models in Python](https://www.databricks.com/blog/2019/10/29/scaling-hyperopt-to-tune-machine-learning-models-in-python.html)  
Kaggle \- [Tutorial on hyperopt](https://www.kaggle.com/code/fanvacoolt/tutorial-on-hyperopt) 

##### Optuna {#optuna}

[Hyperparameter tuning with Optuna](https://docs.databricks.com/en/machine-learning/automl-hyperparam-tuning/optuna.html) 

#### Model Operations (MLOps) {#model-operations-(mlops)}

##### [MLOps Definition and Benefits | Databricks](https://www.databricks.com/glossary/mlops#:~:text=The%20primary%20benefits%20of%20MLOps,and%20faster%20deployment%20and%20production) [Inference tables for monitoring and debugging models](https://docs.databricks.com/en/machine-learning/model-serving/inference-tables.html) [Databricks MLOps Stacks](https://github.com/databricks/mlops-stacks/blob/main/README.md#databricks-mlops-stacks) [MLOps workflows on Databricks](https://docs.databricks.com/en/machine-learning/mlops/mlops-workflow.html#mlops-workflows-on-databricks)   [Monitor metric tables \- Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/lakehouse-monitoring/monitor-output#drift-metrics-table) [Best practices for operational excellence](https://docs.databricks.com/en/lakehouse-architecture/operational-excellence/best-practices.html) {#mlops-definition-and-benefits-|-databricks-inference-tables-for-monitoring-and-debugging-models-databricks-mlops-stacks-mlops-workflows-on-databricks-monitor-metric-tables---azure-databricks-best-practices-for-operational-excellence}

##### 	DataOps {#dataops}

###### [How does Databricks support CI/CD for machine learning?](https://docs.databricks.com/en/machine-learning/mlops/ci-cd-for-ml.html#dataops-tasks-and-tools-in-databricks) {#how-does-databricks-support-ci/cd-for-machine-learning?}

### Generative AI Reference {#generative-ai-reference}

#### Blogs and General Resources {#blogs-and-general-resources}

##### Databricks Documentation {#databricks-documentation}

[Databricks Generative AI Cookbook](https://ai-cookbook.io/)  
[AI Playground](https://dbc-da0888e8-3c50.cloud.databricks.com/ml/playground)  
[Databricks Marketplace](https://dbc-da0888e8-3c50.cloud.databricks.com/marketplace)  
[Podcast Archive \- Latent Space](https://www.latent.space/podcast/archive?sort=new)  
[The Cognitive Revolution](https://www.cognitiverevolution.ai/)  
[Machine Learning Street Talk (MLST)](https://www.mlst.ai/)  
[ARC Prize](https://arcprize.org/)

#### GenAI Solution Development {#genai-solution-development}

##### Data preparation {#data-preparation}

Semantic Chunking:  
LangChain: [How to split text based on semantic similarity | 🦜️🔗 LangChain](https://python.langchain.com/docs/how_to/semantic-chunker/)   
Llama Index: [Semantic Chunking Llama Pack](https://llamahub.ai/l/llama-packs/llama-index-packs-node-parser-semantic-chunking?from=all)   
Data prep  [Related Research Paper](https://arxiv.org/pdf/2307.03172.pdf)   
 N[eedle in haystack test](https://github.com/gkamradt/LLMTest_NeedleInAHaystack)

##### Vector Search {#vector-search}

[Mosaic AI Vector Search](https://docs.databricks.com/en/generative-ai/vector-search.html#mosaic-ai-vector-search)  
[Announcing Hybrid Search General Availability in Mosaic AI Vector Search | Databricks Blog](https://www.databricks.com/blog/announcing-hybrid-search-general-availability-mosaic-ai-vector-search) [Creating and querying vector search indices](https://docs.databricks.com/en/generative-ai/create-query-vector-search.html)  
[ChunkViz Helper Tool](https://chunkviz.up.railway.app/)  
 [Hierarchical Navigable Small Worlds (HNSW) | Pinecone](https://www.pinecone.io/learn/hnsw/)  
[IVFPQ \+ HNSW for Billion-scale Similarity Search | by Peggy Chang | Towards Data Science | Towards Data Science](https://towardsdatascience.com/ivfpq-hnsw-for-billion-scale-similarity-search-89ff2f89d90e)  
[Hierarchical Navigable Small Worlds (HNSW) \- Zilliz Vector database blog](https://zilliz.com/blog/hierarchical-navigable-small-worlds-HNSW) 

##### Retrieval Augmented Generation (RAG) {#retrieval-augmented-generation-(rag)}

[Deconstructing RAG](https://blog.langchain.dev/deconstructing-rag/)   
[Welcome to GraphRAG](https://microsoft.github.io/graphrag/)   
[Implementing ‘From Local to Global’ GraphRAG with Neo4j and LangChain](https://neo4j.com/developer-blog/global-graphrag-neo4j-langchain/)   
[Metrics | Ragas](https://docs.ragas.io/en/stable/concepts/metrics/index.html) 

#### GenAI Application Development {#genai-application-development}

##### 

##### [Build an unstructured data pipeline for RAG](https://docs.databricks.com/en/generative-ai/tutorials/ai-cookbook/quality-data-pipeline-rag.html)  {#build-an-unstructured-data-pipeline-for-rag}

##### [Mastering RAG Chunking Techniques for Enhanced Document Processing](https://ai.gopubby.com/mastering-rag-chunking-techniques-for-enhanced-document-processing-8d5fd88f6b72) {#mastering-rag-chunking-techniques-for-enhanced-document-processing}

##### [Semantic Chunking for RAG](https://www.youtube.com/watch?v=TcRRfcbsApw) {#semantic-chunking-for-rag}

##### [MTEB Leaderboard \- a Hugging Face Space by mteb](https://huggingface.co/spaces/mteb/leaderboard) {#mteb-leaderboard---a-hugging-face-space-by-mteb}

##### Apps {#apps}

##### [Introducing Databricks Apps](https://www.databricks.com/blog/introducing-databricks-apps)  {#introducing-databricks-apps}

[What is Databricks Apps?](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html)  
[Compute for Apps | Databricks](https://www.databricks.com/product/pricing/compute-for-apps)  
[Databricks Apps](https://www.databricks.com/product/pricing/databricks-apps)  Pricing

##### Python {#python}

[PEP 8 – Style Guide for Python Code](https://peps.python.org/pep-0008/#method-names-and-instance-variables) (Naming Conventions)   
[PEP 257 – Docstring Conventions | peps.python.org](https://peps.python.org/pep-0257/)  
[dataclasses — Data Classes — Python 3.12.5 documentation](https://docs.python.org/3/library/dataclasses.html#module-dataclasses)  
[asyncio \- Asynchronous I/O](https://docs.python.org/3/library/asyncio.html)

##### Databricks LLM APIS   {#databricks-llm-apis}

[Create generative AI model serving endpoints | Databricks](https://docs.databricks.com/en/machine-learning/model-serving/create-foundation-model-endpoints.html)  
[Query a Vector Search Endpoint](https://docs.databricks.com/en/generative-ai/create-query-vector-search.html#query-a-vector-search-endpoint)  
MLFlow [DatabricksDeploymentClient](https://mlflow.org/docs/latest/python_api/mlflow.deployments.html?highlight=mlflow%20deployments#mlflow.deployments.DatabricksDeploymentClient)  
[Query a text completion model](https://docs.databricks.com/en/machine-learning/model-serving/score-foundation-models.html#query-a-text-completion-model)  
[Query a chat completion model](https://docs.databricks.com/en/machine-learning/model-serving/score-foundation-models.html#query-a-chat-completion-model)

##### Prompt Engineering/LangChain {#prompt-engineering/langchain}

LLM frameworks

* [LangChain](https://www.langchain.com/)  
* [LLamaIndex](https://www.llamaindex.ai/)    
* [Haystack](https://haystack.deepset.ai/)  
* [DSPy](https://github.com/stanfordnlp/dspy)   
* [DSPy vs LangChain: A Comprehensive Framework Comparison \- Qdrant](https://qdrant.tech/blog/dspy-vs-langchain/)  
* [Comparing LangChain and LlamaIndex with 4 tasks | by Ming | Medium](https://blog.myli.page/comparing-langchain-and-llamaindex-with-4-tasks-2970140edf33)  

Mixed findings  on CoT  
[Large Language Models are Zero-Shot Reasoners](https://openreview.net/pdf?id=e2TBb5y0yFf)   
[**\[2305.04388\] Language Models Don't Always Say What They Think: Unfaithful Explanations in Chain-of-Thought Prompting**](https://arxiv.org/abs/2305.04388)   
[**\[2307.13702\] Measuring Faithfulness in Chain-of-Thought Reasoning**](https://arxiv.org/abs/2307.13702)   
[**LLMs with Chain-of-Thought Are Non-Causal Reasoners**](https://arxiv.org/html/2402.16048v1)   
[LangGraph Studio: The first agent IDE](https://blog.langchain.dev/langgraph-studio-the-first-agent-ide/)   
[A Survey of Techniques for Maximizing LLM Performance](https://www.youtube.com/watch?v=ahnGLM-RC1Y&list=PLOXw6I10VTv-exVCRuRjbT6bqkfO74rWz&index=4)  
[LLM and Temperature](https://www.hopsworks.ai/dictionary/llm-temperature)  
[Prompt Engineering for Developers](https://www.deeplearning.ai/short-courses/chatgpt-prompt-engineering-for-developers/)  
[Prompt Engineering Guide](https://lnkd.in/dC6jBSDZ)  
[Github- Prompt Engineering Guide](https://lnkd.in/dC6jBSDZ)

##### Agents {#agents}

##### [OpenAI's Bet on a Cognitive Architecture](https://blog.langchain.dev/openais-bet-on-a-cognitive-architecture/)  {#openai's-bet-on-a-cognitive-architecture}

[Auto-GPT](https://news.agpt.co/)   
[Building a Python React Agent Class: A Step-by-Step Guide](https://www.neradot.com/post/building-a-python-react-agent-class-a-step-by-step-guide)  
[ReAct vs Plan-and-Execute: A Practical Comparison of LLM Agent Patterns](https://dev.to/jamesli/react-vs-plan-and-execute-a-practical-comparison-of-llm-agent-patterns-4gh9)  
[Introducing the Model Context Protocol \\ Anthropic](https://www.anthropic.com/news/model-context-protocol)

Agent Bricks  
[Introducing Agent Bricks: Auto-Optimized Agents Using Your Data](https://www.databricks.com/blog/introducing-agent-bricks)  
[Agent Bricks: Building Multi-Agent Systems for Structured and Unstructured Information](https://www.youtube.com/watch?v=w8y_CGo1KQU)    
[Turn Documents into Structured Insights with Agent Bricks](https://www.youtube.com/watch?v=VEYYgy0HrqM)  
 

MultiModal Architectures  
[CLIP](https://huggingface.co/docs/transformers/model_doc/clip) Contrastive Language-Image Pre-Training  
[GPT-4V(ision)](https://openai.com/index/gpt-4v-system-card/) 

#### GenAI Evaluation & Governance {#genai-evaluation-&-governance}

##### Data Legality & Guardrails {#data-legality-&-guardrails}

[Query foundation models and external models](https://docs.databricks.com/en/machine-learning/model-serving/score-foundation-models.html#)  
[John Snow Labs](https://www.johnsnowlabs.com/) 

##### Securing & Governing AI Systems {#securing-&-governing-ai-systems}

[Llama Guard](https://marketplace.databricks.com/details/a4bc6c21-0888-40e1-805e-f4c99dca41e4/Databricks_Llama-Guard-Model)  
[Databricks AI Security Framework whitepaper](https://www.databricks.com/resources/whitepaper/databricks-ai-security-framework-dasf)  
[Announcing advanced security and governance in Mosaic AI Gateway | Databricks Blog](https://www.databricks.com/blog/new-updates-mosaic-ai-gateway-bring-security-and-governance-genai-models) 

##### Evaluating LLMs {#evaluating-llms}

[MLflow’s Custom LLM-evaluation Metrics](https://mlflow.org/docs/latest/llms/llm-evaluate/index.html#create-llm-as-judge-evaluation-metrics-category-1)

#### GenAI Deployment & Monitoring {#genai-deployment-&-monitoring}

##### Batch Inference {#batch-inference}

[EdinburghNLP/xsum · Datasets at Hugging Face](https://huggingface.co/datasets/xsum)  
[T5 Text-To-Text Transfer Transformer](https://huggingface.co/t5-smal)  
[Databricks Blog: LLM Inference Best Practices](https://www.databricks.com/blog/llm-inference-performance-engineering-best-practices)  
[TensorRT™](https://github.com/NVIDIA/TensorRT)

* [TensorRT™](https://github.com/NVIDIA/TensorRT) [Sample Notebook](https://docs.databricks.com/en/_extras/notebooks/source/deep-learning/tensorflow-tensorrt.html)

[vLLM](https://github.com/vllm-project/vllm)

* [Samples for DBRX, Mixtral 8x-7B & Mistral-7B](https://github.com/stikkireddy/llm-batch-inference) 

[MLflow](https://mlflow.org/) 

* [MLflow Models](https://mlflow.org/docs/latest/models.html#built-in-model-flavors)   
* [MLflow Tracking](https://mlflow.org/docs/latest/tracking.html#organizing-runs-in-experiments)   
* [MLflow’s LLM Tracking Capabilities](https://mlflow.org/docs/latest/llms/llm-tracking/index.html)   
* [MLflow.start.run documentation](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.start_run)

Unity Catalog's Model Registry [AWS](https://docs.databricks.com/en/machine-learning/manage-model-lifecycle/index.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/manage-model-lifecycle/) | [GCP](https://docs.gcp.databricks.com/en/machine-learning/manage-model-lifecycle/index.html)  
[MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)   
[Apache Spark page](https://spark.apache.org/)  
[Delta Lake page](https://delta.io/)  
[Manage model lifecycle in Unity Catalog | Databricks on AWS](https://docs.databricks.com/machine-learning/manage-model-lifecycle/index.html#use-model-for-inference)  
the ai\_query() SQL function [AWS](https://docs.databricks.com/en/sql/language-manual/functions/ai_query.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/ai_query) 

##### Realtime Model Deployment {#realtime-model-deployment}

[Serve multiple models to a Model Serving endpoint | Databricks on AWS](https://docs.databricks.com/en/machine-learning/model-serving/serve-multiple-models-to-serving-endpoint.html) 

##### Online Monitoring {#online-monitoring}

[Inference tables for monitoring and debugging models | Databricks on AWS](https://docs.databricks.com/en/machine-learning/model-serving/inference-tables.html)  
[Model serving with Databricks](https://docs.databricks.com/en/machine-learning/model-serving/index.html)  
[Introduction to Databricks Lakehouse Monitoring](https://docs.databricks.com/en/lakehouse-monitoring/index.html)  
[DataOps](https://en.wikipedia.org/wiki/DataOps) \+ [DevOps](https://en.wikipedia.org/wiki/DevOps) \+ [ModelOps](https://en.wikipedia.org/wiki/ModelOps)  
[Databricks Empowers CareSource to Provide Better Healthcare to Members](https://www.youtube.com/watch?v=AqO0BLYo0eg)  
[Saving Mothers with ML: How CareSource uses MLOps to Improve Healthcare in High-Risk Obstetrics](https://www.databricks.com/blog/2023/04/03/saving-mothers-ml-how-mlops-improves-healthcare-high-risk-obstetrics.html)  Delta's [Change-Data-Feed](https://docs.delta.io/2.0.0/delta-change-data-feed.html) and special character [column mapping](https://docs.delta.io/latest/delta-column-mapping.html)  
 

### Orchestration and CI/CD   {#orchestration-and-ci/cd}

#### Workflows, Tasks  & CI/CD: {#workflows,-tasks-&-ci/cd:}

##### General {#general-3}

[Installing libraries (cluster, notebook environment management, library precedence)](https://docs.databricks.com/en/libraries/index.html)  
[Quartz Cron Syntax Generator](https://www.connectplaza.com/doc/en/technical+guide/constructor/the-quartz-cron-generator.html)   
[Managing Service Principals](https://docs.databricks.com/en/admin/users-groups/service-principals.html#add-sp-account)  
[Running Databricks Jobs from Airflow](https://docs.databricks.com/en/workflows/jobs/how-to/use-airflow-with-jobs.html)  
[Adding PagerDuty to a Workspace](https://docs.databricks.com/en/admin/workspace-settings/notification-destinations.html)  
[What is a dynamic value reference?](https://docs.databricks.com/en/workflows/jobs/parameter-value-references.html)  
[Run jobs continuously](https://docs.databricks.com/en/jobs/continuous.html)  
[Databricks Workflows Through Terraform](https://www.databricks.com/blog/2022/12/5/databricks-workflows-through-terraform.html)   
[Use task values to pass information between tasks](https://docs.databricks.com/en/jobs/task-values.html)  
[https://docs.databricks.com/en/jobs/task-parameters.html](https://docs.databricks.com/en/jobs/task-parameters.html)  
[Schedule and orchestrate workflows](https://docs.databricks.com/en/jobs/index.html)  
[Add email and system notifications for job events](https://docs.databricks.com/en/jobs/notifications.html)    
[Databricks SDK for Python](https://docs.databricks.com/aws/en/dev-tools/sdk-python#create-a-job) \- Jobs   
[CI/CD with Databricks Git folders (Repos)](https://docs.databricks.com/aws/en/repos/ci-cd-techniques-with-repos)

##### Databricks Asset Bundles (DAB) {#databricks-asset-bundles-(dab)}

[Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)  
[Databricks Asset Bundle configuration](https://docs.databricks.com/en/dev-tools/bundles/settings.html)   
[Substitutions and variables in Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/variables)  
[Manage Databricks apps using Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/apps-tutorial)

##### DBT {#dbt}

[Quickstart for dbt Cloud and Databricks](https://docs.getdbt.com/guides/databricks)  
[A dbt adapter for Databricks.](https://github.com/databricks/dbt-databricks)  
 

##### Lakehouse Monitoring {#lakehouse-monitoring}

[View Lakehouse Monitoring expenses | Databricks on AWS](https://docs.databricks.com/en/lakehouse-monitoring/expense.html)  
[Learn How to Reliably Monitor Your Data and Model Quality in the Lakehouse](https://youtu.be/3TLBZSKeYTk?si=KSaJPGcSOkUfqYXE&t=547)   
[Lakehouse Monitoring GA: Profiling, Diagnosing, and Enforcing Data Quality with Intelligence](https://youtu.be/aDBPoKyA0DQ) 

##### Repos/DevOps  {#repos/devops}

[Git integration with Databricks Git folders](https://docs.databricks.com/repos/index.html)   
[Using Repos and Python module relative paths](https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo)

##### Secrets: {#secrets:}

[Create a secret scope](https://docs.databricks.com/en/security/secrets/secret-scopes.html).   
[Add secrets to the scope](https://docs.databricks.com/en/security/secrets/secrets.html)  
A[ssign access control](https://docs.databricks.com/en/security/auth/access-control/index.html#secrets) 

# Blog and Technical Articles {#blog-and-technical-articles}

[StreamNative and Databricks Unite to Power Real-Time Data Processing with Pulsar-Spark Connector](https://www.databricks.com/blog/streamnative-and-databricks-unite-power-real-time-data-processing-pulsar-spark-connector)  
[A Deep Dive into the Latest Performance Improvements of Stateful Pipelines in Apache Spark Structured Streaming](https://www.databricks.com/blog/deep-dive-latest-performance-improvements-stateful-pipelines-apache-spark-structured-streaming)  
[Performance Improvements for Stateful Pipelines in Apache Spark Structured Streaming](https://www.databricks.com/blog/performance-improvements-stateful-pipelines-apache-spark-structured-streaming)  
[What’s New in Data Engineering and Streaming \- January 2024](https://www.databricks.com/blog/whats-new-data-engineering-and-streaming-january-2024)  
[Introducing Databricks LakeFlow: A unified, intelligent solution for data engineering](https://www.databricks.com/blog/introducing-databricks-lakeflow)  
[Introducing the Open Variant Data Type in Delta Lake and Apache Spark](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)  
[Announcing simplified XML data ingestion](https://www.databricks.com/blog/announcing-simplified-xml-data-ingestion)  
[Announcing the General Availability of Databricks Asset Bundles](https://www.databricks.com/blog/announcing-general-availability-databricks-asset-bundles)  
[SQLite](https://www.sqlite.org/index.html)

[image1]: <data:image/png;base64,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>