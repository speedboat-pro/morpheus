# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Install dependencies
# MAGIC %pip install sentence-transformers==3.0.1 accelerate datasets -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports and configuration
import mlflow
import torch
import time
import pandas as pd
from sentence_transformers import SentenceTransformer
from sentence_transformers.losses import CachedMultipleNegativesRankingLoss
from sentence_transformers.trainer import SentenceTransformerTrainer
from sentence_transformers.training_args import SentenceTransformerTrainingArguments
from sentence_transformers.evaluation import EmbeddingSimilarityEvaluator, SimilarityFunction
from datasets import Dataset
from databricks.sdk import WorkspaceClient                         # Databricks SDK
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    AutoCaptureConfigInput,
    EndpointStateReady,
    EndpointStateConfigUpdate,
)

# ── Workspace config (users.<username> schema per workspace policy) ────
CATALOG       = "users"
SCHEMA        = "drew_minkin"
MODEL_NAME    = f"{CATALOG}.{SCHEMA}.bge_fine_tuned"
BASE_MODEL    = "BAAI/bge-small-en-v1.5"   # swap to bge-large-en-v1.5 on GPU
ENDPOINT_NAME = "bge-fine-tuned-endpoint"

# Derive current user dynamically for the MLflow experiment path
current_user = spark.sql("SELECT current_user()").collect()[0][0]
EXPERIMENT   = f"/Users/{current_user}/bge-fine-tuning"

# WorkspaceClient is pre-authenticated inside Databricks notebooks
w = WorkspaceClient()

# Point MLflow to Unity Catalog model registry instead of the legacy registry
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT)

gpu = torch.cuda.is_available()
print(f"GPU available : {gpu}")
print(f"Base model    : {BASE_MODEL}")
print(f"UC model name : {MODEL_NAME}")
if not gpu:
    print("\u26a0\ufe0f  No GPU detected — attach GPU compute before running the fine-tuning cell.")

# COMMAND ----------

# DBTITLE 1,Prepare training data
# Domain-specific (anchor, positive) pairs for contrastive fine-tuning.
# Replace with pairs drawn from your own corpus for better domain adaptation.
train_pairs = [
    ("What is Delta Lake?",
     "Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark."),
    ("How do I read a CSV file in PySpark?",
     "Use spark.read.csv() with options header=True and inferSchema=True to load a CSV."),
    ("What is MLflow?",
     "MLflow is an open-source platform for managing the end-to-end machine learning lifecycle."),
    ("What is Unity Catalog?",
     "Unity Catalog is Databricks\u2019 unified governance layer for all data and AI assets."),
    ("How does Auto Loader work?",
     "Auto Loader incrementally ingests new files using cloud storage notifications or directory listing."),
    ("How do I optimize a Delta table?",
     "Run the OPTIMIZE command to compact small files; add ZORDER for column-level co-location."),
    ("What is Photon?",
     "Photon is Databricks\u2019 native vectorized query engine that accelerates SQL and DataFrame operations."),
    ("What is Change Data Feed?",
     "Change Data Feed records row-level inserts, updates, and deletes in a Delta table."),
    ("How do I access Databricks secrets?",
     "Use dbutils.secrets.get(scope='my-scope', key='my-key') to retrieve secrets securely."),
    ("What is a streaming table?",
     "A streaming table continuously ingests data from a stream source and stores it in Delta format."),
    ("How do I register a model in Unity Catalog?",
     "Set mlflow.set_registry_uri('databricks-uc') and pass registered_model_name='catalog.schema.model'."),
    ("What is Databricks Feature Store?",
     "Feature Store is a centralized repository for discovering, sharing, and serving ML features."),
    ("How do I schedule a Databricks job?",
     "Create a Job in Databricks and configure a CRON schedule or file-arrival trigger."),
    ("What is a materialized view in Databricks SQL?",
     "A materialized view pre-computes and caches query results for faster repeated access."),
    ("What is the Databricks Model Serving endpoint?",
     "Model Serving provides a REST endpoint to serve registered MLflow models at production scale."),
    ("How do I clone a Delta table?",
     "Use CREATE TABLE CLONE for a shallow copy or DEEP CLONE to copy both data and metadata."),
    ("What is Z-Ordering in Delta Lake?",
     "Z-Ordering co-locates related data in the same files to reduce the amount of data read by queries."),
    ("How do I use the Databricks SDK?",
     "Import WorkspaceClient from databricks.sdk; it is pre-authenticated inside Databricks notebooks."),
    ("What is Liquid Clustering?",
     "Liquid Clustering is an incremental clustering strategy that replaces static partition schemes in Delta."),
    ("How does Lakeflow handle schema evolution?",
     "Pipelines can automatically evolve the target table schema when new columns appear in the source."),
]

# Evaluation pairs with human similarity scores (1.0 = very similar, 0.0 = unrelated)
eval_pairs = [
    ("Explain ACID properties in Delta Lake",
     "Delta supports atomicity, consistency, isolation, and durability via a transaction log.", 1.0),
    ("How do I track experiments with MLflow?",
     "Use mlflow.start_run() and log_metric() / log_param() to track runs in an experiment.", 1.0),
    ("What is a Databricks widget?",
     "Widgets let you add interactive parameters to notebooks and dashboards.", 0.2),
    ("How do I list files in DBFS?",
     "Use dbutils.fs.ls('/path') to list files in the Databricks File System.", 0.1),
]

train_ds = Dataset.from_dict({
    "anchor":   [p[0] for p in train_pairs],
    "positive": [p[1] for p in train_pairs],
})

print(f"Training pairs : {len(train_ds)}")
print(f"Eval pairs     : {len(eval_pairs)}")
display(pd.DataFrame(train_pairs[:5], columns=["Anchor", "Positive"]))

# COMMAND ----------

# DBTITLE 1,Baseline evaluation
# Measure similarity performance of the base model before any fine-tuning
model = SentenceTransformer(BASE_MODEL)

evaluator = EmbeddingSimilarityEvaluator(
    sentences1=[p[0] for p in eval_pairs],
    sentences2=[p[1] for p in eval_pairs],
    scores    =[p[2] for p in eval_pairs],
    similarity_fn_name=SimilarityFunction.COSINE,
    name="databricks-qa",
)

baseline_spearman = evaluator(model)
print(f"Baseline Spearman correlation: {baseline_spearman:.4f}")

# COMMAND ----------

# DBTITLE 1,Fine-tune with MLflow tracking
# ⚠️  GPU compute strongly recommended — CPU fine-tuning is significantly slower.
# Switch to a GPU-enabled cluster before running this cell.

training_args = SentenceTransformerTrainingArguments(
    output_dir               = "/tmp/bge-fine-tuned",
    num_train_epochs         = 3,
    per_device_train_batch_size = 16,
    warmup_ratio             = 0.1,
    learning_rate            = 2e-5,
    fp16                     = gpu,       # mixed-precision only on GPU
    bf16                     = False,
    eval_strategy            = "epoch",
    save_strategy            = "epoch",
    load_best_model_at_end   = True,
    logging_steps            = 5,
    report_to                = "mlflow",  # auto-logs step metrics to Databricks MLflow
)

loss    = CachedMultipleNegativesRankingLoss(model)
trainer = SentenceTransformerTrainer(
    model         = model,
    args          = training_args,
    train_dataset = train_ds,
    loss          = loss,
    evaluator     = evaluator,
)

with mlflow.start_run(run_name="bge-finetune") as run:
    mlflow.log_params({
        "base_model"  : BASE_MODEL,
        "train_pairs" : len(train_ds),
        "epochs"      : training_args.num_train_epochs,
        "lr"          : training_args.learning_rate,
        "fp16"        : training_args.fp16,
    })
    mlflow.log_metric("baseline_spearman", baseline_spearman)

    trainer.train()

    final_spearman = evaluator(model)
    mlflow.log_metric("final_spearman", final_spearman)
    RUN_ID = run.info.run_id

print(f"Spearman : {baseline_spearman:.4f} → {final_spearman:.4f}")
print(f"MLflow run ID : {RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Register model to Unity Catalog
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

# Ensure the target schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

sig = ModelSignature(
    inputs =Schema([ColSpec("string", "text")]),
    outputs=Schema([ColSpec("double", "embedding")]),
)
example = pd.DataFrame({"text": ["What is Delta Lake?", "Explain MLflow tracking"]})

# mlflow.sentence_transformers is the native MLflow flavor for SentenceTransformer models
with mlflow.start_run(run_id=RUN_ID):
    info = mlflow.sentence_transformers.log_model(
        model                 = model,
        name                  = "fine_tuned_bge",
        signature             = sig,
        input_example         = example,
        registered_model_name = MODEL_NAME,   # catalog.schema.model → Unity Catalog
    )

MODEL_VERSION = info.registered_model_version
print(f"Registered : {MODEL_NAME}  version {MODEL_VERSION}")

# COMMAND ----------

# DBTITLE 1,Deploy endpoint via Databricks SDK
def _wait_for_endpoint(name: str, timeout_s: int = 1800, poll_s: int = 20) -> None:
    """Poll using SDK enums until the endpoint is READY and NOT_UPDATING."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        s = w.serving_endpoints.get(name).state
        if (s.ready == EndpointStateReady.READY and
                s.config_update == EndpointStateConfigUpdate.NOT_UPDATING):
            print(f"\u2713 Endpoint '{name}' is ready.")
            return
        print(f"  … ready={s.ready}  config={s.config_update}")
        time.sleep(poll_s)
    raise TimeoutError(f"Endpoint '{name}' not ready after {timeout_s}s")


endpoint_config = EndpointCoreConfigInput(
    served_entities=[ServedEntityInput(
        entity_name           = MODEL_NAME,
        entity_version        = str(MODEL_VERSION),
        scale_to_zero_enabled = True,
        workload_size         = "Small",
    )],
    auto_capture_config=AutoCaptureConfigInput(
        catalog_name      = CATALOG,
        schema_name       = SCHEMA,
        table_name_prefix = ENDPOINT_NAME,
        enabled           = True,          # logs all requests/responses to a UC table
    ),
)

try:
    w.serving_endpoints.get(ENDPOINT_NAME)
    print(f"Endpoint '{ENDPOINT_NAME}' exists — updating config...")
    w.serving_endpoints.update_config(
        name            = ENDPOINT_NAME,
        served_entities = endpoint_config.served_entities,
    )
except Exception:
    print(f"Creating endpoint '{ENDPOINT_NAME}'...")
    w.serving_endpoints.create(name=ENDPOINT_NAME, config=endpoint_config)

_wait_for_endpoint(ENDPOINT_NAME)

# COMMAND ----------

# DBTITLE 1,Query endpoint via MLflow Deployments client
# mlflow.deployments is the Databricks-native client for querying serving endpoints
from mlflow.deployments import get_deploy_client

deploy_client = get_deploy_client("databricks")

queries = [
    "What is Delta Lake?",
    "How do I fine-tune an embedding model on Databricks?",
]

response = deploy_client.predict(
    endpoint = ENDPOINT_NAME,
    inputs   = {"inputs": queries},
)

for query, embedding in zip(queries, response["predictions"]):
    print(f"Query : {query}")
    print(f"  dim={len(embedding)}  first_5={[round(v, 4) for v in embedding[:5]]}")
    print()