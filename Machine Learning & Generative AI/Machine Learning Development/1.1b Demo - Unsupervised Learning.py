# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Unsupervised Learning
# MAGIC In this demo, we will explore **unsupervised learning**, a method where the model finds patterns in **unlabeled data** without predefined categories. We will use **text embeddings** to convert text into numerical representations and apply **K-Means clustering** to group similar text documents.
# MAGIC
# MAGIC To improve clustering efficiency, we will **reduce the dimensionality** of embeddings using **Principal Component Analysis (PCA)**. We will also use evaluation techniques like the **Elbow Method** and [**Silhouette Score**](https://en.wikipedia.org/wiki/Silhouette_(clustering) to determine the best number of clusters and assess clustering quality.
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC - **Generate text embeddings** using the embeddings model  [General Text Embeddings (GTE)](https://huggingface.co/thenlper/gte-large) to represent text numerically.  
# MAGIC - **Apply dimensionality reduction** (PCA) to optimize clustering performance.  
# MAGIC - **Train an unsupervised K-Means model** to discover patterns in text data.  
# MAGIC - **Determine the optimal number of clusters** using the **Elbow Method**.  
# MAGIC - **Evaluate clustering quality** using **Silhouette Score**.  
# MAGIC - **Visualize clustering results** for better interpretability.

# COMMAND ----------

# MAGIC %pip install --upgrade threadpoolctl scikit-learn
# MAGIC %pip install kneed
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC    - In the drop-down, select **More**.
# MAGIC    - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC   
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run this notebook, you need to use one of the following Databricks runtime(s): **16.3.x-cpu-ml-scala2.12**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script. This script will define configuration variables necessary for the demo. Execute the following cell:

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1.1bUS

# COMMAND ----------

# MAGIC %md
# MAGIC **Other Conventions:**
# MAGIC
# MAGIC Throughout this demo, we'll refer to the object `DA`. This object, provided by Databricks Academy, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"Dataset Location:  {DA.paths.datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data & Generate Embeddings
# MAGIC
# MAGIC Before we can apply **unsupervised learning**, we need to load and process our dataset. In this step, we will:
# MAGIC
# MAGIC - Load the **AG News dataset** from a **Databricks feature table**.
# MAGIC - Extract the **text column** for processing.
# MAGIC - Prepare the data for **embedding generation**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the Dataset
# MAGIC We use the **AG News dataset**, which contains news articles, to perform text clustering. The dataset is stored in a **Databricks feature table**, and we will load it as a **Spark DataFrame**.

# COMMAND ----------

import os
from pyspark.sql.functions import col

# Load AG News dataset as a Spark DataFrame (Feature Table)
table_name = f"{DA.catalog_name}.{DA.schema_name}.ag_news_features"
news_df = spark.read.table(table_name)

# Select only the 'text' column (avoiding unnecessary columns)
news_texts_df = news_df.select(col("text"))

# Display the Spark DataFrame
display(news_texts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Text Embeddings Using gte-large
# MAGIC
# MAGIC Now that we have loaded our text dataset, the next step is to convert the text data into **numerical representations** using **text embeddings**. Here we will demonstrate how easy it is to take our text and embed it using a foundational model from Mosaic AI Model Serving. In particular, we will be using the `get_open_ai_client()` method, which is part of the databricks SDK that provides a convenient way to create an OpenAI-compatible client for interacting with the foundation model. For other methods of querying, please see [this documentation](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving/score-foundation-models). 
# MAGIC
# MAGIC **Steps:**
# MAGIC - Step 1: Initialize OpenAI Client
# MAGIC - Step 2: Define Embedding Function
# MAGIC - Step 3: Convert Text to Embeddings
# MAGIC - Step 4: Convert embeddings list to a Spark DataFrame

# COMMAND ----------

from pyspark.sql.functions import col
from databricks.sdk import WorkspaceClient
# Initialize Databricks OpenAI Client
workspace_client = WorkspaceClient()
openai_client = workspace_client.serving_endpoints.get_open_ai_client()

# Function to get embeddings for a batch of text
def get_embeddings_batch(text):
    response = openai_client.embeddings.create(
        model="databricks-gte-large-en",
        input=text
    )
    return [res.embedding for res in response.data]
    
# Convert DataFrame to list on the driver
news_texts_list = news_texts_df.select("text").rdd.map(lambda row: row["text"]).collect()
# Process in batches to reduce API calls
batch_size = 100  # Adjust as needed based on API rate limits
embeddings_list = []

for i in range(0, len(news_texts_list), batch_size):
    batch = news_texts_list[i:i + batch_size]
    embeddings_list.extend(get_embeddings_batch(batch))

# Create DataFrame with embeddings
embeddings_df = spark.createDataFrame(zip(news_texts_list, embeddings_list), ["text", "embedding"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization and Dimensionality Reduction
# MAGIC
# MAGIC Now that we have generated **text embeddings**, we need to prepare them for clustering by applying **standardization** and **dimensionality reduction**.
# MAGIC
# MAGIC **Why Do We Need This Step?**
# MAGIC - **Standardization** ensures that all features have a similar scale, preventing some features from dominating others.
# MAGIC - **Dimensionality Reduction** using [**Principal Component Analysis (PCA)**](https://en.wikipedia.org/wiki/Principal_component_analysis) helps reduce the number of features while retaining important information. This makes clustering more efficient and easier to visualize. In particular, we will be converting our embedding from 1024 dimensions down to 2 dimensions.

# COMMAND ----------

embeddings_df.createOrReplaceTempView("embeddingsvw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select stddev(len(text)) ,
# MAGIC min(len(text)) ,
# MAGIC max(len(text)) 
# MAGIC , stddev(size(embedding))from embeddingsvw

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct (size(embedding))from embeddingsvw

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import numpy as np

# Convert Spark DataFrame to NumPy array (Extract embeddings)
embeddings_np = np.array([row["embedding"] for row in embeddings_df.select("embedding").collect()])

# Step 1: Standardization
scaler = StandardScaler()
embeddings_scaled = scaler.fit_transform(embeddings_np)

# Step 2: Dimensionality Reduction using PCA
pca = PCA(n_components=2)  # Reduce to 2D for visualization
embeddings_pca = pca.fit_transform(embeddings_scaled)

# Convert back to Spark DataFrame
pca_df = spark.createDataFrame(
    [(int(i), float(pc1), float(pc2)) for i, (pc1, pc2) in enumerate(embeddings_pca)],
    ["unique_id", "PC1", "PC2"]
)

# Display the transformed embeddings
display(pca_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determine the Optimal Number of Clusters (Elbow Method)
# MAGIC
# MAGIC Before applying [K-Means](https://en.wikipedia.org/wiki/K-means_clustering) clustering, we need to determine the **best number of clusters (K)**.  
# MAGIC We use the [**Elbow Method**](https://en.wikipedia.org/wiki/Knee_of_a_curve), which helps identify the point where adding more clusters **no longer significantly reduces inertia (sum of squared distances to cluster centers).**  
# MAGIC
# MAGIC **How Does the Elbow Method Work?**
# MAGIC - We run **K-Means** clustering for different values of K (from 1 to 10).
# MAGIC - We measure **inertia** (how well points fit within their assigned cluster).
# MAGIC - We plot inertia against K and look for the [**elbow point**](https://en.wikipedia.org/wiki/Elbow_method_(clustering) where the decrease in inertia slows down.
# MAGIC - The **optimal K** is found using **KneeLocator**, which detects the elbow point automatically.
# MAGIC
# MAGIC **Why not just minimize inertia?**
# MAGIC - Minimizing inertia can lead to _overfitting_ (continuously decreasing while increasing the number of clusters will fit noise rather than meaningful patterns).
# MAGIC - The elbow method provides interpretability and voids arbitrary decision-making by providing a point of diminishing returns.
# MAGIC
# MAGIC > We manually set the environment variable `OMP_NUM_THREADS` to 1 to avoid multithreading and parallelism to ensure that each run uses the same computational resources. This prevents the creation of too many threads across processes, preventing inefficient CPU utilization.

# COMMAND ----------

import os
import threadpoolctl
import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans
from kneed import KneeLocator 

# Apply fixes for parallel processing
os.environ["OMP_NUM_THREADS"] = "1"
threadpoolctl.threadpool_limits(limits=1, user_api="blas")

# Perform K-Means clustering and compute inertia
inertia = []
k_values = range(1, 10)  # Try values from 1 to 10

for k in k_values:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(embeddings_scaled)  # Ensure embeddings_scaled is preprocessed
    inertia.append(kmeans.inertia_)

# Use KneeLocator to find the elbow point
knee_locator = KneeLocator(k_values, inertia, curve="convex", direction="decreasing")
optimal_k = knee_locator.elbow

# Plot Elbow Method with detected optimal k
plt.figure(figsize=(8,6))
plt.plot(k_values, inertia, marker='o', linestyle='--', label='Inertia')
plt.axvline(x=optimal_k, color='r', linestyle='--', label=f'Optimal K={optimal_k}')
plt.xlabel('Number of Clusters (K)')
plt.ylabel('Inertia')
plt.title('Elbow Method for Optimal K')
plt.legend()
plt.show()

print(f"Optimal number of clusters: {optimal_k}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Clustering Algorithm
# MAGIC
# MAGIC We will now apply **K-Means Clustering** to group similar news articles together based on their embeddings.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. **Train the K-Means model** using the **optimal_k**.
# MAGIC 2. **Assign cluster labels** to each news article.
# MAGIC 3. **Store clustering results** in a Spark DataFrame.

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
from sklearn.cluster import KMeans
import pandas as pd

# Apply K-Means clustering on the reduced embeddings
kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
kmeans.fit(embeddings_scaled)  # Fit the model on the standardized embeddings

# Get cluster labels
labels = kmeans.labels_

# Convert labels to a Spark DataFrame
labels_df = pd.DataFrame({"unique_id": range(len(labels)), "Cluster": labels})
labels_spark_df = spark.createDataFrame(labels_df)

# Join PCA-transformed Spark DataFrame with cluster labels
clusters_spark_df = pca_df.join(labels_spark_df, "unique_id")

# Display the resulting clustered DataFrame
display(clusters_spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Clustering Performance
# MAGIC
# MAGIC Once the **K-Means clustering** is applied, we need to assess how well the clusters are formed. A common metric for this evaluation is the **Silhouette Score**.
# MAGIC
# MAGIC ### Silhouette Score
# MAGIC _The silhouette value measures how well an object fits its assigned cluster compared to other clusters_, ranging from -1 to +1, with higher values indicating better clustering. It provides a metric for evaluating clustering quality, with average scores above 0.5 considered reasonable, though high-dimensional data may yield lower scores due to the curse of dimensionality.

# COMMAND ----------

from sklearn.metrics import silhouette_score

silhouette_avg = silhouette_score(embeddings_scaled, labels)
print(f"Silhouette Score for K-Means with {optimal_k} clusters: {silhouette_avg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Clustering Results
# MAGIC
# MAGIC we will visualize the clusters to gain insights into how the news articles are grouped based on their embeddings. Here we will be using the method ConvexHull to help visualize. This compute the convex hull in N dimensions (here N = 2). This helps us identify the boundary of a set of clusters.

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.decomposition import PCA
from scipy.spatial import ConvexHull

# Convert Spark DataFrame to Pandas
clusters_pd = clusters_spark_df.toPandas()

# Define color palette
num_clusters = clusters_pd["Cluster"].nunique()
colors = sns.color_palette("husl", num_clusters)  # Distinct colors

plt.figure(figsize=(10, 7))

# Scatter plot with better visibility
for cluster, color in zip(range(num_clusters), colors):
    subset = clusters_pd[clusters_pd["Cluster"] == cluster]
    
    plt.scatter(
        subset["PC1"], subset["PC2"],
        label=f"Cluster {cluster}",
        color=color, s=80, alpha=0.6, edgecolors='k'  # Larger points, transparency, black edges
    )

    # Convex Hull for cluster boundary (only if there are enough points)
    if len(subset) > 2:
        hull = ConvexHull(subset[["PC1", "PC2"]])
        for simplex in hull.simplices:
            plt.plot(subset.iloc[simplex]["PC1"], subset.iloc[simplex]["PC2"], color=color, alpha=0.5)

plt.xlabel("Principal Component 1")
plt.ylabel("Principal Component 2")
plt.title("Clustering Visualization of News Articles")
plt.legend()
plt.grid(True)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this demo, we explored the process of **training an unsupervised model** using **K-Means clustering** on **text embeddings**. We generated embeddings with **Databricks foundation models**, standardized the data, and applied **dimensionality reduction (PCA)** to optimize clustering. By using the **Elbow Method** and **Silhouette Score**, we determined the optimal number of clusters and evaluated the quality of our model. This approach helps in discovering hidden patterns in text data, making it a powerful technique for **automated categorization and pattern recognition** in **real-world applications**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
