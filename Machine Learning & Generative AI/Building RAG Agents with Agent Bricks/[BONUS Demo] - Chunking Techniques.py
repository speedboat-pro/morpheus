# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC
# MAGIC # [BONUS Demo] - Chunking Techniques
# MAGIC ## Comparing Chunking Strategies & Embedding Profiles for RAG
# MAGIC
# MAGIC ### Overview
# MAGIC This notebook explores **multiple chunking strategies** beyond Databricks' `ai_prep_search`, using popular open-source Python libraries. We'll apply each strategy to the same source data (from the parsed Airbnb PDFs), generate embeddings, and compare the resulting chunk profiles — size distributions, overlap characteristics, and semantic coherence.
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC 1. Understand the main families of text chunking: fixed-size, recursive, semantic, and document-aware
# MAGIC 2. Implement chunking with LangChain, LlamaIndex, and spaCy
# MAGIC 3. Compare chunk size distributions across strategies
# MAGIC 4. Analyze embedding similarity and diversity profiles for each approach
# MAGIC 5. Understand trade-offs for RAG retrieval quality
# MAGIC
# MAGIC ### Source Data
# MAGIC We'll use the `airbnb_structured_listings` table (from Demo 04) and `house_rules_chunks` table (from Demo 06) as our source text.

# COMMAND ----------

# DBTITLE 1,Install chunking libraries
# MAGIC %pip install langchain langchain-text-splitters llama-index-core sentence-transformers spacy tiktoken numpy --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Classroom Setup
# MAGIC %run ./Includes/Classroom-Setup-04

# COMMAND ----------

# DBTITLE 1,Load source text data
# Load text content from the structured listings table created in Demo 04
source_df = spark.sql(f"""
  SELECT 
    content,
    category::STRING AS category,
    property_name::STRING AS property_name
  FROM {catalog_name}.{schema_name}.airbnb_structured_listings
  WHERE content IS NOT NULL AND LENGTH(content) > 100
""")

# Collect text for Python-based chunking
documents = [row['content'] for row in source_df.collect()]
print(f"Loaded {len(documents)} text blocks for chunking")
print(f"Total characters: {sum(len(d) for d in documents):,}")
print(f"Average length: {sum(len(d) for d in documents) // len(documents):,} chars")

# COMMAND ----------

# DBTITLE 1,Section A Header
# MAGIC %md
# MAGIC ## A. Chunking Strategy 1: Fixed-Size Chunking (Baseline)
# MAGIC
# MAGIC The simplest approach: split text into chunks of a fixed character count with optional overlap. This is fast and predictable but ignores semantic boundaries.
# MAGIC
# MAGIC **Pros:** Deterministic, fast, uniform chunk sizes for embedding models with token limits  
# MAGIC **Cons:** Splits mid-sentence, mid-paragraph, or mid-word; loses context at boundaries  
# MAGIC **Best for:** Structured text, log files, or when you need guaranteed chunk sizes

# COMMAND ----------

# DBTITLE 1,Fixed-size chunking
from langchain_text_splitters import CharacterTextSplitter

# Fixed-size chunking with overlap
fixed_splitter = CharacterTextSplitter(
    separator="",  # Split on any character boundary
    chunk_size=300,
    chunk_overlap=50,
    length_function=len,
)

fixed_chunks = []
for doc in documents:
    chunks = fixed_splitter.split_text(doc)
    fixed_chunks.extend(chunks)

print(f"Fixed-size chunking (300 chars, 50 overlap):")
print(f"  Total chunks: {len(fixed_chunks)}")
print(f"  Avg chunk size: {sum(len(c) for c in fixed_chunks) / len(fixed_chunks):.0f} chars")
print(f"  Min: {min(len(c) for c in fixed_chunks)} | Max: {max(len(c) for c in fixed_chunks)}")
print(f"\nSample chunk:")
print(f"  '{fixed_chunks[0][:150]}...'")

# COMMAND ----------

# DBTITLE 1,Section B Header
# MAGIC %md
# MAGIC ## B. Chunking Strategy 2: Recursive Character Splitting
# MAGIC
# MAGIC LangChain's `RecursiveCharacterTextSplitter` tries a hierarchy of separators (`\n\n`, `\n`, ` `, `""`) to split at the most meaningful boundary first. It preserves paragraph and sentence structure better than fixed-size.
# MAGIC
# MAGIC **Pros:** Respects natural text boundaries (paragraphs > sentences > words)  
# MAGIC **Cons:** Chunk sizes vary more; still rule-based, not semantic  
# MAGIC **Best for:** General-purpose RAG on well-formatted documents

# COMMAND ----------

# DBTITLE 1,Recursive character splitting
from langchain_text_splitters import RecursiveCharacterTextSplitter

# Recursive splitting respects paragraph/sentence boundaries
recursive_splitter = RecursiveCharacterTextSplitter(
    chunk_size=300,
    chunk_overlap=50,
    separators=["\n\n", "\n", ". ", " ", ""],
    length_function=len,
)

recursive_chunks = []
for doc in documents:
    chunks = recursive_splitter.split_text(doc)
    recursive_chunks.extend(chunks)

print(f"Recursive character splitting (300 chars, 50 overlap):")
print(f"  Total chunks: {len(recursive_chunks)}")
print(f"  Avg chunk size: {sum(len(c) for c in recursive_chunks) / len(recursive_chunks):.0f} chars")
print(f"  Min: {min(len(c) for c in recursive_chunks)} | Max: {max(len(c) for c in recursive_chunks)}")
print(f"\nSample chunk (note cleaner boundaries):")
print(f"  '{recursive_chunks[0][:150]}...'")

# COMMAND ----------

# DBTITLE 1,Section C Header
# MAGIC %md
# MAGIC ## C. Chunking Strategy 3: Token-Based Splitting
# MAGIC
# MAGIC Embedding models have **token** limits, not character limits. Token-based splitting ensures each chunk fits within the model's context window. This uses `tiktoken` (OpenAI's tokenizer) or similar.
# MAGIC
# MAGIC **Pros:** Guarantees chunks fit model token limits; more accurate for LLM processing  
# MAGIC **Cons:** Tokenizer-specific; still splits at arbitrary token boundaries  
# MAGIC **Best for:** When you need precise control over tokens sent to embedding/LLM models

# COMMAND ----------

# DBTITLE 1,Token-based splitting
from langchain_text_splitters import TokenTextSplitter

# Token-based splitting using tiktoken (cl100k_base encoding)
token_splitter = TokenTextSplitter(
    encoding_name="cl100k_base",  # GPT-4/text-embedding tokenizer
    chunk_size=100,  # 100 tokens per chunk
    chunk_overlap=20,
)

token_chunks = []
for doc in documents:
    chunks = token_splitter.split_text(doc)
    token_chunks.extend(chunks)

print(f"Token-based splitting (100 tokens, 20 overlap):")
print(f"  Total chunks: {len(token_chunks)}")
print(f"  Avg chunk size: {sum(len(c) for c in token_chunks) / len(token_chunks):.0f} chars")
print(f"  Min: {min(len(c) for c in token_chunks)} | Max: {max(len(c) for c in token_chunks)}")

# Show token count verification
import tiktoken
enc = tiktoken.get_encoding("cl100k_base")
sample_token_counts = [len(enc.encode(c)) for c in token_chunks[:10]]
print(f"\nToken counts for first 10 chunks: {sample_token_counts}")
print(f"  (All should be <= 100 tokens)")

# COMMAND ----------

# DBTITLE 1,Section D Header
# MAGIC %md
# MAGIC ## D. Chunking Strategy 4: Sentence-Based Splitting with spaCy
# MAGIC
# MAGIC Using NLP sentence detection ensures chunks never break mid-sentence. spaCy's sentence segmentation uses a trained model for accurate boundary detection.
# MAGIC
# MAGIC **Pros:** Never splits mid-sentence; linguistically informed boundaries  
# MAGIC **Cons:** Sentence lengths vary widely; may produce very small or very large chunks  
# MAGIC **Best for:** Q&A systems where complete sentences provide better context

# COMMAND ----------

# DBTITLE 1,Sentence-based splitting with spaCy
import spacy

# Load spaCy's English model for sentence segmentation
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    import subprocess
    subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"], check=True)
    nlp = spacy.load("en_core_web_sm")

# Increase max length for longer documents
nlp.max_length = 1_500_000

def sentence_chunk(text, max_chunk_size=300, overlap_sentences=1):
    """Group sentences into chunks up to max_chunk_size chars with sentence overlap."""
    doc = nlp(text)
    sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]
    
    chunks = []
    current_chunk = []
    current_length = 0
    
    for i, sent in enumerate(sentences):
        if current_length + len(sent) > max_chunk_size and current_chunk:
            chunks.append(" ".join(current_chunk))
            # Keep overlap_sentences for context continuity
            current_chunk = current_chunk[-overlap_sentences:] if overlap_sentences > 0 else []
            current_length = sum(len(s) for s in current_chunk)
        current_chunk.append(sent)
        current_length += len(sent)
    
    if current_chunk:
        chunks.append(" ".join(current_chunk))
    
    return chunks

# Apply sentence-based chunking
sentence_chunks = []
for doc in documents:
    chunks = sentence_chunk(doc, max_chunk_size=300, overlap_sentences=1)
    sentence_chunks.extend(chunks)

print(f"Sentence-based chunking (spaCy, 300 char target, 1 sentence overlap):")
print(f"  Total chunks: {len(sentence_chunks)}")
print(f"  Avg chunk size: {sum(len(c) for c in sentence_chunks) / len(sentence_chunks):.0f} chars")
print(f"  Min: {min(len(c) for c in sentence_chunks)} | Max: {max(len(c) for c in sentence_chunks)}")
print(f"\nSample chunk:")
print(f"  '{sentence_chunks[0][:200]}...'")

# COMMAND ----------

# DBTITLE 1,Section E Header
# MAGIC %md
# MAGIC ## E. Chunking Strategy 5: Semantic Splitting (LlamaIndex)
# MAGIC
# MAGIC Semantic chunking uses embedding similarity to decide where to split. Adjacent sentences with high similarity stay together; a significant drop in similarity triggers a split boundary. This is the most sophisticated approach.
# MAGIC
# MAGIC **Pros:** Chunks are semantically coherent; each chunk is "about one thing"  
# MAGIC **Cons:** Slower (requires embedding computation); chunk sizes highly variable  
# MAGIC **Best for:** Complex documents where topics shift within paragraphs; high-quality RAG

# COMMAND ----------

# DBTITLE 1,Semantic splitting with sentence-transformers
from sentence_transformers import SentenceTransformer
import numpy as np

# Load a lightweight embedding model for semantic similarity
embed_model = SentenceTransformer("all-MiniLM-L6-v2")

def semantic_chunk(text, similarity_threshold=0.75, max_chunk_size=500):
    """Split text into semantically coherent chunks based on embedding similarity."""
    # Split into sentences first
    doc = nlp(text)
    sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]
    
    if len(sentences) <= 1:
        return [text]
    
    # Compute embeddings for all sentences
    embeddings = embed_model.encode(sentences)
    
    # Find split points where similarity drops
    chunks = []
    current_chunk = [sentences[0]]
    
    for i in range(1, len(sentences)):
        # Cosine similarity between consecutive sentences
        sim = np.dot(embeddings[i-1], embeddings[i]) / (
            np.linalg.norm(embeddings[i-1]) * np.linalg.norm(embeddings[i])
        )
        
        current_text = " ".join(current_chunk)
        
        # Split if similarity drops below threshold or chunk is too large
        if sim < similarity_threshold or len(current_text) + len(sentences[i]) > max_chunk_size:
            chunks.append(current_text)
            current_chunk = [sentences[i]]
        else:
            current_chunk.append(sentences[i])
    
    if current_chunk:
        chunks.append(" ".join(current_chunk))
    
    return chunks

# Apply semantic chunking (use a subset for speed)
semantic_chunks = []
for doc_text in documents[:15]:  # Limit for demo speed
    chunks = semantic_chunk(doc_text, similarity_threshold=0.65, max_chunk_size=400)
    semantic_chunks.extend(chunks)

print(f"Semantic chunking (similarity threshold=0.65, max 400 chars):")
print(f"  Documents processed: {min(15, len(documents))}")
print(f"  Total chunks: {len(semantic_chunks)}")
print(f"  Avg chunk size: {sum(len(c) for c in semantic_chunks) / max(len(semantic_chunks), 1):.0f} chars")
if semantic_chunks:
    print(f"  Min: {min(len(c) for c in semantic_chunks)} | Max: {max(len(c) for c in semantic_chunks)}")
    print(f"\nSample semantic chunk:")
    print(f"  '{semantic_chunks[0][:200]}...'")

# COMMAND ----------

# DBTITLE 1,Section F Header
# MAGIC %md
# MAGIC ## F. Chunking Strategy 6: Databricks `ai_prep_search` (Reference)
# MAGIC
# MAGIC For comparison, let's also load the chunks produced by Databricks' built-in `ai_prep_search` from Demo 06's `house_rules_chunks` table. This uses AI-powered semantic boundaries optimized for vector search.
# MAGIC
# MAGIC **Pros:** Production-ready, optimized for Databricks AI Search, generates both `chunk_to_embed` and `chunk_to_retrieve`  
# MAGIC **Cons:** Databricks-specific; less control over parameters  
# MAGIC **Best for:** Production RAG on Databricks with AI Search integration

# COMMAND ----------

# DBTITLE 1,Load Databricks ai_prep_search chunks
# Load the chunks from ai_prep_search (Demo 06)
try:
    db_chunks_df = spark.sql(f"""
      SELECT chunk_text, retrieval_text 
      FROM {catalog_name}.{schema_name}.house_rules_chunks
    """)
    db_chunks = [row['chunk_text'] for row in db_chunks_df.collect()]
    print(f"Databricks ai_prep_search chunks:")
    print(f"  Total chunks: {len(db_chunks)}")
    print(f"  Avg chunk size: {sum(len(c) for c in db_chunks) / max(len(db_chunks), 1):.0f} chars")
    if db_chunks:
        print(f"  Min: {min(len(c) for c in db_chunks)} | Max: {max(len(c) for c in db_chunks)}")
except Exception as e:
    print(f"Note: house_rules_chunks table not available ({e})")
    print("Run Demo 06 first to generate this data.")
    db_chunks = []

# COMMAND ----------

# DBTITLE 1,Section G Header
# MAGIC %md
# MAGIC ## G. Comparative Analysis: Chunk Size Distributions
# MAGIC
# MAGIC Now let's visualize and compare the chunk profiles across all strategies. Understanding the distribution helps you choose the right approach for your embedding model and retrieval needs.

# COMMAND ----------

# DBTITLE 1,Compare chunk distributions
import pandas as pd
import numpy as np

# Collect statistics for all strategies
strategies = {
    "Fixed-Size (300c)": fixed_chunks,
    "Recursive (300c)": recursive_chunks,
    "Token-Based (100t)": token_chunks,
    "Sentence (spaCy)": sentence_chunks,
    "Semantic (embed)": semantic_chunks,
}

if db_chunks:
    strategies["ai_prep_search"] = db_chunks

# Build comparison DataFrame
comparison_data = []
for name, chunks in strategies.items():
    if not chunks:
        continue
    sizes = [len(c) for c in chunks]
    comparison_data.append({
        "Strategy": name,
        "Num Chunks": len(chunks),
        "Mean Size (chars)": int(np.mean(sizes)),
        "Median Size (chars)": int(np.median(sizes)),
        "Std Dev": int(np.std(sizes)),
        "Min Size": min(sizes),
        "Max Size": max(sizes),
        "P25": int(np.percentile(sizes, 25)),
        "P75": int(np.percentile(sizes, 75)),
        "CV (variability)": round(np.std(sizes) / np.mean(sizes), 2),
    })

comparison_df = pd.DataFrame(comparison_data)
display(comparison_df)

# COMMAND ----------

# DBTITLE 1,Visualize distributions
import matplotlib.pyplot as plt

fig, axes = plt.subplots(2, 3, figsize=(15, 8))
fig.suptitle("Chunk Size Distributions by Strategy", fontsize=14, fontweight='bold')

colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']

for idx, (name, chunks) in enumerate(strategies.items()):
    if not chunks or idx >= 6:
        continue
    ax = axes[idx // 3, idx % 3]
    sizes = [len(c) for c in chunks]
    ax.hist(sizes, bins=30, color=colors[idx], alpha=0.7, edgecolor='black', linewidth=0.5)
    ax.axvline(np.mean(sizes), color='red', linestyle='--', linewidth=1.5, label=f'Mean: {np.mean(sizes):.0f}')
    ax.axvline(np.median(sizes), color='green', linestyle=':', linewidth=1.5, label=f'Median: {np.median(sizes):.0f}')
    ax.set_title(name, fontsize=11)
    ax.set_xlabel('Chunk size (chars)')
    ax.set_ylabel('Count')
    ax.legend(fontsize=8)

# Hide unused subplots
for idx in range(len(strategies), 6):
    axes[idx // 3, idx % 3].set_visible(False)

plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Section H Header
# MAGIC %md
# MAGIC ## H. Embedding Profile Analysis
# MAGIC
# MAGIC Now let's generate embeddings for chunks from each strategy and analyze their profiles:
# MAGIC - **Intra-strategy similarity**: How similar are chunks within the same strategy? (Higher = more redundancy)
# MAGIC - **Embedding spread**: How diverse are the vectors? (Higher diversity = better coverage of the semantic space)
# MAGIC - **Nearest-neighbor distances**: How close are the closest chunks? (Relevant for retrieval recall)

# COMMAND ----------

# DBTITLE 1,Generate embeddings for comparison
from sentence_transformers import SentenceTransformer
import numpy as np

# Use a compact model for fast embedding comparison
model = SentenceTransformer("all-MiniLM-L6-v2")

# Sample chunks from each strategy (limit for speed)
SAMPLE_SIZE = 50

def sample_chunks(chunks, n=SAMPLE_SIZE):
    """Randomly sample n chunks."""
    if len(chunks) <= n:
        return chunks
    indices = np.random.choice(len(chunks), n, replace=False)
    return [chunks[i] for i in indices]

np.random.seed(42)
sampled_strategies = {}
for name, chunks in strategies.items():
    if chunks:
        sampled = sample_chunks(chunks)
        sampled_strategies[name] = {
            'chunks': sampled,
            'embeddings': model.encode(sampled)
        }

print(f"Generated embeddings for {len(sampled_strategies)} strategies")
print(f"Embedding dimension: {list(sampled_strategies.values())[0]['embeddings'].shape[1]}")

# COMMAND ----------

# DBTITLE 1,Compute embedding metrics
from numpy.linalg import norm

def cosine_similarity_matrix(embeddings):
    """Compute pairwise cosine similarity matrix."""
    norms = norm(embeddings, axis=1, keepdims=True)
    normalized = embeddings / norms
    return normalized @ normalized.T

def compute_embedding_metrics(embeddings):
    """Compute key metrics about an embedding set."""
    sim_matrix = cosine_similarity_matrix(embeddings)
    n = len(embeddings)
    
    # Get upper triangle (exclude self-similarity)
    upper_indices = np.triu_indices(n, k=1)
    pairwise_sims = sim_matrix[upper_indices]
    
    # Nearest neighbor distances (for each chunk, how close is the nearest other?)
    np.fill_diagonal(sim_matrix, -1)  # Exclude self
    nn_sims = np.max(sim_matrix, axis=1)
    
    return {
        'mean_pairwise_sim': float(np.mean(pairwise_sims)),
        'median_pairwise_sim': float(np.median(pairwise_sims)),
        'std_pairwise_sim': float(np.std(pairwise_sims)),
        'mean_nn_similarity': float(np.mean(nn_sims)),
        'max_nn_similarity': float(np.max(nn_sims)),
        'min_nn_similarity': float(np.min(nn_sims)),
        'embedding_spread': float(np.std(embeddings)),  # Overall vector spread
        'high_sim_pairs_pct': float(np.mean(pairwise_sims > 0.8) * 100),  # % of pairs with >0.8 sim
    }

# Compute metrics for each strategy
embedding_metrics = []
for name, data in sampled_strategies.items():
    metrics = compute_embedding_metrics(data['embeddings'])
    metrics['Strategy'] = name
    embedding_metrics.append(metrics)

metrics_df = pd.DataFrame(embedding_metrics)[[
    'Strategy', 'mean_pairwise_sim', 'median_pairwise_sim', 'std_pairwise_sim',
    'mean_nn_similarity', 'embedding_spread', 'high_sim_pairs_pct'
]]
metrics_df.columns = [
    'Strategy', 'Mean Sim', 'Median Sim', 'Sim Std Dev',
    'Mean NN Sim', 'Embedding Spread', '% Pairs > 0.8 Sim'
]
metrics_df = metrics_df.round(3)
display(metrics_df)

# COMMAND ----------

# DBTITLE 1,Visualize embedding profiles
import matplotlib.pyplot as plt
import numpy as np

fig, axes = plt.subplots(1, 3, figsize=(16, 5))
fig.suptitle("Embedding Profile Comparison Across Chunking Strategies", fontsize=13, fontweight='bold')

strategy_names = list(sampled_strategies.keys())
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']

# Plot 1: Pairwise similarity distributions
ax1 = axes[0]
for idx, name in enumerate(strategy_names):
    embeddings = sampled_strategies[name]['embeddings']
    sim_matrix = cosine_similarity_matrix(embeddings)
    upper_indices = np.triu_indices(len(embeddings), k=1)
    pairwise_sims = sim_matrix[upper_indices]
    ax1.hist(pairwise_sims, bins=30, alpha=0.5, label=name, color=colors[idx % len(colors)])

ax1.set_xlabel('Cosine Similarity')
ax1.set_ylabel('Frequency')
ax1.set_title('Pairwise Similarity Distribution')
ax1.legend(fontsize=7)

# Plot 2: Mean similarity and spread comparison
ax2 = axes[1]
names_short = [n.split('(')[0].strip() for n in strategy_names]
mean_sims = [compute_embedding_metrics(sampled_strategies[n]['embeddings'])['mean_pairwise_sim'] for n in strategy_names]
spreads = [compute_embedding_metrics(sampled_strategies[n]['embeddings'])['embedding_spread'] for n in strategy_names]

x = np.arange(len(strategy_names))
width = 0.35
bars1 = ax2.bar(x - width/2, mean_sims, width, label='Mean Pairwise Sim', color='steelblue')
bars2 = ax2.bar(x + width/2, spreads, width, label='Embedding Spread', color='coral')
ax2.set_xticks(x)
ax2.set_xticklabels(names_short, rotation=45, ha='right', fontsize=8)
ax2.set_ylabel('Score')
ax2.set_title('Similarity vs. Spread')
ax2.legend()

# Plot 3: Nearest neighbor similarity
ax3 = axes[2]
nn_means = [compute_embedding_metrics(sampled_strategies[n]['embeddings'])['mean_nn_similarity'] for n in strategy_names]
nn_maxes = [compute_embedding_metrics(sampled_strategies[n]['embeddings'])['max_nn_similarity'] for n in strategy_names]

ax3.barh(names_short, nn_means, color='steelblue', alpha=0.7, label='Mean NN Sim')
ax3.barh(names_short, nn_maxes, color='coral', alpha=0.4, label='Max NN Sim')
ax3.set_xlabel('Cosine Similarity')
ax3.set_title('Nearest Neighbor Similarity')
ax3.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Section I Header
# MAGIC %md
# MAGIC ## I. Retrieval Quality Simulation
# MAGIC
# MAGIC Let's simulate a retrieval scenario: given a query, how do different chunking strategies perform at surfacing relevant content? We'll measure **top-k retrieval** overlap and **relevance score distributions**.

# COMMAND ----------

# DBTITLE 1,Retrieval simulation
# Simulate retrieval: embed a query and find top-k chunks from each strategy
query = "What amenities are available at the property?"
query_embedding = model.encode([query])[0]

def retrieve_top_k(query_emb, chunk_embeddings, chunks, k=5):
    """Retrieve top-k chunks by cosine similarity to query."""
    sims = np.dot(chunk_embeddings, query_emb) / (
        norm(chunk_embeddings, axis=1) * norm(query_emb)
    )
    top_indices = np.argsort(sims)[::-1][:k]
    return [(chunks[i], float(sims[i])) for i in top_indices]

print(f"Query: '{query}'\n")
print("=" * 80)

for name, data in sampled_strategies.items():
    results = retrieve_top_k(query_embedding, data['embeddings'], data['chunks'], k=3)
    print(f"\n--- {name} ---")
    for i, (chunk, score) in enumerate(results, 1):
        preview = chunk[:100].replace('\n', ' ')
        print(f"  [{i}] Score: {score:.3f} | '{preview}...'")

# COMMAND ----------

# DBTITLE 1,Cross-strategy retrieval heatmap
# Compare retrieval scores across strategies for multiple queries
test_queries = [
    "What amenities are available at the property?",
    "Where is the listing located?",
    "What is the price per night?",
    "Tell me about the host",
    "What are the house rules?",
]

# Compute mean top-3 retrieval scores per strategy per query
heatmap_data = []
for query in test_queries:
    q_emb = model.encode([query])[0]
    row = {'Query': query[:40]}
    for name, data in sampled_strategies.items():
        results = retrieve_top_k(q_emb, data['embeddings'], data['chunks'], k=3)
        mean_score = np.mean([score for _, score in results])
        row[name] = round(mean_score, 3)
    heatmap_data.append(row)

heatmap_df = pd.DataFrame(heatmap_data)
display(heatmap_df)

# COMMAND ----------

# DBTITLE 1,Retrieval heatmap visualization
import matplotlib.pyplot as plt
import numpy as np

# Create a heatmap of retrieval scores
fig, ax = plt.subplots(figsize=(12, 5))

# Extract numeric data
query_labels = heatmap_df['Query'].tolist()
strategy_cols = [c for c in heatmap_df.columns if c != 'Query']
data_matrix = heatmap_df[strategy_cols].values

im = ax.imshow(data_matrix, cmap='YlOrRd', aspect='auto', vmin=0.1, vmax=0.7)

# Labels
ax.set_xticks(range(len(strategy_cols)))
ax.set_xticklabels([s.split('(')[0].strip() for s in strategy_cols], rotation=45, ha='right')
ax.set_yticks(range(len(query_labels)))
ax.set_yticklabels(query_labels)

# Annotate cells
for i in range(len(query_labels)):
    for j in range(len(strategy_cols)):
        ax.text(j, i, f"{data_matrix[i, j]:.2f}", ha='center', va='center', fontsize=9)

ax.set_title("Mean Top-3 Retrieval Score by Strategy and Query", fontsize=12, fontweight='bold')
plt.colorbar(im, ax=ax, label='Cosine Similarity')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Section J Header
# MAGIC %md
# MAGIC ## J. Strategy Selection Guide
# MAGIC
# MAGIC | Strategy | Best Use Case | Chunk Uniformity | Semantic Coherence | Speed | RAG Quality |
# MAGIC | --- | --- | --- | --- | --- | --- |
# MAGIC | Fixed-Size | Logs, structured text, baseline | High | Low | Fastest | Baseline |
# MAGIC | Recursive | General documents, well-formatted text | Medium | Medium | Fast | Good |
# MAGIC | Token-Based | Strict model token limits | High | Low-Medium | Fast | Good |
# MAGIC | Sentence (spaCy) | Q&A, conversational retrieval | Low | Medium-High | Medium | Good |
# MAGIC | Semantic (embeddings) | Complex docs, topic shifts | Low | High | Slow | Best |
# MAGIC | ai_prep_search | Production Databricks RAG | Medium | High | Medium | Best |
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Fixed-size** is your baseline — fast and predictable, but splits break context
# MAGIC 2. **Recursive** is the best general-purpose choice for most documents
# MAGIC 3. **Token-based** is essential when you have strict model input limits
# MAGIC 4. **Sentence-based** shines for Q&A where complete sentences matter
# MAGIC 5. **Semantic** produces the most coherent chunks but is computationally expensive
# MAGIC 6. **`ai_prep_search`** combines semantic intelligence with production readiness on Databricks
# MAGIC
# MAGIC ### Recommendations
# MAGIC - Start with **Recursive** as your default
# MAGIC - Use **Semantic** for high-value documents where retrieval quality justifies compute cost
# MAGIC - Use **`ai_prep_search`** for production Databricks deployments with AI Search
# MAGIC - Always **benchmark** with your actual queries — the "best" strategy depends on your data and questions

# COMMAND ----------

# DBTITLE 1,Conclusion
# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this bonus demo, you explored:
# MAGIC
# MAGIC 1. **Six chunking strategies** — from simple fixed-size to AI-powered semantic splitting
# MAGIC 2. **Implementation details** with LangChain, spaCy, sentence-transformers, and tiktoken
# MAGIC 3. **Quantitative comparison** of chunk size distributions and variability
# MAGIC 4. **Embedding profile analysis** — measuring intra-strategy similarity, spread, and nearest-neighbor distances
# MAGIC 5. **Retrieval simulation** — how each strategy performs at surfacing relevant content for different queries
# MAGIC 6. **Selection guidance** for choosing the right strategy based on your use case
# MAGIC
# MAGIC The key insight: there's no single "best" chunking strategy. The right choice depends on your document types, query patterns, latency requirements, and quality targets. Always benchmark with representative queries against your actual corpus.
