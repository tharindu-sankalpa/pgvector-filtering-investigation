# Dataset Guide

This guide describes the two Parquet datasets used by the benchmark framework, the PostgreSQL table schema they map to, and how to bring your own dataset.

## About the Source Data

The _Wheel of Time_ (WoT) is a high fantasy book series by Robert Jordan (and later Brandon Sanderson), spanning **14 novels plus a prequel** — over 4.4 million words in total. I regularly use this corpus for my RAG, vector database, and generative AI experiments because of its scale and richly interconnected world-building: it produces a large number of semantically dense text chunks with natural metadata partitioning (by book, by chapter) and a wide variety of searchable concepts — characters, locations, events, and magic system terminology.

The benchmark uses two Parquet datasets: a **vector dataset** containing the text chunk embeddings, and a **query dataset** containing the retrieval test queries. Both are stored in Azure Blob Storage and mounted into benchmark jobs via a Kubernetes PersistentVolumeClaim.

## Vector Dataset Schema

The base dataset contains **100,105 real text chunk embeddings** generated using OpenAI's `text-embedding-ada-002` model (1,536 dimensions). To test pgvector at production scale, this was synthetically expanded to **2.5 million vectors** by duplicating text chunks with varied metadata combinations and generating synthetic embeddings, preserving the original data distribution characteristics while reaching a dataset size that stresses PostgreSQL's query planner and buffer cache. Each row in the Parquet file has the following columns:

| Column           | Type                       | Description                                                        |
| ---------------- | -------------------------- | ------------------------------------------------------------------ |
| `text`           | `string`                   | The original text chunk from the book                              |
| `embedding`      | `list<float>` (1,536 dims) | OpenAI Ada-002 embedding of the text chunk                         |
| `book_name`      | `string`                   | Name of the book (e.g., `"00. New Spring"`, `"06. Lord of Chaos"`) |
| `chapter_number` | `int` / `string`           | Chapter number (`"N/A"` for prologues/epilogues)                   |
| `chapter_title`  | `string`                   | Title of the chapter                                               |

### PostgreSQL Table Schema

On insert, each row is loaded into a PostgreSQL table with the following schema:

```sql
CREATE TABLE wot_chunks_2_5m (
    id          bigserial PRIMARY KEY,
    content     text,                    -- from 'text' column
    metadata    jsonb,                   -- {"book_name": "...", "chapter_number": ..., "chapter_title": "..."}
    embedding   vector(1536)             -- from 'embedding' column
);
```

The `book_name` field serves as the metadata filter for filtered search benchmarks. It has **16 distinct values** (one per book), with the smallest book (`"00. New Spring"`) containing 63,945 rows (2.6% of total) and the largest (`"06. Lord of Chaos"`) containing 209,226 rows (8.4%). This uneven distribution is intentional — it tests the query planner's behavior across different filter selectivities.

## Query Dataset Schema

The query dataset contains **4,373 retrieval test queries** — real questions about the Wheel of Time series, each pre-processed to support all three search patterns (vector, filtered, and hybrid) from a single row:

| Column            | Type                       | Description                                                                         |
| ----------------- | -------------------------- | ----------------------------------------------------------------------------------- |
| `query_text`      | `string`                   | The natural language question (e.g., `"What is the Dragon Reborn's true name?"`)    |
| `query_embedding` | `list<float>` (1,536 dims) | OpenAI Ada-002 embedding of the question                                            |
| `keywords`        | `list<string>`             | Extracted keywords for hybrid search (e.g., `["Dragon", "Reborn", "true", "name"]`) |
| `filter_field`    | `string`                   | Metadata field to filter on (always `"book_name"` in this dataset)                  |
| `filter_value`    | `string`                   | The specific book to filter by (e.g., `"01. The Eye of the World"`)                 |

Each query row is used across all three benchmark search patterns:

- **Vector search**: Uses `query_embedding` to find nearest neighbors by cosine distance
- **Filtered search**: Uses `query_embedding` + `filter_field`/`filter_value` to find nearest neighbors within a specific book
- **Hybrid search**: Uses `query_embedding` + `keywords` (joined as a space-separated string) for combined vector + full-text RRF scoring

## Bringing Your Own Dataset

The benchmark framework is **dataset-agnostic**. You can test your own vectors and query patterns by providing two Parquet files that conform to the schemas above, and a YAML configuration file that maps your column names to the framework's expected fields:

```yaml
# data/your_dataset.yaml
name: your_dataset
data_file: ../data/your_vectors.parquet
query_file: ../data/your_queries.parquet
embedding_column: embedding # column containing vector embeddings
text_column: text # column containing text for full-text search
filter_columns: # columns available for filtered search
  - your_filter_field
query_embedding_column: query_embedding
query_text_column: query_text
metadata:
  embedding_model: text-embedding-ada-002
  embedding_dimensions: 1536
```

Set the `DATASET_CONFIG` environment variable to point to your YAML file when running the benchmarks. The insert, index creation, and retrieval scripts will automatically adapt to your schema.

For the reference WoT dataset configuration, see [`data/wot_dataset.yaml`](../data/wot_dataset.yaml).
