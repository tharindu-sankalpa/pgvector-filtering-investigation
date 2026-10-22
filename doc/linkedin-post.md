Post text for the seven-image LinkedIn post. Replace `<ARTICLE_URL>` before posting.

---

I asked PostgreSQL for 41 nearest neighbours instead of 40, and the same filtered vector query went from 1.5 ms to 358 ms.

The setup: PostgreSQL 18.6 with pgvector 0.8.7 on Amazon EKS, run by CloudNativePG, with 2.5 million 1,536-dimension embeddings and a benchmark covering vector, filtered and hybrid search at 144 combinations of concurrency and result size.

Filtered search had a cliff. Past LIMIT 40, PostgreSQL stopped using the HNSW vector index and computed the distance to every row of the book instead. Under load, most of those queries never finished.

The cause was not pgvector. The planner was guessing. The filter was on a JSONB field with an expression index, and PostgreSQL only collects statistics for an indexed expression when ANALYZE runs after the index exists. An ordinary load (insert, create indexes, serve) never does that, so the planner assumed 0.5% of the table matched every book: 12,500 rows instead of 63,945.

One ANALYZE fixed it. The planner kept the vector index at every LIMIT up to 200, and all 629,712 benchmark queries completed.

Two problems survive the fix. Without iterative index scans, filtered queries can quietly return fewer rows than requested (18 of 40 here). And hybrid search stays about 3x slower than vector search, because PostgreSQL's full-text index finds matching rows but cannot rank them.

The images walk through the setup, the trap, the cause and the fix. The article has every query plan and why the usual workarounds don't help; the repository has the EKS setup guide, manifests, benchmark code and raw results.

Article: <ARTICLE_URL>
Repo: github.com/tharindu-sankalpa/pgvector-filtering-investigation

#PostgreSQL #pgvector #Kubernetes #AmazonEKS #VectorSearch #DatabasePerformance
