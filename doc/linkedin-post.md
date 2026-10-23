Post text for the ten-image LinkedIn post. Replace `<ARTICLE_URL>` before posting.

---

I ran PostgreSQL with pgvector as a production-style vector database on Amazon EKS and benchmarked it the way it would be used: 2.5 million 1,536-dimension embeddings, vector, filtered and hybrid search, 1 to 100 concurrent clients, six result sizes, 629,712 queries per run. Three performance traps showed up. Two have fixes.

The setup: PostgreSQL 18.6 with pgvector 0.8.7, run by CloudNativePG on a dedicated 16 vCPU node, with the benchmark Jobs, a results database and Prometheus beside it in the same cluster.

What that node delivered once the fixes were in: at 32 concurrent clients and top_k 10, 3,166 queries per second for vector search with a p95 of 8.5 ms, 1,700 QPS for filtered search and 471 QPS for hybrid search. Every query completed.

Trap 1 was a cliff. I asked for 41 nearest neighbours instead of 40 and the same filtered query went from 1.5 ms to 358 ms. Under load, 85% of the filtered queries at top_k 50 and 100 never finished. The cause was not pgvector. The planner was guessing: the filter was on a JSONB field with an expression index, and PostgreSQL only collects statistics for an indexed expression when ANALYZE runs after the index exists. An ordinary load (insert, create indexes, serve) never does that, so the planner assumed 0.5% of the table matched every book, 12,500 rows instead of 63,945. One ANALYZE fixed it, and the planner kept the vector index at every LIMIT up to 200.

Trap 2: without iterative index scans, filtered queries can quietly return fewer rows than requested, 18 of 40 here. One setting fixes it.

Trap 3 has no full fix. Hybrid search stays about 3x slower than vector search, because PostgreSQL's full-text index finds matching rows but cannot rank them.

The ten images walk through the cluster, the AWS setup, the benchmark, what the node delivered, the three traps and the checks to run before shipping your own. The article has every query plan and why the usual workarounds don't help; the repository has the EKS setup guide, manifests, benchmark code and raw results.

Article: <ARTICLE_URL>
Repo: github.com/tharindu-sankalpa/pgvector-filtering-investigation

#PostgreSQL #pgvector #Kubernetes #AmazonEKS #VectorSearch #DatabasePerformance
