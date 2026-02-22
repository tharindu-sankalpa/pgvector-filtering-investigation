"""
Shared Reciprocal Rank Fusion (RRF) Logic for Hybrid Search.

This module provides a consistent RRF implementation for all vector database benchmarks
to ensure fair and comparable hybrid search scoring.
"""

def rrf_score(rank, k=60):
    """
    Calculate RRF score for a single rank.
    Formula: 1 / (k + rank)
    
    Args:
        rank (int): 1-based rank position
        k (int): RRF constant (default 60)
    """
    return 1.0 / (k + rank)

def fuse_results(sparse_results, dense_results, k=60, limit=None):
    """
    Combine sparse and dense search results using Reciprocal Rank Fusion (RRF).
    
    Args:
        sparse_results (list): List of sparse search results.
                             Each item must be a dict with at least {'id': ...}
                             and optional 'score'.
        dense_results (list): List of dense vector search results.
                            Each item must be a dict with at least {'id': ...}
                            and optional 'score'.
        k (int): RRF constant (default 60).
        limit (int): Max number of fused results to return.
        
    Returns:
        list: Fused results sorted by RRF score descending.
              Each item is {'id': ..., 'rrf_score': ..., 'sparse_rank': ..., 'dense_rank': ...}
    """
    # Map IDs to scores
    scores = {}
    
    # Process Sparse Results
    for rank, item in enumerate(sparse_results, 1):
        doc_id = item['id']
        if doc_id not in scores:
            scores[doc_id] = {'rrf_score': 0.0, 'sparse_rank': None, 'dense_rank': None, 'item': item}
        
        scores[doc_id]['rrf_score'] += rrf_score(rank, k)
        scores[doc_id]['sparse_rank'] = rank

    # Process Dense Results
    for rank, item in enumerate(dense_results, 1):
        doc_id = item['id']
        if doc_id not in scores:
            scores[doc_id] = {'rrf_score': 0.0, 'sparse_rank': None, 'dense_rank': None, 'item': item}
            
        scores[doc_id]['rrf_score'] += rrf_score(rank, k)
        scores[doc_id]['dense_rank'] = rank

    # Sort by RRF score descending
    sorted_results = sorted(scores.values(), key=lambda x: x['rrf_score'], reverse=True)
    
    if limit:
        sorted_results = sorted_results[:limit]
        
    return sorted_results

