"""Small, deterministic BM25 and score combination helpers."""

import math
import re
from collections import Counter

K1 = 1.2
B = 0.75


def tokenize(query: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", query.lower())


def bm25(tf: int, doc_length: int, avg_length: float, doc_count: int, df: int) -> float:
    if not tf or not df or not doc_count or not avg_length:
        return 0.0
    idf = math.log(1 + (doc_count - df + 0.5) / (df + 0.5))
    return idf * tf * (K1 + 1) / (tf + K1 * (1 - B + B * doc_length / avg_length))


def rank(query: str, documents: dict, index: dict, pagerank: dict, limit: int = 10) -> list[dict]:
    terms = Counter(tokenize(query))
    if not terms or not documents:
        return []
    avg_length = sum(d["doc_length"] for d in documents.values()) / len(documents)
    bm25_scores = {}
    for term, query_tf in terms.items():
        postings = index.get(term, {})
        for doc_id, tf in postings.items():
            bm25_scores[doc_id] = bm25_scores.get(doc_id, 0.0) + query_tf * bm25(
                tf, documents[doc_id]["doc_length"], avg_length, len(documents), len(postings)
            )
    if not bm25_scores:
        return []
    max_bm25 = max(bm25_scores.values())
    max_pr = max(pagerank[doc_id] for doc_id in bm25_scores)
    results = []
    for doc_id, raw_bm25 in bm25_scores.items():
        doc = documents[doc_id]
        score = 0.7 * raw_bm25 / max_bm25 + 0.3 * (pagerank[doc_id] / max_pr if max_pr else 0)
        results.append({"doc_id": doc_id, "title": doc["title"], "url": doc["url"], "score": round(score, 6)})
    return sorted(results, key=lambda r: (-r["score"], r["doc_id"]))[:limit]
