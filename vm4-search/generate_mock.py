"""Create a complete v0 export for standalone development."""

import json
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ranking import tokenize


SAMPLES = [
    ("d1", "Distributed Search Basics", "https://example.org/search/basics", "distributed search indexes documents across machines for fast keyword search"),
    ("d2", "Python Search Tutorial", "https://example.org/search/python", "python tutorial builds a simple search index with document ranking"),
    ("d3", "Understanding PageRank", "https://example.org/ranking/pagerank", "pagerank measures links and improves ranking of web documents"),
    ("d4", "BM25 Ranking Guide", "https://example.org/ranking/bm25", "bm25 ranking uses term frequency and document length for search relevance"),
    ("d5", "Parquet Data Exports", "https://example.org/data/parquet", "parquet stores columnar documents and search index data efficiently"),
]


def generate(root: Path = Path(__file__).parent / "serving", version: str = "v0"):
    if version not in {"v0", "v1"}:
        raise ValueError("Version must be v0 or v1")
    folder = Path(root) / version
    folder.mkdir(parents=True, exist_ok=True)
    docs, entries, ranks = [], [], []
    for i, (doc_id, title, url, body) in enumerate(SAMPLES):
        counts = Counter(tokenize(title + " " + body))
        docs.append({"doc_id": doc_id, "title": title, "url": url, "doc_length": sum(counts.values())})
        entries.extend({"term": term, "doc_id": doc_id, "tf": count} for term, count in sorted(counts.items()))
        ranks.append({"doc_id": doc_id, "pagerank": [0.32, 0.23, 0.19, 0.16, 0.10][i]})
    pq.write_table(pa.Table.from_pylist(docs), folder / "documents.parquet")
    pq.write_table(pa.Table.from_pylist(entries), folder / "index.parquet")
    pq.write_table(pa.Table.from_pylist(ranks), folder / "pagerank.parquet")
    (folder / "manifest.json").write_text(json.dumps({"version": version, "doc_count": len(docs), "status": "ready"}, indent=2), encoding="utf-8")
    print(f"Generated {folder}")


if __name__ == "__main__":
    generate()
