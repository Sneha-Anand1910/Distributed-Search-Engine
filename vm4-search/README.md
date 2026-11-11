# VM4 Search Node

Standalone FastAPI search node using local Parquet exports. Python 3.10+ is required.

## Setup and run

From `vm4-search/`:

```sh
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# Ubuntu: source .venv/bin/activate
python -m pip install -r requirements.txt
python generate_mock.py
python -m uvicorn app:app --reload
```

Open http://127.0.0.1:8000/ for the search page. Run tests with `python -m pytest -q`.

## Export layout and activation

`serving/v0/` and `serving/v1/` each contain `documents.parquet` (`doc_id`, `title`, `url`, `doc_length`), `index.parquet` (`term`, `doc_id`, `tf`), `pagerank.parquet` (`doc_id`, `pagerank`), and `manifest.json` (`version`, `doc_count`, `status`). Set `status` to `ready` only after all files are fully written. Terms must be lowercase; document IDs must be unique; every document needs one PageRank value.

The app validates a complete export before activation. On first start it activates `v0` if present. The selected version is persisted in `serving/active_version.json` using an atomic file replacement. A failed activation leaves the current data and pointer intact. Prepare `v1` separately, then activate it:

```sh
curl -X POST http://127.0.0.1:8000/version/activate -H "Content-Type: application/json" -d '{"version":"v1"}'
```

API: `GET /health`, `GET /version`, `GET /search?q=keyword&limit=10`, `POST /version/activate`. `limit` ranges from 1 to 100. Search returns only inverted-index candidates. BM25 uses `k1=1.2`, `b=0.75`; each query term contributes according to its count. Within the candidate set, BM25 and PageRank are each divided by their maximum, then combined as `0.7 × BM25 + 0.3 × PageRank`.
