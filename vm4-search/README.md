# VM4 Search Node

Standalone FastAPI search node using local VM3 Parquet exports. Python 3.10+ is required. Run commands from `vm4-search/`.

## Setup and run

```sh
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# WSL Ubuntu: source .venv/bin/activate
python -m pip install -r requirements.txt
python generate_mock.py
python -m pytest -q
python -m uvicorn app:app --reload
```

Open http://127.0.0.1:8000/ for the search page. On Windows, use `py` in place of `python` if needed.

## Export format and activation

`serving/v0/` and `serving/v1/` each contain Spark-style directories `documents.parquet/`, `index.parquet/`, and `pagerank.parquet/`, with one or more `part-*.parquet` files in each. Required columns are `doc_id,title,url,doc_length`; `term,doc_id,tf`; and `doc_id,pagerank`, respectively. Each version also contains `manifest.json` with `version`, `doc_count`, and `status="ready"`, plus an `_READY` file. Write the marker only after all parts are complete. Terms must match lowercase `[a-z0-9]+`; document IDs must be unique; every document needs one PageRank value.

The app validates all datasets before activation. It starts with `v0` if there is no active pointer, and persists the selected version in `serving/active_version.json`. A failed activation leaves the current version active. Once VM3's export is available locally in `serving/v1/`, validate and activate it:

```sh
python store.py v1
curl -X POST http://127.0.0.1:8000/version/activate -H "Content-Type: application/json" -d '{"version":"v1"}'
```

On Windows PowerShell, use `curl.exe` instead of `curl`. Substitute `v0` to validate or reactivate Phase 0. To generate mock v1 for a local switch test, run `python -c "from generate_mock import generate; generate(version='v1')"`. Do not run the mock generator over real exports.

## Phase 0 and Phase 1 verification

Run these after activating `v0` for Phase 0, then repeat after activating `v1` for Phase 1. `/health` should show the corresponding version. Search results depend on the exported corpus.

```sh
curl http://127.0.0.1:8000/health
curl "http://127.0.0.1:8000/search?q=beard&limit=10"
curl "http://127.0.0.1:8000/search?q=cooking&limit=10"
curl "http://127.0.0.1:8000/search?q=plant%20growth&limit=10"
```

On Windows PowerShell, use `curl.exe` for these commands.

API: `GET /health`, `GET /version`, `GET /search?q=keyword&limit=10`, `POST /version/activate`. `limit` ranges from 1 to 100. Query text is lowercased and split on characters outside `[a-z0-9]`, without stemming or stopword removal. Search uses inverted-index candidates. BM25 uses `k1=1.2`, `b=0.75`; within the candidate set, BM25 and PageRank are each divided by their maximum and combined as `0.7 * BM25 + 0.3 * PageRank`.
