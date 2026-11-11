import json
import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import create_app
from generate_mock import generate
from ranking import bm25, rank
from store import InvalidExport, SearchStore


def test_bm25_formula():
    expected = math.log(1 + (3 - 1 + .5) / (1 + .5)) * (2 * 2.2) / (2 + 1.2 * (1 - .75 + .75 * 12 / 10))
    assert bm25(2, 12, 10, 3, 1) == pytest.approx(expected)


def test_ranking_multiple_terms_and_candidates():
    docs = {"a": {"title": "A", "url": "a", "doc_length": 10}, "b": {"title": "B", "url": "b", "doc_length": 10}}
    results = rank("python python search", docs, {"python": {"a": 3}, "search": {"b": 1}}, {"a": .1, "b": .9})
    assert [r["doc_id"] for r in results] == ["a", "b"]
    assert results[0]["score"] > results[1]["score"]
    assert rank("unknown", docs, {}, {"a": .1, "b": .9}) == []


def test_api_and_restart(tmp_path):
    generate(tmp_path)
    client = TestClient(create_app(tmp_path))
    assert client.get("/health").json() == {"status": "ok", "version": "v0"}
    assert client.get("/version").json() == {"version": "v0"}
    assert client.get("/search", params={"q": "search ranking", "limit": 2}).json()["results"]
    assert len(client.get("/search", params={"q": "search", "limit": 2}).json()["results"]) == 2
    assert client.get("/search", params={"q": "nonsense"}).json()["results"] == []
    assert client.get("/search", params={"q": "search", "limit": 0}).status_code == 422
    assert client.get("/").status_code == 200
    assert TestClient(create_app(tmp_path)).get("/version").json()["version"] == "v0"


def test_invalid_export_preserves_active_version(tmp_path):
    generate(tmp_path)
    store = SearchStore(tmp_path)
    generate(tmp_path, "v1")
    table = pq.read_table(tmp_path / "v1" / "documents.parquet")
    pq.write_table(table.drop(["doc_length"]), tmp_path / "v1" / "documents.parquet")
    with pytest.raises(InvalidExport):
        store.activate("v1")
    assert store.version == "v0"
    assert json.loads((tmp_path / "active_version.json").read_text())["version"] == "v0"
    assert TestClient(create_app(tmp_path)).post("/version/activate", json={"version": "v1"}).status_code == 400


def test_switch_version_persists(tmp_path):
    generate(tmp_path)
    generate(tmp_path, "v1")
    client = TestClient(create_app(tmp_path))
    assert client.post("/version/activate", json={"version": "v1"}).json() == {"version": "v1"}
    assert TestClient(create_app(tmp_path)).get("/version").json() == {"version": "v1"}
    assert client.post("/version/activate", json={"version": "../v0"}).status_code == 400
