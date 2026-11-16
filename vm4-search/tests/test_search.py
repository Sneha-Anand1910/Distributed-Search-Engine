import json
import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import create_app
from generate_mock import generate
from ranking import bm25, rank, tokenize
from store import InvalidExport, SearchStore


def test_bm25_formula():
    expected = math.log(1 + (3 - 1 + .5) / (1 + .5)) * (2 * 2.2) / (2 + 1.2 * (1 - .75 + .75 * 12 / 10))
    assert bm25(2, 12, 10, 3, 1) == pytest.approx(expected)


def test_vm3_tokenization():
    assert tokenize("Plant---GROWTH! beard_42, cooking") == ["plant", "growth", "beard", "42", "cooking"]


def test_ranking_multiple_terms_and_candidates():
    docs = {"a": {"title": "A", "url": "a", "doc_length": 10}, "b": {"title": "B", "url": "b", "doc_length": 10}}
    results = rank("python python search", docs, {"python": {"a": 3}, "search": {"b": 1}}, {"a": .1, "b": .9})
    assert [r["doc_id"] for r in results] == ["a", "b"]
    assert results[0]["score"] > results[1]["score"]
    assert rank("unknown", docs, {}, {"a": .1, "b": .9}) == []


def test_api_and_restart(tmp_path):
    generate(tmp_path)
    assert len(list((tmp_path / "v0" / "index.parquet").glob("part-*.parquet"))) > 1
    assert (tmp_path / "v0" / "_READY").is_file()
    client = TestClient(create_app(tmp_path))
    assert client.get("/health").json() == {"status": "ok", "version": "v0"}
    assert client.get("/version").json() == {"version": "v0"}
    assert client.get("/search", params={"q": "search ranking", "limit": 2}).json()["results"]
    assert len(client.get("/search", params={"q": "search", "limit": 2}).json()["results"]) == 2
    assert client.get("/search", params={"q": "nonsense"}).json()["results"] == []
    result = client.get("/search", params={"q": "search ranking"}).json()["results"][0]
    assert {"doc_id", "title", "url", "score"} <= result.keys()
    assert client.get("/search", params={"q": "search", "limit": 0}).status_code == 422
    assert client.get("/").status_code == 200
    assert TestClient(create_app(tmp_path)).get("/version").json()["version"] == "v0"


def test_invalid_export_preserves_active_version(tmp_path):
    generate(tmp_path)
    store = SearchStore(tmp_path)
    generate(tmp_path, "v1")
    part = next((tmp_path / "v1" / "documents.parquet").glob("part-*.parquet"))
    table = pq.read_table(part)
    pq.write_table(table.drop(["doc_length"]), part)
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
    assert client.get("/health").json() == {"status": "ok", "version": "v1"}
    assert TestClient(create_app(tmp_path)).get("/version").json() == {"version": "v1"}
    assert client.post("/version/activate", json={"version": "../v0"}).status_code == 400


@pytest.mark.parametrize("problem", ["missing_marker", "marker_directory", "bad_status", "missing_dataset"])
def test_invalid_export_conditions(tmp_path, problem):
    generate(tmp_path)
    generate(tmp_path, "v1")
    folder = tmp_path / "v1"
    if problem == "missing_marker":
        (folder / "_READY").unlink()
    elif problem == "marker_directory":
        (folder / "_READY").unlink()
        (folder / "_READY").mkdir()
    elif problem == "bad_status":
        manifest = json.loads((folder / "manifest.json").read_text())
        manifest["status"] = "writing"
        (folder / "manifest.json").write_text(json.dumps(manifest))
    else:
        (folder / "index.parquet").rename(folder / "index_missing")
    client = TestClient(create_app(tmp_path))
    assert client.post("/version/activate", json={"version": "v1"}).status_code == 400
    assert client.get("/health").json() == {"status": "ok", "version": "v0"}
    assert json.loads((tmp_path / "active_version.json").read_text())["version"] == "v0"
