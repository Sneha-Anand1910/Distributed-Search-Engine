"""Validated local exports and an atomic active-version pointer."""

import json
import math
import os
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class InvalidExport(ValueError):
    pass


class SearchStore:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.active_file = self.root / "active_version.json"
        self.version = None
        self.documents = {}
        self.index = {}
        self.pagerank = {}
        if self.active_file.exists():
            version = json.loads(self.active_file.read_text(encoding="utf-8"))["version"]
            self.activate(version, persist=False)
        elif (self.root / "v0").exists():
            self.activate("v0")

    def load(self, version: str):
        if not re.fullmatch(r"v[01]", version):
            raise InvalidExport("Version must be v0 or v1")
        folder = self.root / version
        try:
            manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
            if manifest != {"version": version, "doc_count": manifest.get("doc_count"), "status": "ready"}:
                raise InvalidExport("Manifest must contain version, doc_count and status=ready")
            tables = {}
            for name, columns in {
                "documents": ["doc_id", "title", "url", "doc_length"],
                "index": ["term", "doc_id", "tf"],
                "pagerank": ["doc_id", "pagerank"],
            }.items():
                table = pq.read_table(folder / f"{name}.parquet")
                if table.column_names != columns:
                    raise InvalidExport(f"Invalid {name} columns")
                tables[name] = table.to_pylist()
            docs = {}
            for row in tables["documents"]:
                doc_id = row["doc_id"]
                if not isinstance(doc_id, str) or not doc_id or doc_id in docs or not isinstance(row["title"], str) or not isinstance(row["url"], str) or not isinstance(row["doc_length"], int) or row["doc_length"] <= 0:
                    raise InvalidExport("Invalid or duplicate document")
                docs[doc_id] = row
            if not isinstance(manifest["doc_count"], int) or manifest["doc_count"] != len(docs):
                raise InvalidExport("Manifest document count mismatch")
            index = {}
            for row in tables["index"]:
                term, doc_id, tf = row["term"], row["doc_id"], row["tf"]
                if not isinstance(term, str) or not term or term != term.lower() or doc_id not in docs or not isinstance(tf, int) or tf <= 0 or tf > docs[doc_id]["doc_length"]:
                    raise InvalidExport("Invalid index entry")
                postings = index.setdefault(term, {})
                if doc_id in postings:
                    raise InvalidExport("Duplicate index entry")
                postings[doc_id] = tf
            pr = {}
            for row in tables["pagerank"]:
                doc_id, value = row["doc_id"], row["pagerank"]
                if doc_id not in docs or doc_id in pr or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                    raise InvalidExport("Invalid PageRank entry")
                pr[doc_id] = value
            if set(pr) != set(docs):
                raise InvalidExport("PageRank must cover every document")
            return docs, index, pr
        except (OSError, KeyError, ValueError, TypeError, pa.ArrowException) as exc:
            if isinstance(exc, InvalidExport):
                raise
            raise InvalidExport(f"Invalid export: {exc}") from exc

    def activate(self, version: str, persist: bool = True):
        docs, index, pr = self.load(version)
        if persist:
            self.root.mkdir(parents=True, exist_ok=True)
            temporary = self.root / "active_version.json.tmp"
            temporary.write_text(json.dumps({"version": version}), encoding="utf-8")
            os.replace(temporary, self.active_file)
        self.documents, self.index, self.pagerank = docs, index, pr
        self.version = version
