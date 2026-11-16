"""Validated local exports and an atomic active-version pointer."""

import json
import math
import os
import re
import argparse
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
            if not (folder / "_READY").is_file():
                raise InvalidExport("Missing _READY marker")
            manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
            if manifest.get("version") != version or manifest.get("status") != "ready" or "doc_count" not in manifest:
                raise InvalidExport("Manifest must contain version, doc_count and status=ready")
            tables = {}
            for name, columns in {
                "documents": ["doc_id", "title", "url", "doc_length"],
                "index": ["term", "doc_id", "tf"],
                "pagerank": ["doc_id", "pagerank"],
            }.items():
                dataset = folder / f"{name}.parquet"
                if not dataset.is_dir() or not list(dataset.glob("part-*.parquet")):
                    raise InvalidExport(f"Missing {name} Parquet parts")
                table = pq.read_table(dataset)
                if not set(columns).issubset(table.column_names):
                    raise InvalidExport(f"Invalid {name} columns")
                tables[name] = table.to_pylist()
            docs = {}
            for row in tables["documents"]:
                doc_id = row["doc_id"]
                if not isinstance(doc_id, (str, int)) or isinstance(doc_id, bool) or doc_id == "" or doc_id in docs or not isinstance(row["title"], str) or not isinstance(row["url"], str) or not isinstance(row["doc_length"], int) or row["doc_length"] <= 0:
                    raise InvalidExport("Invalid or duplicate document")
                docs[doc_id] = row
            if not isinstance(manifest["doc_count"], int) or manifest["doc_count"] != len(docs):
                raise InvalidExport("Manifest document count mismatch")
            index = {}
            for row in tables["index"]:
                term, doc_id, tf = row["term"], row["doc_id"], row["tf"]
                if not isinstance(term, str) or not re.fullmatch(r"[a-z0-9]+", term) or doc_id not in docs or not isinstance(tf, int) or tf <= 0 or tf > docs[doc_id]["doc_length"]:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate a VM3 export without activating it")
    parser.add_argument("version", choices=("v0", "v1"))
    parser.add_argument("--root", type=Path, default=Path(__file__).parent / "serving")
    args = parser.parse_args()
    validator = SearchStore.__new__(SearchStore)
    validator.root = args.root
    docs, _, _ = validator.load(args.version)
    print(f"Valid {args.version} export: {len(docs)} documents")
