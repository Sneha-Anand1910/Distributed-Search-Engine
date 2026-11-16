# VM3 — Distributed Compute and Search Export

## Responsibilities

- Run the Spark master, Spark worker, and HDFS DataNode.
- Build the distributed inverted index.
- Compute PageRank.
- Validate and publish versioned search exports.

## Initial setup

Copy the configuration template:

```bash
cp conf/cluster.env.example conf/cluster.env
```

Edit conf/cluster.env for the local environment.

## Local demo

```bash
bash scripts/run_demo.sh
```

Expected success messages:

```text
DEMO_INDEX_CHECK_PASSED
VALIDATION_PASSED
```

The local demo does not test the distributed cluster or Iceberg integration.

## Shared-cluster execution

After configuring HDFS, Iceberg, and the Spark cluster:

```bash
bash scripts/run_cluster.sh inspect phase0 <SHARED_SNAPSHOT_ID>
bash scripts/run_cluster.sh build phase0 <SHARED_SNAPSHOT_ID>
```

## Expected normalized input

- doc_id: string
- title: string
- url: string
- text: string
- links: array of target document IDs

## Export contract

- documents.parquet/: doc_id, title, url, doc_length
- index.parquet/: term, doc_id, tf
- pagerank.parquet/: doc_id, pagerank
- manifest.json: version, doc_count, status
- provenance.json: processing metadata
- _READY: completion marker

The .parquet paths are dataset directories containing Spark part files.

## Tokenization

- Lowercase ASCII alphanumeric tokens.
- No stemming or stopword removal.
- Only document text is indexed.
- Titles are exported for display.
