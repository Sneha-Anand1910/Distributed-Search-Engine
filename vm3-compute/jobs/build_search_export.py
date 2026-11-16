import argparse
import json
import math

from pyspark.sql import SparkSession, functions as F


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def has_rows(df):
    return df.limit(1).count() > 0


parser = argparse.ArgumentParser()

source = parser.add_mutually_exclusive_group(required=True)
source.add_argument("--demo", action="store_true")
source.add_argument("--table")

parser.add_argument("--snapshot-id")
parser.add_argument("--output", required=True)
parser.add_argument("--checkpoint", required=True)

parser.add_argument("--id-col", default="doc_id")
parser.add_argument("--title-col", default="title")
parser.add_argument("--url-col", default="url")
parser.add_argument("--text-col", default="text")
parser.add_argument("--links-col", default="links")

parser.add_argument("--iterations", type=int, default=10)
parser.add_argument("--damping", type=float, default=0.85)
parser.add_argument("--partitions", type=int, default=4)

args = parser.parse_args()

require(args.iterations >= 10, "At least 10 iterations required")
require(0 < args.damping < 1, "Damping must be between 0 and 1")
require(args.partitions > 0, "Partitions must be positive")

if args.table:
    require(
        args.snapshot_id is not None,
        "--snapshot-id is required when using --table",
    )

spark = (
    SparkSession.builder
    .appName("VM3-Search-Export")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir(args.checkpoint)

spark.conf.set(
    "spark.sql.shuffle.partitions",
    str(args.partitions),
)

output = args.output.rstrip("/")


def hadoop_path(path_string):
    return spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
        path_string
    )


def filesystem_for(path):
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    return path.getFileSystem(conf)


def write_parquet(df, name):
    df.write.mode("errorifexists").parquet(
        f"{output}/{name}"
    )


def write_json_file(path_string, value):
    path = hadoop_path(path_string)
    filesystem = filesystem_for(path)

    stream = filesystem.create(path, False)

    writer = (
        spark.sparkContext._jvm.java.io.OutputStreamWriter(
            stream,
            "UTF-8",
        )
    )

    try:
        writer.write(
            json.dumps(value, indent=2, ensure_ascii=True) + "\n"
        )
    finally:
        writer.close()


def mark_ready():
    path = hadoop_path(f"{output}/_READY")
    filesystem = filesystem_for(path)

    stream = filesystem.create(path, False)
    stream.close()


try:
    # Refuse to mix a new run with any existing export.
    output_path = hadoop_path(output)
    output_fs = filesystem_for(output_path)

    require(
        not output_fs.exists(output_path),
        f"Output already exists: {output}",
    )

    version = output_path.getName()

    # 1. Load demo documents or a pinned Iceberg snapshot.
    if args.demo:
        raw = spark.createDataFrame(
            [
                (
                    "A",
                    "Spark Search",
                    "https://example.org/A",
                    "spark spark search",
                    ["B"],
                ),
                (
                    "B",
                    "Search Engines",
                    "https://example.org/B",
                    "search engine",
                    ["A"],
                ),
                (
                    "C",
                    "Distributed Systems",
                    "https://example.org/C",
                    "distributed systems spark",
                    [],
                ),
            ],
            """
            doc_id string,
            title string,
            url string,
            text string,
            links array<string>
            """,
        )

    else:
        raw = (
            spark.read
            .format("iceberg")
            .option("snapshot-id", args.snapshot_id)
            .load(args.table)
        )

    # 2. Normalize the input columns.
    required_columns = {
        args.id_col,
        args.title_col,
        args.url_col,
        args.text_col,
        args.links_col,
    }

    require(
        required_columns.issubset(set(raw.columns)),
        (
            f"Required columns: {sorted(required_columns)}. "
            f"Available columns: {raw.columns}"
        ),
    )

    docs = raw.select(
        F.col(args.id_col).cast("string").alias("doc_id"),
        F.col(args.title_col).cast("string").alias("title"),
        F.col(args.url_col).cast("string").alias("url"),
        F.col(args.text_col).cast("string").alias("text"),
        F.col(args.links_col).alias("links"),
    ).cache()

    invalid_documents = docs.filter(
        F.col("doc_id").isNull()
        | (F.length(F.trim(F.col("doc_id"))) == 0)
        | F.col("title").isNull()
        | F.col("url").isNull()
        | F.col("text").isNull()
        | F.col("links").isNull()
    )

    require(
        not has_rows(invalid_documents),
        "Null or empty required source fields",
    )

    duplicate_ids = (
        docs.groupBy("doc_id")
        .count()
        .filter(F.col("count") > 1)
    )

    require(
        not has_rows(duplicate_ids),
        "Duplicate document IDs: resolve during ingestion",
    )

    num_docs = docs.count()
    require(num_docs > 0, "Corpus is empty")

    print("INPUT_MODE:", "demo" if args.demo else "iceberg")
    print("DOCUMENT_COUNT:", num_docs)

    # 3. Tokenize the text field.
    # Titles are exported for display, but are not indexed.
    tokenized = (
        docs
        .withColumn(
            "tokens",
            F.split(
                F.trim(
                    F.regexp_replace(
                        F.lower(F.col("text")),
                        r"[^a-z0-9]+",
                        " ",
                    )
                ),
                r"\s+",
            ),
        )
        .withColumn(
            "tokens",
            F.expr("filter(tokens, x -> x <> '')"),
        )
        .withColumn(
            "doc_length",
            F.size("tokens").cast("long"),
        )
        .cache()
    )

    documents = tokenized.select(
        "doc_id",
        "title",
        "url",
        "doc_length",
    )

    # 4. Build the inverted index.
    postings = (
        tokenized
        .select(
            "doc_id",
            F.explode("tokens").alias("term"),
        )
        .groupBy("term", "doc_id")
        .agg(F.count("*").alias("tf"))
        .select("term", "doc_id", "tf")
        .cache()
    )

    total_tokens = int(
        documents
        .agg(F.sum("doc_length").alias("total"))
        .first()["total"]
    )

    require(total_tokens > 0, "No searchable tokens")

    # 5. Construct the graph.
    ids = docs.select("doc_id")

    edges = (
        docs
        .select(
            F.col("doc_id").alias("src"),
            F.explode("links").alias("target"),
        )
        .select(
            "src",
            F.col("target").cast("string").alias("dst"),
        )
    )

    require(
        not has_rows(edges.filter(F.col("dst").isNull())),
        "Null link target",
    )

    edges = edges.distinct()

    valid_targets = ids.select(
        F.col("doc_id").alias("dst")
    )

    missing_targets = edges.join(
        valid_targets,
        on="dst",
        how="left_anti",
    )

    require(
        not has_rows(missing_targets),
        "Links reference documents that do not exist",
    )

    edges = edges.repartition(
        args.partitions,
        "src",
    ).cache()

    outdegree = (
        edges
        .groupBy("src")
        .agg(F.count("*").alias("outdegree"))
        .cache()
    )

    dangling_ids = (
        ids.join(
            outdegree.select(
                F.col("src").alias("doc_id")
            ),
            on="doc_id",
            how="left_anti",
        )
        .cache()
    )

    print("EDGE_COUNT:", edges.count())
    print("DANGLING_COUNT:", dangling_ids.count())

    # 6. Compute PageRank.
    ranks = (
        ids
        .withColumn(
            "pagerank",
            F.lit(1.0 / num_docs),
        )
        .checkpoint(eager=True)
    )

    for iteration in range(args.iterations):
        dangling_mass = (
            ranks
            .join(dangling_ids, on="doc_id")
            .agg(F.sum("pagerank").alias("mass"))
            .first()["mass"]
        )

        dangling_mass = float(dangling_mass or 0.0)

        contributions = (
            edges
            .join(outdegree, on="src")
            .join(
                ranks.select(
                    F.col("doc_id").alias("src"),
                    "pagerank",
                ),
                on="src",
            )
            .select(
                F.col("dst").alias("doc_id"),
                (
                    F.col("pagerank")
                    / F.col("outdegree")
                ).alias("contribution"),
            )
            .groupBy("doc_id")
            .agg(
                F.sum("contribution").alias("incoming")
            )
        )

        base_rank = (
            (1.0 - args.damping) / num_docs
            + args.damping * dangling_mass / num_docs
        )

        ranks = (
            ids
            .join(
                contributions,
                on="doc_id",
                how="left",
            )
            .select(
                "doc_id",
                (
                    F.lit(base_rank)
                    + F.lit(args.damping)
                    * F.coalesce(
                        F.col("incoming"),
                        F.lit(0.0),
                    )
                ).alias("pagerank"),
            )
            .checkpoint(eager=True)
        )

        print(
            "PAGERANK_ITERATION:",
            f"{iteration + 1}/{args.iterations}",
        )

    # 7. Validate the known demo index.
    if args.demo:
        actual_postings = {
            (row["term"], row["doc_id"]): row["tf"]
            for row in postings.collect()
        }

        expected_postings = {
            ("spark", "A"): 2,
            ("search", "A"): 1,
            ("search", "B"): 1,
            ("engine", "B"): 1,
            ("distributed", "C"): 1,
            ("systems", "C"): 1,
            ("spark", "C"): 1,
        }

        require(
            actual_postings == expected_postings,
            "Demo postings mismatch",
        )

        require(
            total_tokens == 8,
            "Demo token count mismatch",
        )

        print("DEMO_INDEX_CHECK_PASSED")

    # 8. Write the exact agreed dataset names.
    write_parquet(documents, "documents.parquet")
    write_parquet(postings, "index.parquet")
    write_parquet(ranks, "pagerank.parquet")

    # 9. Read the written datasets back.
    exported_docs = spark.read.parquet(
        f"{output}/documents.parquet"
    )

    exported_index = spark.read.parquet(
        f"{output}/index.parquet"
    )

    exported_ranks = spark.read.parquet(
        f"{output}/pagerank.parquet"
    )

    require(
        exported_docs.count() == num_docs,
        "Exported document count mismatch",
    )

    require(
        exported_ranks.count() == num_docs,
        "Exported PageRank count mismatch",
    )

    require(
        not has_rows(
            exported_docs
            .groupBy("doc_id")
            .count()
            .filter(F.col("count") != 1)
        ),
        "Duplicate exported document IDs",
    )

    require(
        not has_rows(
            exported_ranks
            .groupBy("doc_id")
            .count()
            .filter(F.col("count") != 1)
        ),
        "Duplicate PageRank IDs",
    )

    exported_ids = exported_docs.select("doc_id")

    require(
        not has_rows(
            exported_ids.join(
                exported_ranks.select("doc_id"),
                on="doc_id",
                how="left_anti",
            )
        ),
        "Documents missing PageRank scores",
    )

    require(
        not has_rows(
            exported_index.join(
                exported_ids,
                on="doc_id",
                how="left_anti",
            )
        ),
        "Index references missing documents",
    )

    require(
        not has_rows(
            exported_index
            .groupBy("term", "doc_id")
            .count()
            .filter(F.col("count") != 1)
        ),
        "Duplicate index entries",
    )

    invalid_postings = exported_index.filter(
        F.col("term").isNull()
        | (F.length(F.col("term")) == 0)
        | F.col("tf").isNull()
        | (F.col("tf") <= 0)
    )

    require(
        not has_rows(invalid_postings),
        "Invalid term frequencies or terms",
    )

    # Check token totals per document.
    per_document_tf = (
        exported_index
        .groupBy("doc_id")
        .agg(F.sum("tf").alias("tf_total"))
    )

    length_check = (
        exported_docs
        .join(
            per_document_tf,
            on="doc_id",
            how="left",
        )
        .filter(
            F.col("doc_length")
            != F.coalesce(F.col("tf_total"), F.lit(0))
        )
    )

    require(
        not has_rows(length_check),
        "Index frequencies do not match document lengths",
    )

    invalid_ranks = exported_ranks.filter(
        F.col("pagerank").isNull()
        | F.isnan("pagerank")
        | (F.col("pagerank") < 0)
        | (F.col("pagerank") > 1)
    )

    require(
        not has_rows(invalid_ranks),
        "Invalid PageRank values",
    )

    rank_sum = float(
        exported_ranks
        .agg(F.sum("pagerank").alias("total"))
        .first()["total"]
    )

    require(
        math.isfinite(rank_sum)
        and abs(rank_sum - 1.0) < 1e-6,
        f"PageRank sum is invalid: {rank_sum}",
    )

    # 10. Write an actual JSON file, not a Spark text directory.
    manifest = {
        "version": version,
        "doc_count": num_docs,
        "status": "ready",
    }

    write_json_file(
        f"{output}/manifest.json",
        manifest,
    )

    # Extra provenance is separate so manifest.json keeps
    # exactly the three fields agreed with VM4.
    provenance = {
        "input_mode": "demo" if args.demo else "iceberg",
        "source_table": args.table,
        "source_snapshot_id": args.snapshot_id,
        "tokenizer": "lowercase_ascii_alphanumeric_v1",
        "indexed_fields": ["text"],
        "pagerank_iterations": args.iterations,
        "pagerank_damping": args.damping,
    }

    write_json_file(
        f"{output}/provenance.json",
        provenance,
    )

    # Written last, after all data and metadata succeed.
    mark_ready()

    print("VALIDATION_PASSED")
    print("EXPORT_VERSION:", version)
    print("READY_EXPORT:", output)

finally:
    spark.stop()
