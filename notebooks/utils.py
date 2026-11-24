"""Fonctions utilitaires : système de fichiers S3A (MinIO), checkpoints, parsing Kafka."""
import json


# ---------------------------------------------------------------- système de fichiers (via Hadoop FS)
def _fs(spark, path):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p, p.getFileSystem(spark._jsc.hadoopConfiguration())


def fs_exists(spark, path):
    p, fs = _fs(spark, path)
    return fs.exists(p)


def fs_ls(spark, path, show=True):
    p, fs = _fs(spark, path)
    if not fs.exists(p):
        if show:
            print("(absent)", path)
        return []
    names = sorted(f.getPath().getName() for f in fs.listStatus(p))
    if show:
        for n in names:
            print(n)
    return names


def fs_rm(spark, path):
    p, fs = _fs(spark, path)
    if fs.exists(p):
        fs.delete(p, True)
        print("supprimé :", path)
    else:
        print("absent   :", path)


def fs_cat(spark, path):
    return "\n".join(r[0] for r in spark.read.text(path).collect())


# ---------------------------------------------------------------- checkpoints Structured Streaming
def _ids(names):
    return sorted(int(n) for n in names if n.isdigit())


def checkpoint_offsets(spark, ck, committed_only=True):
    """Dernier batch (commité par défaut) d'un checkpoint : (batch_id, {topic: {partition: offset}})."""
    base = ck.rstrip("/")
    commits = _ids(fs_ls(spark, base + "/commits", show=False))
    offs = _ids(fs_ls(spark, base + "/offsets", show=False))
    if not offs:
        return None, None
    bid = commits[-1] if (committed_only and commits) else offs[-1]
    lines = fs_cat(spark, f"{base}/offsets/{bid}").split("\n")
    return bid, json.loads(lines[-1])


def show_checkpoint(spark, ck):
    base = ck.rstrip("/")
    print("== contenu de", base)
    for sub in ("offsets", "commits", "sources", "metadata"):
        print(f"-- {sub}/")
        fs_ls(spark, f"{base}/{sub}")
    bid, off = checkpoint_offsets(spark, ck, committed_only=False)
    if bid is not None:
        print(f"\n== offsets/{bid} (dernier batch planifié) ==")
        print(fs_cat(spark, f"{base}/offsets/{bid}"))


# ---------------------------------------------------------------- parsing
def parse_events(kafka_df, trip_schema, keep_meta=False):
    """Kafka (value binaire) -> colonnes typées. Écarte les lignes sans trip_id ni event_time."""
    from pyspark.sql import functions as F
    cols = ["t.*"] + (["partition", "offset"] if keep_meta else [])
    sel = [F.from_json(F.col("value").cast("string"), trip_schema).alias("t")]
    if keep_meta:
        sel += [F.col("partition"), F.col("offset")]
    out = (kafka_df.select(*sel).select(*cols)
           .withColumn("event_time", F.to_timestamp("event_time")))
    return out.filter(F.col("trip_id").isNotNull() & F.col("event_time").isNotNull())


def kafka_distinct_events(spark, end_offsets, trip_schema, topic="trips"):
    """Lecture BATCH bornée du topic (earliest -> end_offsets) : (nb messages, nb (trip_id,status) distincts)."""
    df = (spark.read.format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", topic)
          .option("startingOffsets", "earliest")
          .option("endingOffsets", json.dumps(end_offsets))
          .load())
    parsed = parse_events(df, trip_schema)
    return df.count(), parsed.select("trip_id", "status").distinct().count()


# ---------------------------------------------------------------- référentiel
def read_stations(spark, path="s3a://lake/reference/stations.json"):
    """Lit stations.json avec schéma explicite (JSON Lines, sinon tableau multi-lignes)."""
    from schemas import station_schema
    df = spark.read.schema(station_schema).json(path)
    if df.filter("station_id IS NOT NULL").count() == 0:
        df = spark.read.schema(station_schema).option("multiLine", True).json(path)
    return df
