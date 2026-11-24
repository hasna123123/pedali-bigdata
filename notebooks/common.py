"""Module partagé : création de la SparkSession connectée au cluster Pedali."""
import pyspark
from pyspark.sql import SparkSession

BASE_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]


def get_spark(app_name, extra_packages=None, extra_conf=None, cores_max=4):
    """SparkSession sur le cluster standalone + accès MinIO (S3A).

    extra_packages : coordonnées Maven supplémentaires (Kafka, Delta, GraphFrames...)
    extra_conf     : dict de configurations supplémentaires
    """
    packages = BASE_PACKAGES + (extra_packages or [])
    builder = (SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.driver.host", "jupyter")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", str(cores_max))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def kafka_package():
    """Connecteur Kafka aligné sur la version de Spark de l'image (3.5.0 ou 3.5.3...)."""
    return f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
