# Pedali.ma — Plateforme Big Data (mobilité partagée)

Projet Écosystème Big Data 2 — EMSI 5ème année — 2026/2027
Binôme : kourdi hasnae et benzine aya 

Chaîne : `producer.py` → **Kafka** → **Spark Structured Streaming** → **MinIO** (Bronze / Silver / Gold) → **GraphX / GraphFrames**.

## 1. Versions et dépendances

| Composant | Version | Remarque |
|---|---|---|
| Docker Desktop / Compose | récent | 6 Go de RAM alloués minimum |
| Apache Spark | 3.5.3 (Scala 2.12) | image `quay.io/jupyter/pyspark-notebook:spark-3.5.3` (Master, 2 Workers, Driver) |
| Apache Kafka | 3.7.0, KRaft | image `apache/kafka:3.7.0` |
| MinIO | latest | image `quay.io/minio/minio` ; API :9000, console :9001 |
| hadoop-aws / aws-java-sdk-bundle | 3.3.4 / 1.12.262 | connecteur S3A |
| spark-sql-kafka-0-10 | 3.5.3 (_2.12) | Parties 4-5 |
| delta-spark | 3.2.0 (_2.12) | Partie 5 (Gold) |
| GraphFrames | 0.8.3-spark3.5-s_2.12 | Partie 6 ; dépôt `https://repos.spark-packages.org/` ; `pip install graphframes` dans le conteneur |
| GraphX | inclus dans Spark 3.5.3 | lancé en Scala via `spark-shell -i` |

## 2. Arborescence

```
pedali-bigdata/
├── docker-compose.yml
├── data/        producer.py, stations.json (trips_history.jsonl ignoré par git)
├── notebooks/   01..07 + common.py, schemas.py, utils.py
├── graphx/      graphx_analysis.scala
├── scripts/     run_graphx.ps1, reset_kafka.ps1, proofs.ps1
└── report/      captures + rapport PDF
```

## 3. Lancement (PowerShell, Windows)

```powershell
docker compose up -d
docker compose ps
docker logs kafka-init          # topic trips, 3 partitions
```
Créer le bucket `lake` dans la console MinIO (http://localhost:9001, minioadmin / minioadmin).

| Service | URL |
|---|---|
| Spark Master | http://localhost:8080 (2 Workers) |
| Spark UI application | http://localhost:4040 |
| MinIO | http://localhost:9001 |
| Jupyter | http://localhost:8888 (token `pedali`) |

## 4. Données

```powershell
docker exec -it jupyter python /home/jovyan/data/producer.py --mode file --n 50000
```
Puis déposer via la console MinIO : `data/trips_history.jsonl` → `lake/bronze/history/` et `data/stations.json` → `lake/reference/`.

## 5. Exécution, dans l'ordre

**Un seul notebook Spark actif à la fois** (Kernel → Shut Down Kernel après chacun : chaque application réserve les 4 cœurs).

| # | Notebook | Partie |
|---|---|---|
| 1 | `01_cluster_setup` | Cluster et SparkSession |
| 2 | `02_rdd_batch` | RDD |
| 3 | `03_dataframes` | DataFrames, SQL, UDF, Silver, `explain` |
| 4 | `04_streaming_kafka` | Streaming (producteur `--mode kafka --rate 20` dans un autre terminal) |
| 5 | `05_pipeline_production` | Pipeline Bronze/Silver/Gold, fenêtres, reprise (`.\scripts\reset_kafka.ps1` avant) |
| 6 | `06_graphframes` | GraphFrames (`pip install graphframes` dans le conteneur) |
| 7 | `.\scripts\run_graphx.ps1` | GraphX (Scala) |
| 8 | `07_comparaison_graphx_graphframes` | Comparaison |

Preuves (MinIO, Kafka) : `.\scripts\proofs.ps1`.

## 6. Dépannage

| Symptôme | Solution |
|---|---|
| « Initial job has not accepted any resources » | Un autre kernel utilise les cœurs : l'arrêter |
| Workers absents sur :8080 | `docker compose restart spark-worker-1 spark-worker-2` |
| `ClassNotFoundException: S3AFileSystem` | Relancer la cellule de création de la SparkSession (téléchargement Maven) |
| Sink fichier n'écrit rien après suppression d'un checkpoint | Supprimer aussi le dossier de sortie (`RESET = True` dans le notebook 05) |
| Repartir de zéro | `docker compose down -v && docker compose up -d` |

## 7. Livrables
Rapport PDF dans `report/`. Jalon 1 : 22/11/2026 (Parties 1-3). Jalon 2 : 20/12/2026 (Parties 4-6 + soutenance).
