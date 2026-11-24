# Pedali.ma — Projet Big Data

**Projet Écosystème Big Data 2 — EMSI, 5ème année — 2026/2027**

Binôme : Kourdi Hasnae et Benzine Aya

## Jalon 1 — Cluster Spark, RDD et DataFrames

Ce dépôt présente les trois premières parties du projet Pedali.ma, une plateforme Big Data dédiée à l'analyse de la mobilité partagée.

### 1. Environnement technique

- Apache Spark 3.5.3
- Docker Desktop et Docker Compose
- Jupyter Notebook
- Python
- Apache Hadoop / S3A
- MinIO

### 2. Organisation du projet

```text
pedali-bigdata/
├── docker-compose.yml
├── docker-compose.dockerhub.yml
├── notebooks/
│   ├── 01_cluster_setup.ipynb
│   ├── 02_rdd_batch.ipynb
│   ├── 03_dataframes.ipynb
│   ├── common.py
│   ├── schemas.py
│   └── utils.py
├── scripts/
│   └── proofs.ps1
└── report/
    └── PLAN_RAPPORT.md
```

### 3. Travaux réalisés

**Partie 1 — Configuration du cluster**

- Configuration du cluster Spark 3.5.3.
- Connexion au Spark Master.
- Vérification des ressources et des workers.
- Exécution d'un calcul distribué : somme de 1 à 1 000 000.

**Partie 2 — Traitement avec RDD**

- Calcul du chiffre d'affaires par ville.
- Durée moyenne par ville et véhicule.
- Identification des 5 stations principales.
- Analyse du lineage et des doublons.

**Partie 3 — Traitement avec DataFrames**

- Analyse des données avec Spark SQL.
- Vérification de la cohérence du chiffre d'affaires avec les RDD.
- Identification des 400 stations.
- Stockage des données Silver au format Parquet.
- Analyse du plan d'exécution avec `explain(True)` et BroadcastHashJoin.

### 4. Exécution

Démarrer le cluster depuis PowerShell :

```powershell
docker compose up -d
docker compose ps
```

Accéder aux interfaces :

- Spark Master : http://localhost:8080
- Spark UI : http://localhost:4040
- Jupyter : http://localhost:8888
- MinIO : http://localhost:9001

Exécuter les notebooks dans l'ordre : 01, 02 puis 03.

### 5. Rapport et preuves

Le dossier `report/` contient le plan du rapport du Jalon 1.

Le script `scripts/proofs.ps1` permet de récupérer les preuves de stockage MinIO.

**Livrable : Jalon 1 — Parties 1, 2 et 3.**