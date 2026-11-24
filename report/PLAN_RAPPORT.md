# Plan du rapport PDF (10 pages maximum) — Pedali.ma

> Règle : captures **petites et recadrées** ; chaque capture a une légende et est commentée en une phrase. Les chiffres viennent de VOS mesures.

| Section | Pages | Contenu |
|---|---|---|
| Page de garde + architecture | 0,5 | Binôme, schéma Pedali → Kafka → Spark → MinIO (Bronze/Silver/Gold) → GraphX/GraphFrames |
| 1. Cluster | 0,75 | Rôles Driver / Executors / Cluster Manager / SparkContext dans VOTRE déploiement ; 2 limites de MapReduce reliées au projet |
| 2. RDD | 1,25 | Tableaux CA par ville, durée moyenne, top 5 ; lineage ; DAG + nb de stages ; shuffle de `reduceByKey` ; lineage et tolérance aux pannes |
| 3. DataFrames | 1,75 | Résultats identiques à la Partie 2 ; UDF vs `when/otherwise` (temps + pourquoi) ; `explain(True)` commenté (logique / optimisé Catalyst / BroadcastHashJoin) ; tableau RDD vs DataFrame vs Dataset avec VOS temps |
| 4. Streaming | 1,25 | Modèle table infinie ; micro-batch vs continu ; 3 modes de sortie ; watermark + exemple d'événement à −15 min tiré de vos logs |
| 5. Production | 1,75 | Dédoublonnage (nb de doublons retirés) ; fenêtres tumbling/sliding/session ; Gold Delta + DESCRIBE HISTORY ; reprise sur offset (avant/après) ; exactly-once ; pourquoi la console ne l'offre pas |
| 6. Graphes | 2,5 | Schémas sommets/arêtes ; captures GraphFrames et GraphX ; seuils explicites ; liste des stations à rééquilibrer ; scénarios retrouvés ; comparaison ; limites ; versions + lancement Scala |
| Marge | 0,25 | |

## Captures obligatoires (cocher)
**Jalon 1**
- [ ] Spark Master UI : 2 Workers actifs
- [ ] MinIO : bucket `lake`
- [ ] DAG + stages (Partie 2)
- [ ] DAG / SQL + stages (Partie 3)
- [ ] `explain(True)` (Partie 3)
- [ ] `mc ls --recursive local/lake/` (Bronze history, Silver trips_history)

**Jalon 2**
- [ ] Sink console avec plusieurs micro-batchs (Partie 4)
- [ ] Bronze : fichiers JSON + checkpoint
- [ ] `lastProgress` avec watermark + événement en retard
- [ ] `mc ls --recursive local/lake/` : Bronze + Silver + Gold
- [ ] `DESCRIBE HISTORY` sur la table Delta Gold
- [ ] `kafka-consumer-groups.sh --describe` (+ `kafka-get-offsets`)
- [ ] Contenu de `checkpoints/.../offsets` + comptage avant/après redémarrage
- [ ] Partie 6 : modèle du graphe ; motif GraphFrames (aller-retour ou boucle de 3) ; PageRank GraphX ; liste des stations à rééquilibrer ; comparaison des degrés ; exports Parquet ; visualisation d'une ville

## Limites à citer (honnêteté = points)
- Petit volume (50 000 trajets) : écarts de performance RDD/DataFrame/UDF faibles.
- Date d'un trajet = événement ENDED (arrivée) : les périodes matin/soir décrivent les arrivées.
- PageRank non pondéré (GraphX et GraphFrames) ; mesure l'attractivité, pas la correspondance.
- Seuils de rééquilibrage = percentiles de l'historique, à recalibrer avec des données réelles.
- Pas de réplication Kafka / un seul broker (preuve de concept).
