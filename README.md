<<<<<<< HEAD
# 🚀 Big Data Pipeline — Fraud Detection

> Pipeline de détection de fraude financière end-to-end sur cluster Hadoop/Spark/YARN (Docker)

![Hadoop](https://img.shields.io/badge/Hadoop-3.2.1-blue) ![Spark](https://img.shields.io/badge/Spark-3.5.1-orange) ![Hive](https://img.shields.io/badge/Hive-2.3.2-yellow) ![MLlib](https://img.shields.io/badge/MLlib-RandomForest-green) ![Docker](https://img.shields.io/badge/Docker-Compose-blue) ![Python](https://img.shields.io/badge/Python-3.8-blue)

---

## 📋 Description

Ce projet implémente un pipeline Big Data complet de bout en bout :
- **Ingestion** de 555 000 transactions financières réelles dans HDFS
- **Analyse SQL distribuée** avec Spark SQL
- **Entrepôt de données** Apache Hive
- **Modèle ML distribué** avec Spark MLlib (Random Forest — AUC=0.98)

Toute l'infrastructure tourne sur **Docker** — déployable en une commande.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Cluster Docker                        │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────────┐ │
│  │NameNode  │  │DataNode  │  │  ResourceManager YARN  │ │
│  │ (HDFS)   │  │ (HDFS)   │  │  + NodeManager        │ │
│  └──────────┘  └──────────┘  └───────────────────────┘ │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────────┐ │
│  │  Hive    │  │  Hive    │  │   Apache Spark 3.5.1  │ │
│  │ Metastore│  │ Server2  │  │   (PySpark + MLlib)   │ │
│  └──────────┘  └──────────┘  └───────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

---

## 🛠️ Stack Technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Stockage distribué | Apache Hadoop HDFS | 3.2.1 |
| Gestion ressources | YARN (ResourceManager + NodeManager) | 3.2.1 |
| Traitement distribué | Apache Spark / PySpark | 3.5.1 |
| Requêtes SQL | Spark SQL | 3.5.1 |
| Data Warehouse | Apache Hive + PostgreSQL Metastore | 2.3.2 |
| Machine Learning | Spark MLlib (Random Forest) | 3.5.1 |
| Infrastructure | Docker + Docker Compose | — |
| Format de sortie | Parquet + Snappy | — |
| Langage | Python | 3.8 |

---

## 📊 Dataset

- **Source** : [Credit Card Fraud Detection — Kaggle](https://www.kaggle.com/datasets/kartik2112/fraud-detection)
- **Volume** : 555 719 transactions financières (150 MB)
- **Colonnes clés** : merchant, category, amt, city, state, is_fraud
- **Taux de fraude** : 0.39% (dataset très déséquilibré)

---

## 🚀 Démarrage Rapide

### Prérequis
- Docker Desktop installé et en cours d'exécution
- 8 GB RAM disponibles
- 10 GB d'espace disque

### Lancement du cluster

```bash
git clone https://github.com/<votre-username>/bigdata-fraud-detection
cd bigdata-fraud-detection
docker compose up -d
docker ps
```

### Vérification

| Interface | URL |
|-----------|-----|
| Hadoop HDFS | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
| Hive Server | jdbc:hive2://localhost:10000 |

---

## 📁 Structure du Projet

```
bigdata-fraud-detection/
├── docker-compose.yml          # Infrastructure complète
├── configs/
│   ├── core-site.xml           # Config Hadoop/HDFS
│   └── yarn-site.xml           # Config YARN
├── data/
│   └── fraudTest.csv           # Dataset (à télécharger depuis Kaggle)
├── notebooks/
│   ├── 01_wordcount.py         # Test initial WordCount
│   ├── 02_spark_sql.py         # Analyse Spark SQL
│   └── 03_mllib_fraud.py       # Modèle Random Forest
├── docs/
│   ├── Etape1_SparkSQL.docx
│   ├── Etape2_YARN.docx
│   ├── Etape3_Hive.docx
│   └── Etape4_MLlib.docx
└── README.md
```

---

## 📈 Résultats MLlib

### Métriques du modèle Random Forest

| Métrique | Valeur |
|----------|--------|
| AUC (Area Under ROC) | **0.9838** |
| Précision globale | **99.8%** |
| F1-Score | **0.9979** |

### Matrice de confusion (111 332 transactions test)

| | Prédit : Légitime | Prédit : Fraude |
|---|---|---|
| **Réel : Légitime** | 110 844 ✅ | 66 ❌ |
| **Réel : Fraude** | 156 ❌ | 266 ✅ |

### Importance des features

```
amt (montant)    ████████████████████████████ 53.6%
category         █████████████ 24.9%
state            ████ 8.5%
city_pop         ██ 4.7%
merch_lat        ██ 3.7%
merch_long       ██ 3.4%
gender           █ 1.3%
```

---

## 🔄 Pipeline Complet

```
CSV (Kaggle)
    │
    ▼
HDFS /data/transactions/          ← Ingestion
    │
    ├──► Spark SQL                 ← Analyse analytique
    │    └── Parquet /output/sql/
    │
    ├──► Hive (fraud_db)          ← Data Warehouse
    │    └── EXTERNAL TABLE
    │
    └──► MLlib Pipeline           ← Machine Learning
         ├── StringIndexer
         ├── VectorAssembler
         ├── RandomForestClassifier
         └── Modèle /output/fraud_model
```

---

## 📝 Étapes Détaillées

### Étape 1 — WordCount (validation)
```python
sc.textFile("hdfs://namenode:8020/input/words.txt") \
  .flatMap(lambda line: line.split(" ")) \
  .map(lambda word: (word, 1)) \
  .reduceByKey(lambda a, b: a + b) \
  .collect()
```

### Étape 2 — Spark SQL
```python
df.createOrReplaceTempView("transactions")
spark.sql("""
    SELECT category, COUNT(*) as nb_fraudes, ROUND(AVG(amt), 2) as montant_moyen
    FROM transactions WHERE is_fraud = 1
    GROUP BY category ORDER BY nb_fraudes DESC
""").show()
```

### Étape 3 — Hive
```sql
CREATE EXTERNAL TABLE transactions (...)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'hdfs://namenode:8020/data/transactions/';
```

### Étape 4 — MLlib
```python
pipeline = Pipeline(stages=[
    StringIndexer(inputCol="category", outputCol="category_idx"),
    VectorAssembler(inputCols=["amt", "category_idx", ...], outputCol="features"),
    RandomForestClassifier(numTrees=50, maxDepth=10, labelCol="is_fraud")
])
model = pipeline.fit(train)
# AUC = 0.9838
```

---

## 🔑 Insights Business

| Insight | Détail |
|---------|--------|
| Taux de fraude | 0.39% — dataset très déséquilibré |
| Catégorie la plus risquée | shopping_net : montant fraude ×13.8 vs légitime |
| Signal principal | Montant (amt) explique 53.6% du pouvoir prédictif |
| État le plus touché | New York : 95 fraudes, 49 446$ de pertes |
| Test de carte | gas_transport : fraudes à 12$ en moyenne |

---

## 👤 Auteur

**DAOUDA** — Data Engineer

---

## 📄 Licence

MIT License — libre d'utilisation pour des fins éducatives et professionnelles.
=======
# hadoop-spark-docker
Pipeline Big Data complet pour l’analyse de données financières et la détection de fraude. Basé sur Hadoop/Spark (Docker), il couvre ingestion dans HDFS, traitement avec Spark SQL, orchestration via YARN, Data Warehouse avec Hive et Machine Learning avec MLlib.
>>>>>>> 12055a02504c77b499c803204aa9d31acac3d63a
