# 🚀 Pipeline Data Streaming : Ingestion Clients (Kafka & Spark)

Ce projet implémente un pipeline de données **Temps Réel** (Real-Time) robuste sur un environnement Big Data (HDP Sandbox). Il simule l'arrivée continue de nouveaux clients, les ingère via Kafka, les traite avec Spark Structured Streaming, et les stocke sur HDFS selon leur validité. Avec une partie de liaison à une base de données en ligne, SupaBase, dont le lien est le suivant : https://supabase.com/dashboard/org/tvugxmxqzkwjuphokzai.

## 📋 Architecture du Pipeline

Le flux de données traverse les composants suivants :

1.  **Source** : Fichier CSV (`customers.csv`) simulant une base de données source.
2.  **Ingestion (Producer)** : Script Python (`kafka_producer.py`) qui publie les enregistrements en JSON dans **Kafka**.
3.  **Traitement (Spark Engine)** : Job Spark Streaming (`spark_streaming_job_kafka_hdfs.py`) qui :
    * Lit le flux Kafka en continu (`readStream`).
    * Parse la structure JSON et nettoie les types de données.
    * Filtre les données : sépare les clients "USA" valides des données incomplètes ("Alerts").
4.  **Stockage (HDFS)** :
    * `/user/maria_dev/customers_usa` : Données propres (Parquet/JSON).
    * `/user/maria_dev/customers_alerts` : Rejets et erreurs pour analyse.
5.  **Stockage BD (SupaBase)** : Job Spark Streaming (`spark_streaming_job_db.py`) qui envoie les messages kafka dans une base de donnée **SupaBase**.

---

## 📂 Structure du Projet

Voici l'organisation recommandée des fichiers pour ce projet :

```text
customer-streaming-pipeline/
│
├── data/
│   └── customers.csv                      # Fichier source (Dataset)
│
├── jars/                                  # Dépendances Java pour Spark
│   ├── kafka-clients-1.1.1.jar
│   └── spark-sql-kafka-0-10_2.11-2.3.2.jar
│
├── src/
│   ├── kafka_producer.py                  # Le Producer Kafka (Python)
│   ├── spark_streaming_job_kafka_hdfs.py  # Le Job Spark Streaming (Pyspark) KAFKA HDFS
│   └── spark_streaming_job_db.py          # Le Job Spark Streaming (Pyspark) KAFKA SUPABASE
│
├── scripts/
│   └── reset_environment.sh      # Script de nettoyage (HDFS + Checkpoints)
│
└── README.md                     # Documentation du projet
```

---

## 📦 Gestion des Dépendances Python (PIP)

Sur la Sandbox HDP, il est nécessaire d'installer manuellement le gestionnaire de paquets `pip` pour pouvoir installer les librairies Python comme `kafka-python`.

### 1. Installation manuelle de PIP
Nous utilisons le script officiel d'installation (bootstrap) :

```bash
# 1. Télécharger le script d'installation
curl "https://bootstrap.pypa.io/pip/2.7/get-pip.py" -o "get-pip.py"

# 2. Exécuter le script avec Python (root ou sudo peut être requis selon les droits)
python get-pip.py

# 3. Vérifier l'installation
pip --version
```
*(Note : Si vous utilisez Python 3, remplacez `python` par `python3`)*

### 2. Installation des librairies du projet
Une fois `pip` installé, installez le connecteur Kafka nécessaire au script `kafka_producer.py`.

```bash
pip install kafka-python==2.0.2
```

---

## 🛠️ Pré-requis

* **Environnement** : Hortonworks Data Platform (HDP Sandbox) ou Cluster Spark/Kafka.
* **Spark** : Version 2.3+ (Compatible Structured Streaming).
* **Kafka** : Topic configuré.
* **Python** : 2.7 (dans notre sandbox).
* **Postgres** : Supabase directement.

---

## 🚀 Installation et Démarrage

### 1. Configuration de Kafka
Création du topic qui recevra les données brutes :

```bash
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh \\
  --create \\
  --zookeeper sandbox-hdp.hortonworks.com:2181 \\
  --replication-factor 1 \\
  --partitions 1 \\
  --topic customers-raw
```

Et des deux autres topic (customers-raw ainsi que customers-alerts) de la même manière.

### 2. Exécution du Job Spark
#### 1. Dans le cas de KAFKA ou HDFS
Soumettez le job à YARN ou en local via `spark-submit`. Notez l'utilisation des `.jars` pour le connecteur Kafka.

```bash
spark-submit \\
  --jars jars/spark-sql-kafka-0-10_2.11-2.3.2.jar,jars/kafka-clients-1.1.1.jar \\
  spark_streaming_job_kafka_hdfs.py
```

#### 2. Dans le cas d'une insertion en base de données
Soumettez le job à YARN ou en local via `spark-submit`. Notez l'utilisation des `packages` pour spark.

```bash
spark-submit \\
  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 \\
  spark_streaming_job_db.py
```

### 3. Démarrage du Producer
Ce script va lire le fichier CSV depuis S3 avec boto3 et envoyer les messages un par un dans Kafka (le topic customers-raw).

```bash
python src/kafka_producer.py
```

---

## ⚙️ Configuration Technique & Robustesse

Ce projet a été configuré pour gérer les erreurs courantes :

### 1. Gestion des Pertes de Données (Data Loss)
Kafka peut supprimer des anciens messages (rétention) avant que Spark ne les lise. Pour éviter que le job ne crash avec une erreur `OffsetOutOfRangeException`, nous utilisons :

```python
.option("failOnDataLoss", "false")
```

### 2. Checkpointing (Tolérance aux pannes)
Spark utilise des dossiers de checkpoints locaux pour sauvegarder l'état du flux (offsets).
* **Chemin** : `/tmp/checkpoint_customers_...`
Cela garantit la sémantique **"Exactly-Once"** (aucun doublon, aucune perte) en cas de redémarrage.

---

## 🧹 Procédure de Reset (Dépannage)

Si vous rencontrez des erreurs de type `Metadata Log` ou `IllegalStateException` (conflit entre le checkpoint et HDFS), ou si vous souhaitez relancer le traitement depuis le début (offset 0), **suivez impérativement cette procédure de nettoyage** :

Pour ne pas se compliquer la tâche juste lancer le script : `reset_environnements.sh`
```bash
sh scripts/reset_environnements.sh
```

- Voici le detail des actions pour son exécution correcte

**1. Arrêter le Producer et le Job Spark (Ctrl+C).**

**2. Supprimer les métadonnées locales (Le Cerveau) :**
```bash
rm -rf /tmp/checkpoint_customers_usa
rm -rf /tmp/checkpoint_customers_alerts
```

**3. Supprimer les données sur HDFS (La Destination) :**
```bash
hdfs dfs -rm -r /user/maria_dev/customers_usa
hdfs dfs -rm -r /user/maria_dev/customers_alerts
```

**4. Relancer le Producer puis le Job Spark.**

---

## 📊 Vérification des Résultats

Pour vérifier que les données arrivent bien sur HDFS :

```bash
# Lister les fichiers
hdfs dfs -ls /user/maria_dev/customers_usa

# Lire le contenu d'un fichier (exemple)
hdfs dfs -cat /user/maria_dev/customers_usa/part-00000-....json
```
