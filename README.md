# 🚀 Pipeline Data Streaming : Ingestion Clients (Kafka & Spark)

Ce projet implémente un pipeline de données **Temps Réel** (Real-Time) robuste sur un environnement Big Data (HDP Sandbox). Il simule l'arrivée continue de nouveaux clients, les ingère via Kafka, les traite avec Spark Structured Streaming, et les stocke sur HDFS selon leur validité.

## 📋 Architecture du Pipeline

Le flux de données traverse les composants suivants :

1.  **Source** : Fichier CSV (\`customers.csv\`) simulant une base de données source.
2.  **Ingestion (Producer)** : Script Python (\`kafka_producer.py\`) qui publie les enregistrements en JSON dans **Kafka**.
3.  **Traitement (Spark Engine)** : Job Spark Streaming (\`spark_streaming_job.py\`) qui :
    * Lit le flux Kafka en continu (\`readStream\`).
    * Parse la structure JSON et nettoie les types de données.
    * Filtre les données : sépare les clients "USA" valides des données incomplètes ("Alerts").
4.  **Stockage (HDFS)** :
    * \`/user/maria_dev/customers_usa_v2\` : Données propres (Parquet/JSON).
    * \`/user/maria_dev/customers_alerts_v2\` : Rejets et erreurs pour analyse.

---

## 📂 Structure du Projet

Voici l'organisation recommandée des fichiers pour ce projet :

customer-streaming-pipeline/
│
├── data/
│   └── customers.csv            # Données sources (échantillon)
│
├── jars/
│   ├── kafka-clients-1.1.1.jar
│   └── spark-sql-kafka-0-10_2.11-2.3.2.jar
│
├── src/
│   ├── kafka_producer.py        # Le Producer (envoie les données à Kafka)
│   └── spark_streaming_job.py   # Le Job Spark (traitement temps réel)
│
├── scripts/
│   ├── create_topic.sh          # Commande pour créer le topic Kafka
│   └── clean_env.sh             # Script de nettoyage HDFS/Checkpoints (utile en dev)
│
├── requirements.txt             # Dépendances Python (kafka-python, etc.)
├── .gitignore                   # Ignorer les fichiers temporaires (.pyc, tmp, .idea)
└── README.md                    # Ce fichier

---

## 🛠️ Pré-requis

* **Environnement** : Hortonworks Data Platform (HDP Sandbox) ou Cluster Spark/Kafka.
* **Spark** : Version 2.3+ (Compatible Structured Streaming).
* **Kafka** : Topic configuré.
* **Python** : 2.7 ou 3.6+.

---

## 🚀 Installation et Démarrage

### 1. Configuration de Kafka
Création du topic qui recevra les données brutes :

\`\`\`bash
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh \\
  --create \\
  --zookeeper sandbox-hdp.hortonworks.com:2181 \\
  --replication-factor 1 \\
  --partitions 1 \\
  --topic customers-raw
\`\`\`

### 2. Démarrage du Producer
Ce script va lire le fichier CSV et envoyer les messages un par un dans Kafka.

\`\`\`bash
python kafka_producer.py
\`\`\`
*Laissez ce terminal ouvert ou lancez-le en arrière-plan.*

### 3. Exécution du Job Spark
Soumettez le job à YARN ou en local via \`spark-submit\`. Notez l'utilisation des \`.jars\` pour le connecteur Kafka.

\`\`\`bash
spark-submit \\
  --jars spark-sql-kafka-0-10_2.11-2.3.2.jar,kafka-clients-1.1.1.jar \\
  python_spark_job.py
\`\`\`

---

## ⚙️ Configuration Technique & Robustesse

Ce projet a été configuré pour gérer les erreurs courantes en production :

### 1. Gestion des Pertes de Données (Data Loss)
Kafka peut supprimer des anciens messages (rétention) avant que Spark ne les lise. Pour éviter que le job ne crash avec une erreur \`OffsetOutOfRangeException\`, nous utilisons :
\`\`\`python
.option("failOnDataLoss", "false")
\`\`\`

### 2. Checkpointing (Tolérance aux pannes)
Spark utilise des dossiers de checkpoints locaux pour sauvegarder l'état du flux (offsets).
* **Chemin** : \`/tmp/checkpoint_customers_...\`
Cela garantit la sémantique **"Exactly-Once"** (aucun doublon, aucune perte) en cas de redémarrage.

---

## 🧹 Procédure de Reset (Dépannage)

Si vous rencontrez des erreurs de type \`Metadata Log\` ou \`IllegalStateException\` (conflit entre le checkpoint et HDFS), ou si vous souhaitez relancer le traitement depuis le début (offset 0), **suivez impérativement cette procédure de nettoyage** :

**1. Arrêter le Producer et le Job Spark (Ctrl+C).**

**2. Supprimer les métadonnées locales (Le Cerveau) :**
\`\`\`bash
rm -rf /tmp/checkpoint_customers_usa_v2
rm -rf /tmp/checkpoint_customers_alerts_v2
\`\`\`

**3. Supprimer les données sur HDFS (La Destination) :**
\`\`\`bash
hdfs dfs -rm -r /user/maria_dev/customers_usa_v2
hdfs dfs -rm -r /user/maria_dev/customers_alerts_v2
\`\`\`

**4. Relancer le Producer puis le Job Spark.**

---

## 📊 Vérification des Résultats

Pour vérifier que les données arrivent bien sur HDFS :

\`\`\`bash
# Lister les fichiers
hdfs dfs -ls /user/maria_dev/customers_usa_v2

# Lire le contenu d'un fichier (exemple)
hdfs dfs -cat /user/maria_dev/customers_usa_v2/part-00000-....json
\`\`\`
