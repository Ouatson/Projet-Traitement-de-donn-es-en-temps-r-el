# -*- coding: utf-8 -*-
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, upper, udf
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# --- Configuration ---
KAFKA_BOOTSTRAP = "sandbox-hdp.hortonworks.com:6667"
INPUT_TOPIC = "customers-raw"
OUTPUT_TOPIC_USA = "customers-usa"
OUTPUT_TOPIC_ALERTS = "customers-alerts"
HDFS_PATH_USA = "/user/maria_dev/customers_usa/"
HDFS_PATH_ALERTS = "/user/maria_dev/customers_alerts/"
CHECKPOINT_DIR = "/tmp/checkpoint_customers/"
CITIES_FILE_PATH = "../data/us_cities.csv"

# --- CHARGEMENT DU RÉFÉRENTIEL (LOOKUP TABLE) ---
print("Chargement de la liste des villes US...")
us_cities_set = set()
try:
    with open(CITIES_FILE_PATH, 'r') as f:
        reader = csv.reader(f, delimiter='|') # Le fichier source utilise '|' comme séparateur
        for row in reader:
            # Le format est : City|State short|State full|County|City Alias
            # On prend la première colonne (City) et on la met en majuscule
            if len(row) > 0:
                us_cities_set.add(row[0].strip().upper())

    us_cities_set.add("WASHINGTON")
    print("Succes : " + str(len(us_cities_set)) + " villes US chargees en memoire.")
except Exception as e:
    print("ERREUR CRITIQUE : Impossible de lire le fichier des villes : " + str(e))

# Fonction de vérification (UDF)
def check_is_usa_dynamic(location_str):
    if not location_str:
        return False

    # Nettoyage de la donnée entrante
    loc_upper = location_str.upper().strip()

    # 1. Vérification exacte
    if loc_upper in us_cities_set:
        return True

    # 2. Vérification partielle
    # On découpe par la virgule pour voir si la partie gauche est une ville connue
    parts = loc_upper.split(',')
    if len(parts) > 0:
        city_part = parts[0].strip()
        if city_part in us_cities_set:
            return True

    return False

# Enregistrement UDF
is_usa_udf = udf(check_is_usa_dynamic, BooleanType())

# --- Main ---
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("email", StringType(), True),
    StructField("location", StringType(), True)
])

# 1. Lecture
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 2. Parsing et Préparation des colonnes
parsed_stream = raw_stream.select(
    col("value"),
    from_json(col("value").cast("string"), schema).alias("p")
).select(
    col("value"),
    col("p"),
    col("p.*"),
    col("p.location").alias("country")
)

# 3. Séparation des flux

# --- FLUX USA : Données valides ET location contient une des cités des Etats-Unis ---

usa_stream = parsed_stream.filter(
    (col("id").isNotNull()) &
    (is_usa_udf(col("country")))
)

# A. Sortie HDFS USA
query_usa_hdfs = usa_stream.select("id", "first_name", "last_name", "age", "email", "country") \
    .writeStream \
    .format("json") \
    .option("path", HDFS_PATH_USA) \
    .option("checkpointLocation", CHECKPOINT_DIR + "usa_hdfs") \
    .start()

# B. Sortie KAFKA USA
query_usa_kafka = usa_stream.selectExpr("to_json(struct(id, first_name, last_name, age, email, country)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", OUTPUT_TOPIC_USA) \
    .option("checkpointLocation", CHECKPOINT_DIR + "usa_kafka") \
    .start()


# --- FLUX ALERTS : Données corrompues ou location vide ---
alerts_stream = parsed_stream.filter(
    (col("p").isNull()) |
    (col("country").isNull()) |
    (col("country") == "")
)

# C. Sortie HDFS ALERTS
query_alerts_hdfs = alerts_stream.writeStream \
    .format("json") \
    .option("path", HDFS_PATH_ALERTS) \
    .option("checkpointLocation", CHECKPOINT_DIR + "alerts_hdfs") \
    .start()

# D. Sortie KAFKA ALERTS
query_alerts_kafka = alerts_stream.selectExpr("CAST(value AS STRING) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", OUTPUT_TOPIC_ALERTS) \
    .option("checkpointLocation", CHECKPOINT_DIR + "alerts_kafka") \
    .start()

# Attendre que les flux terminent
spark.streams.awaitAnyTermination()
