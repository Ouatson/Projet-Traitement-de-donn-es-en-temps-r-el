# -*- coding: utf-8 -*-
import sys
import json
import csv
import urllib2
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import col, udf

reload(sys)
sys.setdefaultencoding('utf-8')

# --- CONFIGURATION ---
SUPABASE_URL = "https://nxywelcclgtbygtxwewe.supabase.co"
SUPABASE_KEY = "sb_secret_Y0uWXzya096RI_8Jo-U22w_zXqcrxdq"

KAFKA_BOOTSTRAP = "sandbox-hdp.hortonworks.com:6667"
INPUT_TOPIC = "customers-raw"
CITIES_FILE_PATH = "us_cities.csv"

# --- 1. FONCTION D'ENVOI SUPABASE (HTTP/443) ---
def send_batch_to_supabase(data_list, table_name):
    if not data_list:
        return

    url = "{}/rest/v1/{}".format(SUPABASE_URL, table_name)
    json_data = json.dumps(data_list)

    req = urllib2.Request(url, json_data)
    req.add_header('apikey', SUPABASE_KEY)
    req.add_header('Authorization', 'Bearer ' + SUPABASE_KEY)
    req.add_header('Content-Type', 'application/json')
    req.add_header('Prefer', 'return=minimal')

    try:
        urllib2.urlopen(req)
        print(">>> [SUPABASE] SUCCES: {} lignes envoyees a la table {}".format(len(data_list), table_name))
    except urllib2.HTTPError as e:
        print("!!! [SUPABASE] ERREUR HTTP {} sur {}: {}".format(e.code, table_name, e.read()))
    except Exception as e:
        print("!!! [SUPABASE] ERREUR: {}".format(str(e)))

# --- 2. FONCTION DE CHARGEMENT CSV ---
def load_us_cities():
    cities = set()
    try:
        with open(CITIES_FILE_PATH, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for row in reader:
                if len(row) > 0:
                    cities.add(row[0].strip().upper())
        cities.add("WASHINGTON") # Ajout manuel au cas où
        print(">>> [INIT] {} villes US chargees.".format(len(cities)))
        return cities
    except Exception as e:
        print("!!! ERREUR LECTURE CSV: " + str(e))
        return set()

# --- MAIN ---
def main():
    # Initialisation Spark
    spark = SparkSession.builder \
        .appName("KafkaToSupabaseSmart") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # 3. CHARGEMENT ET BROADCAST DU CSV
    # On le charge une seule fois sur le driver et on l'envoie aux workers
    cities_set = load_us_cities()
    cities_broadcast = sc.broadcast(cities_set)

    ssc = StreamingContext(sc, 5) # Batch de 5 secondes

    # Schema correspondant à ton Code B (avec 'location')
    schema = StructType([
        StructField("id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("age", StringType()),
        StructField("email", StringType()),
        StructField("location", StringType())
    ])

    # --- 4. LOGIQUE DE TRAITEMENT (Exécutée toutes les 5s) ---
    def process_partition(rdd):
        if rdd.isEmpty():
            return

        # Récupération de la session Spark (Singleton)
        spark_session = SparkSession.builder.getOrCreate()

        # Récupération de la liste des villes depuis le Broadcast
        valid_cities = cities_broadcast.value

        # --- DEFINITION DE L'UDF (Logique métier du Code B) ---
        def check_is_usa(location_str):
            if not location_str:
                return False
            loc_upper = location_str.upper().strip()

            # Vérification exacte
            if loc_upper in valid_cities:
                return True

            # Vérification partielle (ex: "New York, NY")
            parts = loc_upper.split(',')
            if len(parts) > 0:
                city_part = parts[0].strip()
                if city_part in valid_cities:
                    return True
            return False

        # Enregistrement UDF
        is_usa_udf = udf(check_is_usa, BooleanType())

        try:
            # Conversion RDD JSON -> DataFrame
            json_rdd = rdd.map(lambda x: x[1])
            df = spark_session.read.schema(schema).json(json_rdd)

            # Application du Filtre USA
            # On ajoute une colonne temporaire 'is_usa' pour filtrer
            df_tagged = df.withColumn("is_usa", is_usa_udf(col("location")))

            # Séparation des flux
            usa_df = df_tagged.filter(col("is_usa") == True).drop("is_usa")

            alerts_df = df_tagged.filter(
                (col("is_usa") == False) | (col("location").isNull())
            ).drop("is_usa")

            # --- ECRITURE SUPABASE ---

            # USA
            if usa_df.count() > 0:
                # Renommage de 'location' vers 'country' si ta table Supabase attend 'country'
                # Sinon laisse 'location'
                usa_final = usa_df.withColumnRenamed("location", "country")
                rows = usa_final.toJSON().map(lambda j: json.loads(j)).collect()
                send_batch_to_supabase(rows, "customers_usa")

            # ALERTS
            if alerts_df.count() > 0:
                alerts_final = alerts_df.withColumnRenamed("location", "country")
                rows = alerts_final.toJSON().map(lambda j: json.loads(j)).collect()
                send_batch_to_supabase(rows, "customers_alerts")

        except Exception as e:
            print("ERREUR PROCESS BATCH: {}".format(str(e)))

    # Configuration Kafka
    kafka_params = {
        "metadata.broker.list": KAFKA_BOOTSTRAP,
        "auto.offset.reset": "smallest"
    }

    # Lancement du Stream
    kafka_stream = KafkaUtils.createDirectStream(ssc, [INPUT_TOPIC], kafka_params)
    kafka_stream.foreachRDD(process_partition)

    print(">>> Démarrage du Streaming INTELLIGENT avec Lookup CSV...")
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
