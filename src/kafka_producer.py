# -*- coding: utf-8 -*-
import json
import csv
import boto3
from kafka import KafkaProducer
from botocore import UNSIGNED
from botocore.config import Config

# --- Configuration ---
KAFKA_BROKER = "sandbox-hdp.hortonworks.com:6667"
TOPIC_NAME = "customers-raw"
S3_BUCKET = "snowflake-assignments-mc"
S3_KEY = "gettingstarted/customers.csv"
LOCAL_FILE = "customers.csv"

def run_producer():
    # 1. Initialisation du Producer Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            # Le serializer s'attend maintenant à recevoir du propre
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print("Erreur : Impossible de se connecter a Kafka ({})".format(e))
        return

    # 2. Tentative de récupération des données (S3 puis Local)
    data_source = None

    # Tentative S3 Anonyme
    try:
        print("Tentative de lecture sur S3 (Mode Anonyme)...")
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)

        # CORRECTION ICI : On lit les bytes bruts (.read()), on ne fait PAS de .decode('utf-8')
        # Le module CSV de Python 2 veut des bytes, pas de l'unicode.
        data_source = response['Body'].read().splitlines()

        print("Succes : Donnees recuperees depuis S3.")
    except Exception as e:
        print("Echec S3 : {}. Passage au mode local...".format(e))
        # Fallback Local
        try:
            with open(LOCAL_FILE, 'r') as f:
                data_source = f.readlines() # open() en Python 2 lit deja des bytes
            print("Succes : Donnees recuperees depuis le fichier local.")
        except Exception as e_local:
            print("Erreur critique : Fichier local introuvable ({})".format(e_local))
            return

    # 3. Traitement et envoi des données
    if data_source:
        reader = csv.DictReader(data_source)

        count = 0
        for row in reader:
            try:
                # NETTOYAGE (Important pour Python 2) :
                # Le CSV a lu des bytes. Pour que le JSON soit propre dans Supabase,
                # on convertit les valeurs (bytes) -> Unicode (utf-8) ici.
                clean_row = {}
                for k, v in row.iteritems():
                    # On décode si c'est une chaine de bytes
                    if isinstance(v, str):
                        clean_row[k] = v.decode('utf-8')
                    else:
                        clean_row[k] = v

                # Envoi vers le topic customers-raw
                producer.send(TOPIC_NAME, value=clean_row)
                count += 1

                # Petit log pour voir que ça avance
                if count % 10 == 0:
                    print("Envoye {} lignes...".format(count))

            except Exception as e_send:
                print("Erreur lors de l'envoi d'une ligne : {}".format(e_send))

        producer.flush()
        producer.close()
        print("Termine : {} messages envoyes vers le topic '{}'.".format(count, TOPIC_NAME))

if __name__ == "__main__":
    run_producer()
