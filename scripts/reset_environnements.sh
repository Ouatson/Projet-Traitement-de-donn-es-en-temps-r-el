#!/bin/bash
echo "Nettoyage de l'environnement..."

# Suppression des Checkpoints locaux
rm -rf /tmp/checkpoint_customers_usa
rm -rf /tmp/checkpoint_customers_alerts

echo "Checkpoints locaux supprimes."

# Suppression des données HDFS
hdfs dfs -rm -r /user/maria_dev/customers_usa
hdfs dfs -rm -r /user/maria_dev/customers_alerts

echo "Donnees HDFS supprimees."

echo "Pret pour un nouveau test !"