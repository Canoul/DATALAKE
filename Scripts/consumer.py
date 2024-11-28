from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson import ObjectId  # Utilisation de bson pour ObjectId

import json

# Configuration du consommateur Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest',  # Commencer depuis le début des messages
}

# Connexion à MongoDB Atlas
client = MongoClient(
    "mongodb+srv://atlasdatalake.livy2.mongodb.net/",
    username="geoffreyboilay",
    password="0KEiCgjtSFemIA3R",
    server_api=ServerApi('1')  # Configuration de l'API versionnée
)

db = client["raw_db"]
collection_logs = db["logs_raw"]  # Collection pour les logs TXT
collection_social = db["social_data_raw"]  # Collection pour les données sociales
collection_transactions = db["transactions_raw"]  # Collection pour les transactions
collection_ad_campaign = db["ad_campaign_raw"]  # Nouvelle collection pour les campagnes publicitaires

# Création du consommateur Kafka
consumer = Consumer(conf)
consumer.subscribe(['logs_topic'])

def identify_message(message_value):
    """
    Identifie le type de message basé sur ses clés tout en ignorant les messages invalides.
    Nettoie les champs `_id` contenant un `$oid`.
    """
    try:
        message_json = json.loads(message_value)
        
        # Vérifie si une clé contient un caractère "$" dans ses noms de clé
        if any("$" in key for key in message_json.keys()):
            print("Message contenant des clés invalides, ignoré.")
            return None, None
        
        # Nettoyer le champ '_id' si nécessaire
        if '_id' in message_json and isinstance(message_json['_id'], dict) and '$oid' in message_json['_id']:
            # Convertir l'ObjectId à un format compatible MongoDB
            message_json['_id'] = ObjectId(message_json['_id']['$oid'])
        
        # Identification du type de message
        if "ad_id" in message_json and "campaign_id" in message_json:
            return "ad_campaign", message_json
        elif "transaction_id" in message_json and "customer_id" in message_json:
            return "transaction", message_json
        elif "log" in message_json:  # Vérification de la présence de la clé "log"
            return "log", message_json
        else:
            return "social_data", message_json
    except Exception as e:
        print(f"Erreur lors de l'identification du message : {e}")
        return None, None

def consume_messages():
    """
    Fonction principale de consommation et d'insertion des messages.
    """
    try:
        while True:
            msg = consumer.poll(1.0)  # Attente d'un message pendant 1 seconde
            if msg is None:
                print("Aucun message reçu, en attente...")
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                message_value = msg.value().decode('utf-8')
                print(f"Message reçu : {message_value}")

                # Identification et traitement du message
                msg_type, msg_content = identify_message(message_value)
                if not msg_type or not msg_content:
                    print("Message non identifié ou invalide, ignoré.")
                    continue

                # Insertion dans la collection MongoDB correspondante
                if msg_type == "log":
                    collection_logs.insert_one(msg_content)
                    print("Message inséré dans logs_raw.")
                elif msg_type == "social_data":
                    collection_social.insert_one(msg_content)
                    print("Message inséré dans social_data_raw.")
                elif msg_type == "transaction":
                    collection_transactions.insert_one(msg_content)
                    print("Message inséré dans transactions_raw.")
                elif msg_type == "ad_campaign":
                    collection_ad_campaign.insert_one(msg_content)
                    print("Message inséré dans ad_campaign_raw.")

    except KeyboardInterrupt:
        print("Arrêt du consommateur.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()
