import os
import csv
import json
import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from confluent_kafka import Producer

# Chemins des fichiers de logs à surveiller
file_path_social_data = '/datalake/social_data.json'
file_path_server_logs = '/datalake/server_logs.txt'
file_path_transactions = '/datalake/transactions.csv'
file_path_ad_campaign = '/datalake/ad_campaign_data.json'
topic = 'logs_topic'

class LogHandler(FileSystemEventHandler):
    def __init__(self, producer, topic, file_paths):
        self.producer = producer
        self.topic = topic
        self.file_paths = file_paths  # Liste des fichiers surveillés
        self.last_processed = {}  # Stocke les timestamps des fichiers traités

    def process_json_file(self, file_path):
        """Traite un fichier JSON et envoie chaque entrée à Kafka."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = f.read()
                if data:
                    log_entries = json.loads(data)  # Charger le JSON
                    for log_entry in log_entries:
                        message = json.dumps(log_entry)
                        self.producer.produce(self.topic, message.encode('utf-8'))
                        print(f"Message JSON envoyé depuis {file_path}: {message}")
        except json.JSONDecodeError as e:
            print(f"Erreur de décodage JSON dans {file_path} : {e}")
        except Exception as e:
            print(f"Erreur inattendue lors du traitement du JSON {file_path} : {e}")

    def process_csv_file(self, file_path):
        """Traite un fichier CSV et envoie chaque ligne transformée à Kafka."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                csv_reader = csv.DictReader(f)  # Lecture du CSV en tant que dictionnaire
                for row in csv_reader:
                    message = json.dumps(row)  # Convertir chaque ligne en JSON
                    self.producer.produce(self.topic, message.encode('utf-8'))
                    print(f"Message CSV envoyé depuis {file_path}: {message}")
        except Exception as e:
            print(f"Erreur inattendue lors du traitement du CSV {file_path} : {e}")

    def process_txt_file(self, file_path):
        """Traite un fichier TXT non structuré et envoie chaque ligne transformée à Kafka."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():  # Ignore les lignes vides
                        transformed_entry = {"log": line.strip()}
                        message = json.dumps(transformed_entry)
                        self.producer.produce(self.topic, message.encode('utf-8'))
                        print(f"Message TXT envoyé depuis {file_path}: {message}")
        except Exception as e:
            print(f"Erreur inattendue lors du traitement du TXT {file_path} : {e}")

    def handle_file(self, file_path):
        """Détecte et traite le fichier selon son extension."""
        if file_path.endswith('.json'):
            self.process_json_file(file_path)
        elif file_path.endswith('.csv'):
            self.process_csv_file(file_path)
        elif file_path.endswith('.txt'):
            self.process_txt_file(file_path)

    def on_modified(self, event):
        """Déclenché lorsqu'un fichier est modifié."""
        if not event.is_directory and event.src_path in self.file_paths:
            file_path = event.src_path
            print(f"Modification détectée dans le fichier : {file_path}")

            # Empêcher les lectures redondantes en vérifiant le timestamp
            current_mtime = os.path.getmtime(file_path)
            last_mtime = self.last_processed.get(file_path, 0)

            if current_mtime != last_mtime:  # Traite uniquement si le fichier a changé
                self.last_processed[file_path] = current_mtime
                self.handle_file(file_path)

    def on_created(self, event):
        """Déclenché lorsqu'un fichier est créé."""
        if not event.is_directory and event.src_path in self.file_paths:
            print(f"Nouveau fichier détecté : {event.src_path}")
            self.handle_file(event.src_path)

def send_to_kafka(file_paths, topic):
    # Configuration du producteur Kafka
    conf = {
        'bootstrap.servers': 'localhost:9092',
    }
    producer = Producer(conf)

    # Création de l'observateur pour surveiller les changements de fichier
    event_handler = LogHandler(producer, topic, file_paths)
    observer = Observer()
    for file_path in file_paths:
        observer.schedule(event_handler, path=os.path.dirname(file_path), recursive=False)

    observer.start()
    print(f"Surveillance des fichiers dans : {set(os.path.dirname(fp) for fp in file_paths)}")

    try:
        while True:
            time.sleep(1)
            producer.poll(0)  # Permet au producteur de traiter les messages en attente
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

    producer.flush()

# Chemins des fichiers à surveiller
file_paths = [file_path_social_data, file_path_server_logs, file_path_transactions, file_path_ad_campaign]

# Exécution de la fonction pour lancer la surveillance
send_to_kafka(file_paths, topic)
