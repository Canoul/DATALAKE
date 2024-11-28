from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_date, to_timestamp, isnan, lit, row_number, count
from pyspark.sql.types import IntegerType, DoubleType
from datetime import datetime
from bson import ObjectId

# URI MongoDB Atlas
atlas_uri = "mongodb+srv://geoffreyboilay:0KEiCgjtSFemIA3R@atlasdatalake.livy2.mongodb.net"
query_params = "?authSource=admin&ssl=true"

# Chemin vers le connecteur MongoDB Spark Connector
mongo_connector_jar = "mongo-spark/build/libs/mongo-spark-connector-10.5.0-SNAPSHOT.jar"  # Remplacez par le chemin correct

# Initialiser la session Spark avec le connecteur MongoDB
spark = SparkSession.builder \
    .appName("MongoDB Deduplication and Governance") \
    .config("spark.jars", mongo_connector_jar) \
    .getOrCreate()

# Générer la version selon la date actuelle
def generate_version():
    today = datetime.now()
    return f"1.{today.month}.{today.day}"

# Fonction pour ajouter une colonne version au DataFrame
def add_version_column(df):
    version_value = generate_version()  # Calculer la version actuelle
    return df.withColumn("version", lit(version_value))

# Fonction pour écrire les KPIs dans la table de gouvernance
def write_kpi_to_governance(kpi_name, kpi_value, governance_uri):
    kpi_df = spark.createDataFrame(
        [(kpi_name, kpi_value)], ["KPI_name", "KPI_value"]
    )
    kpi_df.write.format("mongo").mode("append").option("uri", governance_uri).save()
    print(f"KPI {kpi_name} written with value {kpi_value:.4f}")




# Pipeline principal pour le traitement et l'écriture
def deduplicate_and_write(source_db, source_collection, target_db, target_collection, invalid_data_db, invalid_collection, governance_db, governance_collection):
    # Construire les URI source, cible et gouvernance
    source_uri = f"{atlas_uri}/{source_db}.{source_collection}{query_params}"
    target_uri = f"{atlas_uri}/{target_db}.{target_collection}{query_params}"
    invalid_uri = f"{atlas_uri}/{invalid_data_db}.{invalid_collection}{query_params}"
    governance_uri = f"{atlas_uri}/{governance_db}.{governance_collection}{query_params}"


    
    # Lire les données de MongoDB
    df = spark.read.format("mongo").option("uri", source_uri).load()
    
    # Initialisation par défaut
    cleaned_df = df  # Par défaut, les données sont non filtrées
    invalid_df = spark.createDataFrame([], df.schema)  # Un DataFrame vide avec le même schéma que df
    
    # Prétraitement des données
    if source_collection == "transactions_raw":
        cleaned_df = df.filter(
            (col("transaction_id").isNotNull()) & 
            (col("customer_id").isNotNull()) &                 
            (col("payment_method").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("quantity").isNotNull()) &
            (col("quantity").cast("int").isNotNull()) &
            (col("quantity") > 0) &
            ~isnan(col("quantity")) &
            (col("total_amount").isNotNull()) &
            (col("total_amount").cast("int").isNotNull()) &
            ~isnan(col("total_amount")) &
            (col("total_amount") > 0) &
            (col("transaction_date").isNotNull()) &
            (col("transaction_status").isNotNull())
        )
        invalid_df = df.subtract(cleaned_df)

    elif source_collection == "ad_campaign_raw":
        cleaned_df = df.dropDuplicates(["ad_id", "campaign_id"]).filter(
            (col("impressions") >= col("clicks")) &
            (col("ad_id").isNotNull()) & 
            (col("campaign_id").isNotNull()) &                 
            (col("clicks").isNotNull()) &
            (col("clicks").cast("int").isNotNull()) &
            (col("conversions").isNotNull()) &
            (col("conversions").cast("int").isNotNull()) &
            (col("impressions").isNotNull()) &
            (col("impressions").cast("int").isNotNull()) &
            (col("spend").isNotNull()) &
            (col("spend").cast("double").isNotNull()) &
            (col("timestamp").isNotNull())
        )
        invalid_df = df.subtract(cleaned_df)

    elif source_collection == "social_data_raw":
        cleaned_df = df.dropDuplicates(["post_id"]).filter(
            (col("post_id").isNotNull()) &
            (col("reactions.likes").isNotNull()) &
            (col("reactions.likes").cast("double").isNotNull()) &
            (col("reactions.shares").isNotNull()) &
            (col("reactions.shares").cast("double").isNotNull()) &
            (col("reactions.comments").isNotNull()) &
            (col("reactions.comments").cast("double").isNotNull()) &
            (col("text").isNotNull()) &
            (col("timestamp").isNotNull())
        )
        invalid_df = df.subtract(cleaned_df)

    elif source_collection == "server_logs_raw":
        cleaned_df = df.filter(
            (col("ip").isNotNull()) & 
            (col("method").isNotNull()) &                 
            (col("status").cast("int").isNotNull()) &
            (col("url").isNotNull()) &
            (col("timestamp").isNotNull())
        )
        invalid_df = df.subtract(cleaned_df)

    # Ajouter la version aux données nettoyées
    cleaned_df = add_version_column(cleaned_df)

    # Calcul des KPI de fiabilité
    len_cleaned = cleaned_df.count()
    len_invalid = invalid_df.count()
    kpi_fiability = len_cleaned / (len_cleaned + len_invalid) if (len_cleaned + len_invalid) > 0 else 0.0

    # Nom du KPI pour cette collection
    kpi_name = f"KPI_fiability_{source_collection.replace('_raw', '')}"

    # Écriture des KPIs dans la table de gouvernance
    write_kpi_to_governance(kpi_name, kpi_fiability, governance_uri)

    # Log des données invalides
    print(f"Number of invalid records for {source_db}.{source_collection}: {len_invalid}")
    invalid_df.show(truncate=False)
    
    # Écrire les données nettoyées et invalides
    cleaned_df.write.format("mongo").mode("overwrite").option("uri", target_uri).save()
    print(f"Cleaned data written to {target_db}.{target_collection}")
    
    if len_invalid > 0:  # Écrire uniquement si des données invalides existent
        invalid_df.write.format("mongo").mode("overwrite").option("uri", invalid_uri).save()
        print(f"Invalid data written to {invalid_data_db}.{invalid_collection}")

# Définir les mappages entre collections sources et cibles
tables_mapping = [
    ("raw_db", "transactions_raw", "staging_db", "transactions_cleaned", "invalid_data_db", "transactions_invalid"),
    ("raw_db", "logs_raw", "staging_db", "logs_cleaned", "invalid_data_db", "logs_invalid"),
    ("raw_db", "social_data_raw", "staging_db", "social_data_cleaned", "invalid_data_db", "social_data_invalid"),
    ("raw_db", "ad_campaign_raw", "staging_db", "ad_campaign_cleaned", "invalid_data_db", "ad_campaign_invalid"),
]

# Gouvernance table settings
governance_db = "governance_db"
governance_collection = "fiability"

# Nettoyer et transférer les données
# Vider la collection de gouvernance avant d'écrire les nouveaux KPIs
governance_uri = f"{atlas_uri}/{governance_db}.{governance_collection}{query_params}"
spark.read.format("mongo").option("uri", governance_uri).load().write.format("mongo").mode("overwrite").option("uri", governance_uri).save()

for source_db, source_collection, target_db, target_collection, invalid_data_db, invalid_collection in tables_mapping:
    deduplicate_and_write(source_db, source_collection, target_db, target_collection, invalid_data_db, invalid_collection, governance_db, governance_collection)

# Arrêter Spark
spark.stop()