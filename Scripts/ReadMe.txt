#DEMARRER ZOOKEEPER

/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties


#DEMARRER KAFKA

/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties


#CONSUMER
python3 /mnt/c/Users/geoff/Documents/data_lake/data_lake/Script/consumer.py

#PRODUCER
python3 /mnt/c/Users/geoff/Documents/data_lake/data_lake/Script/producer.py


#OUVRIR SPARK

source ~/.bash_profile

spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /datalake/script/clean_mongo_data.py

spark-shell

#API

## allumer spring-boo

./mvnw clean spring-boot:run

## Requête

root@Geoffrey:/datalake/script/API# curl -X GET "http://localhost:8080/transactions/search/Credit%20Card?limit=5" -H "Content-Type: application/json"

root@Geoffrey:/datalake/script/API# curl -X GET "http://localhost:8080/transactions/social/search/user_01?limit=5" -H "Content-Type: application/json"

curl -X GET "http://localhost:8080/transactions/adcampaign/search?minClicks=40" -H "Content-Type: application/json"


## Swagger

http://localhost:8080/swagger-ui/index.html#/





