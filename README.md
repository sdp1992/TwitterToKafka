# TwitterToKafka
This application is receiving tweets related to mentioned keywords(Here Narendra Modi and Rahul Gandhi) and sending tweets to Kafka Topic.

# Kafka Cluster Configuration
Number of Nodes: 1
Replication Factor: 1
Log Retention Time: 1 Hr

## To run the application as Python application:
nohup python TwitterToKafka.py &

## To run in Docker:
docker run --detach -v /path/to/your/config.ini:/app/config.ini --network="host" image_id (Make sure Kafka consumers are reachable at localhost:9092)

