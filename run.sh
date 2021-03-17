#!/bin/bash


docker-compose up -d

sleep 10

MESSAGES=2000

docker-compose exec broker /bin/sh /bin/kafka-topics --create --topic topic-1 --replication-factor 1 --partitions 3 --zookeeper zookeeper:2181
docker-compose exec broker /bin/sh /bin/kafka-verifiable-producer  --topic topic-1 --max-messages $MESSAGES --broker-list localhost:9092

docker-compose exec broker /bin/sh /bin/kafka-topics --create --topic topic-2 --replication-factor 1 --partitions 3 --zookeeper zookeeper:2181
docker-compose exec broker /bin/sh /bin/kafka-verifiable-producer  --topic topic-2 --max-messages $MESSAGES --broker-list localhost:9092

./gradlew clean build run


