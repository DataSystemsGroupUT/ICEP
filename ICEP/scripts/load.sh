#!/usr/bin/env bash

#export _JAVA_OPTIONS="-Xmx10g"

bootstrap=$1
topic=$2
input=$3
maxevents=$4

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/root/platforms/confluent-5.3.1"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/root/ICEP/ICEP/ICEP"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi

# Setup topic
echo "Setting up the topic $topic"
$KAFKA_HOME/bin/kafka-topics --zookeeper localhost:2181 --delete --topic "$topic" --if-exists
$KAFKA_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 9 --topic "$topic"
sleep 10

# Execute producer
echo "Start loading:"
java -Xmx2g -cp $PROJECT_DIR/target/D2IA-0.1-SNAPSHOT-launcher.jar ee.ut.cs.dsg.example.linearroad.datagenerator.LinearRoadKafkaDataProducer ${bootstrap} ${topic} ${input} ${maxevents} &> loading.out
echo "Producer finished"
