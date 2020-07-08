#!/usr/bin/env bash

experiment=$1
broker_count=$2
echo $broker_count
echo $experiment

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/Users/samuelelanghi/Documents/polimi/master/anno_5/tesi/confluent-5.3.1"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/root/kEPLr"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi


# stop brokers
echo "Stopping brokers"
for i in $(seq 0 $broker_count)
  do
    $KAFKA_HOME/bin/kafka-server-stop $PROJECT_DIR/configs/server-$i.properties &
  done
sleep 5

# stop zookeeper
echo "Stopping zookeeper"
$KAFKA_HOME/bin/zookeeper-server-stop $KAFKA_HOME/etc/kafka/zookeeper.properties
sleep 5

rm -rf /tmp/*
