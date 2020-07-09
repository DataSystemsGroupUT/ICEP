#!/usr/bin/env bash

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/root/platforms/confluent-5.3.1"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

# stop zookeeper
echo "Stopping zookeeper"
$KAFKA_HOME/bin/zookeeper-server-stop $KAFKA_HOME/etc/kafka/zookeeper.properties & sleep 5

#clean logs:
echo "Cleaning zookeeper folders from /tmp"
rm -rf /tmp/zookeeper;