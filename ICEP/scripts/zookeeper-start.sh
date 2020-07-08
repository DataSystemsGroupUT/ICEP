#!/usr/bin/env bash

export _JAVA_OPTIONS="-Xmx8g"

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/Users/samuelelanghi/Documents/polimi/master/anno_5/tesi/confluent-5.3.1"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

#clean logs:
echo "Cleaning /kafka_log* and zookeeper folders from /tmp"
rm -rf /tmp/zookeeper;

#start zookeeper
echo "Starting zookeeper"
$KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties &
