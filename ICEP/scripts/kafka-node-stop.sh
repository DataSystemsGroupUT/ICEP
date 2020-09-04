#!/usr/bin/env bash

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

# stop broker
echo "Stopping broker"
$KAFKA_HOME/bin/kafka-server-stop


rm -rf /tmp/kafka-logs*;