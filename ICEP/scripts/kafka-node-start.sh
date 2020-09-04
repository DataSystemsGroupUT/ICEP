#!/usr/bin/env bash

export KAFKA_HEAP_OPTS="-Xmx5G -Xms1G"
broker_count=$1

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




#start broker
echo "Starting broker"
$KAFKA_HOME/bin/kafka-server-start $PROJECT_DIR/scripts/configs/server-$broker_count.properties &


