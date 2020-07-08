#!/usr/bin/env bash

export _JAVA_OPTIONS="-Xmx10g"
broker_count=$1

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




#start broker
echo "Starting broker"
$KAFKA_HOME/bin/kafka-server-start $PROJECT_DIR/scripts/configs/server-$broker_count.properties &


