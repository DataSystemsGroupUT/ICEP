#!/usr/bin/env bash

export _JAVA_OPTIONS="-Xmx10g"

experiment=$1
broker_count=$2

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/Users/samuelelanghi/Documents/polimi/master/anno_5/tesi/confluent-5.3.1"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

#clean logs:
echo "Cleaning /kafka_log* and zookeeper folders from /tmp"
rm -rf /tmp/zookeeper;
rm -rf /tmp/kafka-logs*;

#start zookeeper
echo "Starting zookeeper"
$KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties & sleep 10

#start brokers
echo "Starting brokers"
for i in $(seq 0 $((broker_count-1)))
  do
    $KAFKA_HOME/bin/kafka-server-start /Users/samuelelanghi/Documents/ut/ICEP/ICEP/ICEP/src/main/java/ee/ut/cs/dsg/example/linearroad/datagenerator/configs/server-$i.properties &
  done
sleep 15

# start producers
# Setup topic
echo "Setting up producer topic"
$KAFKA_HOME/bin/kafka-topics --zookeeper localhost:2181 --delete --topic "$experiment" --if-exists
$KAFKA_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 9 --topic "$experiment"
sleep 5


# Execute producer...
