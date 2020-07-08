#!/usr/bin/env bash

export _JAVA_OPTIONS="-Xmx10g"

bootstrap=$1
topic=$2
exp=$3
maxevents=$4
type=$5

# Execute producer
echo "Start loading:"
java -cp /Users/samuelelanghi/Documents/projects/ICEP/ICEP/ICEP/target/D2IA-0.1-SNAPSHOT-launcher.jar ee.ut.cs.dsg.d2ia.esper.KafkaAdaptedEsper ${bootstrap} ${topic} ${exp} ${maxevents} ${type} &
echo "Producer finished"
