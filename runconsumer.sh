#!/usr/bin/env bash

echo compile...

# pass <topicName> <numOfRecsToProduce> as args

mvn -q clean compile exec:java \
 -Dexec.mainClass="com.demo.kafka.Application" \
 -Dexec.args="consumer $1 $2"