#!/usr/bin/env bash

source /etc/hadoop/conf/hadoop-env.sh

INPUT_DATA="/user/$USER/mapreduce-chain/input/"
OUTPUT_DATA="/user/$USER/mapreduce-chain/output"

echo "Creating input data ..."
hdfs dfs -mkdir /user/$USER/mapreduce-chain/
hdfs dfs -mkdir $INPUT_DATA
hdfs dfs -copyFromLocal test/example_input.txt $INPUT_DATA

echo "Creating output data folder ..."
hdfs dfs -mkdir $OUTPUT_DATA

MAIN_JAR="mapreduce-chain.jar"

LIST_INTO_LIBJARS="$(find "lib/" -iname '*.*' | paste -sd,)"

echo "Running mapreduce ..."
hadoop jar $MAIN_JAR -libjars $LIST_INTO_LIBJARS $INPUT_DATA $OUTPUT_DATA
