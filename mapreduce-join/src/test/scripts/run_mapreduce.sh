#!/usr/bin/env bash

source /etc/hadoop/conf/hadoop-env.sh

INPUT_DATA_SALES="/user/$USER/mapreduce-join/input/sales"
INPUT_DATA_USERS="/user/$USER/mapreduce-join/input/users"
OUTPUT_DATA="/user/$USER/mapreduce-join/output"

echo "Creating input data ..."
hdfs dfs -mkdir /user/$USER/mapreduce-join/
hdfs dfs -mkdir /user/$USER/mapreduce-join/input
hdfs dfs -mkdir $INPUT_DATA_SALES
hdfs dfs -mkdir $INPUT_DATA_USERS
hdfs dfs -copyFromLocal test/sales.txt $INPUT_DATA_SALES
hdfs dfs -copyFromLocal test/users.txt $INPUT_DATA_USERS

echo "Creating output data folder ..."
hdfs dfs -mkdir $OUTPUT_DATA

MAIN_JAR="mapreduce-join.jar"

LIST_INTO_LIBJARS="$(find "lib/" -iname '*.*' | paste -sd,)"

echo "Running mapreduce ..."
hadoop jar $MAIN_JAR -libjars $LIST_INTO_LIBJARS $INPUT_DATA_SALES $INPUT_DATA_USERS $OUTPUT_DATA
