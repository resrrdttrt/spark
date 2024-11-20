#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"
echo "SPARK_MASTER_HOST: $SPARK_MASTER_HOST"
echo "SPARK_MASTER: $SPARK_MASTER"

if [ "$SPARK_WORKLOAD" == "master" ];
then
  # start-master.sh -h $SPARK_MASTER_HOST -p $SPARK_MASTER_PORT
  start-master.sh -p 7077
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  start-worker.sh spark://spark-master:7077 -c 1 -m 1G
  # start-worker.sh $SPARK_MASTER -c 1 -m 1G
elif [ "$SPARK_WORKLOAD" == "history" ]
then
  start-history-server.sh
fi
