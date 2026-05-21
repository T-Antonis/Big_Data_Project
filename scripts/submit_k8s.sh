#!/bin/bash

APP_FILE=$1
EXECUTORS=$2
CORES=$3
MEMORY=$4

DSML_USER=${DSML_USER:-dsml00284}
NAMESPACE="${DSML_USER}-priv"

spark-submit \
  --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
  --deploy-mode cluster \
  --name dsml-bigdata-job \
  --conf spark.kubernetes.namespace=$NAMESPACE \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=apache/spark:3.5.8-scala2.12-java11-python3-ubuntu \
  --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME=$DSML_USER \
  --conf spark.executorEnv.HADOOP_USER_NAME=$DSML_USER \
  --conf spark.executor.instances=$EXECUTORS \
  --conf spark.executor.cores=$CORES \
  --conf spark.executor.memory=$MEMORY \
  --conf spark.driver.memory=2g \
  --conf spark.kubernetes.file.upload.path=hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/.spark-upload \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/logs \
  --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/logs \
  --conf spark.speculation=false \
  --conf spark.sql.shuffle.partitions=200 \
  --py-files src/common.py \
  $APP_FILE ${@:5}