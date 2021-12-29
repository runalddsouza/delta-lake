#!/usr/bin/env bash

PY_JOB_MODULE_PATH=$1
PY_FILES=$2
SOURCE=$3
OUTPUT_LOCATION=$4

spark-submit --packages io.delta:delta-core_2.12:1.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--master "local[1]" \
--py-files "$PY_FILES" \
"$PY_JOB_MODULE_PATH/pricing_stream.py" \
--topic pricing \
--partitionkeys 10 \
--servers "$SOURCE" \
--key id \
--outputprefix "$OUTPUT_LOCATION"
