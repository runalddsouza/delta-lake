#!/usr/bin/env bash

PY_JOB_MODULE_PATH=$1
PY_FILES=$2
OUTPUT_LOCATION=$3

spark-submit --packages io.delta:delta-core_2.12:1.1.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--master "local[*]" \
--py-files "$PY_FILES" \
"$PY_JOB_MODULE_PATH/spi_build_table.py" \
--partitionkeys 10 \
--partitions 5 \
--key primary_key \
--input "https://projects.fivethirtyeight.com/soccer-api/club/spi_matches_latest.csv" \
--outputprefix "$OUTPUT_LOCATION" \
--table spi_matches_latest

spark-submit --packages io.delta:delta-core_2.12:1.1.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--master "local[*]" \
--py-files "$PY_FILES" \
"$PY_JOB_MODULE_PATH/pricing_load.py" \
--partitionkeys 10 \
--partitions 5 \
--key id \
--input none \
--outputprefix "$OUTPUT_LOCATION" \
--table pricing