CREATE DATABASE IF NOT EXISTS delta;

CREATE EXTERNAL TABLE IF NOT EXISTS `delta`.`pricing`(
id BIGINT,
current_price INTEGER,
profit_percent INTEGER,
selling_price DOUBLE,
`time` TIMESTAMP,
partition_id STRING)
STORED BY 'io.delta.hive.DeltaStorageHandler'
LOCATION 'hdfs://namenode:8020/pricing';
