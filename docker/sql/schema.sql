CREATE DATABASE IF NOT EXISTS delta;

CREATE EXTERNAL TABLE IF NOT EXISTS `delta`.`spi_matches_latest`(
season BIGINT,
`date` STRING,
league_id BIGINT,
league STRING,
team1 STRING,
team2 STRING,
spi1 DOUBLE,
spi2 DOUBLE,
prob1 DOUBLE,
prob2 DOUBLE,
probtie DOUBLE,
proj_score1 DOUBLE,
proj_score2 DOUBLE,
importance1 DOUBLE,
importance2 DOUBLE,
score1 DOUBLE,
score2 DOUBLE,
xg1 DOUBLE,
xg2 DOUBLE,
nsxg1 DOUBLE,
nsxg2 DOUBLE,
adj_score1 DOUBLE,
adj_score2 DOUBLE,
primary_key STRING,
partition_id STRING)
STORED BY 'io.delta.hive.DeltaStorageHandler'
LOCATION 'hdfs://namenode:8020/spi_matches_latest';

CREATE EXTERNAL TABLE IF NOT EXISTS `delta`.`pricing`(
id BIGINT,
current_price INTEGER,
profit_percent INTEGER,
selling_price DOUBLE,
`time` TIMESTAMP,
partition_id STRING)
STORED BY 'io.delta.hive.DeltaStorageHandler'
LOCATION 'hdfs://namenode:8020/pricing';
