[# delta-lake

| App| Version |
| --- | --- |
| Spark/PySpark | 3.2.0 |
| Delta Lake | 1.1.0 |

This project consists of below jobs:

- Build Table Job -> Creates a Delta Table from Spark DataFrame
- Stream Table Job -> Read update stream and merge changes into Delta Table

<b>Command:</b>
```
spark-submit --packages io.delta:delta-core_2.12:1.1.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--master <master> \
spi_build_table.py \
--partitionkeys 10 \
--partitions 5 \
--key primary_key \
--input "https://projects.fivethirtyeight.com/soccer-api/club/spi_matches_latest.csv" \
--outputprefix <your-prefix> \
--table spi_matches_latest
```
Documentation: <a href='https://docs.delta.io/1.1.0/delta-intro.html'>Delta Lake (v1.1.0)</a>