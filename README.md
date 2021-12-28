# delta-lake

<b>Command:</b>

`spark-submit --packages io.delta:delta-core_2.12:1.1.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--master <master> \
spi_build_table.py \
--partitionkeys 10 \
--partitions 5 \
--key primary_key \
--input "https://projects.fivethirtyeight.com/soccer-api/club/spi_matches_latest.csv" \
--outputprefix <your-prefix> \
--table spi_matches_latest`

