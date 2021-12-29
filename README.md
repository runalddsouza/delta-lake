# Delta Lake Ingestion

| App| Version |
| --- | --- |
| Spark/PySpark | 3.2.0 |
| Delta Lake | 1.1.0 |

This project consists of below jobs:

- Build Table Job -> Creates a Delta Table from Spark DataFrame
- Stream Table Job -> Consume update stream and merge changes into existing Delta Table

### Docker Setup
- Hadoop (Namenode + 1 Datanode)
- Hive (Server + Postgres Metastore)
- Delta table load (Spark)
- Kafka
- Zookeeper
- Kafka Producer
- Delta table stream (Spark)
- Hue

### Steps:
- Clone repository
- Run: `cd docker`
- Start services: `docker-compose up`

Documentation: <a href='https://docs.delta.io/1.1.0/delta-intro.html'>Delta Lake (v1.1.0)</a>