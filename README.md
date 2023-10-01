# flink-apache-iceberg-nessie-java

A simple example of using Apache Flink with Apache Iceberg and Nessie.

Based on: https://www.dremio.com/blog/using-flink-with-apache-iceberg-and-nessie/

## Running

### Start all services

```bash
docker-compose up
```

### Get s3 endpoint

```bash
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' storage
```

### Run Flink job

```bash
cd flink-iceberg-nessie
mvn clean package
```

- Flink UI: http://localhost:8081
- Entry Class: dev.rafadao.App
- Program Arguments: http://{s3 endpoint}:9000


![image](https://github.com/RafaelAdao/flink-apache-iceberg-nessie-java/assets/5923706/0483a5ab-61fb-4e36-92a0-89eeda8a2e5c)

![image](https://github.com/RafaelAdao/flink-apache-iceberg-nessie-java/assets/5923706/9df5b42b-a5db-435d-8276-eb585f70fb73)


