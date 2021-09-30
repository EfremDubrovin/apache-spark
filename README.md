# apache-spark

test project deployed to a docker container cluster with one master and two worker nodes
note first you need to build the jar which is mentioned in the configuration in the main app `target/spark.jar`
using
```shell
mvn clean install
```
after which you can start the docker containers
```shell
docker-compose up -d
```
and run the main app which will send the code to the configured master node `spark://localhost:7077`

in the local UI you can view that your job was executed
```
http://localhost:8080/
```

spark-submit --class eu.deltasource.SparkApplication --master spark://localhost:7077 /bitnami/spark/spark.jar 100
