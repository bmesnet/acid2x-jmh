# ACID 2.x JMH implementation

This repository contains code cut-pasted from Apache Hive 3.x branch to be able to test Hive's ACID 2.0 reader core without including the rest of ACID or ORC.

To run

`mvn clean package; java -jar target/benchmarks.jar -prof stack`
