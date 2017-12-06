# ACID 2.x JMH implementation

This repository contains code cut-pasted from Apache Hive 3.x branch to be able to test Hive's ACID 2.0 reader core without including the rest of ACID or ORC.

To run

`mvn clean package; java -jar target/benchmarks.jar -prof stack`

BM Modification to add CAPI SNAP code
`git clone https://github.com/bmesnet/snap.git`
variable ROOT has been set to /root in snap/compile

`cd ~/acid2x-jmh`
`./compile_all; ./execute_all`
variable ROOT has been set to /root in acid2x-jmh/compile_all, and acid2x-jmh/execute_all
