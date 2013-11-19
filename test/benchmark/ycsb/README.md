kundera-benchmarks
==================

Kundera performance benchmarking API. Current version provides YCSB based performance benchmarking for Kundera.
Project includes benchmarking for Cassandra,MongoDB,Neo4j,Redis.

{Performance Benchmarking}

Cassandra
=========
1. Kundera's thrift client vs. raw Thrift client.
2. Kundera's pelops client vs. raw Pelops client.

MongoDB
=======
1. Kundera's MongoDB client vs. MongoDB client.

Redis
=====
1. Kundera Redis client vs. Jedis

Neo4j
=====
1. Kundera Neo4j vs. Native neo4j client.


How to run:
========== 
 <b>Step1: <b>
 
  Modify properties file specific to nosql database. For example, in case of Cassandra it is db-cassandra.properties. 
 You need to modify for "ycsbjar.location" and "clientjar.location" according to system configuration.
 
 <b>Step2: <b>
 
 Command to run a test is as follows:
 mvn -Dtest=RedisYCSBTest test -DfileName=src/main/resources/db-redis.properties
 Here "RedisYCSBTest" is junit name and -DfileName is for corresponding properties file.
 {Above command to execute Redis YCSB benchmark test}. 
 Other test cases are CassandraYCSBTest,MongoDBYCSBTest,Neo4jYCSBTest.


For more details on usage of YCSB framework, please refer:

1. https://github.com/brianfrankcooper/YCSB
2. https://github.com/brianfrankcooper/YCSB/wiki/Getting-Started
