Overview
=========
The idea behind Kundera is to make working with NoSQL Databases drop-dead simple and fun. Kundera is being developed with following objectives:

*  To make working with NoSQL as simple as working with SQL
*  To serve as JPA Compliant mapping solution for NoSQL Datastores.
*	To help developers, forget the complexity of NoSQL stores and focus on Domain Model.
*	To make switching across data-stores as easy as changing a configuration.

[Downloads] (https://github.com/impetus-opensource/Kundera/wiki/Stable-Release "Downloads")

Note
====
With 2.9.1 release onwards artifact group id has been changed from

```
	<dependency>
		<groupId>com.impetus.client</groupId>
		<artifactId>kundera-cassandra</artifactId>
		<version>2.9.1</version>
	</dependency>
```	
TO
```
	<dependency>
		<groupId>com.impetus.kundera.client</groupId>
		<artifactId>kundera-cassandra</artifactId>
		<version>2.9.1</version>
	</dependency>
```
Note: Above information is to notify regarding change in groupId. In case using HBase or any other supported datastore. You need to simply point to corresponding artifactId.

Up and running in 5 minutes
============================
If you have worked upon Hibernate or any JPA Compliant ORM Solution, then the whole process, right from learning to coming up with first sample implementation will not take more than 5 minutes. Please follow below steps from [this link] (https://github.com/impetus-opensource/Kundera/wiki/Getting-Started-in-5-minutes "Getting started in 5 minutes"):

+ Set up Cassandra server.
+ Download and include Kundera Jar
+ Write persistence.xml file
+ Write Entity class
+ Moment of Truth!


Currently Supported Datasources
================================
*  Cassandra
*  MongoDB
*  HBase
*  Redis
*  OracleNoSQL
*  Neo4j
*  Elastic-search
*  Couchdb
*  Relational databases


Recent Releases
================================
####Please Note: trunk is current development branch. kundera-2.15 is released tag.

#### 12-January-2015 - Kundera 2.15 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), additional  improvements and the following new features compared to version 2.14:

 * Support added for Redis-2.8.x version.
 * Support added for MongoDB-2.6.x version.
 * Support added for Elastic-search 1.4.2 version.

#### 17-October-2014 - Kundera 2.14 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), additional  improvements and the following new features compared to version 2.13:

 * Or clause and in query support enabled for Hbase


#### 4-July-2014 - Kundera 2.13 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), additional  improvements and the following new features compared to version 2.12:

* Support added for Hbase-0.96
* Support for composite keys over Lucene and  Hbase enabled

#### 21-May-2014 - Kundera 2.12 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), additional  improvements and the following new features compared to version 2.12:

* Support added for Oracle-KV 3.0.5
* Support  for JPA-2.1 enabled


#### 24-March-2014 - Kundera 2.11 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.10:

* Support added for Cassandra datastax java driver
* Support added for in clause with setParameter for collection object

#### 31-January-2014 - Kundera 2.10 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.9:

* Support added for bean validation.

#### 13-December-2013 - Kundera 2.9 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.8:
* Support for Secondary table.
* Support Abstract entity.

#### 21-October-2013 - Kundera 2.8 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.7:
* Support for couchdb as a datastore.
* Support for MappedSuperclass and JPA Inheritence strategy.

#### 03-September-2013 - Kundera 2.7 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.6:

* Support for pagination over Mongodb. 
* Added elastic search as datastore and fallback indexing mechanism. 


#### 06-July-2013 - Kundera 2.6 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.5:

* Lazy fetching of relationships.
* Multiple node support for Cassandra.
* Pagination support for Cassandra and HBase


#### 30-Apr-2013 - Kundera 2.5 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.4:

* Support for OracleNoSQL
   [Please use the Oracle NoSQL jars from the Oracle NoSQL distribution at:
   http://download.oracle.com/otn-pub/otn_software/nosql-database/kv-ce-2.0.26.zip.
   For the convenience of those who want to build Kundera from source we have additionally
   placed the jars at http://kundera.googlecode.com/svn/maven2/maven-missing-resources/]
 
* CQL 3.0 interoperability with thrift


#### 08-Mar-2013 - Kundera 2.4 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.3:

* Support for Neo4j graph database
* Primary key auto-generation
* Support for JoinByPrimaryKey relationship
* Custom Secondary indexing hooks
* Hadoop 0.94.3 migration


#### 24-Jan-2013 - Kundera 2.3 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.2:

* Redis Support
* Cassandra 1.2 migration.
* HBase schema handling changes
* Stronger query support, like selective column/id search via JPQL.
* Enabled support for @Transient for embedded columns and mapped super class.
* Record limit setting on search for mongodb .


#### 08-Nov-2012 - Kundera 2.2 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.1:

* Geospatial Persistence and Queries for MongoDB
* Composite keys support for Cassandra and MongoDB
* Cassandra 1.1.6 migration
* Support for enum data type
* Named and Native queries support for REST based access

#### 21-Sep-2012 - Kundera 2.1 released
This release includes [bug fixes] (https://github.com/impetus-opensource/Kundera/wiki/Bug-Fixes), performance improvements and the following new features compared to version 2.0.7:

* CQL Version configuration
* Batch insert/update for Cassandra/MongoDB/HBase
* JPA MetaModel and TypedQuery implementation
* Raw thrift client support for Cassandra
* Datastore specific XML configuration. (.properties file deprecated) 
* Stronger query support:
   - JPQL support over all data types and associations
   - JPQL support to query using primary key alongwith other columns
* JPA compliance
   - EntityManager methods implementation (contains, refresh, detach, setProperty)
   - EntityManagerFactory methods (getPersistenceUnitUtil)
   - Persistence methods (getProviderUtil)


#### 25-July-2012 - Kundera 2.0.7 released
This release includes bug fixes, performance improvements and the following new features compared to version 2.0.6:


* HBase 0.92.1 migration
* Hadoop 1.0.2 migration
* Cassandra 1.1.2 migration
* MongoDB 2.0.4 migration
* JPA EntityTransaction commit and rollback
* JTA Transactions integration over web server
* Kundera-REST API
* Support for Counter column in cassandra
* Inverted wide-row indexing support for Cassandra
* Login Authentication support for Cassandra and MongoDB
* Filters and filters list for HBase
* Deprecated Lucene based indexing for HBase.
* Datastore specific configuration files for specifying:
   - Replication factor
   - Placement strategy
   - Consistency level per operation.
   - Counter column family configuration
   - Inverted indexing switch
   - Zookeeper host and port
   - Hbase column family configurations
   - MongoDB servers list, read preference and socket timeout.
 ...etc.


#### 20-Apr-2012 - Kundera 2.0.6 released
This release includes bug fixes and the following new features compared to version 2.0.5:

* HBase 0.90.x migration.
* Enhanced Persistence Context.
* Named and Native queries support (including CQL support for cassandra)
* UPDATE and DELETE queries support.
* DDL auto-schema creation.
* Performance improvements.

#### 06-Feb-2012 - Kundera 2.0.5 released
This release includes bug fixes and the following new features compared to version 2.0.4:

* Cassandra 1.x migration.
* Support for Many-to-Many relationship (via Join table)
* Transitive persistence.
* Datastore native secondary index support in addition to Lucene based indexing. An optional switch provided to change between two.
* Query support for >, < , >=,<=,!=, like, order by, like, logical operators and between.
* Connection pooling settings provided for all datastores.
* Support for all data types as required by JPA.
* Range queries for cassandra (via between clause in JPA-QL)
* Bug fixes related to self join. 


#### 09-Dec-2011 - Kundera 2.0.4 released
This release includes bug fixes, performance improvements and the following new features compared to version 2.0.3:

* Cross-datastore persistence.
* Support for relational databases.
* Moved out solandra and replaced with lucene.

#### 08-Aug-2011 - Kundera 2.0.3 released
This release includes bug fixes and the following new features compared to version 2.0.2:

* Cassandra 0.8.x support added

#### 31-July-2011 - Kundera 2.0.2 released
This release includes bug fixes and the following new features compared to version 2.0.1:

* Kundera is now JPA 2.0 compliant. 
* Embedded objects/ collections support for HBase.

#### 12-July-2011 - Kundera 2.0.1 released
This release includes bug fixes and the following new features compared to initial revision:

* Cassandra 0.7.x support added
* @Embedded annotation fields persisted co located with parent entity
* Search within embedded objects.
* Selective index.

[Downloads] (https://github.com/impetus-opensource/Kundera/wiki/Stable-Release "Downloads")


About Us
========
Kundera is backed by Impetus Labs - iLabs. iLabs is a R&D consulting division of Impetus Technologies (http://www.impetus.com). iLabs focuses on innovations with next generation technologies and creates practice areas and new products around them. iLabs is actively involved working on High Performance computing technologies, ranging from distributed/parallel computing, Erlang, grid softwares, GPU based software, Hadoop, Hbase, Cassandra, CouchDB and related technologies. iLabs is also working on various other Open Source initiatives.
