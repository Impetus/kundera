Overview
=========
The idea behind Kundera is to make working with NoSQL Databases drop-dead simple and fun. Kundera is being developed with following objectives:

* To make working with NoSQL as simple as working with SQL
*	To serve as JPA Compliant mapping solution for NoSQL Datastores.
*	To help developers, forget the complexity of NoSQL stores and focus on Domain Model.
*	To make switching across data-stores as easy as changing a configuration.


[Downloads] (https://github.com/impetus-opensource/Kundera/wiki/Kundera-releases "Downloads")


Up and running in 5 minutes
============================
If you have worked upon Hibernate or any JPA Compliant ORM Solution, then the whole process, right from learning to coming up with first sample implementation will not take more than 5 minutes. Please follow below steps from [this link] (https://github.com/impetus-opensource/Kundera/wiki/Getting-Started-in-5-minutes "Getting started in 5 minutes"):

+	Set up Cassandra server.
+	Download and include Kundera Jar
+ Write persistence.xml file
+ Write Entity class
+	Moment of Truth!


Currently Supported Datasources
================================
*	Cassandra
*	MongoDB
*	HBase
* Relational databases

Recent Releases
================================
#### 09-Dec-2011 - Kundera 2.0.4 released
This release includes bug fixes, performance improvements and the following new features compared to version 2.0.3:

* Cross-datastore persistence.
* Support for relational databases.
* Moved out solandra with lucene.

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

[Downloads] (https://github.com/impetus-opensource/Kundera/wiki/Kundera-releases "Downloads")


About Us
========
Kundera is backed by Impetus Labs - iLabs. iLabs is a R&D consulting division of Impetus Technologies (http://www.impetus.com). iLabs focuses on innovations with next generation technologies and creates practice areas and new products around them. iLabs is actively involved working on High Performance computing technologies, ranging from distributed/parallel computing, Erlang, grid softwares, GPU based software, Hadoop, Hbase, Cassandra, CouchDB and related technologies. iLabs is also working on various other Open Source initiatives.
