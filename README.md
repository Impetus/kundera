Overview
=========
The idea behind Kundera is to make working with NoSQL Databases drop-dead simple and fun. Kundera is being developed with following objectives:

*  To make working with NoSQL as simple as working with SQL
*  To serve as JPA Compliant mapping solution for NoSQL Datastores.
*	To help developers, forget the complexity of NoSQL stores and focus on Domain Model.
*	To make switching across data-stores as easy as changing a configuration.


[Downloads] (https://github.com/impetus-opensource/Kundera/wiki/Stable-Release "Downloads")


Structure
=========
Current project structure contains:
* "src" folder contains Kundera source code.
* "benchmark" folder contains YCSB based Kundera benchmark module.
* "container" contains deployable web container modules.

How to build
============
Each module is a maven based project. To build a specific module(for example src). You need to execute a command as 

* mvn clean install -Dfile src/pom.xml

More details about Kundera source code structure and build process are available at => 
https://github.com/impetus-opensource/Kundera/blob/trunk/src/README.md


About Us
========
Kundera is backed by Impetus Labs - iLabs. iLabs is a R&D consulting division of Impetus Technologies (http://www.impetus.com). iLabs focuses on innovations with next generation technologies and creates practice areas and new products around them. iLabs is actively involved working on High Performance computing technologies, ranging from distributed/parallel computing, Erlang, grid softwares, GPU based software, Hadoop, Hbase, Cassandra, CouchDB and related technologies. iLabs is also working on various other Open Source initiatives.
