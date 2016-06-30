Overview [![Join the chat at https://gitter.im/impetus-opensource/Kundera](https://badges.gitter.im/impetus-opensource/Kundera.svg)](https://gitter.im/impetus-opensource/Kundera?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Follow us on Twitter](http://i.imgur.com/wWzX9uB.png)](https://twitter.com/kundera_impetus) 
=========
Kundera is a "Polyglot Object Mapper" with a JPA interface. The idea behind Kundera is to make working with NoSQL Databases drop-dead simple and fun. Kundera is being developed with following objectives:
* To make working with NoSQL as simple as working with SQL
* To serve as JPA Compliant mapping solution for NoSQL Datastores.
*	To help developers, forget the complexity of NoSQL stores and focus on Domain Model.
*	To make switching across data-stores as easy as changing a configuration.

Latest 
======
* Kundera now supports **Apache Kudu**. Check [Kundera with Kudu](https://github.com/impetus-opensource/Kundera/wiki/Kundera-with-Kudu) for more details.
* Want to step out of JPA world and still take advantage of Kundera? Check [Kundera Data as Object](https://github.com/impetus-opensource/Kundera/wiki/Kundera-Data-As-Object).
* Want to save your large files in MongoDB GridFS in the same JPA way. Check [GridFS support in Kundera](https://github.com/impetus-opensource/Kundera/wiki/GridFS-support-with-Kundera-MongoDB).
* Perform SQL queries over big data using Kundera with **Apache Spark** as the query engine. Check [Kundera with Spark](https://github.com/impetus-opensource/Kundera/wiki/Kundera-with-Spark) for more details. 
* We are active on **stackoverflow.com**. Ask questions & check existing [Kundera Tagged Questions](http://stackoverflow.com/questions/tagged/kundera) on stackoverflow. 


Supported Datastores 
=====================
Kundera currently supports following data stores :
*  Cassandra
*  MongoDB
*  HBase
*  Redis
*  OracleNoSQL
*  Neo4j
*  Couchdb
*  Kudu
*  Relational databases
*  Apache Spark
  
You can find the list of data stores(specific versions) supported by Kundera [here] (https://github.com/impetus-opensource/Kundera/wiki/Datastores-Supported).

Getting Started
===============
The latest stable release of Kundera is <b>3.5</b>.
It is a maven based project . You can either download it directly from github and build using following command :

<b>mvn clean install -Dfile src/pom.xml </b>

<b>Or</b> it can be directly added as maven dependency in your project in the following manner :
 
  * Add the following repository to pom.xml :
   
   ```
    <repository>
        <id>sonatype-nexus</id>
        <name>Kundera Public Repository</name>
        <url>https://oss.sonatype.org/content/repositories/releases</url>
       <releases>
           <enabled>true</enabled>
       </releases>
       <snapshots>
           <enabled>false</enabled>
       </snapshots>
    </repository>
    <repository>
        <id>kundera-missing</id>
        <name>Kundera Public Missing Resources Repository</name>
        <url>http://kundera.googlecode.com/svn/maven2/maven-missing-resources</url>
        <releases>
           <enabled>true</enabled>
      </releases>
      <snapshots>
          <enabled>true</enabled>
      </snapshots>
    </repository>
   ```
  
  * Add the data store specific Kundera module as a dependency (e.g. Cassandra below) :
  
  ```
    <dependency>
          <groupId>com.impetus.kundera.client</groupId>
          <artifactId>kundera-cassandra</artifactId>
          <version>${kundera.version}</version>
    </dependency>
  ```

Build your project with the above changes to your pom.xml and start using Kundera !



Important Links
===============
* [Kundera in 5 minutes] (https://github.com/impetus-opensource/Kundera/wiki/Getting-Started-in-5-minutes)
* [Data Store specific Configurations] (https://github.com/impetus-opensource/Kundera/wiki/Data-store-Specific-Configuration)
* Features :
   * [Polyglot Persistence] (https://github.com/impetus-opensource/Kundera/wiki/Polyglot-Persistence)
   * [JPQL] (https://github.com/impetus-opensource/Kundera/wiki/JPQL) & [Native Query] (https://github.com/impetus-opensource/Kundera/wiki/Native-queries) Support
   * [Schema Generation] (https://github.com/impetus-opensource/Kundera/wiki/Schema-Generation)
   * [Transaction Management] (https://github.com/impetus-opensource/Kundera/wiki/Transaction-Management)
   * [Rest Based Access] (https://github.com/impetus-opensource/Kundera/wiki/REST-Based-Access)
* Tutorials :
   * [Kundera with Openshift] (https://github.com/impetus-opensource/Kundera/wiki/Deploying-Polyglot-(RDBMS---NoSQL)-Applications-on-Openshift)
   * [Kundera with Play Framework] (https://github.com/impetus-opensource/Kundera/wiki/Using-Kundera-with-Play!-Framework)
   * [Kundera with GWT] (https://github.com/impetus-opensource/Kundera/wiki/Using-Kundera-with-GWT)
   * [Kundera with JBoss] (https://github.com/impetus-opensource/Kundera/wiki/Using-Kundera-with-Jboss)
   * [Kundera with Spring] (https://github.com/impetus-opensource/Kundera/wiki/Building-Applications-with-Kundera-and-Spring)
   * [Kundera with Spark] (https://github.com/impetus-opensource/Kundera/wiki/Kundera-with-Spark)
* [Kundera Tagged Questions on stackoverflow.com] (http://stackoverflow.com/questions/tagged/kundera)
* [Releases] (https://github.com/impetus-opensource/Kundera/blob/trunk/src/README.md)



About Us
========
Kundera is backed by Impetus Labs - iLabs. iLabs is a R&D consulting division of Impetus Technologies (http://www.impetus.com). iLabs focuses on innovations with next generation technologies and creates practice areas and new products around them. iLabs is actively involved working on High Performance computing technologies, ranging from distributed/parallel computing, Erlang, grid softwares, GPU based software, Hadoop, Hbase, Cassandra, CouchDB and related technologies. iLabs is also working on various other Open Source initiatives.

Follow us on [Twitter] (https://twitter.com/kundera_impetus).
