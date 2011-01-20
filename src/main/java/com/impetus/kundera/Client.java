/**
 * 
 */
package com.impetus.kundera;

import java.util.List;

import javax.persistence.EntityManager;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * @author impetus
 *
 */
public interface Client {
	
	/**
	 * Write multiple columns into a column-family.
	 * 
	 * @param keyspace
	 *            the keyspace
	 * @param columnFamily
	 *            The name of the super column family to operate on
	 * @param key
	 *            The key of the row to modify
	 * @param columns
	 *            Array of columns to write
	 * @throws Exception
	 *             The exception
	 */
    void writeColumns(String keyspace, String columnFamily, String key, List<EntityMetadata.Column> columns,EnhancedEntity e) throws Exception;

    /**
	 * Retrieve columns from a column-family row.
	 * 
	 * @param keyspace
	 *            the keyspace
	 * @param columnFamily
	 *            The name of the super column family to operate on
	 * @param key
	 *            The key of the row
	 * @return A list of matching columns
	 * @throws Exception
	 *             the exception
	 */
     <E> E  loadColumns(EntityManagerImpl em,Class<E> clazz, String keyspace, String columnFamily, String key, EntityMetadata m) throws Exception;

    /**
	 * Retrieve columns from multiple rows of a column-family.
	 * 
	 * @param keyspace
	 *            the keyspace
	 * @param columnFamily
	 *            The name of the super column family to operate on
	 * @param keys
	 *            Array of row keys
	 * @return A Map of row and corresponding list of columns.
	 * @throws Exception
	 *             the exception
	 */
     <E> List<E>  loadColumns(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily, EntityMetadata m,String... keys) throws Exception;


     /**
      * Set Cassandra nodes.
      * 
      * @param contactNodes
      *            the contact nodes
      */
     void setContactNodes(String... contactNodes);

     /**
      * Set default port. Default is 9160
      * 
      * @param defaultPort
      *            the default port
      */
     void setDefaultPort(int defaultPort);
     
     /**
      * Set key space.
      * @param keySpace key space.
      */
     void setKeySpace(String keySpace);
     
     /**
      * Shutdown.
      */
     void shutdown();

     /**
      * connects to Cassandra DB.
      */
     void connect();
     
     /**
 	 * Delete a row from either column-family or super-column-family.
 	 * 
 	 * @param keyspace
 	 *            the keyspace
 	 * @param columnFamily
 	 *            The name of the super column family to operate on
 	 * @param rowId
 	 *            the row id
 	 * @throws Exception
 	 *             the exception
 	 */
     void delete(String keyspace, String columnFamily, String rowId) throws Exception;
     
     /**
      * Returns type of nosql database
      * @return dbType database type.
      */
     DBType getType();
}
