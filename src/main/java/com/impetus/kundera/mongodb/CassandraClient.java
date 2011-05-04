/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * Interface used to interact with Cassandra Clients.
 * 
 * @see com.impetus.kundera.client.PelopsClient
 * @author animesh.kumar
 * @since 0.1
 */
public interface CassandraClient  extends com.impetus.kundera.Client{

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
	 *//*
    void writeColumns(String keyspace, String columnFamily, String key, List<EntityMetadata.Column> columns,EnhancedEntity e) throws Exception;
*/
    /**
	 * Write multiple super-columns into a super-column-family.
	 * 
	 * @param keyspace
	 *            the keyspace
	 * @param columnFamily
	 *            The name of the super column family to operate on
	 * @param key
	 *            The key of the row to modify
	 * @param superColumns
	 *            Array of super-columns to write
	 * @throws Exception
	 *             The exception
	 */
    void writeSuperColumns(String keyspace, String columnFamily, String key, SuperColumn... superColumns) throws Exception;

 /*   *//**
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
	 *//*
    List<Column> <E> E  loadColumns(EntityManagerImpl em,Class<E> clazz, String keyspace, String columnFamily, String key, EntityMetadata m) throws Exception;
*/
   /* *//**
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
	 *//*
    Map<String, List<Column>> <E> List<E>  loadColumns(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily, EntityMetadata m,String... keys) throws Exception;
*/
   
    /**
	 * Load super-columns from a super-column-family row.
	 * 
	 * @param keyspace
	 *            the keyspace
	 * @param columnFamily
	 *            The name of the super column family to operate on
	 * @param key
	 *            The key of the row
	 * @param superColumnNames
	 *            Array of super-column names to fetch from the row
	 * @return A list of matching super-columns
	 * @throws Exception
	 *             the exception
	 */
    List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String key, String... superColumnNames) throws Exception;

    /**
	 * Load super-columns from multiple rows of a super-column-family.
	 * 
	 * @param keyspace
	 *            the keyspace
	 * @param columnFamily
	 *            The name of the super column family to operate on
	 * @param keys
	 *            Array of row keys
	 * @return A Map of row and corresponding list of super-columns.
	 * @throws Exception
	 *             the exception
	 */
    Map<String, List<SuperColumn>> loadSuperColumns(String keyspace, String columnFamily, String... keys) throws Exception;
    

    /**
     * Gets the cassandra client.
     * 
     * @return the cassandra client
     * 
     * @throws Exception
     *             the exception
     */
    Client getCassandraClient() throws Exception;

}
