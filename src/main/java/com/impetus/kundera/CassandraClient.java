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

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.Cassandra.Client;

/**
 * Interface used to interact with Cassandra Clients.
 * 
 * @see com.impetus.kundera.client.PelopsClient
 * @author animesh.kumar
 * @since 0.1
 */
public interface CassandraClient {

	/**
	 * Write multiple columns into a column-family.
	 * 
	 * @param columnFamily  	The name of the super column family to operate on
	 * @param key 				The key of the row to modify
	 * @param columns 			Array of columns to write
	 * @throws Exception 		The exception
	 */
	void writeColumns(String columnFamily, String key, Column... columns) throws Exception;

	/**
	 * Write multiple super-columns into a super-column-family.
	 * 
	 * @param columnFamily 		The name of the super column family to operate on
	 * @param key	 			The key of the row to modify
	 * @param superColumns 		Array of super-columns to write
	 * @throws Exception 		The exception
	 */
	void writeSuperColumns(String columnFamily, String key, SuperColumn... superColumns) throws Exception;

	/**
	 * Retrieve columns from a column-family row.
	 * 
	 * @param columnFamily
	 *            The name of the super column family to operate on
	 * @param key
	 *            The key of the row
	 * @return A list of matching columns
	 * @throws Exception
	 *             the exception
	 */
	List<Column> loadColumns(String columnFamily, String key) throws Exception;

	/**
	 * Retrieve columns from multiple rows of a column-family.
	 * 
	 * @param columnFamily
	 *            The name of the super column family to operate on
	 * @param keys
	 *            Array of row keys
	 * @return A Map of row and corresponding list of columns.
	 * @throws Exception
	 *             the exception
	 */
	Map<String, List<Column>> loadColumns(String columnFamily, String... keys) throws Exception;

	/**
	 * Delete a row from either column-family or super-column-family.
	 * 
	 * @param columnFamily
	 *            The name of the super column family to operate on
	 * @param rowId
	 *            the row id
	 * @throws Exception
	 *             the exception
	 */
	void delete(String columnFamily, String rowId) throws Exception;

	/**
	 * Load super-columns from a super-column-family row.
	 * 
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
	List<SuperColumn> loadSuperColumns(String columnFamily, String key, String... superColumnNames) throws Exception;

	/**
	 * Load super-columns from multiple rows of a super-column-family.
	 * 
	 * @param columnFamily
	 *            The name of the super column family to operate on
	 * @param keys
	 *            Array of row keys
	 * @return A Map of row and corresponding list of super-columns.
	 * @throws Exception
	 *             the exception
	 */
	Map<String, List<SuperColumn>> loadSuperColumns(String columnFamily, String... keys) throws Exception;

	/**
	 * Set Cassandra KeySpace. Default is "localhost"
	 * 
	 * @param keySpace
	 *            the key space
	 */
	void setKeySpace(String keySpace);

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
	 * @return
	 * @throws Exception
	 */
	Client getCassandraClient() throws Exception;

	/**
	 * 
	 */
	void shutdown();

	/**
	 * connects to Cassandra DB
	 */
	void connect();

}
