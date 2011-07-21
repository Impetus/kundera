/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.cassandra.client;

import java.util.List;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.SuperColumn;

/**
 * Interface used to interact with Cassandra Clients.
 *
 * @see com.impetus.kundera.cassandra.client.pelops.PelopsClient
 * @author animesh.kumar
 * @since 0.1
 */
public interface CassandraClient extends com.impetus.kundera.Client
{

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
    void writeSuperColumns(String keyspace, String columnFamily, String key, SuperColumn... superColumns)
            throws Exception;

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
    List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String key, String... superColumnNames)
            throws Exception;

    

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
