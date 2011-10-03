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
package com.impetus.kundera.client;

import java.util.List;
import java.util.Map;

import javax.persistence.Query;

import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.persistence.EntityResolver;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * The Interface Client.
 * 
 * @author impetus
 */
public interface Client
{

    /**
     * Writes Multiple columns.
     * 
     * @param em
     *            Entity Manager
     * @param e
     *            Ehanced Entity
     * @param m
     *            Entity Metadata
     * @throws Exception
     *             the exception
     */
    void writeData(EnhancedEntity e) throws Exception;

    /**
     * Retrieve columns from a column-family row.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param key
     *            The key of the row
     * @param m
     *            the m
     * @return A list of matching columns
     * @throws Exception
     *             the exception
     */
    <E> E loadData(Class<E> entityClass, String key) throws Exception;

    /**
     * Retrieve columns from multiple rows of a column-family.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param m
     *            the m
     * @param keys
     *            Array of row keys
     * @return A Map of row and corresponding list of columns.
     * @throws Exception
     *             the exception
     */
    <E> List<E> loadData(Class<E> entityClass, String... keys) throws Exception;

    /**
     * Load data.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param m
     *            the m
     * @param col
     *            the col
     * @return the list
     * @throws Exception
     *             the exception
     */
    public <E> List<E> loadData(Class<E> entityClass, Map<String, String> col) throws Exception;

    /**
     * Loads columns from multiple rows restricting results to conditions stored
     * in <code>filterClauseQueue</code>.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param m
     *            the m
     * @param query
     *            the query
     * @return the list
     * @throws Exception
     *             the exception
     */
    @SuppressWarnings("unchecked")
    <E> List<E> loadData(Query query) throws Exception;

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
     * 
     * @param schema
     *            key space.
     */
    void setSchema(String schema);

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
     * @param schema
     *            the keyspace
     * @param tableName
     *            The name of the super column family to operate on
     * @param rowId
     *            the row id
     * @throws Exception
     *             the exception
     */
    void delete(EnhancedEntity enhancedEntity) throws Exception;

    /**
     * Returns type of nosql database.
     * 
     * @return dbType database type.
     */
    DBType getType();

    Query getQuery(String queryString);

    String getPersistenceUnit();

    IndexManager getIndexManager();

    EntityResolver getEntityResolver();

    void setEntityResolver(EntityResolver entityResolver);

    void setPersistenceUnit(String persistenceUnit);
}
