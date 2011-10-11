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
    void persist(EnhancedEntity e) throws Exception;

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
    <E> E find(Class<E> entityClass, String key) throws Exception;

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
    <E> List<E> find(Class<E> entityClass, String... keys) throws Exception;

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
    <E> List<E> find(Class<E> entityClass, Map<String, String> col) throws Exception;

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
     * Shutdown.
     */
    void close();

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

    Query createQuery(String queryString);

    String getPersistenceUnit();

    // TODO Do we really need it. This may not be required for few of the
    // clients
    IndexManager getIndexManager();

    void setPersistenceUnit(String persistenceUnit);
}
