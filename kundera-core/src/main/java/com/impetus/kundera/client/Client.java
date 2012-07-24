/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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

import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;

/**
 * Client API. Defines methods which are required for to be implemented by
 * various clients(pelops, Mongo). Any new addition of new client must implement
 * this API to integrate new client with existing Kundera API. It's an extension
 * API to support extension of new client.
 * 
 * @author vivek.mishra
 */
public interface Client<Q extends Query>
{

    /**
     * Retrieve columns from a column-family row.
     * 
     * @param the
     *            element type
     * @param entityClass
     *            the entity class
     * @param key
     *            The key of the row
     * @return A list of matching columns
     * @throws Exception
     *             the exception
     */
    Object find(Class entityClass, Object key);

    /**
     * Retrieve columns from multiple rows of a column-family.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param keys
     *            Array of row keys
     * @return A Map of row and corresponding list of columns.
     * @throws Exception
     *             the exception
     */
    <E> List<E> findAll(Class<E> entityClass, Object... keys);

    /**
     * Load data.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param embeddedColumnMap
     *            the col
     * @return the list
     * @throws Exception
     *             the exception
     */
    <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap);

    /**
     * Shutdown.
     */
    void close();

    /**
     * Delete.
     * 
     * @param entity
     *            the entity
     * @param pKey
     *            the key
     * @throws Exception
     *             the exception
     */
    void delete(Object entity, Object pKey);

    /**
     * Gets the persistence unit.
     * 
     * @return the persistence unit
     */
    String getPersistenceUnit();

    /**
     * Gets the index manager.
     * 
     * @return the index manager
     */
    IndexManager getIndexManager();

    /**
     * Data node to persist entity with specific client.
     * 
     * @param node
     *            data node.
     */
    void persist(Node node);

    void persistJoinTable(JoinTableData joinTableData);

    /**
     * Returns List of column values for given primary key and column name.
     * 
     * @param <E>
     *            Type cast
     * @param tableName
     *            Table/column family name.
     * @param pKeyColumnName
     *            Primary key column name.
     * @param columnName
     *            Name of column to be fetched.
     * @param pKeyColumnValue
     *            primary key value.
     * @return list of values fetched for <columnName>
     */
    <E> List<E> getColumnsById(String tableName, String pKeyColumnName, String columnName, String pKeyColumnValue);

    /**
     * Returns array of primary key for given column name and it's value.
     * 
     * @param tableName
     *            table/column family name.
     * @param pKeyName
     *            primary key column name.
     * @param columnName
     *            column name to be used for search.
     * @param columnValue
     *            value for parameterised <columnName>.
     * @param entity
     *            class entity class
     * @return array containing fetched primary keys.
     */

    Object[] findIdsByColumn(String tableName, String pKeyName, String columnName, Object columnValue, Class entityClazz);

    /**
     * Delete rows from given table for given column name and corresponding
     * value..
     * 
     * @param tableName
     *            Name of the table
     * @param columnName
     *            Name of the column
     * @param columnValue
     *            Name of column value
     */
    void deleteByColumn(String tableName, String columnName, Object columnValue);

    /**
     * Find list of entities for given column name and column value, if index
     * support is provided..
     * 
     * @param colName
     *            the column name
     * @param colValue
     *            the column value
     * @param entityClass
     *            the entity class
     * @return the list list of entities.
     */
    List<Object> findByRelation(String colName, String colValue, Class entityClazz);

    /**
     * Returns entity reader instance bind to specific client.
     * 
     * @return reader entity reader.
     */
    EntityReader getReader();

    /**
     * Returns query implementor class, required for initializing client
     * specific query interface.
     * 
     * @return class instance of configured query interface.
     */
    Class<Q> getQueryImplementor();
}
