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

import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.loader.ClientFactory;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;

/**
 * In Kundera, <b>Clients</b> act as a translator of JPA calls to
 * datastore-specific respective operations call. Clients are constructed via
 * {@link ClientFactory} that are configurable in persistence.xml. This makes it
 * possible for user to choose a {@link Client} implementation for a particular
 * persistence unit.
 * 
 * Client API defines methods that are required to be implemented by various
 * clients implementations (Thrift, Mongo etc).
 * 
 * Any new addition of datastore support must implement this API. This is
 * because kundera-core - after initialization, caching etc, calls clients
 * methods to read from/ write into datastores.
 * 
 * @author vivek.mishra
 */
public interface Client<Q extends Query>
{

    /**
     * Retrieves an entity from datastore
     * 
     * @param entityClass
     *            the entity class
     * @param key
     *            The key of the row
     * @return Entity object
     */
    Object find(Class entityClass, Object key);

    /**
     * Retrieve <code>columnsToSelect</code> from multiple rows of a
     * column-family.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param columnsToSelect
     *            Array of column names that need to be populated into entity
     * @param keys
     *            Array of row keys
     * @return List of entity objects
     */
    <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys);

    /**
     * Finds entities that match a given set of embedded column values provided
     * in the Map
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param embeddedColumnMap
     *            Map of embedded column name and their values that are used as
     *            a criteria for finding entities
     * @return the list of entities
     */
    <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap);

    /**
     * Cleans up client
     */
    void close();

    /**
     * Removes an entity from datastore for a given primary key
     * 
     * @param entity
     *            the entity
     * @param pKey
     *            Primary key of entity to be deleted
     */
    void remove(Object entity, Object pKey);

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
     * @param schemaName
     *            Schema/Keyspace name.
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
    <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType);

    /**
     * Returns array of primary key for given column name and it's value.
     * 
     * @param schemaName
     *            Schema/Keyspace name.
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

    Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz);

    /**
     * Delete rows from given table for given column name and corresponding
     * value..
     * 
     * @param schemaName
     *            Schema Name
     * @param tableName
     *            Name of the table
     * @param columnName
     *            Name of the column
     * @param columnValue
     *            Name of column value
     */
    void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue);

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
    List<Object> findByRelation(String colName, Object colValue, Class entityClazz);

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

    /**
     * Enables executing native script specific to a db client
     * 
     * 
     * @param script
     * @return
     */
    Object executeScript(String script);

    /**
     * Gets the id generator.
     * 
     * @return the id generator
     */
    Generator getIdGenerator();

}
